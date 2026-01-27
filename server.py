import aiohttp
from bs4 import BeautifulSoup
from quart import Quart, jsonify, request, send_from_directory
import os
import asyncio
import time
import random
from datetime import datetime

app = Quart(__name__, static_folder='public', static_url_path='')
BASE = "https://mihaaru.com"

# ===== CACHES =====
LIST_TTL = 10 * 60 * 1000      # 10 min: grid list refresh (increased from 2 min)
ARTICLE_TTL = 30 * 60 * 1000   # 30 min: article page cache (increased from 10 min)

list_cache = {
    "urls": [],
    "timestamp": 0
}

article_cache = {}  # url -> { data, ts }


def now_ms():
    return int(time.time() * 1000)


def uniq(arr):
    return list(dict.fromkeys(arr))


def pick_random(arr, avoid=None):
    if not arr:
        return None
    if len(arr) == 1:
        return arr[0]

    # remove avoid if possible
    pool = arr
    if avoid:
        filtered = [x for x in arr if x != avoid]
        if filtered:
            pool = filtered

    return random.choice(pool)


async def fetch_grid_urls():
    global list_cache
    now = now_ms()

    # Use cached list if fresh
    if list_cache["urls"] and (now - list_cache["timestamp"]) < LIST_TTL:
        return list_cache["urls"]

    # 1) Load Mihaaru news page (the one with your grid)
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(BASE, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as home_res:
                home_html = await home_res.text()
        soup = BeautifulSoup(home_html, 'html.parser')

        # 2) Grab article links from grid items container
        # Works with: <div id="news-grid-XXXX-items"> ... <article> ... <a href="/news/153681">
        found = []

        for a in soup.find_all('article'):
            link = a.find('a', href=True)
            if link:
                href = link.get('href', '')
                if href.startswith('/'):
                    import re
                    if re.match(r'^/(news|world|sports|business|local)/\d+', href):
                        found.append(f"{BASE}{href}")

        urls = uniq(found)[:12]  # first 12 articles from that grid

        if not urls:
            print("DEBUG: No articles found. Trying alternate selector...")
            # Try alternate selector
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                if re.match(r'^/(news|world|sports|business|local)/\d+', href):
                    found.append(f"{BASE}{href}")
            urls = uniq(found)[:12]

        if not urls:
            raise Exception("No article links found in news grid")

        list_cache["urls"] = urls
        list_cache["timestamp"] = now
        print(f"DEBUG: Found {len(urls)} articles")

        return urls
    except Exception as e:
        print(f"ERROR in fetch_grid_urls: {e}")
        raise

async def fetch_article(url):
    global article_cache
    now = now_ms()

    # cache per-article
    if url in article_cache:
        cached = article_cache[url]
        if (now - cached["ts"]) < ARTICLE_TTL:
            return {**cached["data"], "cachedArticle": True}

    headers = {"User-Agent": "Mozilla/5.0"}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as res:
            html = await res.text()
    soup = BeautifulSoup(html, 'html.parser')

    title = ""
    h1 = soup.find('h1')
    if h1:
        title = h1.get_text(strip=True)

    # ---- Robust content extraction (prevents failures) ----
    content = []

    def push_text(t):
        if not t:
            return
        text = ' '.join(t.split())
        if not text:
            return
        if len(text) < 10:  # filter tiny junk
            return
        content.append(text)

    # Try common containers first
    for p in soup.select(".block-wrapper.text-right p"):
        push_text(p.get_text())

    if not content:
        for p in soup.select("article p"):
            push_text(p.get_text())

    if not content:
        for p in soup.select("main p, .prose p, .content p, .post-content p"):
            push_text(p.get_text())

    if not content:
        for p in soup.find_all("p")[:25]:
            push_text(p.get_text())

    data = {
        "url": url,
        "title": title if title else "ލިޔުމެއް",
        "content": content if content else ["ލިޔުމެއް ނުފެނުނު"],
        "fetchedAt": datetime.now().isoformat()
    }

    article_cache[url] = {"data": data, "ts": now}
    return {**data, "cachedArticle": False}


@app.route('/')
async def index():
    return await send_from_directory('public', 'index.html')


@app.route('/api/random-article')
async def random_article():
    try:
        last = request.args.get('last', '').strip()

        urls = await fetch_grid_urls()
        picked = pick_random(urls, last if last else None)

        if not picked:
            raise Exception("Failed to pick random article")

        article = await fetch_article(picked)
        now = now_ms()

        return jsonify({
            **article,
            "cachedList": (now - list_cache["timestamp"]) < LIST_TTL
        })
    except Exception as err:
        print(f"Error: {err}")
        return jsonify({
            "error": "Failed to load random article",
            "detail": str(err)
        }), 500


if __name__ == '__main__':
    app.run(debug=False, host='localhost', port=3000)
