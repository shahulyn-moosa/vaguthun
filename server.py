import aiohttp
from bs4 import BeautifulSoup
from quart import Quart, jsonify, request, send_from_directory
import time
import random
import re
from datetime import datetime

app = Quart(__name__, static_folder="public", static_url_path="")
BASE = "https://mihaaru.com"

# ===== CACHES =====
LIST_TTL = 10 * 60 * 1000      # 10 min
ARTICLE_TTL = 30 * 60 * 1000   # 30 min

list_cache = {"urls": [], "timestamp": 0}
article_cache = {}  # url -> { data, ts }

_http_session: aiohttp.ClientSession | None = None


def now_ms():
    return int(time.time() * 1000)


def uniq(arr):
    return list(dict.fromkeys(arr))


def pick_random(arr, avoid=None):
    if not arr:
        return None
    if len(arr) == 1:
        return arr[0]
    pool = arr
    if avoid:
        filtered = [x for x in arr if x != avoid]
        if filtered:
            pool = filtered
    return random.choice(pool)


async def get_session():
    global _http_session
    if _http_session is None or _http_session.closed:
        timeout = aiohttp.ClientTimeout(total=12)
        _http_session = aiohttp.ClientSession(timeout=timeout)
    return _http_session


@app.before_serving
async def startup():
    await get_session()


@app.after_serving
async def shutdown():
    global _http_session
    if _http_session and not _http_session.closed:
        await _http_session.close()
    _http_session = None


def normalize_url(href: str) -> str | None:
    if not href:
        return None

    # absolute
    if href.startswith("http://") or href.startswith("https://"):
        if href.startswith(BASE):
            return href
        return None

    # relative
    if href.startswith("/"):
        return f"{BASE}{href}"

    return None


# Only allow real article URLs
ARTICLE_RE = re.compile(r"^/(news|world|sports|business|local)/\d+/?$")


async def fetch_grid_urls():
    now = now_ms()

    # Use cached list if fresh
    if list_cache["urls"] and (now - list_cache["timestamp"]) < LIST_TTL:
        return list_cache["urls"]

    headers = {"User-Agent": "Mozilla/5.0"}
    session = await get_session()

    async with session.get(BASE, headers=headers) as home_res:
        home_html = await home_res.text()

    soup = BeautifulSoup(home_html, "html.parser")

    found = []

    # ✅ Prefer links inside visible grid content first
    # Your snippet shows: div.grid ... article ... a[href="/news/123"]
    grid = soup.select_one("div.grid.grid-cols-2")
    if grid:
        for a in grid.select('article a[href^="/"]'):
            href = a.get("href", "")
            if ARTICLE_RE.match(href):
                found.append(f"{BASE}{href}")

    # Fallback 1: any article blocks on page
    if not found:
        for art in soup.find_all("article"):
            a = art.find("a", href=True)
            if not a:
                continue
            href = a.get("href", "")
            if ARTICLE_RE.match(href):
                found.append(f"{BASE}{href}")

    # Fallback 2: scan all anchors
    if not found:
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if ARTICLE_RE.match(href):
                found.append(f"{BASE}{href}")

    urls = uniq(found)[:12]
    if not urls:
        raise Exception("No article links found on mihaaru.com homepage")

    list_cache["urls"] = urls
    list_cache["timestamp"] = now
    return urls


async def fetch_article(url):
    now = now_ms()

    cached = article_cache.get(url)
    if cached and (now - cached["ts"]) < ARTICLE_TTL:
        return {**cached["data"], "cachedArticle": True}

    headers = {"User-Agent": "Mozilla/5.0"}
    session = await get_session()

    async with session.get(url, headers=headers) as res:
        html = await res.text()

    soup = BeautifulSoup(html, "html.parser")

    title = ""
    h1 = soup.find("h1")
    if h1:
        title = h1.get_text(strip=True)

    content = []

    def push_text(t):
        if not t:
            return
        text = " ".join(t.split())
        if len(text) < 10:
            return
        content.append(text)

    # keep your robust extraction (same as your logic)
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
        "title": title or "ލިޔުމެއް",
        "content": content or ["ލިޔުމެއް ނުފެނުނު"],
        "fetchedAt": datetime.now().isoformat(),
    }

    article_cache[url] = {"data": data, "ts": now}
    return {**data, "cachedArticle": False}


@app.route("/")
async def index():
    return await send_from_directory("public", "index.html")


@app.route("/api/random-article")
async def random_article():
    try:
        last = request.args.get("last", "").strip()
        urls = await fetch_grid_urls()
        picked = pick_random(urls, last or None)

        if not picked:
            raise Exception("Failed to pick random article")

        article = await fetch_article(picked)
        now = now_ms()

        return jsonify({**article, "cachedList": (now - list_cache["timestamp"]) < LIST_TTL})
    except Exception as err:
        return jsonify({"error": "Failed to load random article", "detail": str(err)}), 500


if __name__ == "__main__":
    # If you're reverse proxying with nginx, use host="0.0.0.0"
    app.run(debug=False, host="0.0.0.0", port=3000)
