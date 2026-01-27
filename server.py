import aiohttp
from bs4 import BeautifulSoup
from quart import Quart, jsonify, request, send_from_directory
import time
import random
import re
import asyncio
from datetime import datetime

app = Quart(__name__, static_folder="public", static_url_path="")
BASE = "https://mihaaru.com"

# ===== CACHES =====
LIST_TTL = 10 * 60 * 1000      # 10 min
ARTICLE_TTL = 30 * 60 * 1000   # 30 min

list_cache = {"urls": [], "timestamp": 0}
article_cache = {}  # url -> { data, ts }

_http_session: aiohttp.ClientSession | None = None

ARTICLE_RE = re.compile(r"^/(news|world|sports|business|local)/\d+/?$")

# How many articles to prefetch in the background
PREFETCH_COUNT = 6
# How many concurrent background fetches allowed
PREFETCH_CONCURRENCY = 3


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
    # Warm caches in background so first user doesn't wait
    asyncio.create_task(warm_cache())


@app.after_serving
async def shutdown():
    global _http_session
    if _http_session and not _http_session.closed:
        await _http_session.close()
    _http_session = None


async def warm_cache():
    """
    Background: refresh URL list + prefetch a few articles into cache.
    """
    try:
        urls = await fetch_grid_urls(force=True)
        if not urls:
            return

        # Prefetch first N URLs (or random sample)
        targets = urls[:PREFETCH_COUNT]

        sem = asyncio.Semaphore(PREFETCH_CONCURRENCY)

        async def _prefetch_one(u):
            async with sem:
                try:
                    await fetch_article(u, allow_stale_return=False)
                except Exception:
                    pass

        await asyncio.gather(*[_prefetch_one(u) for u in targets])
    except Exception:
        pass


async def fetch_grid_urls(force: bool = False):
    now = now_ms()

    # Use cached list if fresh
    if (not force) and list_cache["urls"] and (now - list_cache["timestamp"]) < LIST_TTL:
        return list_cache["urls"]

    headers = {"User-Agent": "Mozilla/5.0"}
    session = await get_session()

    async with session.get(BASE, headers=headers) as home_res:
        home_html = await home_res.text()

    soup = BeautifulSoup(home_html, "html.parser")
    found = []

    # Prefer links inside visible grid content first
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


def get_cached_any(prefer_fresh: bool = True):
    """
    Return any cached article (fresh preferred), else stale, else None.
    """
    if not article_cache:
        return None

    now = now_ms()

    fresh = []
    stale = []

    for url, entry in article_cache.items():
        age = now - entry["ts"]
        if age < ARTICLE_TTL:
            fresh.append(entry["data"])
        else:
            stale.append(entry["data"])

    if prefer_fresh and fresh:
        picked = random.choice(fresh)
        return {**picked, "cachedArticle": True, "stale": False}

    if fresh:
        picked = random.choice(fresh)
        return {**picked, "cachedArticle": True, "stale": False}

    if stale:
        picked = random.choice(stale)
        return {**picked, "cachedArticle": True, "stale": True}

    return None


async def fetch_article(url, allow_stale_return: bool = True):
    """
    Fetch article. If cached and fresh -> return cached immediately.
    If cached but stale and allow_stale_return -> return stale immediately AND refresh in background.
    Otherwise fetch now.
    """
    now = now_ms()

    cached = article_cache.get(url)
    if cached:
        age = now - cached["ts"]
        if age < ARTICLE_TTL:
            return {**cached["data"], "cachedArticle": True, "stale": False}

        if allow_stale_return:
            # Return stale immediately, refresh in background
            asyncio.create_task(_refresh_article(url))
            return {**cached["data"], "cachedArticle": True, "stale": True}

    # Not cached -> fetch now
    data = await _download_article(url)
    article_cache[url] = {"data": data, "ts": now_ms()}
    return {**data, "cachedArticle": False, "stale": False}


async def _refresh_article(url):
    """
    Background refresh for a single article.
    """
    try:
        data = await _download_article(url)
        article_cache[url] = {"data": data, "ts": now_ms()}
    except Exception:
        pass


async def _download_article(url):
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

    # robust extraction
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

    return {
        "url": url,
        "title": title or "ލިޔުމެއް",
        "content": content or ["ލިޔުމެއް ނުފެނުނު"],
        "fetchedAt": datetime.now().isoformat(),
    }


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

        # ✅ If picked is cached (fresh OR stale) return immediately
        # But if it isn't cached at all, return any cached article immediately
        # and fetch picked in background.
        if picked in article_cache:
            article = await fetch_article(picked, allow_stale_return=True)
        else:
            fallback = get_cached_any(prefer_fresh=True)
            if fallback:
                # start caching the picked one in background
                asyncio.create_task(_refresh_article(picked))
                article = {
                    **fallback,
                    "fallback": True,
                    "requestedUrl": picked,
                }
            else:
                # No cache at all yet (first ever request) -> fetch once
                article = await fetch_article(picked, allow_stale_return=False)

        now = now_ms()

        return jsonify({
            **article,
            "cachedList": (now - list_cache["timestamp"]) < LIST_TTL,
            "cacheSize": len(article_cache),
        })

    except Exception as err:
        return jsonify({"error": "Failed to load random article", "detail": str(err)}), 500


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=3000)
