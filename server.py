# server.py
import os
import re
import time
import json
import random
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any

import aiohttp
from bs4 import BeautifulSoup
from quart import Quart, jsonify, request, send_from_directory

# Parquet persistence
import pandas as pd

app = Quart(__name__, static_folder="public", static_url_path="")

BASE = "https://mihaaru.com"

# ===== CACHE SETTINGS =====
LIST_TTL_MS = 10 * 60 * 1000       # 10 min list refresh
ARTICLE_TTL_MS = 30 * 60 * 1000    # 30 min per-article TTL

# how many links to keep from homepage grid scan
MAX_URLS = 30
# how many articles to prefetch into cache (best to keep small)
PREFETCH_TARGET = 20

# background refresh cadence
BG_LOOP_EVERY_SEC = 30
# do not save parquet more often than this
PARQUET_SAVE_MIN_INTERVAL_SEC = 15

CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
CACHE_FILE = os.path.join(CACHE_DIR, "articles.parquet")

# ===== IN-MEMORY CACHES =====
list_cache = {"urls": [], "timestamp": 0}  # timestamp in ms
article_cache: Dict[str, Dict[str, Any]] = {}  # url -> {"data": dict, "ts": ms}

_http_session: Optional[aiohttp.ClientSession] = None
_bg_task: Optional[asyncio.Task] = None
_cache_lock = asyncio.Lock()
_last_parquet_save_ts = 0.0

# Only allow real article URLs (Mihaaru uses /news/<id> etc)
ARTICLE_RE = re.compile(r"^/(news|world|sports|business|local)/\d+/?$")


# ----------------------------
# Utilities
# ----------------------------
def now_ms() -> int:
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


def to_json_list(val):
    """Convert content into a JSON-serializable Python list of strings."""
    if val is None:
        return []

    # numpy array -> list (happens when reading from parquet)
    try:
        import numpy as np  # type: ignore
        if isinstance(val, np.ndarray):
            val = val.tolist()
    except Exception:
        pass

    # pandas / tuple / set -> list
    if isinstance(val, (tuple, set)):
        val = list(val)

    # single string -> [string]
    if isinstance(val, str):
        return [val]

    # list-like -> ensure strings
    if isinstance(val, list):
        out = []
        for x in val:
            if x is None:
                continue
            if isinstance(x, list):
                out.extend([str(i) for i in x if i is not None])
            else:
                out.append(str(x))
        return out

    # fallback
    return [str(val)]


def normalize_article(data: dict) -> dict:
    """Ensure article dict is always JSON-safe and consistent."""
    data = dict(data or {})
    data["url"] = str(data.get("url", ""))
    data["title"] = str(data.get("title", "ލިޔުމެއް"))
    data["content"] = to_json_list(data.get("content"))
    data["fetchedAt"] = str(data.get("fetchedAt", datetime.now().isoformat()))
    return data


def ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)


# ----------------------------
# Parquet persistence
# ----------------------------
def load_parquet_cache():
    """Load cached articles from parquet into memory (best-effort)."""
    global article_cache
    ensure_cache_dir()
    if not os.path.exists(CACHE_FILE):
        return

    try:
        df = pd.read_parquet(CACHE_FILE)
        loaded = 0

        for _, row in df.iterrows():
            url = str(row.get("url", "")).strip()
            if not url:
                continue

            data = {
                "url": url,
                "title": row.get("title", "ލިޔުމެއް"),
                "content": row.get("content", []),
                "fetchedAt": row.get("fetchedAt", datetime.now().isoformat()),
            }
            data = normalize_article(data)

            ts_val = row.get("ts", now_ms())
            try:
                ts_val = int(ts_val)
            except Exception:
                ts_val = now_ms()

            article_cache[url] = {"data": data, "ts": ts_val}
            loaded += 1

        if loaded:
            print(f"Loaded {loaded} cached articles from {CACHE_FILE}")
    except Exception as e:
        print(f"WARNING: Failed to load parquet cache: {e}")


async def save_parquet_cache_throttled():
    """Save parquet cache but throttle writes to avoid frequent disk IO."""
    global _last_parquet_save_ts
    now = time.time()
    if now - _last_parquet_save_ts < PARQUET_SAVE_MIN_INTERVAL_SEC:
        return

    async with _cache_lock:
        # check again inside lock
        now2 = time.time()
        if now2 - _last_parquet_save_ts < PARQUET_SAVE_MIN_INTERVAL_SEC:
            return

        if not article_cache:
            return

        ensure_cache_dir()
        rows = []
        for url, entry in article_cache.items():
            d = normalize_article(entry.get("data", {}))
            rows.append(
                {
                    "url": d["url"],
                    "title": d["title"],
                    "content": d["content"],   # always plain list now
                    "fetchedAt": d["fetchedAt"],
                    "ts": int(entry.get("ts", now_ms())),
                }
            )

        try:
            pd.DataFrame(rows).to_parquet(CACHE_FILE, index=False)
            _last_parquet_save_ts = time.time()
        except Exception as e:
            print(f"WARNING: Failed to save parquet cache: {e}")


# ----------------------------
# HTTP session lifecycle
# ----------------------------
async def get_session():
    global _http_session
    if _http_session is None or _http_session.closed:
        timeout = aiohttp.ClientTimeout(total=12)
        _http_session = aiohttp.ClientSession(timeout=timeout)
    return _http_session


@app.before_serving
async def startup():
    load_parquet_cache()
    await get_session()
    # kick background loop
    global _bg_task
    _bg_task = asyncio.create_task(background_cache_loop())


@app.after_serving
async def shutdown():
    global _http_session, _bg_task
    if _bg_task:
        _bg_task.cancel()
        _bg_task = None

    if _http_session and not _http_session.closed:
        await _http_session.close()
    _http_session = None


# ----------------------------
# Mihaaru scraping
# ----------------------------
async def fetch_homepage_urls() -> list[str]:
    """Fetch article URLs from mihaaru.com homepage (NOT /news)."""
    now = now_ms()

    # use cached list if still fresh
    if list_cache["urls"] and (now - list_cache["timestamp"]) < LIST_TTL_MS:
        return list_cache["urls"]

    headers = {"User-Agent": "Mozilla/5.0"}
    session = await get_session()

    async with session.get(BASE, headers=headers) as res:
        html = await res.text()

    soup = BeautifulSoup(html, "html.parser")
    found: list[str] = []

    # Prefer visible grid sections first
    # Your page may change; we do layered fallbacks.
    grids = soup.select("div.grid, section, main")

    def add_href(href: str):
        if not href:
            return
        if href.startswith("http://") or href.startswith("https://"):
            if href.startswith(BASE):
                found.append(href)
            return
        if href.startswith("/") and ARTICLE_RE.match(href):
            found.append(f"{BASE}{href}")

    # 1) scan articles inside grid-like areas
    for g in grids[:6]:
        for a in g.select('article a[href]'):
            add_href(a.get("href", ""))

    # 2) fallback: scan all <article>
    if not found:
        for art in soup.find_all("article"):
            a = art.find("a", href=True)
            if a:
                add_href(a.get("href", ""))

    # 3) fallback: scan all anchors
    if not found:
        for a in soup.find_all("a", href=True):
            add_href(a.get("href", ""))

    urls = uniq(found)[:MAX_URLS]
    if not urls:
        raise Exception("No article links found on mihaaru.com homepage")

    list_cache["urls"] = urls
    list_cache["timestamp"] = now
    return urls


async def download_article(url: str) -> dict:
    """Fetch and parse one article page."""
    headers = {"User-Agent": "Mozilla/5.0"}
    session = await get_session()

    async with session.get(url, headers=headers) as res:
        html = await res.text()

    soup = BeautifulSoup(html, "html.parser")

    title = ""
    h1 = soup.find("h1")
    if h1:
        title = h1.get_text(strip=True)

    content: list[str] = []

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

    data = {
        "url": url,
        "title": title or "ލިޔުމެއް",
        "content": content or ["ލިޔުމެއް ނުފެނުނު"],
        "fetchedAt": datetime.now().isoformat(),
    }

    return normalize_article(data)


async def fetch_article_cached_or_download(url: str) -> dict:
    """Return cached if fresh, else download and update cache."""
    now = now_ms()
    cached = article_cache.get(url)
    if cached and (now - int(cached.get("ts", 0))) < ARTICLE_TTL_MS:
        d = normalize_article(cached.get("data", {}))
        return {**d, "cachedArticle": True}

    data = await download_article(url)

    async with _cache_lock:
        article_cache[url] = {"data": data, "ts": now_ms()}

    asyncio.create_task(save_parquet_cache_throttled())
    return {**data, "cachedArticle": False}


def get_random_cached_article(avoid_url: Optional[str] = None) -> Optional[dict]:
    """Pick a random cached article (prefer fresh, but can serve stale if offline)."""
    if not article_cache:
        return None

    urls = list(article_cache.keys())
    if avoid_url and avoid_url in urls and len(urls) > 1:
        urls = [u for u in urls if u != avoid_url] or urls

    picked = random.choice(urls)
    data = normalize_article(article_cache[picked].get("data", {}))
    return data


# ----------------------------
# Background cache loop
# ----------------------------
async def background_refresh_once():
    """
    Best-effort:
    - refresh homepage URL list (if needed)
    - prefetch up to PREFETCH_TARGET articles into cache
    """
    try:
        urls = await fetch_homepage_urls()
    except Exception as e:
        # offline or blocked; keep serving parquet cache
        print(f"BG: homepage fetch failed (offline ok): {e}")
        return

    # prefetch
    want = urls[:PREFETCH_TARGET]
    random.shuffle(want)

    # limit concurrency
    sem = asyncio.Semaphore(4)

    async def worker(u):
        async with sem:
            try:
                await fetch_article_cached_or_download(u)
            except Exception as e:
                print(f"BG: article fetch failed for {u}: {e}")

    await asyncio.gather(*(worker(u) for u in want), return_exceptions=True)


async def background_cache_loop():
    """Runs forever; keeps cache warm without blocking user requests."""
    while True:
        try:
            await background_refresh_once()
        except asyncio.CancelledError:
            return
        except Exception as e:
            print(f"BG loop error: {e}")

        await asyncio.sleep(BG_LOOP_EVERY_SEC)


# ----------------------------
# Routes
# ----------------------------
@app.route("/")
async def index():
    return await send_from_directory("public", "index.html")


@app.route("/api/random-article")
async def random_article():
    """
    ALWAYS respond quickly:
    1) If we have any cached article (parquet or memory), serve it immediately.
       In parallel, refresh cache in background.
    2) If cache is empty, try online fetch; if offline, return a clear error.
    """
    last = request.args.get("last", "").strip()
    last_url = last if last.startswith("http") else None

    # 1) Serve cached immediately if available (offline-safe)
    cached = get_random_cached_article(avoid_url=last_url)
    if cached:
        # warm cache in background (non-blocking)
        asyncio.create_task(background_refresh_once())
        return jsonify(
            {
                **normalize_article(cached),
                "servedFrom": "cache",
                "offlineSafe": True,
                "cachedList": True,  # we have something usable
            }
        )

    # 2) If no cache exists at all, try to fetch live (might fail offline)
    try:
        urls = await fetch_homepage_urls()
        picked = pick_random(urls, last_url)
        if not picked:
            raise Exception("Failed to pick random article")

        article = await fetch_article_cached_or_download(picked)

        return jsonify(
            {
                **normalize_article(article),
                "servedFrom": "live",
                "offlineSafe": False,
                "cachedList": True,
            }
        )
    except Exception as e:
        return jsonify(
            {
                "error": "No cached articles available and mihaaru.com is not reachable",
                "detail": str(e),
                "offlineSafe": False,
            }
        ), 503


@app.route("/api/cache-status")
async def cache_status():
    """Small helper endpoint to see cache size quickly."""
    return jsonify(
        {
            "cachedArticles": len(article_cache),
            "listCached": len(list_cache["urls"]),
            "cacheFile": CACHE_FILE,
            "hasCacheFile": os.path.exists(CACHE_FILE),
        }
    )


if __name__ == "__main__":
    # Development only. In production use hypercorn/uvicorn behind nginx.
    #app.run(debug=False, host="0.0.0.0", port=3000)
    app.run(debug=False, host="localhost", port=3000)