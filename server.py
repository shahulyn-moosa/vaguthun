# server.py
import os
import re
import time
import random
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from urllib.parse import urlparse

import aiohttp
from bs4 import BeautifulSoup
from quart import Quart, jsonify, request, send_from_directory

import pandas as pd

app = Quart(__name__, static_folder="public", static_url_path="")

# ----------------------------
# Sources
# ----------------------------
SOURCES = {
    "mihaaru": {"base": "https://mihaaru.com"},
    "sun": {"base": "https://sun.mv"},
    "vaguthu": {"base": "https://vaguthu.mv"},
}

# Mihaaru: /news/123, /world/123 etc
MIHAARU_ARTICLE_RE = re.compile(r"^/(news|world|sports|business|local)/\d+/?$")

# Sun: /217254
SUN_ARTICLE_RE = re.compile(r"^/\d+/?$")

# Vaguthu: /news/1373224/ or /world-news/...
VAGUTHU_ARTICLE_RE = re.compile(r"^/([a-z-]+)/\d+/?$")

# ----------------------------
# Cache settings
# ----------------------------
LIST_TTL_MS = 10 * 60 * 1000       # 10 min per-source list refresh
ARTICLE_TTL_MS = 30 * 60 * 1000    # 30 min per-article refresh

MAX_URLS_PER_SOURCE = 40
PREFETCH_TARGET_TOTAL = 30         # total prefetch across all sources
BG_LOOP_EVERY_SEC = 5 * 60
PARQUET_SAVE_MIN_INTERVAL_SEC = 15

CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
CACHE_FILE = os.path.join(CACHE_DIR, "articles.parquet")

# ----------------------------
# In-memory caches
# ----------------------------
list_cache: Dict[str, Dict[str, Any]] = {
    "mihaaru": {"urls": [], "timestamp": 0},
    "sun": {"urls": [], "timestamp": 0},
    "vaguthu": {"urls": [], "timestamp": 0},
}

article_cache: Dict[str, Dict[str, Any]] = {}

# failed_url_cache: url -> {"ts": ms, "reason": str}
# Used to avoid retrying pages that consistently fail parsing/validation.
failed_url_cache: Dict[str, Dict[str, Any]] = {}

_http_session: Optional[aiohttp.ClientSession] = None
_bg_task: Optional[asyncio.Task] = None
_cache_lock = asyncio.Lock()
_last_parquet_save_ts = 0.0
_last_bg_refresh_ms = 0


# ----------------------------
# Helpers
# ----------------------------
def now_ms() -> int:
    return int(time.time() * 1000)


def ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)


def uniq(arr: List[str]) -> List[str]:
    return list(dict.fromkeys(arr))


def pick_random(arr: List[str], avoid: Optional[str] = None) -> Optional[str]:
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


def should_blacklist_failure(err: Exception) -> bool:
    msg = str(err or "")
    low = msg.lower()
    # Only blacklist failures that are very likely permanent for that URL.
    return (
        "page does not look like a real article" in low
        or "missing body" in low
        or "too short" in low
        or "invalid article url" in low
    )


def is_failed_url(url: str) -> bool:
    return bool(url and url in failed_url_cache)


def mark_failed_url(url: str, reason: str) -> None:
    if not url:
        return
    failed_url_cache[url] = {"ts": now_ms(), "reason": str(reason or "")}


def domain_to_source(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return "unknown"
    if "mihaaru.com" in host:
        return "mihaaru"
    if "sun.mv" in host:
        return "sun"
    if "vaguthu.mv" in host:
        return "vaguthu"
    return "unknown"


def is_valid_article_url(url: str, source: Optional[str] = None) -> bool:
    src = source or domain_to_source(url)
    try:
        parsed = urlparse(url)
        path = parsed.path or "/"
    except Exception:
        return False

    if src == "mihaaru":
        # must be mihaaru base + /news|world|.../<id>
        if not url.startswith(SOURCES["mihaaru"]["base"]):
            return False
        return bool(MIHAARU_ARTICLE_RE.match(path))

    if src == "sun":
        if not url.startswith(SOURCES["sun"]["base"]):
            return False
        return bool(SUN_ARTICLE_RE.match(path))

    if src == "vaguthu":
        if not url.startswith(SOURCES["vaguthu"]["base"]):
            return False
        return bool(VAGUTHU_ARTICLE_RE.match(path))

    return False


def normalize_content(val) -> List[str]:
    """Ensure JSON-serializable list[str]. Handles parquet/numpy edge cases."""
    if val is None:
        return []
    try:
        import numpy as np  # type: ignore
        if isinstance(val, np.ndarray):
            val = val.tolist()
    except Exception:
        pass

    if isinstance(val, str):
        return [val]

    if isinstance(val, (tuple, set)):
        val = list(val)

    if isinstance(val, list):
        out = []
        for x in val:
            if x is None:
                continue
            out.append(str(x))
        return out

    return [str(val)]


def normalize_article(data: dict) -> dict:
    data = dict(data or {})
    url = str(data.get("url", "")).strip()
    src = data.get("source") or domain_to_source(url)
    # Post-process already-extracted content (including parquet-cached items) to
    # drop "read more"/embed junk that sometimes slips into article text.
    content: List[str] = []
    for line in normalize_content(data.get("content")):
        txt = " ".join(str(line).split())
        if not txt:
            continue
        low = txt.lower()
        if "readmore" in low or "embedurl" in low:
            continue
        if "އިތުރަށް ވިދާޅުވުމަށް" in txt:
            continue
        content.append(txt)
    return {
        "url": url,
        "source": str(src),
        "title": str(data.get("title", "ލިޔުމެއް")),
        "content": content,
        "fetchedAt": str(data.get("fetchedAt", datetime.now().isoformat())),
    }


def looks_like_real_article(content: List[str]) -> bool:
    # Stronger validation: at least 2 paragraphs and enough total text
    if not content:
        return False
    if len(content) < 2:
        return False
    total = len(" ".join(content).strip())
    return total >= 200


# ----------------------------
# Parquet persistence
# ----------------------------
def load_parquet_cache():
    global article_cache
    ensure_cache_dir()
    if not os.path.exists(CACHE_FILE):
        return

    try:
        df = pd.read_parquet(CACHE_FILE)
        loaded = 0
        skipped = 0

        for _, row in df.iterrows():
            url = str(row.get("url", "")).strip()
            if not url:
                skipped += 1
                continue

            src = str(row.get("source", domain_to_source(url)))
            if not is_valid_article_url(url, src):
                skipped += 1
                continue

            data = {
                "url": url,
                "source": src,
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
            print(f"Loaded {loaded} cached articles from {CACHE_FILE} (skipped {skipped})")
    except Exception as e:
        print(f"WARNING: Failed to load parquet cache: {e}")


async def save_parquet_cache_throttled():
    global _last_parquet_save_ts
    now = time.time()
    if now - _last_parquet_save_ts < PARQUET_SAVE_MIN_INTERVAL_SEC:
        return

    async with _cache_lock:
        now2 = time.time()
        if now2 - _last_parquet_save_ts < PARQUET_SAVE_MIN_INTERVAL_SEC:
            return
        if not article_cache:
            return

        ensure_cache_dir()
        rows = []
        for url, entry in article_cache.items():
            d = normalize_article(entry.get("data", {}))
            if not is_valid_article_url(d["url"], d["source"]):
                continue
            rows.append(
                {
                    "url": d["url"],
                    "source": d["source"],
                    "title": d["title"],
                    "content": d["content"],   # list[str]
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
        timeout = aiohttp.ClientTimeout(total=15)
        _http_session = aiohttp.ClientSession(timeout=timeout)
    return _http_session


@app.before_serving
async def startup():
    load_parquet_cache()
    await get_session()
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
# URL list fetchers (per source)
# ----------------------------
async def fetch_mihaaru_urls() -> List[str]:
    base = SOURCES["mihaaru"]["base"]
    now = now_ms()
    if list_cache["mihaaru"]["urls"] and (now - list_cache["mihaaru"]["timestamp"]) < LIST_TTL_MS:
        return list_cache["mihaaru"]["urls"]

    session = await get_session()
    headers = {"User-Agent": "Mozilla/5.0"}

    async with session.get(base, headers=headers) as res:
        html = await res.text()

    soup = BeautifulSoup(html, "html.parser")
    found: List[str] = []

    def add_href(href: str):
        if not href:
            return
        # absolute
        if href.startswith("http"):
            if not href.startswith(base):
                return
            path = urlparse(href).path
            if MIHAARU_ARTICLE_RE.match(path):
                found.append(f"{base}{path}")
            return

        # relative
        if href.startswith("/") and MIHAARU_ARTICLE_RE.match(href):
            found.append(f"{base}{href}")

    # prefer the specific grid block on mihaaru homepage
    grid = soup.select_one('div[x-show*="items.length"] div.grid')
    if grid:
        for a in grid.select('a[href]'):
            add_href(a.get("href", "").strip())
    else:
        for g in soup.select("div.grid, main, section")[:10]:
            for a in g.select('a[href]'):
                add_href(a.get("href", "").strip())

    if not found:
        for a in soup.find_all("a", href=True):
            add_href(a.get("href", "").strip())

    urls = uniq(found)[:MAX_URLS_PER_SOURCE]
    if urls:
        list_cache["mihaaru"]["urls"] = urls
        list_cache["mihaaru"]["timestamp"] = now
    return urls


async def fetch_sun_urls() -> List[str]:
    base = SOURCES["sun"]["base"]
    now = now_ms()
    if list_cache["sun"]["urls"] and (now - list_cache["sun"]["timestamp"]) < LIST_TTL_MS:
        return list_cache["sun"]["urls"]

    session = await get_session()
    headers = {"User-Agent": "Mozilla/5.0"}

    async with session.get(base, headers=headers) as res:
        html = await res.text()

    soup = BeautifulSoup(html, "html.parser")
    found: List[str] = []

    # Prefer: home-v2 latest section
    for a in soup.select(".section-home-v2-latest a.home-v2-news-thumb[href]"):
        href = a.get("href", "").strip()
        if href.startswith(base):
            path = urlparse(href).path
            if SUN_ARTICLE_RE.match(path):
                found.append(href)

    # fallback: any internal numeric-root link
    if not found:
        for a in soup.find_all("a", href=True):
            href = a.get("href", "").strip()
            if href.startswith(base):
                path = urlparse(href).path
                if SUN_ARTICLE_RE.match(path):
                    found.append(href)

    urls = uniq(found)[:MAX_URLS_PER_SOURCE]
    if urls:
        list_cache["sun"]["urls"] = urls
        list_cache["sun"]["timestamp"] = now
    return urls


async def fetch_vaguthu_urls() -> List[str]:
    base = SOURCES["vaguthu"]["base"]
    now = now_ms()
    if list_cache["vaguthu"]["urls"] and (now - list_cache["vaguthu"]["timestamp"]) < LIST_TTL_MS:
        return list_cache["vaguthu"]["urls"]

    session = await get_session()
    headers = {"User-Agent": "Mozilla/5.0"}

    async with session.get(base, headers=headers) as res:
        html = await res.text()

    soup = BeautifulSoup(html, "html.parser")
    found: List[str] = []

    for a in soup.select("section.featured-recent a[href]"):
        href = a.get("href", "").strip()
        if href.startswith(base):
            path = urlparse(href).path
            if VAGUTHU_ARTICLE_RE.match(path):
                found.append(href)

    if not found:
        for a in soup.find_all("a", href=True):
            href = a.get("href", "").strip()
            if href.startswith(base):
                path = urlparse(href).path
                if VAGUTHU_ARTICLE_RE.match(path):
                    found.append(href)

    urls = uniq(found)[:MAX_URLS_PER_SOURCE]
    if urls:
        list_cache["vaguthu"]["urls"] = urls
        list_cache["vaguthu"]["timestamp"] = now
    return urls


async def fetch_all_source_urls() -> List[Tuple[str, str]]:
    results: List[Tuple[str, str]] = []

    async def safe(name: str, fn):
        try:
            urls = await fn()
            for u in urls:
                if is_valid_article_url(u, name):
                    results.append((u, name))
        except Exception as e:
            print(f"URL fetch failed for {name}: {e}")

    await asyncio.gather(
        safe("mihaaru", fetch_mihaaru_urls),
        safe("sun", fetch_sun_urls),
        safe("vaguthu", fetch_vaguthu_urls),
    )

    seen = set()
    out: List[Tuple[str, str]] = []
    for u, s in results:
        if u in seen:
            continue
        seen.add(u)
        out.append((u, s))
    return out


# ----------------------------
# Article parsers
# ----------------------------
async def download_html(url: str) -> str:
    session = await get_session()
    headers = {"User-Agent": "Mozilla/5.0"}
    async with session.get(url, headers=headers) as res:
        return await res.text()


def strip_sun_embeds(soup: BeautifulSoup) -> None:
    """
    Sun sometimes injects embed blocks like:
    <p class="resize"><a class="embedurl" href="..."> ... </a></p>
    Remove them fully.
    """
    # Sun uses <p class="resize"> for normal paragraphs too, so only remove
    # resize paragraphs that actually contain embed/readmore markup.
    for p in soup.select("p.resize"):
        if p.select_one("a.embedurl"):
            p.decompose()
            continue
        txt = " ".join(p.get_text(" ", strip=True).split())
        low = txt.lower()
        if "readmore" in low or "އިތުރަށް ވިދާޅުވުމަށް" in txt:
            p.decompose()

    # remove any remaining embedurl anchors (and their wrappers)
    for a in soup.select("a.embedurl"):
        p = a.find_parent("p")
        if p:
            p.decompose()
        else:
            a.decompose()

    # remove common "read more" containers / widget remnants
    for d in soup.select(".embed, .embedded, .embed-container, .component-sponsor"):
        d.decompose()


def extract_text_blocks(soup: BeautifulSoup) -> List[str]:
    content: List[str] = []

    BAD_SUBSTRINGS = [
        "readmore",
        "embedurl",
        "އިތުރަށް ވިދާޅުވުމަށް",
    ]

    def push(t: str):
        if not t:
            return
        txt = " ".join(t.split())
        if len(txt) < 10:
            return
        low = txt.lower()
        for bad in BAD_SUBSTRINGS:
            if bad.lower() in low:
                return
        content.append(txt)

    for p in soup.select("article p"):
        push(p.get_text())

    if not content:
        for p in soup.select("main p, .content p, .post-content p, .entry-content p, .prose p"):
            push(p.get_text())

    if not content:
        for p in soup.find_all("p")[:40]:
            push(p.get_text())

    return content


async def download_article_any(url: str, source_hint: Optional[str] = None) -> dict:
    source = source_hint or domain_to_source(url)

    # ✅ HARD STOP: never fetch/cache invalid URLs
    if not is_valid_article_url(url, source):
        raise Exception(f"Invalid article URL for source={source}: {url}")

    html = await download_html(url)
    soup = BeautifulSoup(html, "html.parser")

    if source == "sun":
        strip_sun_embeds(soup)

    # title
    title = ""
    h1 = soup.find("h1")
    if h1:
        title = h1.get_text(strip=True)
    if not title:
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            title = og["content"].strip()

    content: List[str] = []

    # Mihaaru special wrapper
    if source == "mihaaru":
        for p in soup.select(".block-wrapper.text-right p"):
            t = p.get_text()
            if t:
                txt = " ".join(t.split())
                if len(txt) >= 10:
                    content.append(txt)

    if not content:
        content = extract_text_blocks(soup)

    # ✅ VALIDATE before caching
    if not looks_like_real_article(content):
        raise Exception("Page does not look like a real article (too short / missing body)")

    data = {
        "url": url,
        "source": source,
        "title": title or "ލިޔުމެއް",
        "content": content,
        "fetchedAt": datetime.now().isoformat(),
    }
    return normalize_article(data)


async def fetch_article_cached_or_download(url: str, source: Optional[str] = None) -> dict:
    if is_failed_url(url):
        raise Exception(f"Previously failed URL (skipping): {url}")

    now = now_ms()
    cached = article_cache.get(url)
    if cached and (now - int(cached.get("ts", 0))) < ARTICLE_TTL_MS:
        d = normalize_article(cached.get("data", {}))
        return {**d, "cachedArticle": True}

    try:
        data = await download_article_any(url, source_hint=source)
    except Exception as e:
        if should_blacklist_failure(e):
            mark_failed_url(url, str(e))
        raise

    async with _cache_lock:
        article_cache[url] = {"data": data, "ts": now_ms()}

    asyncio.create_task(save_parquet_cache_throttled())
    return {**data, "cachedArticle": False}


def get_random_cached_article(avoid_url: Optional[str] = None) -> Optional[dict]:
    if not article_cache:
        return None

    urls = list(article_cache.keys())

    # filter out any invalid cached urls (safety)
    urls = [u for u in urls if is_valid_article_url(u, domain_to_source(u))]
    if not urls:
        return None

    if avoid_url and avoid_url in urls and len(urls) > 1:
        urls = [u for u in urls if u != avoid_url] or urls

    picked = random.choice(urls)
    return normalize_article(article_cache[picked].get("data", {}))


# ----------------------------
# Background cache warming
# ----------------------------
async def background_refresh_once():
    global _last_bg_refresh_ms
    now = now_ms()
    # Throttle background refreshes (also covers on-request triggers).
    if _last_bg_refresh_ms and (now - _last_bg_refresh_ms) < (BG_LOOP_EVERY_SEC * 1000):
        return
    _last_bg_refresh_ms = now

    pairs = await fetch_all_source_urls()
    if not pairs:
        return

    # Don't retry URLs we've already determined are "bad".
    pairs = [(u, s) for (u, s) in pairs if not is_failed_url(u)]
    if not pairs:
        return

    random.shuffle(pairs)
    target = pairs[:PREFETCH_TARGET_TOTAL]

    sem = asyncio.Semaphore(4)

    async def worker(u: str, s: str):
        async with sem:
            try:
                await fetch_article_cached_or_download(u, s)
            except Exception as e:
                if should_blacklist_failure(e):
                    mark_failed_url(u, str(e))
                print(f"BG fetch failed {s} {u}: {e}")

    await asyncio.gather(*(worker(u, s) for u, s in target), return_exceptions=True)


async def background_cache_loop():
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
    last = request.args.get("last", "").strip()
    last_url = last if last.startswith("http") else None

    # Serve cache instantly if available
    cached = get_random_cached_article(avoid_url=last_url)
    if cached:
        asyncio.create_task(background_refresh_once())
        return jsonify(
            {
                **normalize_article(cached),
                "servedFrom": "cache",
                "offlineSafe": True,
                "cachedList": True,
            }
        )

    # If cache empty, try live pull
    try:
        pairs = await fetch_all_source_urls()
        if not pairs:
            raise Exception("No sources reachable")

        urls = [u for u, _ in pairs if not is_failed_url(u)]
        if not urls:
            raise Exception("All candidate URLs are blacklisted due to previous failures")
        picked = pick_random(urls, last_url) or urls[0]

        src = domain_to_source(picked)
        article = await fetch_article_cached_or_download(picked, src)

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
                "error": "No cached articles available and sources are not reachable",
                "detail": str(e),
                "offlineSafe": False,
            }
        ), 503


@app.route("/api/cache-status")
async def cache_status():
    return jsonify(
        {
            "cachedArticles": len(article_cache),
            "listCached": {
                "mihaaru": len(list_cache["mihaaru"]["urls"]),
                "sun": len(list_cache["sun"]["urls"]),
                "vaguthu": len(list_cache["vaguthu"]["urls"]),
            },
            "cacheFile": CACHE_FILE,
            "hasCacheFile": os.path.exists(CACHE_FILE),
        }
    )


@app.route("/api/cache-sources")
async def cache_sources():
    counts: Dict[str, int] = {"mihaaru": 0, "sun": 0, "vaguthu": 0, "unknown": 0}
    for _, entry in article_cache.items():
        d = entry.get("data", {})
        src = d.get("source") or domain_to_source(d.get("url", ""))
        counts[str(src)] = counts.get(str(src), 0) + 1

    return jsonify(
        {
            "counts": counts,
            "total": len(article_cache),
        }
    )


if __name__ == "__main__":
    # Production: use hypercorn behind nginx
    app.run(debug=False, host="0.0.0.0", port=3000)
    #app.run(debug=True, host="localhost", port=3000)
