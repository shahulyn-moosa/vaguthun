import express from "express";
import fetch from "node-fetch";
import { load } from "cheerio";

const app = express();
const BASE = "https://mihaaru.com";

// Serve static files
app.use(express.static("public"));

// ===== CACHES =====
const LIST_TTL = 2 * 60 * 1000;   // 2 min: grid list refresh
const ARTICLE_TTL = 10 * 60 * 1000; // 10 min: article page cache

let listCache = {
  urls: [],
  timestamp: 0
};

const articleCache = new Map(); // url -> { data, ts }

function nowMs() {
  return Date.now();
}

function uniq(arr) {
  return [...new Set(arr)];
}

function pickRandom(arr, avoid) {
  if (!arr.length) return null;
  if (arr.length === 1) return arr[0];

  // remove avoid if possible
  let pool = arr;
  if (avoid) {
    const filtered = arr.filter((x) => x !== avoid);
    if (filtered.length) pool = filtered;
  }

  const idx = Math.floor(Math.random() * pool.length);
  return pool[idx];
}

async function fetchGridUrls() {
  const now = nowMs();

  // Use cached list if fresh
  if (listCache.urls.length && now - listCache.timestamp < LIST_TTL) {
    return listCache.urls;
  }

  // 1) Load Mihaaru news page (the one with your grid)
  const homeRes = await fetch(`${BASE}`, {
    headers: { "User-Agent": "Mozilla/5.0" }
  });
  const homeHtml = await homeRes.text();
  const $ = load(homeHtml);

  // 2) Grab article links from grid items container
  // Works with: <div id="news-grid-XXXX-items"> ... <article> ... <a href="/news/153681">
  const found = [];

$('article a[href^="/"]').each((_, a) => {
  const href = $(a).attr("href");
  if (!href) return;

  if (/^\/(news|world|sports|business|local)\/\d+/.test(href)) {
    found.push(`${BASE}${href}`);
  }
});

  const urls = uniq(found).slice(0, 12); // first 12 articles from that grid

  if (!urls.length) {
    throw new Error("No article links found in news grid");
  }

  listCache.urls = urls;
  listCache.timestamp = now;

  return urls;
}

async function fetchArticle(url) {
  const now = nowMs();

  // cache per-article
  const cached = articleCache.get(url);
  if (cached && now - cached.ts < ARTICLE_TTL) {
    return { ...cached.data, cachedArticle: true };
  }

  const res = await fetch(url, {
    headers: { "User-Agent": "Mozilla/5.0" }
  });
  const html = await res.text();
  const $ = load(html);

  const title = $("h1").first().text().trim();

  // ---- Robust content extraction (prevents failures) ----
  const content = [];

  const pushText = (t) => {
    const text = (t || "").replace(/\s+/g, " ").trim();
    if (!text) return;
    if (text.length < 10) return; // filter tiny junk
    content.push(text);
  };

  // Try common containers first
  $(".block-wrapper.text-right p").each((_, el) => pushText($(el).text()));

  if (!content.length) {
    $("article p").each((_, el) => pushText($(el).text()));
  }

  if (!content.length) {
    $("main p, .prose p, .content p, .post-content p").each((_, el) =>
      pushText($(el).text())
    );
  }

  if (!content.length) {
    $("p").slice(0, 25).each((_, el) => pushText($(el).text()));
  }

  const data = {
    url,
    title: title || "ލިޔުމެއް",
    content:
      content.length
        ? content
        : ["ލިޔުމެއް ނުފެނުނު"],
    fetchedAt: new Date().toISOString()
  };

  articleCache.set(url, { data, ts: now });
  return { ...data, cachedArticle: false };
}

// ✅ Random article API (avoid repeats using ?last=...)
app.get("/api/random-article", async (req, res) => {
  try {
    const last = (req.query.last || "").toString().trim();

    const urls = await fetchGridUrls();
    const picked = pickRandom(urls, last);

    if (!picked) throw new Error("Failed to pick random article");

    const article = await fetchArticle(picked);

    res.json({
      ...article,
      cachedList: nowMs() - listCache.timestamp < LIST_TTL
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Failed to load random article",
      detail: err.message
    });
  }
});

app.listen(3000, () => {
  console.log("✅ Server running at http://localhost:3000");
});
