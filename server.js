import express from "express";
import fetch from "node-fetch";
import { load } from "cheerio";

const app = express();
const BASE = "https://mihaaru.com";

// ===== CACHE =====
let cache = {
  data: null,
  timestamp: 0
};
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes

// Serve static files
app.use(express.static("public"));

app.get("/api/latest-news", async (req, res) => {
  try {
    const now = Date.now();

    // ✅ Return cached data if valid
    if (cache.data && now - cache.timestamp < CACHE_TTL) {
      return res.json({ ...cache.data, cached: true });
    }

    // 1️⃣ Load news homepage
    const homeRes = await fetch(`${BASE}/news`);
    const homeHtml = await homeRes.text();
    const $home = load(homeHtml);

    // 2️⃣ Get latest article link
    const latestLink = $home(".space-y-5 a[href^='/news/']").first().attr("href");
    if (!latestLink) throw new Error("No article found");

    // 3️⃣ Load article page
    const articleRes = await fetch(`${BASE}${latestLink}`);
    const articleHtml = await articleRes.text();
    const $article = load(articleHtml);

    // 4️⃣ Extract title
    const title = $article("h1").first().text().trim();

    // 5️⃣ Extract main content
    const content = [];
    $article(".block-wrapper.text-right p").each((_, el) => {
      const text = $article(el).text().trim();
      if (text) content.push(text);
    });

    const data = {
      url: `${BASE}${latestLink}`,
      title,
      content,
      fetchedAt: new Date().toISOString()
    };

    // ✅ Save to cache
    cache.data = data;
    cache.timestamp = now;

    res.json({ ...data, cached: false });

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to load latest news" });
  }
});

app.listen(3000, () => {
  console.log("✅ Server running at http://localhost:3000");
});
