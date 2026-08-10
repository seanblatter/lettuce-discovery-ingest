// scripts/rss-firehose.mjs (item 4)
// Polls curated RSS/Atom feed list, fetches new items into hot/rss-{ts}.ndjson.gz.
// 15-min cron via GH Actions → always-fresh hot index.

import { putObject, headExists, getObjectBuffer } from "./r2.mjs";
import { extractDoc, isGood } from "./lib-doc.mjs";
import { gzipSync } from "node:zlib";

const UA = "LettuceVision/1.0 (+https://lettuce.vision)";
const FEEDS_KEY = "ingest/rss-feeds.txt";

// Seed feeds if not yet uploaded — top English news + blogs
const DEFAULT_FEEDS = [
  "https://hnrss.org/frontpage",
  "https://feeds.arstechnica.com/arstechnica/index",
  "https://www.theverge.com/rss/index.xml",
  "https://techcrunch.com/feed/",
  "https://www.wired.com/feed/rss",
  "https://feeds.bbci.co.uk/news/rss.xml",
  "https://feeds.npr.org/1001/rss.xml",
  "https://feeds.reuters.com/reuters/topNews",
  "https://www.aljazeera.com/xml/rss/all.xml",
  "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml",
  "https://feeds.washingtonpost.com/rss/homepage",
  "https://feeds.a.dj.com/rss/RSSWSJD.xml",
  "https://feeds.bloomberg.com/markets/news.rss",
  "https://feeds.feedburner.com/venturebeat/SZYF",
  "https://www.smashingmagazine.com/feed/",
  "https://css-tricks.com/feed/",
  "https://blog.cloudflare.com/rss/",
  "https://github.blog/feed/",
  "https://stackoverflow.blog/feed/",
  "https://openai.com/blog/rss.xml",
];

async function loadFeeds() {
  if (await headExists(FEEDS_KEY)) {
    return (await getObjectBuffer(FEEDS_KEY)).toString().split("\n").filter(Boolean);
  }
  await putObject(FEEDS_KEY, DEFAULT_FEEDS.join("\n") + "\n", "text/plain");
  return DEFAULT_FEEDS;
}

function extractItems(xml) {
  const items = [];
  const re = /<(?:item|entry)[\s\S]*?<link[^>]*(?:href=["']([^"']+)["']|>([^<]+)<\/link)[\s\S]*?<\/(?:item|entry)>/g;
  let m;
  while ((m = re.exec(xml))) {
    const url = m[1] || m[2];
    if (url && /^https?:/.test(url)) items.push(url);
    if (items.length >= 40) break;
  }
  return items;
}

async function fetchText(url, timeout = 6000) {
  try {
    const r = await fetch(url, { headers: { "user-agent": UA }, signal: AbortSignal.timeout(timeout) });
    if (!r.ok) return null;
    return await r.text();
  } catch { return null; }
}

const feeds = await loadFeeds();
const docs = [];
const seenUrls = new Set();

for (const feed of feeds) {
  const xml = await fetchText(feed);
  if (!xml) continue;
  const urls = extractItems(xml);
  for (const u of urls) {
    if (seenUrls.has(u)) continue;
    seenUrls.add(u);
    const html = await fetchText(u);
    if (!html) continue;
    const doc = extractDoc(u, html);
    if (isGood(doc)) docs.push(JSON.stringify(doc));
  }
}

const gz = gzipSync(Buffer.from(docs.join("\n") + "\n"));
const key = `hot/rss-${Date.now()}.ndjson.gz`;
await putObject(key, gz, "application/x-ndjson", { ContentEncoding: "gzip" });
console.log(`rss: ${docs.length} fresh docs from ${feeds.length} feeds → ${key}`);
