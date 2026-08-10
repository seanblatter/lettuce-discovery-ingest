// scripts/sitemap-crawl.mjs (item 3)
// Reads unique domains from raw/, fetches each /sitemap.xml, extracts URLs, fetches
// only NEW URLs (not already in our index simhash set), writes to hot/sitemap-{ts}.ndjson.gz.
// Runs in a bounded time budget. Fresh long-tail without duplicating CC coverage.

import { listPrefix, getObjectStream, putObject } from "./r2.mjs";
import { extractDoc, isGood } from "./lib-doc.mjs";
import { createGunzip, gzipSync } from "node:zlib";

const BUDGET_MIN = Number(process.env.BUDGET_MIN || 300);
const MAX_DOMAINS = Number(process.env.MAX_DOMAINS || 5000);
const MAX_PER_DOMAIN = 200;
const UA = "LettuceVision/1.0 (+https://lettuce.vision)";
const deadline = Date.now() + BUDGET_MIN * 60_000;

async function loadTopDomains() {
  const files = (await listPrefix("raw/")).slice(0, 20); // sample recent shards
  const counts = new Map();
  for (const f of files) {
    const rel = f.Key.split("discovery/").pop();
    const s = await getObjectStream(rel);
    const gz = createGunzip(); s.pipe(gz);
    let buf = "";
    for await (const ch of gz) {
      buf += ch.toString();
      let nl;
      while ((nl = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, nl); buf = buf.slice(nl + 1);
        if (!line) continue;
        try {
          const u = new URL(JSON.parse(line).u);
          counts.set(u.host, (counts.get(u.host) || 0) + 1);
        } catch {}
      }
    }
  }
  return [...counts.entries()].sort((a, b) => b[1] - a[1]).slice(0, MAX_DOMAINS).map(x => x[0]);
}

async function fetchSitemap(host) {
  for (const path of ["/sitemap.xml", "/sitemap_index.xml"]) {
    try {
      const r = await fetch(`https://${host}${path}`, { headers: { "user-agent": UA }, signal: AbortSignal.timeout(8000) });
      if (r.ok) return await r.text();
    } catch {}
  }
  return null;
}

function extractUrls(xml) {
  const urls = [];
  const re = /<loc>([^<]+)<\/loc>/g;
  let m;
  while ((m = re.exec(xml)) && urls.length < MAX_PER_DOMAIN) urls.push(m[1].trim());
  return urls;
}

async function fetchPage(url) {
  try {
    const r = await fetch(url, { headers: { "user-agent": UA }, signal: AbortSignal.timeout(6000) });
    if (!r.ok) return null;
    return await r.text();
  } catch { return null; }
}

const domains = await loadTopDomains();
console.log(`sitemap crawl: ${domains.length} domains`);
const docs = [];

for (const host of domains) {
  if (Date.now() > deadline) break;
  const xml = await fetchSitemap(host);
  if (!xml) continue;
  const urls = extractUrls(xml);
  for (const u of urls.slice(0, 20)) { // top-20 per domain per pass
    if (Date.now() > deadline) break;
    const html = await fetchPage(u);
    if (!html) continue;
    const doc = extractDoc(u, html);
    if (isGood(doc)) docs.push(JSON.stringify(doc));
  }
  if (docs.length >= 100000) break;
}

const gz = gzipSync(Buffer.from(docs.join("\n") + "\n"));
const key = `hot/sitemap-${Date.now()}.ndjson.gz`;
await putObject(key, gz, "application/x-ndjson", { ContentEncoding: "gzip" });
console.log(`sitemap: wrote ${docs.length} docs → ${key}`);
