// scripts/crawler-batch.mjs
// Lightweight batched crawler. Each run:
//   1. Loads the hot-shards manifest → gets recent hostnames from ct-tailer +
//      certstream discovery. Cross-references crawler cursor to skip already-
//      fetched URLs.
//   2. Fetches up to BATCH_URLS distinct root URLs (https://host/) with
//      per-host concurrency limit + polite delay + robots.txt honoring.
//   3. Extracts title, meta description, canonical URL, OpenGraph tags,
//      first 2 KB of visible text, all outbound links, and image/video refs.
//   4. Batches into gzipped NDJSON, uploads to
//      crawled/YYYY/MM/DD/{batchId}.ndjson.gz — reused by index-batch.mjs
//      the same way `raw/` is.
//   5. Advances cursor `crawler/cursor.json`.
//
// Storage cost per crawled page: ~2 KB compressed. 100k/day → 6 GB/mo.

import { getObjectBuffer, getStream, putObject, listPrefix } from "./r2.mjs";
import { createGunzip, gzipSync } from "node:zlib";
import { createHash } from "node:crypto";

const BATCH_URLS      = Number(process.env.CRAWL_BATCH_URLS || 5000);
const CONCURRENCY     = Number(process.env.CRAWL_CONCURRENCY || 32);
const PER_HOST_DELAY  = Number(process.env.CRAWL_PER_HOST_DELAY_MS || 800);
const FETCH_TIMEOUT   = Number(process.env.CRAWL_TIMEOUT_MS || 8000);
const MAX_BYTES       = Number(process.env.CRAWL_MAX_BYTES || 512_000);
const CURSOR_KEY      = "crawler/cursor.json";
const UA              = "Lettuce-Vision-Crawler/1.0 (+https://lettuce.vision/crawler)";

// ---- helpers ----
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const now = () => new Date();

function readGzipStream(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    const gunzip = createGunzip();
    stream.pipe(gunzip);
    gunzip.on("data", (c) => chunks.push(c));
    gunzip.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    gunzip.on("error", reject);
    stream.on("error", reject);
  });
}

// ---- cursor ----
let cursor = { fetched: 0, skipped: 0, robotsDeny: 0, errors: 0, seenHostsSample: 0, updatedAt: null, batches: [] };
try {
  const buf = await getObjectBuffer(CURSOR_KEY);
  cursor = { ...cursor, ...JSON.parse(buf.toString()) };
  console.error(`crawler cursor: fetched=${cursor.fetched}, batches=${cursor.batches.length}`);
} catch (_) { console.error("no crawler cursor — starting fresh"); }

// URLs already fetched: not tracked as a full set (too big) — instead each
// batch avoids re-fetching by looking at the cursor's short-term recency
// window. Cheap dedup via a Bloom-ish HashSet loaded from the last 3 batches.
const recentFetched = new Set();
for (const b of cursor.batches.slice(-3)) {
  for (const u of b.samples || []) recentFetched.add(u);
}

// ---- source: hot-shards manifest (recent discovery) + sitemap-urls/ ----
async function loadCandidateUrls() {
  const urls = new Set();
  // 1) Hot-shards (freshly-discovered hostnames from ct-tailer/certstream).
  try {
    const buf = await getObjectBuffer("hot-shards/manifest.json");
    const manifest = JSON.parse(buf.toString());
    if (Array.isArray(manifest.shards)) {
      await Promise.all(manifest.shards.map(async (s) => {
        if (!s.docs) return;
        try {
          const stream = await getStream(s.path);
          const text = await readGzipStream(stream);
          for (const line of text.split("\n")) {
            if (!line) continue;
            try {
              const doc = JSON.parse(line);
              if (!doc || !doc.u) continue;
              if (recentFetched.has(doc.u)) continue;
              urls.add(doc.u);
              if (urls.size >= BATCH_URLS * 8) return;
            } catch (_) {}
          }
        } catch (e) {
          console.warn(`  hot-shard ${s.path} failed: ${e.message}`);
        }
      }));
    }
  } catch (e) {
    console.error(`hot-shards manifest missing (${e.message})`);
  }
  // 2) Sitemap-harvested URLs (deep pages within known sites).
  try {
    const files = await listPrefix("sitemap-urls/");
    // Take most recent 3 daily-folders, in reverse-lex order.
    const recent = files.filter((f) => /\.ndjson\.gz$/.test(f.Key)).sort((a, b) => b.Key.localeCompare(a.Key)).slice(0, 12);
    for (const f of recent) {
      if (urls.size >= BATCH_URLS * 8) break;
      const rel = f.Key.replace(/^.*?discovery\//, "");
      try {
        const stream = await getStream(rel);
        const text = await readGzipStream(stream);
        for (const line of text.split("\n")) {
          if (!line) continue;
          try {
            const doc = JSON.parse(line);
            if (!doc || !doc.u) continue;
            if (recentFetched.has(doc.u)) continue;
            urls.add(doc.u);
            if (urls.size >= BATCH_URLS * 8) break;
          } catch (_) {}
        }
      } catch (_) {}
    }
  } catch (_) {}
  return [...urls];
}

// ---- robots.txt (cheap, per-host cache) ----
const robotsCache = new Map();
async function checkRobots(host) {
  if (robotsCache.has(host)) return robotsCache.get(host);
  const url = `https://${host}/robots.txt`;
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), FETCH_TIMEOUT);
  let policy = { allow: true, delay: 0 };
  try {
    const r = await fetch(url, { headers: { "user-agent": UA, accept: "text/plain,*/*" }, signal: ac.signal, redirect: "follow" });
    if (r.ok) {
      const text = (await r.text()).slice(0, 30_000);
      // Very light robots parse: honor a `User-agent: *` block's Disallow: /
      const lines = text.split(/\r?\n/);
      let inStar = false;
      for (const raw of lines) {
        const line = raw.replace(/#.*/, "").trim();
        if (!line) { inStar = false; continue; }
        const m = line.match(/^([A-Za-z-]+):\s*(.*)$/);
        if (!m) continue;
        const k = m[1].toLowerCase(); const v = m[2].trim();
        if (k === "user-agent") { inStar = v === "*" || v.toLowerCase() === "lettuce-vision-crawler"; continue; }
        if (!inStar) continue;
        if (k === "disallow" && (v === "/" || v === "")) { policy.allow = v !== "/"; }
        if (k === "crawl-delay") { const d = Number(v); if (Number.isFinite(d)) policy.delay = Math.min(10, d) * 1000; }
      }
    }
  } catch (_) { /* if robots.txt is unreachable, default to allow */ }
  finally { clearTimeout(t); }
  robotsCache.set(host, policy);
  return policy;
}

// ---- HTML extraction (regex-based; fast enough for 100k pages/hr) ----
function extractDoc(html, sourceUrl) {
  const t = String(html).slice(0, 400_000); // safety cap
  const pick = (re) => { const m = t.match(re); return m ? m[1].replace(/\s+/g, " ").trim() : ""; };
  const title = pick(/<title[^>]*>([\s\S]*?)<\/title>/i);
  const metaDesc = pick(/<meta[^>]+name=["']description["'][^>]+content=["']([^"']+)["']/i)
                || pick(/<meta[^>]+property=["']og:description["'][^>]+content=["']([^"']+)["']/i);
  const canonical = pick(/<link[^>]+rel=["']canonical["'][^>]+href=["']([^"']+)["']/i);
  const ogTitle = pick(/<meta[^>]+property=["']og:title["'][^>]+content=["']([^"']+)["']/i);
  const ogImage = pick(/<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']/i);
  const lang = pick(/<html[^>]+lang=["']([^"']+)["']/i);
  // Visible text: strip scripts/styles, all tags, decode a few entities.
  const stripped = t.replace(/<script[\s\S]*?<\/script>/gi, " ").replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;|&#160;/g, " ").replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">")
    .replace(/&#39;|&apos;/g, "'").replace(/&quot;/g, '"')
    .replace(/\s+/g, " ").trim();
  const text = stripped.slice(0, 2000);
  // Outbound links (up to 128)
  const links = new Set();
  const linkRe = /<a[^>]+href=["']([^"']+)["']/gi;
  let lm; let count = 0;
  while ((lm = linkRe.exec(t)) !== null && count < 128) {
    const href = lm[1];
    if (/^(https?:)?\/\//i.test(href) || href.startsWith("/")) {
      try {
        const u = new URL(href, sourceUrl);
        if (u.protocol === "http:" || u.protocol === "https:") {
          links.add(u.href.split("#")[0]);
          count++;
        }
      } catch (_) {}
    }
  }
  return { title, metaDesc, canonical, ogTitle, ogImage, lang, text, links: [...links] };
}

// ---- fetch one URL (with size cap + timeout) ----
async function fetchUrl(url) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), FETCH_TIMEOUT);
  try {
    const r = await fetch(url, {
      headers: { "user-agent": UA, "accept": "text/html,application/xhtml+xml", "accept-language": "en;q=0.9" },
      signal: ac.signal,
      redirect: "follow",
    });
    if (!r.ok) return { ok: false, status: r.status };
    const ct = (r.headers.get("content-type") || "").toLowerCase();
    if (!ct.includes("html") && !ct.includes("xml") && !ct.includes("text/plain")) return { ok: false, status: 415 };
    const reader = r.body.getReader();
    const chunks = []; let bytes = 0;
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value); bytes += value.length;
      if (bytes >= MAX_BYTES) { try { await reader.cancel(); } catch (_) {} break; }
    }
    const html = Buffer.concat(chunks.map((c) => Buffer.from(c))).toString("utf8", 0, Math.min(bytes, MAX_BYTES));
    return { ok: true, status: r.status, contentType: ct, html, finalUrl: r.url };
  } catch (e) {
    return { ok: false, error: e.name === "AbortError" ? "timeout" : (e.message || "err") };
  } finally { clearTimeout(t); }
}

// ---- per-host politeness queue ----
const hostLastFetch = new Map();
async function politeFetch(url) {
  let u;
  try { u = new URL(url); } catch { return { ok: false, error: "badurl" }; }
  const host = u.host;
  const robots = await checkRobots(host);
  if (!robots.allow) { cursor.robotsDeny++; return { ok: false, error: "robots" }; }
  const delay = Math.max(PER_HOST_DELAY, robots.delay || 0);
  const prev = hostLastFetch.get(host) || 0;
  const wait = Math.max(0, prev + delay - Date.now());
  if (wait) await sleep(wait);
  hostLastFetch.set(host, Date.now());
  return fetchUrl(url);
}

// ---- main crawl loop ----
const candidates = await loadCandidateUrls();
if (!candidates.length) {
  console.log("::notice title=Crawler::No candidate URLs — hot-shards manifest empty. Waiting for hot-reshard to populate.");
  process.exit(0);
}

// Shuffle so we don't hammer the same TLD contiguously.
candidates.sort(() => Math.random() - 0.5);
const targets = candidates.slice(0, BATCH_URLS);
console.error(`crawler: ${targets.length} URLs to fetch, conc=${CONCURRENCY}, per-host-delay=${PER_HOST_DELAY}ms`);

const docs = [];
let done = 0;
const t0 = Date.now();

async function processUrl(url) {
  const result = await politeFetch(url);
  done++;
  if (!result.ok) { cursor.errors++; return; }
  cursor.fetched++;
  try {
    const ex = extractDoc(result.html, result.finalUrl || url);
    // Skip if effectively empty
    if (!ex.title && !ex.text) return;
    docs.push({
      u: result.finalUrl || url,
      t: ex.ogTitle || ex.title || "",
      d: ex.metaDesc || "",
      c: "",
      k: "",           // legacy field for schema compat with raw/
      text: ex.text,
      canonical: ex.canonical || null,
      image: ex.ogImage || null,
      lang: ex.lang || null,
      links: ex.links.slice(0, 64),
      status: result.status,
      contentType: result.contentType,
      fetchedAt: new Date().toISOString(),
      src: "crawl",
    });
  } catch (_) { cursor.errors++; }
  if (done % 100 === 0 || done === targets.length) {
    const secs = (Date.now() - t0) / 1000;
    console.error(`  crawled ${done}/${targets.length}  ok=${cursor.fetched}  err=${cursor.errors}  robots=${cursor.robotsDeny}  ${(done/secs).toFixed(1)}/s`);
  }
}

async function runPool() {
  let c = 0;
  await Promise.all(Array.from({ length: CONCURRENCY }, async () => {
    while (true) {
      const i = c++;
      if (i >= targets.length) return;
      await processUrl(targets[i]);
    }
  }));
}
await runPool();

// ---- upload batch ----
if (docs.length) {
  const nd = docs.map((d) => JSON.stringify(d)).join("\n") + "\n";
  const gz = gzipSync(Buffer.from(nd));
  const d = now();
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const seq = createHash("sha1").update(nd).digest("hex").slice(0, 10);
  const key = `crawled/${yyyy}/${mm}/${dd}/${hh}-${seq}.ndjson.gz`;
  await putObject(key, gz, "application/gzip");
  cursor.batches.push({ key, docs: docs.length, generatedAt: d.toISOString(), samples: docs.slice(0, 50).map((x) => x.u) });
  if (cursor.batches.length > 30) cursor.batches = cursor.batches.slice(-30);
  console.error(`crawler: wrote ${docs.length} docs → ${key} (${(gz.length/1024).toFixed(1)} KB)`);
}

cursor.updatedAt = new Date().toISOString();
await putObject(CURSOR_KEY, Buffer.from(JSON.stringify(cursor, null, 2)), "application/json", { CacheControl: "public, max-age=60" });

const rate = docs.length ? Math.round(docs.length / ((Date.now() - t0) / 3600000)) : 0;
console.log(`::notice title=Crawler::${docs.length} pages saved (${cursor.errors} errors, ${cursor.robotsDeny} robots-denied). Rate: ${rate.toLocaleString()} pages/hr projected.`);
if (process.env.GITHUB_STEP_SUMMARY) {
  const fs = await import("node:fs");
  fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY,
    `### Crawler run\n\n- **Pages saved this run:** ${docs.length.toLocaleString()}\n- **URLs attempted:** ${targets.length.toLocaleString()}\n- **Errors:** ${cursor.errors.toLocaleString()}\n- **Robots-denied:** ${cursor.robotsDeny.toLocaleString()}\n- **Rate:** ${rate.toLocaleString()} pages/hr\n- **Cumulative:** ${cursor.fetched.toLocaleString()} pages fetched, ${cursor.batches.length} recent batches\n`);
}
console.log(JSON.stringify({ crawled: docs.length, errors: cursor.errors, cumulative: cursor.fetched }));
