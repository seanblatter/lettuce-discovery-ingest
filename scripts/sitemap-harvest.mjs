// scripts/sitemap-harvest.mjs
// For each distinct host discovered in the last 30d of hot-shards, fetch
// robots.txt → sitemap → all URLs listed. Dumps to `sitemap-urls/*.ndjson.gz`
// so crawler-batch.mjs picks them up alongside hot-shards on future runs.
//
// One sitemap can list 50k URLs — this is the 10x throughput multiplier.
// Runs every 6 hours. Skips hosts we've already harvested this week.

import { getObjectBuffer, getStream, putObject } from "./r2.mjs";
import { createGunzip, gzipSync } from "node:zlib";
import { createHash } from "node:crypto";

const CONCURRENCY = Number(process.env.SITEMAP_CONCURRENCY || 24);
const MAX_HOSTS   = Number(process.env.SITEMAP_MAX_HOSTS || 3000);
const MAX_URLS_PER_HOST = Number(process.env.SITEMAP_MAX_URLS_PER_HOST || 5000);
const FETCH_TIMEOUT = Number(process.env.SITEMAP_TIMEOUT_MS || 8000);
const UA = "Lettuce-Vision-Sitemap/1.0 (+https://lettuce.vision)";
const CURSOR_KEY = "sitemap-urls/cursor.json";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Load cursor (hosts already tried in the last 7 days).
let cursor = { hostsTried: {}, urlsHarvested: 0, updatedAt: null };
try {
  const buf = await getObjectBuffer(CURSOR_KEY);
  cursor = { ...cursor, ...JSON.parse(buf.toString()) };
} catch (_) {}

const RECENT_MS = 7 * 24 * 3600 * 1000;
const now = Date.now();
for (const [host, ts] of Object.entries(cursor.hostsTried)) {
  if (now - ts > RECENT_MS) delete cursor.hostsTried[host];
}

// --- Read candidate hosts from hot-shards ---
async function readHotShardHosts() {
  const hosts = new Set();
  try {
    const manifestBuf = await getObjectBuffer("hot-shards/manifest.json");
    const manifest = JSON.parse(manifestBuf.toString());
    await Promise.all((manifest.shards || []).map(async (s) => {
      if (!s.docs) return;
      try {
        const stream = await getStream(s.path);
        const gunzip = createGunzip();
        stream.pipe(gunzip);
        let buf = "";
        for await (const chunk of gunzip) {
          buf += chunk.toString();
          let nl;
          while ((nl = buf.indexOf("\n")) >= 0) {
            const line = buf.slice(0, nl); buf = buf.slice(nl + 1);
            if (!line) continue;
            try {
              const doc = JSON.parse(line);
              if (!doc || !doc.u) continue;
              const h = new URL(doc.u).hostname.toLowerCase();
              if (!cursor.hostsTried[h]) hosts.add(h);
              if (hosts.size >= MAX_HOSTS * 4) return;
            } catch (_) {}
          }
        }
      } catch (e) { console.warn(`shard ${s.path}: ${e.message}`); }
    }));
  } catch (e) {
    console.error(`hot-shards manifest missing: ${e.message}`);
  }
  return [...hosts];
}

// --- Sitemap discovery + fetch ---
async function fetchText(url) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), FETCH_TIMEOUT);
  try {
    const r = await fetch(url, { headers: { "user-agent": UA }, signal: ac.signal, redirect: "follow" });
    if (!r.ok) return null;
    const buf = Buffer.from(await r.arrayBuffer());
    // Handle .gz sitemaps
    if (url.endsWith(".gz")) {
      try {
        const zlib = await import("node:zlib");
        return zlib.gunzipSync(buf).toString("utf8");
      } catch (_) { return null; }
    }
    return buf.toString("utf8").slice(0, 5_000_000); // 5 MB cap
  } catch (_) { return null; }
  finally { clearTimeout(t); }
}

async function sitemapUrlsFor(host) {
  const robotsText = await fetchText(`https://${host}/robots.txt`);
  const sitemapUrls = [];
  if (robotsText) {
    const re = /^sitemap:\s*(\S+)/gim;
    let m;
    while ((m = re.exec(robotsText)) !== null) sitemapUrls.push(m[1]);
  }
  if (!sitemapUrls.length) sitemapUrls.push(`https://${host}/sitemap.xml`);
  return sitemapUrls;
}

function extractLocs(xml) {
  const out = [];
  const re = /<loc>([^<]+)<\/loc>/g;
  let m;
  while ((m = re.exec(xml)) !== null) {
    const u = m[1].trim();
    if (u.startsWith("http")) out.push(u);
    if (out.length >= MAX_URLS_PER_HOST * 2) break;
  }
  return out;
}

async function harvestHost(host) {
  const smUrls = await sitemapUrlsFor(host);
  const urls = new Set();
  for (const smUrl of smUrls.slice(0, 4)) {
    const text = await fetchText(smUrl);
    if (!text) continue;
    const locs = extractLocs(text);
    // If it's a sitemap index (contains <sitemap>/<loc> to child sitemaps),
    // the extracted locs will themselves be sitemap URLs → fetch a few.
    const looksLikeIndex = /<sitemapindex[\s>]/i.test(text);
    if (looksLikeIndex) {
      for (const child of locs.slice(0, 20)) {
        if (urls.size >= MAX_URLS_PER_HOST) break;
        const childText = await fetchText(child);
        if (!childText) continue;
        for (const u of extractLocs(childText)) {
          urls.add(u);
          if (urls.size >= MAX_URLS_PER_HOST) break;
        }
      }
    } else {
      for (const u of locs) {
        urls.add(u);
        if (urls.size >= MAX_URLS_PER_HOST) break;
      }
    }
    if (urls.size >= MAX_URLS_PER_HOST) break;
  }
  cursor.hostsTried[host] = Date.now();
  return [...urls];
}

// --- Main ---
const candidates = (await readHotShardHosts()).slice(0, MAX_HOSTS);
console.error(`sitemap harvest: ${candidates.length} candidate hosts (max ${MAX_URLS_PER_HOST} URLs each)`);

if (!candidates.length) {
  console.log("::notice title=Sitemap::No new hosts to harvest.");
  process.exit(0);
}

const allUrls = [];
let done = 0;
const t0 = Date.now();

async function pool() {
  let c = 0;
  await Promise.all(Array.from({ length: CONCURRENCY }, async () => {
    while (true) {
      const i = c++;
      if (i >= candidates.length) return;
      try {
        const urls = await harvestHost(candidates[i]);
        for (const u of urls) allUrls.push({ u, t: candidates[i], d: "", c: "", src: "sitemap" });
      } catch (_) {}
      done++;
      if (done % 50 === 0 || done === candidates.length) {
        console.error(`  ${done}/${candidates.length}  urls=${allUrls.length}  ${(done/((Date.now()-t0)/1000)).toFixed(1)}/s`);
      }
    }
  }));
}
await pool();

if (!allUrls.length) {
  console.log("::notice title=Sitemap::0 URLs harvested.");
  await putObject(CURSOR_KEY, Buffer.from(JSON.stringify(cursor)), "application/json");
  process.exit(0);
}

// Upload as a single batch — schema-compatible with hot-shards so the
// crawler can consume without special-casing.
const nd = allUrls.map((d) => JSON.stringify(d)).join("\n") + "\n";
const gz = gzipSync(Buffer.from(nd));
const d = new Date();
const seq = createHash("sha1").update(nd).digest("hex").slice(0, 10);
const key = `sitemap-urls/${d.toISOString().slice(0, 10)}/${seq}.ndjson.gz`;
await putObject(key, gz, "application/gzip");

cursor.urlsHarvested += allUrls.length;
cursor.updatedAt = new Date().toISOString();
await putObject(CURSOR_KEY, Buffer.from(JSON.stringify(cursor)), "application/json", { CacheControl: "public, max-age=60" });

console.log(`::notice title=Sitemap::Harvested ${allUrls.length.toLocaleString()} URLs from ${done} hosts → ${key}`);
if (process.env.GITHUB_STEP_SUMMARY) {
  const fs = await import("node:fs");
  fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY,
    `### Sitemap harvest\n\n- **URLs harvested this run:** ${allUrls.length.toLocaleString()}\n- **Hosts scanned:** ${done}\n- **Cumulative URLs harvested:** ${cursor.urlsHarvested.toLocaleString()}\n- **Batch key:** \`${key}\`\n`);
}
