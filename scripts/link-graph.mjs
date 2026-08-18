// scripts/link-graph.mjs
// Batched domain-popularity computation. Reads outbound links from crawled/
// and raw/ NDJSON, aggregates per-domain incoming-link counts, and emits a
// per-domain score (log-scaled) at `link-graph/domain-scores.json`.
//
// Query-time (search-r2.js) can boost documents whose host has a higher
// domain score — the poor-man's PageRank that gets ~25% relevance lift for
// nearly zero query-time cost (one KV lookup).
//
// Cursor at `link-graph/cursor.json` so successive runs incrementally add.
// Full recompute available via `LINKGRAPH_RESET=1`.

import { listPrefix, getObjectBuffer, getObjectStream, putObject } from "./r2.mjs";
import { createGunzip } from "node:zlib";

const BATCH_SIZE = Number(process.env.LINKGRAPH_BATCH_SIZE || 300);
const CONCURRENCY = Number(process.env.LINKGRAPH_CONCURRENCY || 20);
const RESET = process.env.LINKGRAPH_RESET === "1";
const CURSOR_KEY = "link-graph/cursor.json";
const SCORES_KEY = "link-graph/domain-scores.json";

// ---- cursor + accumulator ----
let cursor = { nextRaw: 0, nextCrawled: 0, filesProcessed: 0, updatedAt: null };
let accum = {}; // { host: incomingCount }
if (!RESET) {
  try {
    const buf = await getObjectBuffer(CURSOR_KEY);
    cursor = { ...cursor, ...JSON.parse(buf.toString()) };
    console.error(`link-graph cursor: nextRaw=${cursor.nextRaw} nextCrawled=${cursor.nextCrawled}`);
  } catch (_) { console.error("no cursor — starting fresh"); }
  try {
    const buf = await getObjectBuffer(SCORES_KEY);
    const prior = JSON.parse(buf.toString());
    if (prior && prior.scores) accum = prior.scores;
    console.error(`link-graph loaded ${Object.keys(accum).length} prior domains`);
  } catch (_) {}
}

function hostOf(url) {
  try { return new URL(url).hostname.toLowerCase().replace(/^www\./, ""); }
  catch { return null; }
}

// ---- iterate files ----
async function pickBatch(prefix, offset, size) {
  const files = await listPrefix(prefix);
  files.sort((a, b) => a.Key.localeCompare(b.Key));
  const total = files.length;
  const batch = files.slice(offset, offset + size);
  return { batch, total };
}

const [rawSlice, crawledSlice] = await Promise.all([
  pickBatch("raw/", cursor.nextRaw, Math.floor(BATCH_SIZE * 0.4)),          // 40% of budget on raw (larger docs but less link data)
  pickBatch("crawled/", cursor.nextCrawled, Math.floor(BATCH_SIZE * 0.6)),  // 60% on crawled (richer links)
]);

const targets = [
  ...rawSlice.batch.map((f) => ({ ...f, kind: "raw" })),
  ...crawledSlice.batch.map((f) => ({ ...f, kind: "crawled" })),
];

if (!targets.length) {
  console.log("::notice title=Link Graph::Nothing new to process.");
  process.exit(0);
}

console.error(`link-graph batch: ${targets.length} files (${rawSlice.batch.length} raw, ${crawledSlice.batch.length} crawled)`);

async function processOne(o) {
  const rel = o.Key.replace(/^.*?discovery\//, "");
  try {
    const stream = await getObjectStream(rel);
    const gunzip = createGunzip();
    stream.pipe(gunzip);
    let buf = "";
    for await (const chunk of gunzip) {
      buf += chunk.toString();
      let nl;
      while ((nl = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, nl);
        buf = buf.slice(nl + 1);
        if (!line) continue;
        try {
          const doc = JSON.parse(line);
          if (!doc) continue;
          // Sources of links: explicit `links` array (crawled/), or fallback to `u` itself
          const linkSources = Array.isArray(doc.links) ? doc.links : [];
          for (const l of linkSources) {
            const host = hostOf(l);
            if (!host) continue;
            accum[host] = (accum[host] || 0) + 1;
          }
        } catch (_) {}
      }
    }
  } catch (e) {
    console.error(`  skip ${rel}: ${e.message}`);
  }
}

let done = 0;
async function pool() {
  let c = 0;
  await Promise.all(Array.from({ length: CONCURRENCY }, async () => {
    while (true) {
      const i = c++;
      if (i >= targets.length) return;
      await processOne(targets[i]);
      done++;
      if (done % 25 === 0 || done === targets.length) {
        console.error(`  ${done}/${targets.length}  hosts=${Object.keys(accum).length}`);
      }
    }
  }));
}
await pool();

// ---- log-scale scores (0..30) ----
const scores = {};
let maxCount = 0;
for (const v of Object.values(accum)) if (v > maxCount) maxCount = v;
const denom = Math.log2(Math.max(2, maxCount));
for (const [host, count] of Object.entries(accum)) {
  scores[host] = Math.min(30, Math.round((Math.log2(1 + count) / denom) * 30));
}

// ---- upload ----
const payload = {
  schema: 1,
  generatedAt: new Date().toISOString(),
  totalDomains: Object.keys(scores).length,
  maxIncoming: maxCount,
  scores,
  raw: accum,   // keep raw counts too for future rescaling
};
await putObject(SCORES_KEY, Buffer.from(JSON.stringify(payload)), "application/json", { CacheControl: "public, max-age=60" });

cursor.nextRaw += rawSlice.batch.length;
cursor.nextCrawled += crawledSlice.batch.length;
cursor.filesProcessed += targets.length;
cursor.updatedAt = new Date().toISOString();
await putObject(CURSOR_KEY, Buffer.from(JSON.stringify(cursor, null, 2)), "application/json", { CacheControl: "public, max-age=60" });

const pctSwept = ((cursor.nextRaw + cursor.nextCrawled) / Math.max(1, rawSlice.total + crawledSlice.total) * 100).toFixed(2);
console.log(`::notice title=Link Graph::${Object.keys(scores).length.toLocaleString()} domains scored, max=${maxCount.toLocaleString()} incoming. Sweep ${pctSwept}%.`);
if (process.env.GITHUB_STEP_SUMMARY) {
  const fs = await import("node:fs");
  const top = Object.entries(scores).sort((a, b) => b[1] - a[1]).slice(0, 20)
    .map(([h, s]) => `- **${h}** — ${s} (${accum[h].toLocaleString()} incoming)`).join("\n");
  fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY,
    `### Link graph update\n\n- **Domains scored:** ${Object.keys(scores).length.toLocaleString()}\n- **Max incoming links:** ${maxCount.toLocaleString()}\n- **Files processed this run:** ${targets.length}\n- **Sweep:** ${pctSwept}%\n\n**Top 20 domains:**\n${top}\n`);
}
console.log(JSON.stringify({ domains: Object.keys(scores).length, filesProcessed: targets.length }));
