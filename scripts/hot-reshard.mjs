// scripts/hot-reshard.mjs
// Reshards `hot/*.ndjson.gz` (certstream + gdelt heartbeat drops) into a
// small queryable shard set at `hot-shards/shard-000..015.ndjson.gz` +
// `hot-shards/manifest.json`. Runs hourly and only touches the last
// LOOKBACK_DAYS worth of hot files, so it stays fast + cheap.
//
// This is intentionally independent of the big nightly `raw/ → shards/`
// reshard pipeline. The hot tier is the "search the live web" surface;
// nightly reshard is the "search the cumulative web" surface. Client
// merges both.
//
// Design notes:
//   - Full dedup by canonical URL — hot tier is small (<1M URLs at
//     current heartbeat rates for a 30-day window), everything fits in a
//     single Map.
//   - Hash-partitions into 16 shards (sha1(url) % 16) so the client can
//     compute the shard from a query string.
//   - Writes an atomic manifest.json last so partial uploads never leave
//     the shard set in a torn state.

import { listPrefix, getObjectBuffer, putObject } from "./r2.mjs";
import { gunzipSync, gzipSync } from "node:zlib";
import { createHash } from "node:crypto";

const NUM_SHARDS = Number(process.env.HOT_NUM_SHARDS || 16);
const LOOKBACK_DAYS = Number(process.env.HOT_LOOKBACK_DAYS || 30);
const NOW = Date.now();
const CUTOFF = NOW - LOOKBACK_DAYS * 86400 * 1000;

function shardIdx(url) {
  const h = createHash("sha1").update(url).digest();
  return h.readUInt32BE(0) % NUM_SHARDS;
}

// Extract an ISO-ish timestamp from the filename so we can cheaply skip
// files older than LOOKBACK_DAYS without HEAD'ing each object.
// Formats seen in R2:
//   hot/certstream-2026-08-17T21-26-06-989Z.ndjson.gz
//   hot/gdelt-<ts>.ndjson.gz
function fileTimestamp(key) {
  const m = key.match(/(\d{4})-(\d{2})-(\d{2})T(\d{2})-(\d{2})-(\d{2})/);
  if (!m) return NOW; // if we can't parse, keep it (safer than dropping)
  const [, y, mo, d, h, mi, s] = m;
  return Date.parse(`${y}-${mo}-${d}T${h}:${mi}:${s}Z`);
}

const files = await listPrefix("hot/");
const recent = files.filter((f) => fileTimestamp(f.Key) >= CUTOFF);
console.error(`hot-reshard: ${files.length} total hot files, ${recent.length} within ${LOOKBACK_DAYS}d`);
if (!recent.length) {
  console.log("nothing to shard");
  process.exit(0);
}

// URL → shortest doc for that URL (first wins is fine for hot tier).
const byUrl = new Map();
let bytesRead = 0;
let linesSeen = 0;
let filesRead = 0;

for (const f of recent) {
  const rel = f.Key.replace(/^.*?discovery\//, "");
  let buf;
  try {
    buf = await getObjectBuffer(rel);
  } catch (e) {
    console.error(`  skip ${rel}: ${e.message || e}`);
    continue;
  }
  bytesRead += buf.length;
  filesRead++;
  let text;
  try { text = gunzipSync(buf).toString("utf8"); }
  catch (e) { console.error(`  gunzip ${rel} failed: ${e.message}`); continue; }
  for (const line of text.split("\n")) {
    if (!line) continue;
    linesSeen++;
    let doc;
    try { doc = JSON.parse(line); } catch { continue; }
    if (!doc || !doc.u) continue;
    if (!byUrl.has(doc.u)) byUrl.set(doc.u, doc);
  }
  if (filesRead % 100 === 0) {
    console.error(`  read ${filesRead}/${recent.length} files, ${byUrl.size} unique urls, ${(bytesRead/1e6).toFixed(1)} MB`);
  }
}

console.error(`hot-reshard: read ${filesRead} files, ${linesSeen} lines, ${byUrl.size} unique urls`);

// Partition into shards.
const shardDocs = Array.from({ length: NUM_SHARDS }, () => []);
for (const doc of byUrl.values()) {
  shardDocs[shardIdx(doc.u)].push(doc);
}

// Upload each shard. Empty shards get an empty NDJSON.gz so the client
// can still fetch a shard number without a 404.
const manifest = {
  schema: 1,
  generatedAt: new Date().toISOString(),
  lookbackDays: LOOKBACK_DAYS,
  numShards: NUM_SHARDS,
  totalDocs: byUrl.size,
  filesRead,
  linesSeen,
  bytesRead,
  shards: [],
};
for (let i = 0; i < NUM_SHARDS; i++) {
  const body = shardDocs[i].map((d) => JSON.stringify(d)).join("\n") + (shardDocs[i].length ? "\n" : "");
  const gz = gzipSync(Buffer.from(body));
  const name = `shard-${String(i).padStart(3, "0")}.ndjson.gz`;
  await putObject(`hot-shards/${name}`, gz, "application/x-ndjson", { ContentEncoding: "gzip" });
  manifest.shards.push({ shard: i, docs: shardDocs[i].length, bytes: gz.length, path: `hot-shards/${name}` });
  console.error(`  ↑ hot-shards/${name}: ${shardDocs[i].length} docs, ${(gz.length/1024).toFixed(1)} KB`);
}

// Manifest last — this is the freshness pointer.
await putObject(
  "hot-shards/manifest.json",
  Buffer.from(JSON.stringify(manifest, null, 2)),
  "application/json",
  { CacheControl: "public, max-age=60, must-revalidate" }
);

// Heartbeat summary for the Actions UI.
const pctOfDailyGlobal = ((byUrl.size / 30 / 250000) * 100).toFixed(2);
console.log(`::notice title=Hot shards::${byUrl.size} unique URLs across ${NUM_SHARDS} shards (last ${LOOKBACK_DAYS}d). ~${pctOfDailyGlobal}% of daily global new-domain rate.`);
if (process.env.GITHUB_STEP_SUMMARY) {
  const fs = await import("node:fs");
  fs.appendFileSync(
    process.env.GITHUB_STEP_SUMMARY,
    `### Hot shards rebuilt\n\n- **Unique URLs:** ${byUrl.size.toLocaleString()}\n- **Shards:** ${NUM_SHARDS}\n- **Lookback:** ${LOOKBACK_DAYS} days\n- **Files read:** ${filesRead}\n- **Total gz size:** ${(manifest.shards.reduce((a, s) => a + s.bytes, 0)/1024/1024).toFixed(2)} MB\n- **Manifest:** \`hot-shards/manifest.json\`\n`
  );
}

console.log(JSON.stringify({ hotShards: NUM_SHARDS, uniqueUrls: byUrl.size, filesRead }));
