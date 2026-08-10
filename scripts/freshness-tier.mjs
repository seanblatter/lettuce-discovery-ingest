// scripts/freshness-tier.mjs (item 1)
// Merges hot/*.ndjson.gz into a small "hot index" shard set updated every 15 min.
// Client fetches hot shards first (small, fast), falls back to deep shards (big, slow).
// This is the freshness/staleness split.

import { listPrefix, getObjectStream, putObject } from "./r2.mjs";
import { createGunzip, gzipSync, brotliCompressSync } from "node:zlib";
import { createHash } from "node:crypto";

const HOT_SHARDS = Number(process.env.HOT_SHARDS || 8);

function shardIdx(url) {
  const h = createHash("sha1").update(url).digest();
  return h.readUInt32BE(0) % HOT_SHARDS;
}

const files = await listPrefix("hot/");
const buckets = Array.from({ length: HOT_SHARDS }, () => new Map()); // url -> line (dedup)
console.log(`hot merge: ${files.length} raw hot files`);

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
      try { const d = JSON.parse(line); buckets[shardIdx(d.u)].set(d.u, line); } catch {}
    }
  }
}

for (let i = 0; i < HOT_SHARDS; i++) {
  const body = [...buckets[i].values()].join("\n") + "\n";
  const raw = Buffer.from(body);
  const name = `hot-${String(i).padStart(2, "0")}.ndjson`;
  await putObject(`shards-hot/${name}.gz`, gzipSync(raw), "application/x-ndjson", { ContentEncoding: "gzip" });
  await putObject(`shards-hot/${name}.br`, brotliCompressSync(raw), "application/x-ndjson", { ContentEncoding: "br" });
  console.log(`hot shard ${i}: ${buckets[i].size} docs, ${raw.length} bytes`);
}

// Prune source hot/*.gz older than 7 days to keep hot storage tiny
const cutoff = Date.now() - 7 * 86400 * 1000;
const { deleteObject } = await import("./r2.mjs");
for (const f of files) {
  const m = f.Key.match(/-(\d+)\.ndjson\.gz$/);
  if (m && Number(m[1]) < cutoff) {
    const rel = f.Key.split("discovery/").pop();
    await deleteObject(rel);
  }
}
