// scripts/reshard.mjs
// Merges all raw/*.ndjson.gz into 128 final shards under shards/shard-000..127.ndjson.{gz,br}
// Hash-partitions by URL so client can compute shard index from query.
import { listPrefix, getObjectStream, putObject } from "./r2.mjs";
import { createGunzip, gzipSync, brotliCompressSync } from "node:zlib";
import { createHash } from "node:crypto";

const NUM_SHARDS = Number(process.env.NUM_SHARDS || 128);

function shardIdx(url) {
  const h = createHash("sha1").update(url).digest();
  return h.readUInt32BE(0) % NUM_SHARDS;
}

const buckets = Array.from({ length: NUM_SHARDS }, () => []);
const raws = await listPrefix("raw/");
console.error(`resharding ${raws.length} raw files -> ${NUM_SHARDS} shards`);

let idx = 0;
for (const o of raws) {
  const rel = o.Key.replace(/^.*?discovery\//, "");
  const stream = await getObjectStream(rel);
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
        buckets[shardIdx(doc.u)].push(line);
      } catch {}
    }
  }
  if (++idx % 50 === 0) console.error(`  merged ${idx}/${raws.length}`);
}

for (let i = 0; i < NUM_SHARDS; i++) {
  const body = buckets[i].join("\n") + "\n";
  const buf = Buffer.from(body);
  const name = `shard-${String(i).padStart(3, "0")}.ndjson`;
  await putObject(`shards/${name}.gz`, gzipSync(buf), "application/x-ndjson", { ContentEncoding: "gzip" });
  await putObject(`shards/${name}.br`, brotliCompressSync(buf), "application/x-ndjson", { ContentEncoding: "br" });
  console.error(`shard ${i}: ${buckets[i].length} docs`);
}
console.log(JSON.stringify({ shards: NUM_SHARDS, total_docs: buckets.reduce((s, b) => s + b.length, 0) }));
