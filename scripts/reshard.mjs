// scripts/reshard.mjs
// Merges all raw/*.ndjson.gz into 128 final shards under shards/shard-000..127.ndjson.{gz,br}
// Hash-partitions by URL so client can compute shard index from query.
//
// Streaming design (memory-safe):
//   - Never buffer full shards in RAM.
//   - Each shard is appended line-by-line to a temp file on disk.
//   - After all raws are streamed in, each temp file is read once, gzip+brotli
//     compressed, uploaded to R2, then deleted.
//   - Peak RAM is bounded by a single raw's gunzip buffer plus one shard's
//     compressed output, regardless of total corpus size.
import { listPrefix, getObjectStream, putObject } from "./r2.mjs";
import { createGunzip, gzipSync, brotliCompressSync } from "node:zlib";
import { createHash } from "node:crypto";
import { createWriteStream, readFileSync, mkdirSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const NUM_SHARDS = Number(process.env.NUM_SHARDS || 128);
const SPILL_DIR = join(tmpdir(), `reshard-${process.pid}`);
mkdirSync(SPILL_DIR, { recursive: true });

function shardIdx(url) {
  const h = createHash("sha1").update(url).digest();
  return h.readUInt32BE(0) % NUM_SHARDS;
}

const writers = Array.from({ length: NUM_SHARDS }, (_, i) =>
  createWriteStream(join(SPILL_DIR, `shard-${String(i).padStart(3, "0")}.ndjson`), {
    flags: "a",
    highWaterMark: 1 << 20,
  })
);
const counts = new Array(NUM_SHARDS).fill(0);

function writeLine(i, line) {
  counts[i]++;
  const ok = writers[i].write(line + "\n");
  if (!ok) return new Promise((resolve) => writers[i].once("drain", resolve));
}

const raws = await listPrefix("raw/");
console.error(`resharding ${raws.length} raw files -> ${NUM_SHARDS} shards (spill=${SPILL_DIR})`);

let idx = 0;
let totalLines = 0;
for (const o of raws) {
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
          if (!doc || !doc.u) continue;
          const p = writeLine(shardIdx(doc.u), line);
          if (p) await p;
          totalLines++;
        } catch {}
      }
    }
  } catch (e) {
    console.error(`  skip ${rel}: ${e.message || e}`);
  }
  if (++idx % 100 === 0) {
    console.error(`  merged ${idx}/${raws.length}  (lines=${totalLines})`);
  }
}

await Promise.all(
  writers.map(
    (w) =>
      new Promise((resolve, reject) => {
        w.end((err) => (err ? reject(err) : resolve()));
      })
  )
);

console.error(`spill complete: ${totalLines} lines across ${NUM_SHARDS} shards; compressing + uploading...`);

let uploadedDocs = 0;
for (let i = 0; i < NUM_SHARDS; i++) {
  const path = join(SPILL_DIR, `shard-${String(i).padStart(3, "0")}.ndjson`);
  let size = 0;
  try { size = statSync(path).size; } catch {}
  if (!size) {
    console.error(`shard ${i}: 0 docs (skipped)`);
    continue;
  }
  const body = readFileSync(path);
  const name = `shard-${String(i).padStart(3, "0")}.ndjson`;
  await putObject(`shards/${name}.gz`, gzipSync(body), "application/x-ndjson", { ContentEncoding: "gzip" });
  await putObject(`shards/${name}.br`, brotliCompressSync(body), "application/x-ndjson", { ContentEncoding: "br" });
  uploadedDocs += counts[i];
  console.error(`shard ${i}: ${counts[i]} docs (${size} bytes raw)`);
  try { rmSync(path); } catch {}
}

try { rmSync(SPILL_DIR, { recursive: true, force: true }); } catch {}

console.log(JSON.stringify({ shards: NUM_SHARDS, total_docs: uploadedDocs }));
