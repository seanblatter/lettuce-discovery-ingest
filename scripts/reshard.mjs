// scripts/reshard.mjs
// Merges all raw/*.ndjson.gz into 128 final shards under shards/shard-000..127.ndjson.{gz,br}
// Hash-partitions by URL so client can compute shard index from query.
//
// Design:
//   - Downloads CONCURRENCY raws in parallel (I/O bound, gunzip+parse in workers).
//   - Each shard is appended to a temp file on disk (bounded RAM).
//   - After all raws are streamed in, temp files are compressed and uploaded one by one.
import { listPrefix, getObjectStream, putObject } from "./r2.mjs";
import { createGunzip, gzipSync, brotliCompressSync } from "node:zlib";
import { createHash } from "node:crypto";
import { createWriteStream, readFileSync, mkdirSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const NUM_SHARDS = Number(process.env.NUM_SHARDS || 128);
const CONCURRENCY = Number(process.env.CONCURRENCY || 48);
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
const writeLocks = new Array(NUM_SHARDS).fill(null);
const counts = new Array(NUM_SHARDS).fill(0);

// Serialize writes per shard to avoid interleaved lines across concurrent workers.
async function writeLine(i, line) {
  const prev = writeLocks[i] || Promise.resolve();
  const next = prev.then(() => new Promise((resolve) => {
    counts[i]++;
    const ok = writers[i].write(line + "\n");
    if (ok) resolve();
    else writers[i].once("drain", resolve);
  }));
  writeLocks[i] = next.catch(() => {});
  return next;
}

const raws = await listPrefix("raw/");
console.error(`resharding ${raws.length} raw files -> ${NUM_SHARDS} shards (concurrency=${CONCURRENCY}, spill=${SPILL_DIR})`);

let idx = 0;
let totalLines = 0;
const t0 = Date.now();

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
          if (!doc || !doc.u) continue;
          await writeLine(shardIdx(doc.u), line);
          totalLines++;
        } catch {}
      }
    }
  } catch (e) {
    console.error(`  skip ${rel}: ${e.message || e}`);
  }
  const done = ++idx;
  if (done % 200 === 0 || done === raws.length) {
    const secs = (Date.now() - t0) / 1000;
    const rate = done / secs;
    const eta = ((raws.length - done) / rate).toFixed(0);
    console.error(`  merged ${done}/${raws.length}  lines=${totalLines}  rate=${rate.toFixed(1)}/s  eta=${eta}s`);
  }
}

// Simple worker pool.
async function runPool() {
  let cursor = 0;
  const workers = Array.from({ length: CONCURRENCY }, async () => {
    while (true) {
      const i = cursor++;
      if (i >= raws.length) return;
      await processOne(raws[i]);
    }
  });
  await Promise.all(workers);
}
await runPool();

await Promise.all(
  writers.map((w) => new Promise((resolve, reject) => {
    w.end((err) => (err ? reject(err) : resolve()));
  }))
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
