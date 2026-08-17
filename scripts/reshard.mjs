// scripts/reshard.mjs
// Batched reshard: each run processes BATCH_SIZE raw NDJSON files from
// `raw/*.ndjson.gz`, hash-partitions their URLs into NUM_SHARDS shards,
// and uploads them to `shards/batches/{startIdx}/shard-000..127.ndjson.gz`.
//
// A cursor at `shards/reshard-cursor.json` tracks which raws have been
// consumed. Runs are safe to interrupt and resume — successive runs pick
// up from wherever the last cursor pointed. A single sweep of the current
// ~99k raws takes ~20 days at BATCH_SIZE=200 + hourly cron. Bump either
// knob (or the workflow matrix) to speed up.
//
// This is the "safe + quick" replacement for the previous 340-minute
// single-shot design that has been OOM'ing / running out of disk since
// Aug 14. Disk usage per run is bounded by BATCH_SIZE × avg-raw-ungzipped
// (~50 MB) which stays comfortably inside a runner's 14 GB scratch space.

import { listPrefix, getObjectStream, getObjectBuffer, putObject } from "./r2.mjs";
import { createGunzip, gzipSync, brotliCompressSync } from "node:zlib";
import { createHash } from "node:crypto";
import { createWriteStream, readFileSync, mkdirSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const NUM_SHARDS = Number(process.env.NUM_SHARDS || 128);
const CONCURRENCY = Number(process.env.CONCURRENCY || 24);
const BATCH_SIZE = Number(process.env.BATCH_SIZE || 200);
const CURSOR_KEY = "shards/reshard-cursor.json";
const MANIFEST_KEY = "shards/manifest.json";
const SPILL_DIR = join(tmpdir(), `reshard-${process.pid}`);
mkdirSync(SPILL_DIR, { recursive: true });

function shardIdx(url) {
  const h = createHash("sha1").update(url).digest();
  return h.readUInt32BE(0) % NUM_SHARDS;
}

// ---- Load cursor (optional on first run) ------------------------------
let cursor = { version: 1, next: 0, totalRaws: 0, processedRaws: 0, totalDocs: 0, batches: [], updatedAt: null };
try {
  const buf = await getObjectBuffer(CURSOR_KEY);
  const parsed = JSON.parse(buf.toString());
  cursor = { ...cursor, ...parsed };
  console.error(`cursor: next=${cursor.next}, ${cursor.batches.length} prior batches, ${cursor.totalDocs} docs so far`);
} catch (e) {
  console.error(`no cursor found at ${CURSOR_KEY} — starting fresh`);
}

// ---- Pick this batch's raws -------------------------------------------
const raws = await listPrefix("raw/");
// Deterministic sort so cursor-based slicing is reproducible across runs
// even as the raw/ listing grows.
raws.sort((a, b) => a.Key.localeCompare(b.Key));
cursor.totalRaws = raws.length;

const start = Math.min(cursor.next, raws.length);
const end = Math.min(start + BATCH_SIZE, raws.length);
const batchRaws = raws.slice(start, end);
const batchId = String(start).padStart(6, "0");
console.error(`reshard batch ${batchId}: raws[${start}..${end}] of ${raws.length}, concurrency=${CONCURRENCY}`);

if (!batchRaws.length) {
  console.log("::notice title=Reshard::All raws consumed — nothing to do.");
  console.log(JSON.stringify({ batchId, done: true, totalRaws: raws.length, cursor: cursor.next }));
  process.exit(0);
}

// ---- Fan out downloads → per-shard tmpfile appenders ------------------
const writers = Array.from({ length: NUM_SHARDS }, (_, i) =>
  createWriteStream(join(SPILL_DIR, `shard-${String(i).padStart(3, "0")}.ndjson`), {
    flags: "a",
    highWaterMark: 1 << 20,
  })
);
const writeLocks = new Array(NUM_SHARDS).fill(null);
const counts = new Array(NUM_SHARDS).fill(0);

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
  if (done % 50 === 0 || done === batchRaws.length) {
    const secs = (Date.now() - t0) / 1000;
    const rate = done / secs;
    const eta = ((batchRaws.length - done) / Math.max(rate, 0.01)).toFixed(0);
    console.error(`  merged ${done}/${batchRaws.length}  lines=${totalLines}  rate=${rate.toFixed(1)}/s  eta=${eta}s`);
  }
}

async function runPool() {
  let c = 0;
  const workers = Array.from({ length: CONCURRENCY }, async () => {
    while (true) {
      const i = c++;
      if (i >= batchRaws.length) return;
      await processOne(batchRaws[i]);
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

console.error(`spill complete: ${totalLines} lines; compressing + uploading batch ${batchId}`);

// ---- Upload this batch's shards ---------------------------------------
const batchInfo = { batchId, startIdx: start, endIdx: end, docs: totalLines, shards: [], generatedAt: new Date().toISOString() };
for (let i = 0; i < NUM_SHARDS; i++) {
  const path = join(SPILL_DIR, `shard-${String(i).padStart(3, "0")}.ndjson`);
  let size = 0;
  try { size = statSync(path).size; } catch {}
  if (!size) continue;
  const body = readFileSync(path);
  const name = `shard-${String(i).padStart(3, "0")}.ndjson`;
  const gzKey = `shards/batches/${batchId}/${name}.gz`;
  const brKey = `shards/batches/${batchId}/${name}.br`;
  await putObject(gzKey, gzipSync(body), "application/x-ndjson", { ContentEncoding: "gzip" });
  await putObject(brKey, brotliCompressSync(body), "application/x-ndjson", { ContentEncoding: "br" });
  batchInfo.shards.push({ shard: i, docs: counts[i], rawBytes: size, gzKey });
  try { rmSync(path); } catch {}
}
try { rmSync(SPILL_DIR, { recursive: true, force: true }); } catch {}

// ---- Advance cursor + write manifest ----------------------------------
cursor.batches.push(batchInfo);
cursor.next = end;
cursor.processedRaws = end;
cursor.totalDocs += totalLines;
cursor.updatedAt = new Date().toISOString();
await putObject(CURSOR_KEY, Buffer.from(JSON.stringify(cursor, null, 2)), "application/json", {
  CacheControl: "public, max-age=60, must-revalidate",
});

// Compact per-shard manifest for query clients: which batches cover each shard.
const manifest = {
  schema: 1,
  generatedAt: cursor.updatedAt,
  numShards: NUM_SHARDS,
  totalDocs: cursor.totalDocs,
  totalBatches: cursor.batches.length,
  processedRaws: cursor.processedRaws,
  totalRaws: cursor.totalRaws,
  shards: Array.from({ length: NUM_SHARDS }, (_, i) => ({
    shard: i,
    batches: cursor.batches
      .flatMap((b) => b.shards.filter((s) => s.shard === i).map((s) => ({ batchId: b.batchId, docs: s.docs, path: s.gzKey }))),
  })),
};
await putObject(MANIFEST_KEY, Buffer.from(JSON.stringify(manifest, null, 2)), "application/json", {
  CacheControl: "public, max-age=60, must-revalidate",
});

// ---- Heartbeat summary ------------------------------------------------
const pctSwept = ((end / raws.length) * 100).toFixed(2);
console.log(`::notice title=Reshard::Batch ${batchId} — ${totalLines.toLocaleString()} docs. Cursor: ${end}/${raws.length} (${pctSwept}%) raws swept, ${cursor.totalDocs.toLocaleString()} total docs indexed.`);
if (process.env.GITHUB_STEP_SUMMARY) {
  const fs = await import("node:fs");
  fs.appendFileSync(
    process.env.GITHUB_STEP_SUMMARY,
    `### Reshard batch ${batchId}\n\n- **Docs added this run:** ${totalLines.toLocaleString()}\n- **Raws consumed:** ${batchRaws.length} (${start}..${end} of ${raws.length})\n- **Sweep progress:** ${pctSwept}%\n- **Cumulative docs:** ${cursor.totalDocs.toLocaleString()} across ${cursor.batches.length} batches\n- **Manifest:** \`${MANIFEST_KEY}\`\n- **Cursor:** \`${CURSOR_KEY}\`\n`
  );
}

console.log(JSON.stringify({ batchId, docsAdded: totalLines, cursor: cursor.next, totalDocs: cursor.totalDocs }));
