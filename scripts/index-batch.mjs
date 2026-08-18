// scripts/index-batch.mjs
// Batched inverted-index builder. Each run:
//   1. Consumes BATCH_SIZE raws from `raw/*.ndjson.gz` starting at cursor.
//   2. Tokenizes {title,desc,url,keywords} of each doc.
//   3. Groups postings by term-prefix bucket (first 2 chars of sha1 → 256 buckets).
//   4. Uploads `shards/inverted/batches/{batchId}/bucket-XX.postings.ndjson.gz`
//      — one line per term: {t: term, d: [[urlIdx, tf, fieldMask], ...], u: [urls]}
//   5. Advances cursor at `shards/inverted-cursor.json`.
//
// Query-time (api/discovery/search-r2.js):
//   For each query token → sha1 → bucket → GET all batches' bucket files →
//   union postings → intersect across tokens → score → merge with hot-tier.
//
// Storage: ~5x smaller than raw NDJSON. Grows ~2-5 GB per 10k raws indexed.
// Query cost: ~10-30 GETs per query (query-token-count × batch-count), each
// bucket file is <5 MB compressed → fast on a warm Vercel function.

import { listPrefix, getObjectStream, getObjectBuffer, putObject } from "./r2.mjs";
import { createGunzip, gzipSync } from "node:zlib";
import { createHash } from "node:crypto";

const BATCH_SIZE   = Number(process.env.INDEX_BATCH_SIZE || 200);
const CONCURRENCY  = Number(process.env.INDEX_CONCURRENCY || 16);
const NUM_BUCKETS  = 256;
const CURSOR_KEY   = "shards/inverted-cursor.json";
const MANIFEST_KEY = "shards/inverted/manifest.json";

const STOP = new Set([
  "the","a","an","and","or","but","of","in","on","at","to","for","from","by",
  "with","is","are","was","were","be","been","being","have","has","had","do",
  "does","did","will","would","should","could","may","might","must","can",
  "this","that","these","those","it","its","he","she","they","we","you","i",
  "as","if","not","no","yes","so","up","out","new","one","all","any",
]);

const FIELD_BITS = { t: 0b0001, d: 0b0010, u: 0b0100, k: 0b1000 };

function tokenize(s) {
  if (!s) return [];
  const out = [];
  const re = /[a-z0-9]{2,32}/g;
  let m;
  const lower = String(s).toLowerCase();
  while ((m = re.exec(lower)) !== null) {
    const w = m[0];
    if (!STOP.has(w)) out.push(w);
  }
  return out;
}

function bucketOf(term) {
  const h = createHash("sha1").update(term).digest();
  return h[0]; // 0..255
}

// ---- Cursor ----
let cursor = { version: 1, next: 0, totalRaws: 0, processedRaws: 0, batches: [], updatedAt: null };
try {
  const buf = await getObjectBuffer(CURSOR_KEY);
  cursor = { ...cursor, ...JSON.parse(buf.toString()) };
  console.error(`index cursor: next=${cursor.next}, ${cursor.batches.length} prior batches`);
} catch (_) {
  console.error("no index cursor — starting fresh");
}

// ---- Pick raws ----
const raws = await listPrefix("raw/");
raws.sort((a, b) => a.Key.localeCompare(b.Key));
cursor.totalRaws = raws.length;
const start = Math.min(cursor.next, raws.length);
const end   = Math.min(start + BATCH_SIZE, raws.length);
const batchRaws = raws.slice(start, end);
const batchId = String(start).padStart(6, "0");

if (!batchRaws.length) {
  console.log(`::notice title=Index::All raws indexed — nothing to do (cursor=${cursor.next}/${raws.length})`);
  process.exit(0);
}

console.error(`index batch ${batchId}: raws[${start}..${end}] of ${raws.length}, conc=${CONCURRENCY}`);

// ---- Fan out downloads + tokenize ----
// Postings per bucket: bucket -> Map(term -> Map(url -> {tf, mask, doc}))
const buckets = Array.from({ length: NUM_BUCKETS }, () => new Map());
let docsIndexed = 0;
let uniqueTerms = 0;

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
          docsIndexed++;
          const fields = [
            [FIELD_BITS.t, doc.t || ""],
            [FIELD_BITS.d, doc.d || ""],
            [FIELD_BITS.u, doc.u.replace(/[^a-z0-9]+/gi, " ")],
            [FIELD_BITS.k, doc.k || ""],
          ];
          const perTerm = new Map(); // term -> {tf, mask}
          for (const [mask, text] of fields) {
            for (const tok of tokenize(text)) {
              const cur = perTerm.get(tok);
              if (cur) { cur.tf++; cur.mask |= mask; }
              else perTerm.set(tok, { tf: 1, mask });
            }
          }
          for (const [term, { tf, mask }] of perTerm) {
            const b = bucketOf(term);
            let termMap = buckets[b].get(term);
            if (!termMap) { termMap = new Map(); buckets[b].set(term, termMap); uniqueTerms++; }
            termMap.set(doc.u, { tf, mask, t: doc.t || "", d: doc.d || "" });
          }
        } catch (_) {}
      }
    }
  } catch (e) {
    console.error(`  skip ${rel}: ${e.message || e}`);
  }
}

// Simple worker pool
let idx = 0;
const t0 = Date.now();
async function runPool() {
  let c = 0;
  await Promise.all(Array.from({ length: CONCURRENCY }, async () => {
    while (true) {
      const i = c++;
      if (i >= batchRaws.length) return;
      await processOne(batchRaws[i]);
      const done = ++idx;
      if (done % 25 === 0 || done === batchRaws.length) {
        const secs = (Date.now() - t0) / 1000;
        console.error(`  merged ${done}/${batchRaws.length}  docs=${docsIndexed}  terms=${uniqueTerms}  ${(done/secs).toFixed(1)}/s`);
      }
    }
  }));
}
await runPool();

console.error(`index build: ${docsIndexed} docs, ${uniqueTerms} unique terms, uploading ${NUM_BUCKETS} buckets`);

// ---- Emit bucket files ----
const bucketStats = [];
for (let b = 0; b < NUM_BUCKETS; b++) {
  const termMap = buckets[b];
  if (!termMap.size) continue;
  const lines = [];
  let postings = 0;
  for (const [term, docsMap] of termMap) {
    const docs = [];
    for (const [url, { tf, mask, t, d }] of docsMap) {
      docs.push([url, tf, mask, t.slice(0, 200), d.slice(0, 300)]);
      postings++;
    }
    lines.push(JSON.stringify({ t: term, d: docs }));
  }
  const body = lines.join("\n") + "\n";
  const gz = gzipSync(Buffer.from(body));
  const name = `bucket-${b.toString(16).padStart(2, "0")}.postings.ndjson.gz`;
  const path = `shards/inverted/batches/${batchId}/${name}`;
  await putObject(path, gz, "application/x-ndjson", { ContentEncoding: "gzip" });
  bucketStats.push({ bucket: b, terms: termMap.size, postings, bytes: gz.length, path });
}

// ---- Cursor + manifest ----
cursor.batches.push({ batchId, startIdx: start, endIdx: end, docs: docsIndexed, terms: uniqueTerms, generatedAt: new Date().toISOString(), buckets: bucketStats.length });
cursor.next = end;
cursor.processedRaws = end;
cursor.updatedAt = new Date().toISOString();
await putObject(CURSOR_KEY, Buffer.from(JSON.stringify(cursor, null, 2)), "application/json", { CacheControl: "public, max-age=60" });

const manifest = {
  schema: 1,
  generatedAt: cursor.updatedAt,
  numBuckets: NUM_BUCKETS,
  totalBatches: cursor.batches.length,
  processedRaws: cursor.processedRaws,
  totalRaws: cursor.totalRaws,
  batches: cursor.batches.map((b) => ({ batchId: b.batchId, docs: b.docs, terms: b.terms })),
};
await putObject(MANIFEST_KEY, Buffer.from(JSON.stringify(manifest, null, 2)), "application/json", { CacheControl: "public, max-age=60" });

const pctSwept = ((end / raws.length) * 100).toFixed(2);
console.log(`::notice title=Index::Batch ${batchId} — ${docsIndexed.toLocaleString()} docs, ${uniqueTerms.toLocaleString()} terms across ${bucketStats.length} buckets. Sweep ${pctSwept}%.`);
if (process.env.GITHUB_STEP_SUMMARY) {
  const fs = await import("node:fs");
  fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY,
    `### Inverted-index batch ${batchId}\n\n- **Docs indexed:** ${docsIndexed.toLocaleString()}\n- **Unique terms this batch:** ${uniqueTerms.toLocaleString()}\n- **Buckets written:** ${bucketStats.length}/${NUM_BUCKETS}\n- **Raws consumed:** ${start}..${end} of ${raws.length}\n- **Sweep progress:** ${pctSwept}%\n- **Manifest:** \`${MANIFEST_KEY}\`\n`);
}
console.log(JSON.stringify({ batchId, docsIndexed, uniqueTerms, buckets: bucketStats.length }));
