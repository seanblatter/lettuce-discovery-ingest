// scripts/bloom-index.mjs (item 9)
// Builds one Bloom filter per shard so the client can skip 90%+ of shards
// on rare-term queries. Output: shards/bloom-{shardIdx}.bin (raw bits) +
// shards/bloom-manifest.json (params).
//
// Filter size: 8 KiB per shard × 128 shards = 1 MiB total client download.
// For ~8M docs/shard with 5 terms indexed per doc, false-positive rate ~2%.
//
// The client fetches bloom-manifest.json + all bloom-*.bin (parallelized),
// then for each query term computes hashes and only downloads shards whose
// filters say "maybe present".

import { listPrefix, getObjectStream, putObject } from "./r2.mjs";
import { createGunzip } from "node:zlib";
import { createHash } from "node:crypto";

const NUM_SHARDS = Number(process.env.NUM_SHARDS || 128);
const BITS_PER_SHARD = 8 * 1024 * 8; // 8 KiB → 65536 bits
const NUM_HASHES = 4;
const MIN_TERM_LEN = 3;
const MAX_TERMS_PER_DOC = 40;

function shardIdxOf(url) {
  const h = createHash("sha1").update(url).digest();
  return h.readUInt32BE(0) % NUM_SHARDS;
}

function tokenize(text) {
  return text.toLowerCase().match(/[a-z0-9]{3,}/g) || [];
}

function bloomAdd(bits, term) {
  const h = createHash("md5").update(term).digest();
  for (let i = 0; i < NUM_HASHES; i++) {
    const idx = h.readUInt32BE(i * 4) % BITS_PER_SHARD;
    bits[idx >> 3] |= 1 << (idx & 7);
  }
}

const filters = Array.from({ length: NUM_SHARDS }, () => Buffer.alloc(BITS_PER_SHARD / 8));

// Scan shards/*.gz (final resharded output) — one bloom per shard
const files = (await listPrefix("shards/")).filter(f => f.Key.endsWith(".ndjson.gz"));
console.log(`bloom: building ${files.length} filters`);

let idx = 0;
for (const f of files) {
  const m = f.Key.match(/shard-(\d+)/);
  if (!m) continue;
  const shard = Number(m[1]);
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
      try {
        const d = JSON.parse(line);
        const terms = new Set([...tokenize(d.t || ""), ...tokenize(d.d || ""), ...tokenize(d.c || "")].slice(0, MAX_TERMS_PER_DOC));
        for (const t of terms) bloomAdd(filters[shard], t);
      } catch {}
    }
  }
  if (++idx % 10 === 0) console.log(`  built ${idx}/${files.length}`);
}

// Upload each filter and the manifest
for (let i = 0; i < NUM_SHARDS; i++) {
  const name = `shards/bloom-${String(i).padStart(3, "0")}.bin`;
  await putObject(name, filters[i], "application/octet-stream");
}
const manifest = {
  version: 1,
  num_shards: NUM_SHARDS,
  bits_per_shard: BITS_PER_SHARD,
  num_hashes: NUM_HASHES,
  hash: "md5",
  updated_at: new Date().toISOString(),
};
await putObject("shards/bloom-manifest.json", JSON.stringify(manifest), "application/json");
console.log(`bloom: uploaded ${NUM_SHARDS} filters + manifest`);
