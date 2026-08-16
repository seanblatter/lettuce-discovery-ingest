// scripts/delta-shards.mjs (item 10)
// After each reshard, computes diff vs previous version and writes
// tiny delta files. Client with an old base only downloads deltas.
//
// Strategy: store a manifest per shard version listing doc-URL hashes.
// Delta = { added: [line, ...], removed_hashes: [hash, ...] }.

import { listPrefix, getObjectBuffer, getObjectStream, putObject, headExists } from "./r2.mjs";
import { createHash } from "node:crypto";
import { createGunzip, gzipSync } from "node:zlib";

const NUM_SHARDS = Number(process.env.NUM_SHARDS || 128);

function urlHash(u) {
  return createHash("sha1").update(u).digest().readUInt32BE(0).toString(36);
}

async function readShardDocs(rel) {
  const s = await getObjectStream(rel);
  const gz = createGunzip(); s.pipe(gz);
  const docs = new Map();
  let buf = "";
  for await (const ch of gz) {
    buf += ch.toString();
    let nl;
    while ((nl = buf.indexOf("\n")) >= 0) {
      const line = buf.slice(0, nl); buf = buf.slice(nl + 1);
      if (!line) continue;
      try { const d = JSON.parse(line); docs.set(urlHash(d.u), line); } catch {}
    }
  }
  return docs;
}

const version = new Date().toISOString().slice(0, 10);
const prevManifestRel = "shards/deltas/latest.json";
let prevManifest = null;
if (await headExists(prevManifestRel)) {
  try { prevManifest = JSON.parse((await getObjectBuffer(prevManifestRel)).toString()); } catch {}
}

const manifest = { version, shards: {} };

let skipped = 0;
for (let i = 0; i < NUM_SHARDS; i++) {
  const name = `shard-${String(i).padStart(3, "0")}.ndjson`;
  const shardRel = `shards/${name}.gz`;

  // Skip shards that don't exist yet. Reshard is a long-running job and
  // may not have produced all 128 shards on the first pass; we don't want
  // heartbeat cron to fail red every 15 minutes just because reshard hasn't
  // finished. Once reshard populates the shard, the next delta run picks
  // it up and emits an "added: everything" delta.
  if (!(await headExists(shardRel))) {
    skipped++;
    manifest.shards[i] = { version: null, docs: 0, added: 0, removed: 0, delta_bytes: 0, missing: true };
    continue;
  }

  const cur = await readShardDocs(shardRel);

  let prev = new Map();
  if (prevManifest?.shards?.[i]?.version) {
    const prevRel = `shards/versions/${prevManifest.shards[i].version}/${name}.gz`;
    if (await headExists(prevRel)) prev = await readShardDocs(prevRel);
  }

  const added = [];
  for (const [h, line] of cur) if (!prev.has(h)) added.push(line);
  const removed = [];
  for (const h of prev.keys()) if (!cur.has(h)) removed.push(h);

  const delta = { v: version, base: prevManifest?.shards?.[i]?.version || null, added, removed };
  const deltaBody = gzipSync(Buffer.from(JSON.stringify(delta)));
  const deltaKey = `shards/deltas/${version}/shard-${String(i).padStart(3, "0")}.delta.json.gz`;
  await putObject(deltaKey, deltaBody, "application/json", { ContentEncoding: "gzip" });

  // Also snapshot the full shard under versioned path so future deltas can diff it
  const stream = await getObjectStream(`shards/${name}.gz`);
  const chunks = []; for await (const c of stream) chunks.push(c);
  await putObject(`shards/versions/${version}/${name}.gz`, Buffer.concat(chunks), "application/x-ndjson", { ContentEncoding: "gzip" });

  manifest.shards[i] = { version, docs: cur.size, added: added.length, removed: removed.length, delta_bytes: deltaBody.length };
  if (i % 16 === 0) console.log(`delta ${i}/${NUM_SHARDS}: +${added.length} -${removed.length} = ${deltaBody.length}B`);
}

await putObject(prevManifestRel, JSON.stringify(manifest, null, 2), "application/json");
await putObject(`shards/deltas/${version}/manifest.json`, JSON.stringify(manifest, null, 2), "application/json");
console.log(`delta: wrote version ${version} (skipped ${skipped}/${NUM_SHARDS} missing shards)`);
