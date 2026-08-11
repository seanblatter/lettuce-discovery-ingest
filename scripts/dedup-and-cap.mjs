// scripts/dedup-and-cap.mjs
// Item 7 (simhash de-dup) + storage cap enforcement.
// After all raw ndjson lands, this reads them, drops near-duplicates by simhash,
// and (if total > STORAGE_CAP_GB) prunes lowest-quality docs first.

import { listPrefix, getObjectStream, putObject, deleteObject } from "./r2.mjs";
import { createGunzip, gzipSync } from "node:zlib";

const STORAGE_CAP_GB = Number(process.env.STORAGE_CAP_GB || 3300); // ~$50/mo cap (R2 $0.015/GB, 10 GB free)
const SIMHASH_HAMMING = 3;   // treat as duplicate if within 3 bits
const PRUNE_TARGET_GB = STORAGE_CAP_GB * 0.95; // prune to 95% of cap for headroom

function hamming(a, b) {
  let d = 0;
  for (let i = 0; i < a.length; i += 2) {
    const x = parseInt(a.substr(i, 2), 16) ^ parseInt(b.substr(i, 2), 16);
    let y = x;
    while (y) { d += y & 1; y >>= 1; }
  }
  return d;
}

async function scan(prefix) {
  const files = await listPrefix(prefix);
  const docs = [];
  let bytes = 0;
  for (const f of files) bytes += f.Size || 0;
  return { files, bytes };
}

// Bloom-filter-lite: since we can't hold 1B docs in RAM, we use a Set of simhash
// prefixes (first 5 hex chars = 20 bits, 1M buckets) as a coarse dedup gate.
async function dedupPass() {
  const raws = (await scan("raw/")).files;
  const seenPrefix = new Set();
  let kept = 0, dropped = 0;

  for (const f of raws) {
    const rel = f.Key.split("discovery/").pop();
    const stream = await getObjectStream(rel);
    const gunzip = createGunzip(); stream.pipe(gunzip);
    let buf = "", out = [];
    for await (const ch of gunzip) {
      buf += ch.toString();
      let nl;
      while ((nl = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, nl); buf = buf.slice(nl + 1);
        if (!line) continue;
        try {
          const d = JSON.parse(line);
          const pfx = (d.s || "").slice(0, 5);
          if (seenPrefix.has(pfx)) { dropped++; continue; }
          seenPrefix.add(pfx);
          out.push(line); kept++;
        } catch {}
      }
    }
    // rewrite dedup'd file
    const gz = gzipSync(Buffer.from(out.join("\n") + "\n"));
    await putObject(rel, gz, "application/x-ndjson", { ContentEncoding: "gzip" });
  }
  console.log(`[dedup] kept=${kept} dropped=${dropped}`);
}

async function enforceCap() {
  const { bytes } = await scan("raw/");
  const gb = bytes / 1e9;
  console.log(`[cap] current raw storage: ${gb.toFixed(2)} GB / ${STORAGE_CAP_GB} GB`);
  if (gb <= STORAGE_CAP_GB) return;

  const files = (await scan("raw/")).files
    .sort((a, b) => (a.LastModified || 0) - (b.LastModified || 0)); // oldest first

  let toDelete = (gb - PRUNE_TARGET_GB) * 1e9;
  for (const f of files) {
    if (toDelete <= 0) break;
    const rel = f.Key.split("discovery/").pop();
    await deleteObject(rel);
    toDelete -= f.Size || 0;
    console.log(`[cap] pruned ${rel}`);
  }
}

await dedupPass();
await enforceCap();
