// scripts/pagerank.mjs (item 5)
// Two-pass domain-level PageRank over outlinks extracted from raw/ ndjson.
// Writes ranks/domains.json to R2: { host: rank_0_to_1 }.
// Client can multiply BM25 by (1 + rank) for a big quality boost.

import { listPrefix, getObjectStream, putObject } from "./r2.mjs";
import { createGunzip } from "node:zlib";

// PASS 1: build adjacency: host -> Set of linked hosts
const links = new Map(); // host -> Map(dst -> count)
const seenDocs = new Set();

const files = (await listPrefix("raw/")).slice(0, 200); // sample cap
console.log(`pagerank: scanning ${files.length} shards`);
let scanned = 0;

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
      try {
        const d = JSON.parse(line);
        const src = new URL(d.u).host;
        // NOTE: raw docs currently don't have outlinks. Extract from text bodies
        // where anchor URLs snuck through. Best-effort.
        const re = /https?:\/\/([a-z0-9.-]+)/gi;
        let m; const dsts = new Map();
        while ((m = re.exec(d.c)) && dsts.size < 20) {
          if (m[1] !== src) dsts.set(m[1], 1);
        }
        if (!links.has(src)) links.set(src, new Map());
        const bucket = links.get(src);
        for (const dst of dsts.keys()) bucket.set(dst, (bucket.get(dst) || 0) + 1);
      } catch {}
    }
  }
  if (++scanned % 25 === 0) console.log(`  scanned ${scanned}/${files.length}`);
}

// PASS 2: iterative PageRank
const hosts = new Set();
for (const [src, dsts] of links) { hosts.add(src); for (const d of dsts.keys()) hosts.add(d); }
const N = hosts.size;
console.log(`pagerank: ${N} hosts, ${links.size} src hosts`);

let rank = new Map(); for (const h of hosts) rank.set(h, 1 / N);
const damping = 0.85;

for (let iter = 0; iter < 20; iter++) {
  const next = new Map();
  for (const h of hosts) next.set(h, (1 - damping) / N);
  for (const [src, dsts] of links) {
    const share = damping * (rank.get(src) || 0) / dsts.size;
    for (const dst of dsts.keys()) next.set(dst, (next.get(dst) || 0) + share);
  }
  rank = next;
}

// Normalize to 0..1 and clip top 5% as spam-ish (extreme link farms)
const values = [...rank.values()].sort((a, b) => b - a);
const p95 = values[Math.floor(values.length * 0.05)];
const min = 0, max = p95;
const out = {};
for (const [h, r] of rank) out[h] = Math.min(1, Math.max(0, (r - min) / (max - min)));

await putObject("ranks/domains.json", JSON.stringify(out), "application/json");
console.log(`pagerank: wrote ${Object.keys(out).length} domain ranks`);
