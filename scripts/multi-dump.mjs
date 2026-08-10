// scripts/multi-dump.mjs (item 2)
// Builds warcs.txt from the last N Common Crawl monthly dumps, deduplicating by
// segment filename so we get 4× freshness without 4× storage.
import { putObject } from "./r2.mjs";
import { gunzipSync } from "node:zlib";

const DUMPS = (process.env.CC_DUMPS || "CC-MAIN-2026-30,CC-MAIN-2026-26,CC-MAIN-2026-22,CC-MAIN-2026-18").split(",");

async function fetchDump(id) {
  const url = `https://data.commoncrawl.org/crawl-data/${id}/warc.paths.gz`;
  console.log(`  ${url}`);
  const r = await fetch(url);
  if (!r.ok) return [];
  const raw = gunzipSync(Buffer.from(await r.arrayBuffer())).toString();
  return raw.split("\n").filter(Boolean);
}

const seen = new Set();
const all = [];
for (const d of DUMPS) {
  console.log(`fetching ${d}`);
  const paths = await fetchDump(d);
  for (const p of paths) {
    const m = p.match(/([^/]+)\.warc\.gz$/);
    const key = m ? m[1] : p;
    if (seen.has(key)) continue;
    seen.add(key);
    all.push(p);
  }
}
// deterministic shuffle
let x = 1337;
for (let i = all.length - 1; i > 0; i--) {
  x = (x * 1103515245 + 12345) & 0x7fffffff;
  const j = Math.floor((x / 0x7fffffff) * (i + 1));
  [all[i], all[j]] = [all[j], all[i]];
}
await putObject("ingest/warcs.txt", all.join("\n") + "\n", "text/plain");
console.log(`multi-dump WARC list: ${all.length} unique across ${DUMPS.length} dumps`);
