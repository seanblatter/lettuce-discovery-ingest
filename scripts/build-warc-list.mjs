// scripts/build-warc-list.mjs
// One-time job: downloads the latest Common Crawl WARC path index and uploads
// the shuffled list to R2 at discovery/ingest/warcs.txt. Idempotent — safe to re-run.
import { putObject, headExists, getObjectBuffer } from "./r2.mjs";

const CC_INDEX_URL = process.env.CC_INDEX_URL
  || "https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-30/warc.paths.gz";

const KEY = "ingest/warcs.txt";

async function main() {
  if (await headExists(KEY) && !process.env.FORCE_REBUILD) {
    const buf = await getObjectBuffer(KEY);
    const n = buf.toString().split("\n").filter(Boolean).length;
    console.log(`WARC list already exists (${n} entries). Set FORCE_REBUILD=1 to overwrite.`);
    return;
  }

  console.log(`Downloading ${CC_INDEX_URL}`);
  const res = await fetch(CC_INDEX_URL);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const gz = Buffer.from(await res.arrayBuffer());
  const { gunzipSync } = await import("node:zlib");
  const raw = gunzipSync(gz).toString();

  const paths = raw.split("\n").map(s => s.trim()).filter(Boolean);
  // Shuffle deterministically so parallel workers touch different data centers
  const seed = 1337;
  let x = seed;
  const rand = () => (x = (x * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff;
  for (let i = paths.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    [paths[i], paths[j]] = [paths[j], paths[i]];
  }

  const body = paths.join("\n") + "\n";
  await putObject(KEY, body, "text/plain");
  console.log(`Uploaded ${paths.length} WARC paths to ${KEY}`);
}

main().catch(e => { console.error(e); process.exit(1); });
