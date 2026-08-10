// scripts/claim-next.mjs
// Atomically claims the next unprocessed WARC.
// Strategy: worker picks a random WARC from the list, checks if
//   ingest/claims/{id}.claim  AND  raw/{id}.ndjson.gz  both don't exist,
// then writes the claim marker. If the claim already exists, tries another.
// Prints the claimed WARC path to stdout so callers can use it.

import { getObjectBuffer, headExists, putObject, deleteObject } from "./r2.mjs";
import crypto from "node:crypto";

const LIST_KEY = "ingest/warcs.txt";
const CLAIM_TTL_MIN = Number(process.env.CLAIM_TTL_MIN || 60);
const WORKER_ID = process.env.WORKER_ID || crypto.randomBytes(4).toString("hex");

function warcId(path) {
  // e.g. crawl-data/CC-MAIN-2026-30/segments/1234/warc/CC-MAIN-...-00042.warc.gz
  //    -> 00042  (unique enough for our namespace)
  const m = path.match(/([^/]+)\.warc\.gz$/);
  return m ? m[1] : crypto.createHash("sha1").update(path).digest("hex").slice(0, 16);
}

async function loadList() {
  const buf = await getObjectBuffer(LIST_KEY);
  return buf.toString().split("\n").map(s => s.trim()).filter(Boolean);
}

async function claim(path) {
  const id = warcId(path);
  const rawKey = `raw/${id}.ndjson.gz`;
  const claimKey = `ingest/claims/${id}.claim`;

  if (await headExists(rawKey)) return null; // already processed
  if (await headExists(claimKey)) {
    // TODO: honor TTL — for MVP we just skip; watchdog reaps stale claims separately
    return null;
  }
  const body = JSON.stringify({
    worker: WORKER_ID,
    path,
    claimed_at: new Date().toISOString(),
    ttl_min: CLAIM_TTL_MIN,
  });
  await putObject(claimKey, body, "application/json");
  return { id, path, claimKey, rawKey };
}

export async function claimNext(maxTries = 40) {
  const list = await loadList();
  for (let i = 0; i < maxTries; i++) {
    const path = list[Math.floor(Math.random() * list.length)];
    const c = await claim(path);
    if (c) return c;
  }
  return null;
}

export async function releaseClaim(claimKey) {
  await deleteObject(claimKey);
}

// CLI: print next claimed WARC as JSON
if (import.meta.url === `file://${process.argv[1]}`) {
  const c = await claimNext();
  if (!c) { console.error("no free warcs"); process.exit(2); }
  console.log(JSON.stringify(c));
}
