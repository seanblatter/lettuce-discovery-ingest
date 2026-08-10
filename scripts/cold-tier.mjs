// scripts/cold-tier.mjs (item 11)
// R2 doesn't have per-object storage classes yet (unlike S3), but we can
// segregate cold shards under a prefix so a future Lifecycle rule or manual
// migration to Cloudflare's Infrequent Access class saves ~66% storage.
//
// Also: rewrites cold shards with maximum brotli compression (quality 11),
// which typically saves another 15-25% on top of gzip. For hot shards we
// keep the fast quality-5 brotli since they change often.

import { listPrefix, getObjectStream, putObject, deleteObject } from "./r2.mjs";
import { createGunzip, brotliCompressSync, constants as zconst } from "node:zlib";

const AGE_DAYS = Number(process.env.COLD_AGE_DAYS || 180);
const cutoff = Date.now() - AGE_DAYS * 86400 * 1000;

const files = await listPrefix("shards/versions/");
let migrated = 0, savedBytes = 0;

for (const f of files) {
  if (!f.Key.endsWith(".ndjson.gz")) continue;
  // extract version dir date
  const m = f.Key.match(/versions\/(\d{4}-\d{2}-\d{2})\//);
  if (!m) continue;
  const ts = Date.parse(m[1]);
  if (ts >= cutoff) continue;

  const rel = f.Key.split("discovery/").pop();
  const s = await getObjectStream(rel);
  const gz = createGunzip(); s.pipe(gz);
  const chunks = []; for await (const c of gz) chunks.push(c);
  const raw = Buffer.concat(chunks);

  const br = brotliCompressSync(raw, { params: { [zconst.BROTLI_PARAM_QUALITY]: 11 } });
  const coldKey = rel.replace("shards/versions/", "shards-cold/").replace(/\.gz$/, ".br");
  await putObject(coldKey, br, "application/x-ndjson", { ContentEncoding: "br" });
  await deleteObject(rel);

  savedBytes += (f.Size || 0) - br.length;
  migrated++;
  if (migrated % 50 === 0) console.log(`  migrated ${migrated} shards, saved ${(savedBytes / 1e9).toFixed(2)} GB`);
}

console.log(`cold-tier: migrated ${migrated} shards, saved ${(savedBytes / 1e9).toFixed(2)} GB`);
