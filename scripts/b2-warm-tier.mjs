// scripts/b2-warm-tier.mjs
// Offloads warm shards (30d < age < 180d) from R2 → Backblaze B2.
// Uses Cloudflare Bandwidth Alliance so egress from B2 back through Cloudflare is FREE.
//
// After successful upload+verify, deletes from R2 to reclaim hot-tier space.
//
// Env (all required):
//   R2_*  (source)
//   B2_KEY_ID, B2_APPLICATION_KEY, B2_BUCKET
//   B2_ENDPOINT  default "https://s3.us-west-004.backblazeb2.com"
//   WARM_PREFIX  default "shards/"
//   WARM_MIN_AGE_DAYS  default 30
//   WARM_MAX_AGE_DAYS  default 180  (older → ia-cold-tier instead)
//   WARM_MAX_BYTES  default 50 GB per run
//   WARM_DELETE_FROM_R2  default 1

import { S3Client, GetObjectCommand, PutObjectCommand, HeadObjectCommand, ListObjectsV2Command, DeleteObjectCommand } from "@aws-sdk/client-s3";

const need = (k) => { const v = process.env[k]; if (!v) throw new Error(`Missing env ${k}`); return v; };

if (!process.env.B2_KEY_ID || !process.env.B2_APPLICATION_KEY || !process.env.B2_BUCKET) {
  console.log("b2-warm-tier: B2 creds not set — skipping");
  process.exit(0);
}

const r2 = new S3Client({
  region: "auto",
  endpoint: `https://${need("R2_ACCOUNT_ID")}.r2.cloudflarestorage.com`,
  credentials: { accessKeyId: need("R2_ACCESS_KEY_ID"), secretAccessKey: need("R2_SECRET_ACCESS_KEY") },
});
const R2_BUCKET = need("R2_BUCKET");
const R2_PREFIX = process.env.R2_PREFIX || "discovery/";

const b2 = new S3Client({
  region: "us-west-004",
  endpoint: process.env.B2_ENDPOINT || "https://s3.us-west-004.backblazeb2.com",
  credentials: { accessKeyId: need("B2_KEY_ID"), secretAccessKey: need("B2_APPLICATION_KEY") },
  forcePathStyle: true,
});
const B2_BUCKET = need("B2_BUCKET");

const PREFIX = process.env.WARM_PREFIX || "shards/";
const MIN_AGE = Number(process.env.WARM_MIN_AGE_DAYS || 30);
const MAX_AGE = Number(process.env.WARM_MAX_AGE_DAYS || 180);
const MAX_BYTES = Number(process.env.WARM_MAX_BYTES || 50 * 1024 * 1024 * 1024);
const DELETE_FROM_R2 = process.env.WARM_DELETE_FROM_R2 !== "0";

const K = (rel) => `${R2_PREFIX}${rel}`;

async function main() {
  console.log(`b2-warm-tier: prefix=${PREFIX}, age=${MIN_AGE}-${MAX_AGE}d, budget=${(MAX_BYTES/1e9).toFixed(1)}GB`);
  const now = Date.now();
  const cutoffMin = now - MIN_AGE * 86400_000;
  const cutoffMax = now - MAX_AGE * 86400_000;

  const eligible = [];
  let token;
  do {
    const r = await r2.send(new ListObjectsV2Command({ Bucket: R2_BUCKET, Prefix: K(PREFIX), ContinuationToken: token }));
    for (const o of r.Contents || []) {
      const ts = o.LastModified?.getTime() || 0;
      if (ts > cutoffMin) continue;  // too new — leave hot
      if (ts < cutoffMax) continue;  // too old — will be handled by ia-cold-tier
      eligible.push({ key: o.Key, size: o.Size, ts });
    }
    token = r.IsTruncated ? r.NextContinuationToken : null;
  } while (token);
  eligible.sort((a, b) => a.ts - b.ts); // oldest first
  console.log(`${eligible.length} shards in warm window`);

  let moved = 0, bytes = 0;
  for (const o of eligible) {
    if (bytes + o.size > MAX_BYTES) break;
    const rel = o.key;
    try {
      // Skip if already in B2
      try { await b2.send(new HeadObjectCommand({ Bucket: B2_BUCKET, Key: rel })); continue; } catch {}

      const g = await r2.send(new GetObjectCommand({ Bucket: R2_BUCKET, Key: rel }));
      const chunks = []; for await (const c of g.Body) chunks.push(c);
      const buf = Buffer.concat(chunks);
      if (buf.length !== o.size) { console.warn(`size mismatch ${rel}`); continue; }

      await b2.send(new PutObjectCommand({ Bucket: B2_BUCKET, Key: rel, Body: buf, ContentType: g.ContentType || "application/octet-stream" }));

      // Verify
      const head = await b2.send(new HeadObjectCommand({ Bucket: B2_BUCKET, Key: rel }));
      if (head.ContentLength !== buf.length) throw new Error("post-put size mismatch");

      if (DELETE_FROM_R2) await r2.send(new DeleteObjectCommand({ Bucket: R2_BUCKET, Key: rel }));
      moved++; bytes += o.size;
      if (moved % 20 === 0) console.log(`  moved ${moved}, ${(bytes/1e9).toFixed(2)} GB`);
    } catch (e) {
      console.warn(`skip ${rel}: ${e.message}`);
    }
  }
  console.log(`done: moved ${moved} objects, ${(bytes/1e9).toFixed(2)} GB to B2`);
}

main().catch(e => { console.error(e); process.exit(1); });
