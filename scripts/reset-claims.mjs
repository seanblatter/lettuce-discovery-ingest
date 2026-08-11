// scripts/reset-claims.mjs
// Deletes all .claim markers under discovery/ingest/claims/ so a subsequent
// ingest run re-processes every WARC. Raw files (raw/<warc>.ndjson.gz) are
// keyed by WARC name so they overwrite cleanly with the current (more
// permissive) filter settings — this recovers docs the old strict filter
// previously dropped.

import { S3Client, ListObjectsV2Command, DeleteObjectsCommand } from "@aws-sdk/client-s3";

const s3 = new S3Client({
  region: "auto",
  endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY_ID,
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
  },
});
const B = process.env.R2_BUCKET;
const P = (process.env.R2_PREFIX || "discovery/").replace(/\/+$/, "/");

let token, batch = [], total = 0;
async function flush() {
  if (!batch.length) return;
  await s3.send(new DeleteObjectsCommand({ Bucket: B, Delete: { Objects: batch, Quiet: true } }));
  total += batch.length;
  console.log(`[reset-claims] deleted ${total}…`);
  batch = [];
}
do {
  const r = await s3.send(new ListObjectsV2Command({
    Bucket: B, Prefix: `${P}ingest/claims/`, ContinuationToken: token,
  }));
  for (const o of r.Contents || []) {
    batch.push({ Key: o.Key });
    if (batch.length === 1000) await flush();
  }
  token = r.IsTruncated ? r.NextContinuationToken : null;
} while (token);
await flush();
console.log(`[reset-claims] done. deleted ${total} claim markers.`);
