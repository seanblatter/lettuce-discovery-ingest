// scripts/r2.mjs — shared R2 client (S3-compatible)
import { S3Client, GetObjectCommand, PutObjectCommand, HeadObjectCommand, DeleteObjectCommand, ListObjectsV2Command } from "@aws-sdk/client-s3";

const need = (k) => {
  const v = process.env[k];
  if (!v) throw new Error(`Missing env: ${k}`);
  return v;
};

export const R2_BUCKET = need("R2_BUCKET");
export const R2_PREFIX = process.env.R2_PREFIX || "discovery/";

export const s3 = new S3Client({
  region: "auto",
  endpoint: `https://${need("R2_ACCOUNT_ID")}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: need("R2_ACCESS_KEY_ID"),
    secretAccessKey: need("R2_SECRET_ACCESS_KEY"),
  },
});

export const key = (rel) => `${R2_PREFIX}${rel}`;

export async function putObject(rel, Body, ContentType = "application/octet-stream", extra = {}) {
  return s3.send(new PutObjectCommand({ Bucket: R2_BUCKET, Key: key(rel), Body, ContentType, ...extra }));
}
export async function getObjectBuffer(rel) {
  const r = await s3.send(new GetObjectCommand({ Bucket: R2_BUCKET, Key: key(rel) }));
  const chunks = [];
  for await (const c of r.Body) chunks.push(c);
  return Buffer.concat(chunks);
}
export async function getObjectStream(rel) {
  const r = await s3.send(new GetObjectCommand({ Bucket: R2_BUCKET, Key: key(rel) }));
  return r.Body;
}
export async function headExists(rel) {
  try { await s3.send(new HeadObjectCommand({ Bucket: R2_BUCKET, Key: key(rel) })); return true; }
  catch (e) { if (e?.$metadata?.httpStatusCode === 404) return false; throw e; }
}
export async function deleteObject(rel) {
  try { await s3.send(new DeleteObjectCommand({ Bucket: R2_BUCKET, Key: key(rel) })); } catch {}
}
export async function listPrefix(rel) {
  const out = [];
  let ContinuationToken;
  do {
    const r = await s3.send(new ListObjectsV2Command({ Bucket: R2_BUCKET, Prefix: key(rel), ContinuationToken }));
    for (const o of r.Contents || []) out.push({ Key: o.Key, Size: o.Size });
    ContinuationToken = r.IsTruncated ? r.NextContinuationToken : undefined;
  } while (ContinuationToken);
  return out;
}
