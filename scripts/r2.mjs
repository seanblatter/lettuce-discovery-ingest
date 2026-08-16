// scripts/r2.mjs — shared R2 client (S3-compatible)
import { S3Client, GetObjectCommand, PutObjectCommand, HeadObjectCommand, DeleteObjectCommand, ListObjectsV2Command } from "@aws-sdk/client-s3";
import { Readable, PassThrough } from "node:stream";

const need = (k) => {
  const v = process.env[k];
  if (!v) throw new Error(`Missing env: ${k}`);
  return v;
};

export const R2_BUCKET = need("R2_BUCKET");
export const R2_PREFIX = process.env.R2_PREFIX || "discovery/";

// Bumped request timeouts + a few extra retries so a mid-stream ECONNRESET
// on a single object doesn't nuke a 340-minute reshard job (see
// 2026-08-15 nightly reshard failure at ~16k/100k raws).
export const s3 = new S3Client({
  region: "auto",
  endpoint: `https://${need("R2_ACCOUNT_ID")}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: need("R2_ACCESS_KEY_ID"),
    secretAccessKey: need("R2_SECRET_ACCESS_KEY"),
  },
  maxAttempts: 6,
  requestHandler: { requestTimeout: 120_000, connectionTimeout: 15_000 },
});

export const key = (rel) => `${R2_PREFIX}${rel}`;

const RETRY_STATUSES = new Set([500, 502, 503, 504]);
const RETRY_CODES = new Set(['ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'ECONNABORTED', 'ENOTFOUND', 'EAI_AGAIN']);

function isRetryable(err) {
  if (!err) return false;
  const code = err.code || err.name;
  if (RETRY_CODES.has(code)) return true;
  const status = err?.$metadata?.httpStatusCode;
  if (status && RETRY_STATUSES.has(status)) return true;
  return /aborted|reset|timeout|socket hang up|premature close/i.test(err.message || '');
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function withRetry(fn, { attempts = 6, label = 'r2' } = {}) {
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    try { return await fn(); }
    catch (err) {
      lastErr = err;
      if (!isRetryable(err) || i === attempts - 1) throw err;
      const delay = Math.min(15_000, 500 * 2 ** i) + Math.floor(Math.random() * 250);
      console.warn(`[${label}] retry ${i + 1}/${attempts - 1} after ${delay}ms: ${err.code || err.name}: ${err.message}`);
      await sleep(delay);
    }
  }
  throw lastErr;
}

export async function putObject(rel, Body, ContentType = "application/octet-stream", extra = {}) {
  return withRetry(
    () => s3.send(new PutObjectCommand({ Bucket: R2_BUCKET, Key: key(rel), Body, ContentType, ...extra })),
    { label: `put ${rel}` }
  );
}
export async function getObjectBuffer(rel) {
  return withRetry(async () => {
    const r = await s3.send(new GetObjectCommand({ Bucket: R2_BUCKET, Key: key(rel) }));
    const chunks = [];
    for await (const c of r.Body) chunks.push(c);
    return Buffer.concat(chunks);
  }, { label: `get ${rel}` });
}
// Returns a Readable that transparently reconnects on mid-stream errors
// by re-issuing GET with Range: bytes=<offset>- from wherever we left off.
// This is what makes reshard survive Cloudflare/edge blips against 100k objects.
export async function getObjectStream(rel) {
  const passthrough = new PassThrough();
  let bytesRead = 0;
  let attempt = 0;
  const MAX = 6;

  const pump = async () => {
    try {
      const range = bytesRead > 0 ? { Range: `bytes=${bytesRead}-` } : {};
      const r = await s3.send(new GetObjectCommand({ Bucket: R2_BUCKET, Key: key(rel), ...range }));
      const upstream = r.Body;
      upstream.on('data', (chunk) => { bytesRead += chunk.length; });
      upstream.on('end', () => passthrough.end());
      upstream.on('error', async (err) => {
        if (attempt < MAX - 1 && isRetryable(err)) {
          attempt++;
          const delay = Math.min(15_000, 500 * 2 ** attempt);
          console.warn(`[stream ${rel}] resume from byte ${bytesRead} after ${delay}ms (${err.code || err.name})`);
          await sleep(delay);
          pump().catch((e) => passthrough.destroy(e));
        } else {
          passthrough.destroy(err);
        }
      });
      upstream.pipe(passthrough, { end: false });
    } catch (err) {
      if (attempt < MAX - 1 && isRetryable(err)) {
        attempt++;
        await sleep(Math.min(15_000, 500 * 2 ** attempt));
        pump().catch((e) => passthrough.destroy(e));
      } else {
        passthrough.destroy(err);
      }
    }
  };
  pump().catch((e) => passthrough.destroy(e));
  return passthrough;
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
