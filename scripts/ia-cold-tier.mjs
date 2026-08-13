// scripts/ia-cold-tier.mjs
// Offloads oldest data (age > IA_MIN_AGE_DAYS) from R2 → Internet Archive as public items.
// IA storage & egress are FREE for public items. Bundles many small shards into
// one ~5 GB tar per IA item to stay under IA's per-item file-count sanity limits.
//
// Env (all required unless noted):
//   R2_*  (source)
//   IA_ACCESS_KEY, IA_SECRET_KEY  (from archive.org/account/s3.php)
//   IA_COLLECTION     default "opensource"  (or your own collection if you have one)
//   IA_ITEM_PREFIX    default "lettuce-discovery-"
//   COLD_PREFIX       default "raw/"        (most costly to keep)
//   COLD_MIN_AGE_DAYS default 180
//   COLD_MAX_BYTES    default 100 GB per run
//   COLD_DELETE_FROM_R2  default 1
//
// After successful IA upload, writes a manifest to R2 discovery/ia-manifest/{item}.json
// so lookups can rehydrate individual keys later.

import { S3Client, GetObjectCommand, ListObjectsV2Command, DeleteObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { spawn } from "node:child_process";
import { createWriteStream, mkdtempSync, rmSync, statSync, createReadStream, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

const need = (k) => { const v = process.env[k]; if (!v) throw new Error(`Missing env ${k}`); return v; };

if (!process.env.IA_ACCESS_KEY || !process.env.IA_SECRET_KEY) {
  console.log("ia-cold-tier: IA creds not set — skipping");
  process.exit(0);
}

const r2 = new S3Client({
  region: "auto",
  endpoint: `https://${need("R2_ACCOUNT_ID")}.r2.cloudflarestorage.com`,
  credentials: { accessKeyId: need("R2_ACCESS_KEY_ID"), secretAccessKey: need("R2_SECRET_ACCESS_KEY") },
});
const R2_BUCKET = need("R2_BUCKET");
const R2_PREFIX = process.env.R2_PREFIX || "discovery/";
const K = (rel) => `${R2_PREFIX}${rel}`;

const IA_KEY = need("IA_ACCESS_KEY");
const IA_SEC = need("IA_SECRET_KEY");
const IA_COLLECTION = process.env.IA_COLLECTION || "opensource";
const IA_ITEM_PREFIX = process.env.IA_ITEM_PREFIX || "lettuce-discovery-";

const PREFIX = process.env.COLD_PREFIX || "raw/";
const MIN_AGE = Number(process.env.COLD_MIN_AGE_DAYS || 180);
const MAX_BYTES = Number(process.env.COLD_MAX_BYTES || 100 * 1024 * 1024 * 1024);
const DEL = process.env.COLD_DELETE_FROM_R2 !== "0";

// IA S3-compatible endpoint
const IA_ENDPOINT = "https://s3.us.archive.org";

async function iaPut(item, name, body, size, meta = {}) {
  const url = `${IA_ENDPOINT}/${item}/${encodeURIComponent(name)}`;
  const headers = {
    "authorization": `LOW ${IA_KEY}:${IA_SEC}`,
    "x-archive-auto-make-bucket": "1",
    "x-archive-meta-collection": IA_COLLECTION,
    "x-archive-meta-mediatype": "data",
    "x-archive-meta-title": meta.title || item,
    "x-archive-meta-description": meta.description || "Lettuce Discovery cold-tier archive",
    "x-archive-meta-creator": "Lettuce Vision",
    "x-archive-meta-subject": "web-crawl;search-index;common-crawl-derivative",
    "content-type": "application/octet-stream",
    "content-length": String(size),
  };
  const r = await fetch(url, { method: "PUT", headers, body, duplex: "half" });
  if (!r.ok) throw new Error(`IA ${r.status} ${await r.text().catch(() => "")}`);
}

async function main() {
  console.log(`ia-cold-tier: prefix=${PREFIX}, min-age=${MIN_AGE}d, budget=${(MAX_BYTES/1e9).toFixed(1)}GB, collection=${IA_COLLECTION}`);
  const cutoff = Date.now() - MIN_AGE * 86400_000;

  const eligible = [];
  let token;
  do {
    const r = await r2.send(new ListObjectsV2Command({ Bucket: R2_BUCKET, Prefix: K(PREFIX), ContinuationToken: token }));
    for (const o of r.Contents || []) {
      const ts = o.LastModified?.getTime() || 0;
      if (ts > cutoff) continue;
      eligible.push({ key: o.Key, size: o.Size, ts });
    }
    token = r.IsTruncated ? r.NextContinuationToken : null;
  } while (token);
  eligible.sort((a, b) => a.ts - b.ts);
  console.log(`${eligible.length} objects eligible for IA cold offload`);

  // Group into ~5 GB items
  const ITEM_TARGET_BYTES = 5 * 1024 * 1024 * 1024;
  const items = [];
  let cur = { size: 0, objs: [] };
  for (const o of eligible) {
    if (cur.size + o.size > ITEM_TARGET_BYTES && cur.objs.length) { items.push(cur); cur = { size: 0, objs: [] }; }
    cur.objs.push(o); cur.size += o.size;
  }
  if (cur.objs.length) items.push(cur);

  const tmp = mkdtempSync(join(tmpdir(), "ia-cold-"));
  let totalMoved = 0, totalBytes = 0;
  for (const grp of items) {
    if (totalBytes + grp.size > MAX_BYTES) break;
    const itemId = `${IA_ITEM_PREFIX}${new Date().toISOString().slice(0, 10)}-${Date.now().toString(36)}`;
    console.log(`\n→ IA item ${itemId} (${grp.objs.length} files, ${(grp.size/1e9).toFixed(2)} GB)`);

    // Upload each object as a file in the item
    const manifest = [];
    let idx = 0;
    for (const o of grp.objs) {
      idx++;
      const name = o.key.replace(new RegExp(`^${R2_PREFIX.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}`), "").replace(/[^\w./-]/g, "_");
      try {
        const g = await r2.send(new GetObjectCommand({ Bucket: R2_BUCKET, Key: o.key }));
        const chunks = []; for await (const c of g.Body) chunks.push(c);
        const buf = Buffer.concat(chunks);
        await iaPut(itemId, name, buf, buf.length, {
          title: `Lettuce Discovery cold-tier ${itemId}`,
          description: `Archived from Cloudflare R2 (${o.key})`,
        });
        manifest.push({ r2Key: o.key, iaItem: itemId, iaName: name, size: o.size, ts: o.ts });
        if (DEL) await r2.send(new DeleteObjectCommand({ Bucket: R2_BUCKET, Key: o.key }));
        if (idx % 25 === 0) console.log(`  ${idx}/${grp.objs.length}`);
      } catch (e) { console.warn(`  skip ${o.key}: ${e.message}`); }
    }

    // Store the manifest in R2 for lookups
    const mBuf = Buffer.from(JSON.stringify({ item: itemId, collection: IA_COLLECTION, files: manifest }, null, 2));
    await r2.send(new PutObjectCommand({
      Bucket: R2_BUCKET,
      Key: K(`ia-manifest/${itemId}.json`),
      Body: mBuf,
      ContentType: "application/json",
    }));

    totalMoved += manifest.length; totalBytes += grp.size;
    console.log(`  ✓ ${manifest.length} files → IA, manifest saved`);
  }
  rmSync(tmp, { recursive: true, force: true });
  console.log(`\ndone: ${totalMoved} objects, ${(totalBytes/1e9).toFixed(2)} GB → Internet Archive`);
}

main().catch(e => { console.error(e); process.exit(1); });
