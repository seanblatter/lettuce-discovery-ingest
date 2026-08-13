// scripts/parquet-convert.mjs
// Converts old ndjson.gz shards → Parquet + Zstd (~3x better compression).
// Uses DuckDB CLI (pre-installed on ubuntu-latest runners, or `apt install duckdb`).
//
// Strategy: convert shards older than PARQUET_MIN_AGE_DAYS from the given prefix,
// upload the .parquet alongside, then delete the .ndjson.gz.
//
// Env:
//   R2_*  (as usual)
//   PARQUET_PREFIX          default "shards/"
//   PARQUET_MIN_AGE_DAYS    default 7
//   PARQUET_MAX_FILES       default 500 per run
//   PARQUET_KEEP_ORIGINAL   default 0 (delete .ndjson.gz after successful convert)

import { listPrefix, getObjectBuffer, putObject, deleteObject } from "./r2.mjs";
import { writeFileSync, readFileSync, mkdtempSync, rmSync } from "node:fs";
import { execSync } from "node:child_process";
import { join } from "node:path";
import { tmpdir } from "node:os";

const PREFIX = process.env.PARQUET_PREFIX || "shards/";
const MIN_AGE_DAYS = Number(process.env.PARQUET_MIN_AGE_DAYS || 7);
const MAX_FILES = Number(process.env.PARQUET_MAX_FILES || 500);
const KEEP = process.env.PARQUET_KEEP_ORIGINAL === "1";

function which(cmd) {
  try { execSync(`command -v ${cmd}`, { stdio: "ignore" }); return true; } catch { return false; }
}

async function main() {
  if (!which("duckdb")) {
    console.error("duckdb CLI not found. On Ubuntu runners: `sudo apt-get install -y duckdb`.");
    process.exit(2);
  }
  console.log(`parquet-convert: prefix=${PREFIX}, min-age=${MIN_AGE_DAYS}d, max=${MAX_FILES}`);
  const cutoff = Date.now() - MIN_AGE_DAYS * 86400_000;

  // r2.mjs listPrefix does not include LastModified; use raw S3 API to get age.
  const { s3, R2_BUCKET, key } = await import("./r2.mjs");
  const { ListObjectsV2Command } = await import("@aws-sdk/client-s3");
  const candidates = [];
  let token;
  do {
    const r = await s3.send(new ListObjectsV2Command({ Bucket: R2_BUCKET, Prefix: key(PREFIX), ContinuationToken: token }));
    for (const o of r.Contents || []) {
      if (!o.Key.endsWith(".ndjson.gz")) continue;
      if (!o.LastModified || o.LastModified.getTime() > cutoff) continue;
      candidates.push({ key: o.Key, size: o.Size, ts: o.LastModified.getTime() });
      if (candidates.length >= MAX_FILES) break;
    }
    token = r.IsTruncated ? r.NextContinuationToken : null;
  } while (token && candidates.length < MAX_FILES);
  console.log(`found ${candidates.length} shards eligible for parquet conversion`);
  if (!candidates.length) return;

  const dir = mkdtempSync(join(tmpdir(), "parquet-"));
  let ok = 0, savedBytes = 0;
  for (const c of candidates) {
    const rel = c.key.replace(new RegExp(`^${key("").replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}`), "");
    const inGz = join(dir, "in.ndjson.gz");
    const inNd = join(dir, "in.ndjson");
    const outPq = join(dir, "out.parquet");
    try {
      const buf = await getObjectBuffer(rel);
      writeFileSync(inGz, buf);
      execSync(`gunzip -f ${inGz}`);
      // DuckDB: read NDJSON, write Parquet with ZSTD (level 3 = fast + strong).
      execSync(
        `duckdb -c "COPY (SELECT * FROM read_json_auto('${inNd}', format='newline_delimited', ignore_errors=true)) ` +
        `TO '${outPq}' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 100000);"`,
        { stdio: ["ignore", "ignore", "inherit"] }
      );
      const pqBuf = readFileSync(outPq);
      const newRel = rel.replace(/\.ndjson\.gz$/, ".parquet");
      await putObject(newRel, pqBuf, "application/vnd.apache.parquet");
      if (!KEEP) await deleteObject(rel);
      const before = c.size, after = pqBuf.length;
      savedBytes += (before - after);
      ok++;
      if (ok % 25 === 0) console.log(`  ${ok}/${candidates.length}, saved ${(savedBytes/1e9).toFixed(2)} GB`);
    } catch (e) {
      console.warn(`skip ${rel}: ${e.message}`);
    } finally {
      try { rmSync(inGz, { force: true }); } catch {}
      try { rmSync(inNd, { force: true }); } catch {}
      try { rmSync(outPq, { force: true }); } catch {}
    }
  }
  rmSync(dir, { recursive: true, force: true });
  console.log(`done: converted ${ok}/${candidates.length}, saved ${(savedBytes/1e9).toFixed(2)} GB`);
}

main().catch(e => { console.error(e); process.exit(1); });
