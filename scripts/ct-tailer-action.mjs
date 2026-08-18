// scripts/ct-tailer-action.mjs
// GitHub-Actions-native CT log tailer. Runs for ~5h20m per invocation, then
// exits cleanly so the next scheduled run picks up where this one left off.
//
// Offsets live in R2 at `ct-tailer-state/{slug}.offset` (small JSON blobs),
// so every runner is stateless. Two runs of ~5h20m per 12h window ⇒ 24/7
// coverage with zero VM.
//
// Env: same as ct-tailer.mjs. No local disk needed beyond scratch tmp.

import { S3Client, PutObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { gzipSync } from "node:zlib";
import { createHash } from "node:crypto";

const need = (k) => { const v = process.env[k]; if (!v) throw new Error(`Missing env ${k}`); return v; };
const R2_BUCKET = need("R2_BUCKET");
const R2_PREFIX = process.env.R2_PREFIX || "discovery/";
const BATCH_SIZE = Number(process.env.CT_BATCH_SIZE || 100_000);
const MAX_CONC   = Number(process.env.CT_MAX_CONC   || 8);
// Stop ~10 min before the 6-hour job timeout so the flush + offset save
// always land.
const RUN_BUDGET_MS = Number(process.env.CT_RUN_BUDGET_MS || 5.5 * 3600 * 1000);
const UA = "LettuceVision-CT-Action/1.0 (+https://lettuce.vision)";

const s3 = new S3Client({
  region: "auto",
  endpoint: `https://${need("R2_ACCOUNT_ID")}.r2.cloudflarestorage.com`,
  credentials: { accessKeyId: need("R2_ACCESS_KEY_ID"), secretAccessKey: need("R2_SECRET_ACCESS_KEY") },
});

const startedAt = Date.now();
const timeUp = () => Date.now() - startedAt > RUN_BUDGET_MS;

// ---------- log list (same source as ct-tailer.mjs) ----------
async function fetchLogList() {
  const r = await fetch("https://www.gstatic.com/ct/log_list/v3/log_list.json", { headers: { "user-agent": UA } });
  if (!r.ok) throw new Error(`log_list: ${r.status}`);
  const j = await r.json();
  const logs = [];
  for (const op of j.operators || []) for (const log of op.logs || []) {
    if (log.state && (log.state.usable || log.state.qualified)) {
      logs.push({
        slug: (op.name + "-" + log.description).toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, ""),
        url:  log.url.replace(/\/$/, ""),
      });
    }
  }
  return logs;
}

// ---------- STH + entries ----------
async function getSTH(log) {
  const r = await fetch(`${log.url}/ct/v1/get-sth`, { headers: { "user-agent": UA }, signal: AbortSignal.timeout(20_000) });
  if (!r.ok) throw new Error(`sth ${log.slug}: ${r.status}`);
  return r.json();
}
async function getEntries(log, start, end) {
  const r = await fetch(`${log.url}/ct/v1/get-entries?start=${start}&end=${end}`, {
    headers: { "user-agent": UA }, signal: AbortSignal.timeout(45_000),
  });
  if (!r.ok) throw new Error(`entries ${log.slug} ${start}-${end}: ${r.status}`);
  return r.json();
}

// ---------- DER hostname scan (same trick as VM ct-tailer) ----------
function extractHostnames(certDer) {
  const out = new Set();
  const buf = Buffer.from(certDer);
  for (let i = 0; i < buf.length - 4; i++) {
    if (buf[i] !== 0x82) continue;
    const len = buf[i + 1];
    if (len < 3 || len > 253) continue;
    let ok = true;
    for (let j = 0; j < len; j++) {
      const b = buf[i + 2 + j];
      if (b < 0x20 || b > 0x7e) { ok = false; break; }
    }
    if (!ok) continue;
    const s = buf.subarray(i + 2, i + 2 + len).toString("ascii").toLowerCase();
    if (!/^[a-z0-9*][a-z0-9.\-*]*[a-z0-9]$/.test(s)) continue;
    if (!s.includes(".")) continue;
    out.add(s);
    i += len + 1;
  }
  return [...out];
}
function parseLeafInput(b64) {
  const buf = Buffer.from(b64, "base64");
  if (buf.length < 15) return null;
  const timestamp = Number(buf.readBigUInt64BE(2));
  const entryType = buf.readUInt16BE(10);
  if (entryType === 0) {
    const certLen = (buf[12] << 16) | (buf[13] << 8) | buf[14];
    return { timestamp, der: buf.subarray(15, 15 + certLen) };
  }
  if (entryType === 1) {
    const off = 12 + 32;
    const tbsLen = (buf[off] << 16) | (buf[off + 1] << 8) | buf[off + 2];
    return { timestamp, der: buf.subarray(off + 3, off + 3 + tbsLen) };
  }
  return null;
}

const BAD_TLDS = /\.(onion|zip|invalid|local|localhost|test|example)$/i;
const ADULT    = /\b(porn|xxx|escort|nsfw|hentai|cams?)\b/i;
const goodHost = (h) => h && h.length <= 253 && !h.startsWith("*.") && !BAD_TLDS.test(h) && !ADULT.test(h) && h.split(".").length >= 2;

// ---------- R2 offset state ----------
async function loadOffset(slug) {
  try {
    const r = await s3.send(new GetObjectCommand({ Bucket: R2_BUCKET, Key: `${R2_PREFIX}ct-tailer-state/${slug}.json` }));
    const s = await r.Body.transformToString();
    return JSON.parse(s).offset || 0;
  } catch { return 0; }
}
async function saveOffset(slug, offset) {
  await s3.send(new PutObjectCommand({
    Bucket: R2_BUCKET,
    Key: `${R2_PREFIX}ct-tailer-state/${slug}.json`,
    Body: Buffer.from(JSON.stringify({ offset, updatedAt: new Date().toISOString() })),
    ContentType: "application/json",
  }));
}

// ---------- batching ----------
let batch = [];
let totalEmitted = 0;
const seenThisRun = new Set();  // dedup within a single run
async function flush(force = false) {
  if (!batch.length) return;
  if (!force && batch.length < BATCH_SIZE) return;
  const toFlush = batch; batch = [];
  const nd = toFlush.map((d) => JSON.stringify(d)).join("\n") + "\n";
  const gz = gzipSync(Buffer.from(nd));
  const d = new Date();
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const seq = createHash("sha1").update(nd).digest("hex").slice(0, 10);
  const key = `${R2_PREFIX}ct-live/${yyyy}/${mm}/${dd}/${hh}-${seq}.ndjson.gz`;
  await s3.send(new PutObjectCommand({ Bucket: R2_BUCKET, Key: key, Body: gz, ContentType: "application/gzip" }));
  totalEmitted += toFlush.length;
  console.log(`[flush] ${toFlush.length} hosts → ${key} (${(gz.length / 1024).toFixed(1)} KB)`);
}

// ---------- per-log tailer (bounded) ----------
async function tailLog(log) {
  let offset = await loadOffset(log.slug);
  const CHUNK = 256;
  let sthSize = 0, sthTs = 0;
  const refreshSTH = async () => {
    try { const s = await getSTH(log); sthSize = s.tree_size; sthTs = Date.now(); }
    catch (e) { console.warn(`[${log.slug}] sth: ${e.message}`); }
  };
  await refreshSTH();
  if (offset === 0 && sthSize > CHUNK * 4) {
    // First encounter of this log — jump to head so we don't chew years of history.
    offset = sthSize;
    await saveOffset(log.slug, offset);
    console.log(`[${log.slug}] first run, starting at head=${offset}`);
  }

  while (!timeUp()) {
    try {
      if (Date.now() - sthTs > 60_000) await refreshSTH();
      if (offset >= sthSize) { await new Promise(r => setTimeout(r, 15_000)); continue; }
      const end = Math.min(offset + CHUNK - 1, sthSize - 1);
      const { entries } = await getEntries(log, offset, end);
      if (!entries?.length) { await new Promise(r => setTimeout(r, 3_000)); continue; }
      for (const e of entries) {
        const leaf = parseLeafInput(e.leaf_input);
        if (!leaf) continue;
        for (const h of extractHostnames(leaf.der)) {
          if (!goodHost(h)) continue;
          if (seenThisRun.has(h)) continue;
          seenThisRun.add(h);
          batch.push({ u: `https://${h}/`, t: h, d: `CT:${log.slug}`, c: "", src: "ct", log: log.slug, ts: leaf.timestamp });
        }
      }
      offset += entries.length;
      if (batch.length >= BATCH_SIZE) await flush(true);
      // Persist offset periodically (cheap — one small PUT per ~50 chunks).
      if (Math.random() < 0.02) await saveOffset(log.slug, offset);
    } catch (e) {
      console.warn(`[${log.slug}] err @${offset}: ${e.message}`);
      await new Promise(r => setTimeout(r, 10_000));
    }
  }
  await saveOffset(log.slug, offset);
  console.log(`[${log.slug}] time budget reached, saved offset=${offset}`);
}

async function pool(items, worker, conc) {
  const running = new Set();
  for (const it of items) {
    const p = worker(it).catch(e => console.error(`worker:`, e.message));
    running.add(p); p.finally(() => running.delete(p));
    if (running.size >= conc) await Promise.race(running);
  }
  await Promise.all(running);
}

async function main() {
  console.log(`ct-tailer-action starting: budget=${(RUN_BUDGET_MS/3600000).toFixed(2)}h, batch=${BATCH_SIZE}, conc=${MAX_CONC}`);
  const logs = await fetchLogList();
  console.log(`discovered ${logs.length} active CT logs`);
  await pool(logs, tailLog, MAX_CONC);
  await flush(true);
  console.log(`::notice title=CT Tailer::${totalEmitted.toLocaleString()} unique hostnames emitted this run across ${logs.length} logs.`);
  if (process.env.GITHUB_STEP_SUMMARY) {
    const fs = await import("node:fs");
    const hrs = (Date.now() - startedAt) / 3600000;
    const projected = Math.round(totalEmitted / hrs * 24);
    fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY,
      `### CT Tailer run\n\n- **Unique hostnames this run:** ${totalEmitted.toLocaleString()}\n- **Runtime:** ${hrs.toFixed(2)}h\n- **Projected 24h rate:** ${projected.toLocaleString()} hostnames/day\n- **Logs tailed:** ${logs.length}\n`);
  }
}

main().catch(e => { console.error("fatal:", e); process.exit(1); });
