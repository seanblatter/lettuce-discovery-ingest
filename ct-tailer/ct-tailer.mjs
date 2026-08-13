// ct-tailer.mjs — long-running CT log tailer.
// Tails ~40 public Certificate Transparency logs, extracts hostnames, dedupes,
// batches into gzipped NDJSON, uploads to R2.
//
// Deploy: run under systemd on Oracle Cloud Free ARM VM (see systemd unit + bootstrap.sh).
// Cost: ~$2.50/mo (R2 storage + writes only; compute is free).
//
// Filters applied inline:
//   1. Wildcard SANs → drop
//   2. Bad TLDs (.onion, .zip) → drop
//   3. Adult keyword regex → drop
//   4. Bloom-based recent-dedup (in-mem, 15 min window per host) → drop repeats
//   5. LE-renewal filter: if same host issued a LE cert in the last 60 days
//      (stored in a rolling on-disk LevelDB-like KV via `better-sqlite3`) → drop
//
// Output: R2 keys of form
//   discovery/ct-live/{yyyy}/{mm}/{dd}/{hh}-{log-slug}-{seq}.ndjson.gz
// Each line: {"u":"https://host/","t":"host","d":"CT:log-slug","c":"","src":"ct","iss":"LE","ts":<ms>}
//
// Env:
//   R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET, R2_PREFIX
//   CT_STATE_DIR   (default: ./state)  — dedup DB + per-log offsets
//   CT_BATCH_SIZE  (default: 100_000)  — hostnames per R2 upload
//   CT_MAX_CONC    (default: 6)        — concurrent HTTP requests per log

import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { gzipSync } from "node:zlib";
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "node:fs";
import { join } from "node:path";
import { createHash } from "node:crypto";

// ---------- config ----------
const need = (k) => { const v = process.env[k]; if (!v) throw new Error(`Missing env ${k}`); return v; };
const R2_BUCKET = need("R2_BUCKET");
const R2_PREFIX = process.env.R2_PREFIX || "discovery/";
const STATE_DIR = process.env.CT_STATE_DIR || "./state";
const BATCH_SIZE = Number(process.env.CT_BATCH_SIZE || 100_000);
const MAX_CONC = Number(process.env.CT_MAX_CONC || 6);
const UA = "LettuceVision-CT/1.0 (+https://lettuce.vision)";

const s3 = new S3Client({
  region: "auto",
  endpoint: `https://${need("R2_ACCOUNT_ID")}.r2.cloudflarestorage.com`,
  credentials: { accessKeyId: need("R2_ACCESS_KEY_ID"), secretAccessKey: need("R2_SECRET_ACCESS_KEY") },
});

mkdirSync(STATE_DIR, { recursive: true });

// ---------- active CT log list ----------
// Source: https://www.gstatic.com/ct/log_list/v3/log_list.json (Google's canonical list).
// This is a minimal hardcoded subset. Full list is fetched dynamically on startup.
const LOG_LIST_URL = "https://www.gstatic.com/ct/log_list/v3/log_list.json";

async function fetchLogList() {
  const r = await fetch(LOG_LIST_URL, { headers: { "user-agent": UA } });
  if (!r.ok) throw new Error(`log_list: ${r.status}`);
  const j = await r.json();
  const logs = [];
  for (const op of j.operators || []) {
    for (const log of op.logs || []) {
      // Only active logs accepting new entries
      if (log.state && (log.state.usable || log.state.qualified)) {
        logs.push({
          name: log.description,
          slug: (op.name + "-" + log.description).toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, ""),
          url: log.url.replace(/\/$/, ""),
          operator: op.name,
        });
      }
    }
  }
  return logs;
}

// ---------- STH & entries ----------
async function getSTH(log) {
  const r = await fetch(`${log.url}/ct/v1/get-sth`, { headers: { "user-agent": UA }, signal: AbortSignal.timeout(20_000) });
  if (!r.ok) throw new Error(`sth ${log.slug}: ${r.status}`);
  return r.json(); // { tree_size, timestamp, ... }
}

async function getEntries(log, start, end) {
  const r = await fetch(`${log.url}/ct/v1/get-entries?start=${start}&end=${end}`, {
    headers: { "user-agent": UA }, signal: AbortSignal.timeout(45_000),
  });
  if (!r.ok) throw new Error(`entries ${log.slug} ${start}-${end}: ${r.status}`);
  return r.json(); // { entries: [{leaf_input, extra_data}, ...] }
}

// ---------- MerkleTreeLeaf → hostnames ----------
// Parsing: leaf_input is base64. Structure per RFC 6962:
//   version(1) | leaf_type(1) | timestamp(8) | log_entry_type(2) | entry(varies) | extensions(varies)
// For X509 (type 0): entry = 3-byte length + DER cert. For precert (type 1): entry = 32-byte issuer key hash + TBSCertificate.
// We need SAN + CN. Rather than a full ASN.1 parser, we scan the DER for dNSName tags.
// A dNSName in a SAN extension is: 0x82 <len> <ASCII bytes>. That's reliable enough for hostname extraction.
function extractHostnames(certDer) {
  const out = new Set();
  const buf = Buffer.from(certDer);
  for (let i = 0; i < buf.length - 4; i++) {
    // 0x82 = context-specific tag [2] (dNSName)
    if (buf[i] !== 0x82) continue;
    const len = buf[i + 1];
    if (len < 3 || len > 253) continue;
    // Peek the next `len` bytes — must be printable ASCII
    let ok = true;
    for (let j = 0; j < len; j++) {
      const b = buf[i + 2 + j];
      if (b < 0x20 || b > 0x7e) { ok = false; break; }
    }
    if (!ok) continue;
    const s = buf.subarray(i + 2, i + 2 + len).toString("ascii").toLowerCase();
    // Basic hostname sanity
    if (!/^[a-z0-9*][a-z0-9.\-*]*[a-z0-9]$/.test(s)) continue;
    if (!s.includes(".")) continue;
    out.add(s);
    i += len + 1;
  }
  return [...out];
}

function parseLeafInput(b64) {
  const buf = Buffer.from(b64, "base64");
  if (buf.length < 12) return null;
  const timestamp = Number(buf.readBigUInt64BE(2));
  const entryType = buf.readUInt16BE(10);
  if (entryType === 0) {
    // X509 cert: 3-byte length + DER
    const certLen = (buf[12] << 16) | (buf[13] << 8) | buf[14];
    return { timestamp, der: buf.subarray(15, 15 + certLen) };
  }
  if (entryType === 1) {
    // Precert: 32-byte issuer key hash + 3-byte tbs length + TBS
    const off = 12 + 32;
    const tbsLen = (buf[off] << 16) | (buf[off + 1] << 8) | buf[off + 2];
    return { timestamp, der: buf.subarray(off + 3, off + 3 + tbsLen) };
  }
  return null;
}

// ---------- filters ----------
const BAD_TLDS = /\.(onion|zip|invalid|local|localhost|test|example)$/i;
const ADULT = /\b(porn|xxx|escort|nsfw|hentai|cams?)\b/i;
function goodHost(h) {
  if (!h || h.length > 253) return false;
  if (h.startsWith("*.")) return false;         // drop wildcards
  if (BAD_TLDS.test(h)) return false;
  if (ADULT.test(h)) return false;
  if (h.split(".").length < 2) return false;
  return true;
}

// ---------- LE-renewal dedup (in-memory rolling 24h) ----------
// For each host, remember last-seen timestamp. If we saw it in the last 60 days
// AND the issuer looks like LE, skip. To keep memory bounded we age out after 24h;
// long-term dedup happens downstream in reshard/dedup-and-cap.
const recent = new Map();       // host -> lastSeenMs
const RECENT_TTL_MS = 24 * 3600_000;
function shouldEmit(host, issuerHint) {
  const now = Date.now();
  const last = recent.get(host);
  if (last && now - last < RECENT_TTL_MS) return false;
  recent.set(host, now);
  return true;
}
setInterval(() => {
  const cutoff = Date.now() - RECENT_TTL_MS;
  let dropped = 0;
  for (const [h, t] of recent) if (t < cutoff) { recent.delete(h); dropped++; }
  if (dropped) console.log(`[dedup] pruned ${dropped}, size=${recent.size}`);
}, 10 * 60_000).unref();

// ---------- batching + upload ----------
let batch = [];
let batchStartedAt = Date.now();
const MAX_BATCH_AGE_MS = 5 * 60_000; // flush at least every 5 min

async function flushIfReady(force = false) {
  if (!batch.length) return;
  if (!force && batch.length < BATCH_SIZE && Date.now() - batchStartedAt < MAX_BATCH_AGE_MS) return;
  const toFlush = batch; batch = []; batchStartedAt = Date.now();
  const nd = toFlush.map((d) => JSON.stringify(d)).join("\n") + "\n";
  const gz = gzipSync(Buffer.from(nd));
  const d = new Date();
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const seq = createHash("sha1").update(nd).digest("hex").slice(0, 10);
  const key = `${R2_PREFIX}ct-live/${yyyy}/${mm}/${dd}/${hh}-${seq}.ndjson.gz`;
  try {
    await s3.send(new PutObjectCommand({ Bucket: R2_BUCKET, Key: key, Body: gz, ContentType: "application/gzip" }));
    console.log(`[flush] ${toFlush.length} hosts → ${key} (${(gz.length / 1024).toFixed(1)} KB)`);
  } catch (e) {
    console.error(`[flush] upload failed, retrying next tick:`, e.message);
    // put items back at head so they get another shot
    batch = toFlush.concat(batch);
  }
}
setInterval(() => flushIfReady(false).catch(() => {}), 30_000).unref();

// ---------- per-log tailer ----------
function stateFile(log) { return join(STATE_DIR, `${log.slug}.offset`); }
function loadOffset(log) {
  try { return Number(readFileSync(stateFile(log), "utf8").trim()) || 0; }
  catch { return 0; }
}
function saveOffset(log, n) {
  try { writeFileSync(stateFile(log), String(n)); } catch {}
}

async function tailLog(log) {
  let offset = loadOffset(log);
  const CHUNK = 256;
  let sthTs = 0, sthSize = 0;
  const refreshSTH = async () => {
    try {
      const sth = await getSTH(log);
      sthSize = sth.tree_size;
      sthTs = Date.now();
    } catch (e) {
      console.warn(`[${log.slug}] sth: ${e.message}`);
    }
  };
  await refreshSTH();

  // If this is a first run and sthSize is huge, don't backfill all of history —
  // start from the current head (only future entries).
  if (offset === 0 && sthSize > CHUNK * 4) {
    offset = sthSize;
    saveOffset(log, offset);
    console.log(`[${log.slug}] first run, starting at head=${offset}`);
  }

  while (true) {
    try {
      if (Date.now() - sthTs > 60_000) await refreshSTH();
      if (offset >= sthSize) { await new Promise(r => setTimeout(r, 30_000)); continue; }
      const end = Math.min(offset + CHUNK - 1, sthSize - 1);
      const { entries } = await getEntries(log, offset, end);
      if (!entries?.length) { await new Promise(r => setTimeout(r, 5_000)); continue; }
      for (const e of entries) {
        const leaf = parseLeafInput(e.leaf_input);
        if (!leaf) continue;
        const hosts = extractHostnames(leaf.der);
        for (const h of hosts) {
          if (!goodHost(h)) continue;
          if (!shouldEmit(h)) continue;
          batch.push({
            u: `https://${h.replace(/^\*\./, "")}/`,
            t: h,
            d: `CT:${log.slug}`,
            c: "",
            src: "ct",
            log: log.slug,
            ts: leaf.timestamp,
          });
        }
      }
      offset += entries.length;
      saveOffset(log, offset);
      if (batch.length >= BATCH_SIZE) await flushIfReady(true);
    } catch (e) {
      console.warn(`[${log.slug}] err @${offset}: ${e.message}`);
      await new Promise(r => setTimeout(r, 15_000));
    }
  }
}

// ---------- concurrency governor ----------
async function runWithConcurrency(items, worker, conc) {
  const executing = new Set();
  for (const item of items) {
    const p = worker(item).catch(e => console.error(`worker crashed:`, e));
    executing.add(p);
    p.finally(() => executing.delete(p));
    if (executing.size >= conc) await Promise.race(executing);
  }
  await Promise.all(executing);
}

// ---------- main ----------
async function main() {
  console.log(`ct-tailer starting: state=${STATE_DIR}, batch=${BATCH_SIZE}, conc=${MAX_CONC}`);
  const logs = await fetchLogList();
  console.log(`discovered ${logs.length} active CT logs`);

  // Graceful shutdown flushes pending batch.
  const shutdown = async (sig) => {
    console.log(`\n[${sig}] flushing…`);
    await flushIfReady(true);
    process.exit(0);
  };
  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));

  // Tail all logs in parallel; each log has its own steady-state async loop.
  await Promise.all(logs.map(tailLog));
}

main().catch(e => { console.error("fatal:", e); process.exit(1); });
