// scripts/certstream-ingest.mjs
// Ingests newly-issued TLS certificates from public Certificate Transparency logs
// via crt.sh (JSON API, no auth). Produces URL seeds for freshly-registered domains.
//
// Cron: every 15 min. Budget: ~1 GB/mo. Extracts registrable domain, writes as
// hot/certstream-{ts}.ndjson.gz with { u, t, d, c:"" } placeholders — real doc
// content is fetched later by the sitemap crawler on next pass.

import { putObject } from "./r2.mjs";
import { gzipSync } from "node:zlib";

const UA = "Mozilla/5.0 (compatible; LettuceVisionBot/1.0; +https://lettuce.vision)";
const LOOKBACK_MIN = Number(process.env.LOOKBACK_MIN || 25); // matches cron cadence + slack
const MAX_DOMAINS = Number(process.env.MAX_DOMAINS || 30000);
const KEEP_SUBDOMAINS = process.env.KEEP_SUBDOMAINS === "1"; // emit full hostnames, not just eTLD+1
// Fetch shards sequentially with delay — crt.sh aggressively rate-limits parallel clients.
const SHARD_DELAY_MS = Number(process.env.SHARD_DELAY_MS || 2000);
// Query multiple crt.sh "views" to broaden coverage per run.
const QUERIES = [
  { q: "%25", label: "wildcard" },       // broadest — any cert containing at least one char
  { q: "%25.com", label: "com" },
  { q: "%25.org", label: "org" },
  { q: "%25.net", label: "net" },
  { q: "%25.io", label: "io" },
  { q: "%25.app", label: "app" },
  { q: "%25.dev", label: "dev" },
  { q: "%25.ai", label: "ai" },
];

// CertSpotter fallback — free, no auth, higher quality (uses public CT logs directly).
// https://sslmate.com/certspotter/api  (no key needed for the /issuances endpoint)
const CERTSPOTTER_URL = "https://api.certspotter.com/v1/issuances?domain=%25&include_subdomains=true&expand=dns_names";

// Public suffix short-list. Good-enough for eTLD+1 extraction on 99% of certs.
const MULTI_TLD = new Set([
  "co.uk","org.uk","ac.uk","gov.uk","co.jp","or.jp","ne.jp","ac.jp",
  "com.au","net.au","org.au","com.br","com.mx","com.ar","com.cn",
  "co.in","co.nz","co.za","co.kr","com.tr","com.tw","com.hk","com.sg",
]);
function eTLDPlus1(host) {
  const p = host.split(".");
  if (p.length < 2) return null;
  const last2 = p.slice(-2).join(".");
  if (p.length >= 3 && MULTI_TLD.has(last2)) return p.slice(-3).join(".");
  return last2;
}

function goodDomain(d) {
  if (!d || d.length > 253) return false;
  if (/[^a-z0-9.-]/.test(d)) return false;
  if (d.startsWith(".") || d.endsWith(".")) return false;
  if (d.startsWith("*.")) return false;                        // ignore wildcards
  if (/\b(porn|xxx|escort|hentai|cams?)\b/i.test(d)) return false;
  return true;
}

async function fetchCrtSh(q) {
  // crt.sh JSON: recent CT entries. Query returns up to ~10k rows per shard.
  const url = `https://crt.sh/?q=${q}&output=json&exclude=expired&limit=10000`;
  // Retry with backoff — crt.sh frequently returns 502/504 under load.
  let lastErr;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const r = await fetch(url, {
        headers: { "user-agent": UA, "accept": "application/json" },
        signal: AbortSignal.timeout(60_000),
      });
      if (r.status === 429 || r.status >= 500) throw new Error(`crt.sh ${r.status}`);
      if (!r.ok) throw new Error(`crt.sh ${r.status}`);
      return await r.json();
    } catch (e) {
      lastErr = e;
      if (attempt < 3) await new Promise(x => setTimeout(x, 2000 * attempt));
    }
  }
  throw lastErr;
}

async function main() {
  console.log(`certstream-ingest: lookback ${LOOKBACK_MIN} min, max ${MAX_DOMAINS} domains, ${QUERIES.length} shards (sequential)`);
  const cutoff = Date.now() - LOOKBACK_MIN * 60_000;

  // Fetch shards SEQUENTIALLY with delay — parallel = instant 429 from crt.sh.
  const allRows = [];
  for (const { q, label } of QUERIES) {
    try {
      const rows = await fetchCrtSh(q);
      console.log(`  shard[${label}]: ${rows.length} rows`);
      allRows.push(...rows);
    } catch (e) {
      console.warn(`  shard[${label}] failed: ${e.message}`);
    }
    await new Promise(r => setTimeout(r, SHARD_DELAY_MS));
  }

  // Fallback: if crt.sh gave us nothing, try CertSpotter (also free, less rate-limited).
  if (allRows.length === 0) {
    console.log("crt.sh returned no rows — trying CertSpotter fallback…");
    try {
      const rows = await fetchCertSpotter();
      console.log(`  certspotter: ${rows.length} rows`);
      allRows.push(...rows);
    } catch (e) {
      console.warn(`  certspotter failed: ${e.message}`);
    }
  }
  // Last resort: hit Google's CT log directly (public HTTP API, no rate-limit,
  // works from any IP including GitHub Actions runners). Pulls the last
  // ~2000 entries from Argon (currently active shard).
  if (allRows.length === 0) {
    console.log("all aggregators failed — pulling directly from Google CT log…");
    try {
      const rows = await fetchGoogleCT();
      console.log(`  google-ct: ${rows.length} rows`);
      allRows.push(...rows);
    } catch (e) {
      console.warn(`  google-ct failed: ${e.message}`);
    }
  }
  console.log(`total rows across sources: ${allRows.length}`);

  const seen = new Set();
  const domains = [];
  for (const row of allRows) {
    // crt.sh returns newest-first (limit=10000 → most-recent). Skip timestamp
    // filter — it drops valid rows because `entry_timestamp` can be null on
    // some records and CertSpotter uses `not_after` (future).
    const names = row._names || String(row.name_value || "").split("\n");
    for (const raw of names) {
      const host = raw.trim().toLowerCase();
      if (!host) continue;
      const reg = eTLDPlus1(host);
      if (!reg || !goodDomain(reg)) continue;
      const emit = KEEP_SUBDOMAINS ? host.replace(/^\*\./, "") : reg;
      if (!goodDomain(emit)) continue;
      if (seen.has(emit)) continue;
      seen.add(emit);
      domains.push(emit);
      if (domains.length >= MAX_DOMAINS) break;
    }
    if (domains.length >= MAX_DOMAINS) break;
  }
  console.log(`extracted ${domains.length} unique registrable domains`);
  if (!domains.length) return;

  const lines = domains.map(d => JSON.stringify({
    u: `https://${d}/`, t: d, d: `Newly discovered domain (CT log): ${d}`, c: "", src: "certstream"
  })).join("\n") + "\n";
  const buf = gzipSync(Buffer.from(lines));
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const rel = `hot/certstream-${ts}.ndjson.gz`;
  await putObject(rel, buf, "application/gzip");
  console.log(`wrote ${rel} (${(buf.length/1024).toFixed(1)} KB)`);
}

// CertSpotter API: returns recent cert issuances with dns_names expanded.
// Free public endpoint; no key required.
async function fetchCertSpotter() {
  const r = await fetch(CERTSPOTTER_URL, {
    headers: { "user-agent": UA, "accept": "application/json" },
    signal: AbortSignal.timeout(60_000),
  });
  if (!r.ok) throw new Error(`certspotter ${r.status}`);
  const j = await r.json();
  // Normalize into the row shape we use in main().
  return (j || []).map(issuance => ({
    _names: issuance.dns_names || [],
    not_after: issuance.not_after,
    _nofilter: true, // CertSpotter always returns fresh — no timestamp filter
  }));
}

// Google's public CT log (Argon 2026, currently active). RFC 6962 protocol.
// - GET /ct/v1/get-sth  → { tree_size }
// - GET /ct/v1/get-entries?start=N&end=M  → { entries: [{ leaf_input, ... }] }
// Then we scan the DER for [2] dNSName tags to extract hostnames.
// This is the exact same trick ct-tailer.mjs uses. Public, no auth, no rate-limit.
const GOOGLE_CT_LOG = "https://ct.googleapis.com/logs/us1/argon2026h2";

async function fetchGoogleCT() {
  const sthR = await fetch(`${GOOGLE_CT_LOG}/ct/v1/get-sth`, {
    headers: { "user-agent": UA }, signal: AbortSignal.timeout(20_000),
  });
  if (!sthR.ok) throw new Error(`get-sth ${sthR.status}`);
  const { tree_size } = await sthR.json();
  const end = tree_size - 1;
  const start = Math.max(0, end - 2047); // pull last ~2048 entries (~4 chunks)
  const rows = [];
  for (let s = start; s <= end; s += 256) {
    const e = Math.min(s + 255, end);
    const r = await fetch(`${GOOGLE_CT_LOG}/ct/v1/get-entries?start=${s}&end=${e}`, {
      headers: { "user-agent": UA }, signal: AbortSignal.timeout(45_000),
    });
    if (!r.ok) continue;
    const j = await r.json();
    for (const entry of j.entries || []) {
      const names = parseLeafForDnsNames(entry.leaf_input);
      if (names.length) rows.push({ _names: names, _nofilter: true });
    }
  }
  return rows;
}

// Extract dNSName SANs by scanning DER for the [2] context tag pattern (0x82 <len> <ascii>).
// Same technique as ct-tailer.mjs. Not a full ASN.1 parser but reliable for hostname pulls.
function parseLeafForDnsNames(b64) {
  const buf = Buffer.from(b64, "base64");
  if (buf.length < 12) return [];
  const entryType = buf.readUInt16BE(10);
  let der;
  if (entryType === 0) {
    const certLen = (buf[12] << 16) | (buf[13] << 8) | buf[14];
    der = buf.subarray(15, 15 + certLen);
  } else if (entryType === 1) {
    const off = 12 + 32;
    const tbsLen = (buf[off] << 16) | (buf[off + 1] << 8) | buf[off + 2];
    der = buf.subarray(off + 3, off + 3 + tbsLen);
  } else return [];
  const out = new Set();
  for (let i = 0; i < der.length - 4; i++) {
    if (der[i] !== 0x82) continue;
    const len = der[i + 1];
    if (len < 3 || len > 253) continue;
    let ok = true;
    for (let j = 0; j < len; j++) {
      const b = der[i + 2 + j];
      if (b < 0x20 || b > 0x7e) { ok = false; break; }
    }
    if (!ok) continue;
    const s = der.subarray(i + 2, i + 2 + len).toString("ascii").toLowerCase();
    if (!/^[a-z0-9*][a-z0-9.\-*]*[a-z0-9]$/.test(s)) continue;
    if (!s.includes(".")) continue;
    out.add(s);
    i += len + 1;
  }
  return [...out];
}

main().catch(e => { console.error("certstream-ingest error (non-fatal):", e.message); process.exit(0); });
