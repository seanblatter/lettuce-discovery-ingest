// scripts/certstream-ingest.mjs
// Ingests newly-issued TLS certificates from public Certificate Transparency logs
// via crt.sh (JSON API, no auth). Produces URL seeds for freshly-registered domains.
//
// Cron: every 15 min. Budget: ~1 GB/mo. Extracts registrable domain, writes as
// hot/certstream-{ts}.ndjson.gz with { u, t, d, c:"" } placeholders — real doc
// content is fetched later by the sitemap crawler on next pass.

import { putObject } from "./r2.mjs";
import { gzipSync } from "node:zlib";

const UA = "LettuceVision/1.0 (+https://lettuce.vision)";
const LOOKBACK_MIN = Number(process.env.LOOKBACK_MIN || 25); // matches cron cadence + slack
const MAX_DOMAINS = Number(process.env.MAX_DOMAINS || 30000);
const KEEP_SUBDOMAINS = process.env.KEEP_SUBDOMAINS === "1"; // emit full hostnames, not just eTLD+1
// Query multiple crt.sh "views" in parallel to broaden coverage per run.
// Each query returns an independent ~10k slice; we dedupe across them.
const QUERIES = [
  { q: "%25", label: "wildcard" },       // broadest — any cert containing at least one char
  { q: "%25.com", label: "com" },        // .com bias
  { q: "%25.org", label: "org" },
  { q: "%25.net", label: "net" },
  { q: "%25.io", label: "io" },
  { q: "%25.app", label: "app" },
  { q: "%25.dev", label: "dev" },
  { q: "%25.ai", label: "ai" },
];

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
  console.log(`certstream-ingest: lookback ${LOOKBACK_MIN} min, max ${MAX_DOMAINS} domains, ${QUERIES.length} shards`);
  const cutoff = Date.now() - LOOKBACK_MIN * 60_000;

  // Fetch all shards in parallel, tolerate individual failures.
  const results = await Promise.allSettled(QUERIES.map(async ({ q, label }) => {
    try {
      const rows = await fetchCrtSh(q);
      console.log(`  shard[${label}]: ${rows.length} rows`);
      return rows;
    } catch (e) {
      console.warn(`  shard[${label}] failed: ${e.message}`);
      return [];
    }
  }));
  const allRows = results.flatMap(r => r.status === "fulfilled" ? r.value : []);
  console.log(`total rows across shards: ${allRows.length}`);

  const seen = new Set();
  const domains = [];
  for (const row of allRows) {
    const t = Date.parse(row.entry_timestamp || row.not_before || 0);
    if (t && t < cutoff) continue;
    const names = String(row.name_value || "").split("\n");
    for (const raw of names) {
      const host = raw.trim().toLowerCase();
      if (!host) continue;
      const reg = eTLDPlus1(host);
      if (!reg || !goodDomain(reg)) continue;
      // Emit either the full hostname (subdomains kept) or just the registrable domain.
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

  // Emit seed docs (u only — real crawl happens later via sitemap pass)
  const lines = domains.map(d => JSON.stringify({
    u: `https://${d}/`, t: d, d: `Newly discovered domain (CT log): ${d}`, c: "", src: "certstream"
  })).join("\n") + "\n";
  const buf = gzipSync(Buffer.from(lines));
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const rel = `hot/certstream-${ts}.ndjson.gz`;
  await putObject(rel, buf, "application/gzip");
  console.log(`wrote ${rel} (${(buf.length/1024).toFixed(1)} KB)`);
}

main().catch(e => { console.error("certstream-ingest error (non-fatal):", e.message); process.exit(0); });
