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
const LOOKBACK_MIN = Number(process.env.LOOKBACK_MIN || 20); // matches cron cadence + slack
const MAX_DOMAINS = Number(process.env.MAX_DOMAINS || 8000);

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

async function fetchCrtSh() {
  // crt.sh JSON: recent CT entries. Query returns up to ~10k rows.
  // We poll for certs issued in the lookback window.
  const url = `https://crt.sh/?q=%25&output=json&exclude=expired&limit=10000`;
  const r = await fetch(url, {
    headers: { "user-agent": UA, "accept": "application/json" },
    signal: AbortSignal.timeout(45000),
  });
  if (!r.ok) throw new Error(`crt.sh ${r.status}`);
  return await r.json();
}

async function main() {
  console.log(`certstream-ingest: lookback ${LOOKBACK_MIN} min, max ${MAX_DOMAINS} domains`);
  const cutoff = Date.now() - LOOKBACK_MIN * 60_000;
  let rows;
  try { rows = await fetchCrtSh(); }
  catch (e) { console.error("crt.sh fetch failed:", e.message); return; }

  const seen = new Set();
  const domains = [];
  for (const row of rows) {
    const t = Date.parse(row.entry_timestamp || row.not_before || 0);
    if (t && t < cutoff) continue;
    const names = String(row.name_value || "").split("\n");
    for (const raw of names) {
      const host = raw.trim().toLowerCase();
      if (!host) continue;
      const d = eTLDPlus1(host);
      if (!d || !goodDomain(d)) continue;
      if (seen.has(d)) continue;
      seen.add(d);
      domains.push(d);
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

main().catch(e => { console.error(e); process.exit(1); });
