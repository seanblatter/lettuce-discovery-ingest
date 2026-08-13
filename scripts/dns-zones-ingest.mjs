// scripts/dns-zones-ingest.mjs
// Ingests DNS zone files → domain seed lists.
//
// Two sources:
//   1) ICANN CZDS (requires free CZDS_TOKEN registered per-TLD, ~1-2 day approval).
//      Enable by setting CZDS_TOKEN + CZDS_TLDS="com,net,org". These give ~200M domains.
//   2) Free/public zone dumps: .se, .nu, .ch, .li, .ee (published openly, no auth).
//
// Cron: weekly (Sunday 06:00 UTC). Budget: ~10-40 GB/mo depending on TLDs.
// Output: hot/zones-{tld}-{ts}.ndjson.gz — one line per registrable domain.

import { putObject } from "./r2.mjs";
import { gunzipSync, gzipSync } from "node:zlib";

const UA = "LettuceVision/1.0 (+https://lettuce.vision)";
const MAX_DOMAINS_PER_TLD = Number(process.env.MAX_DOMAINS_PER_TLD || 500000);

// Public zone URLs (no auth). These are updated daily and free to redistribute.
const PUBLIC_ZONES = {
  se: "https://zonedata.iis.se/se.zone",
  nu: "https://zonedata.iis.se/nu.zone",
  ch: "https://zonefiles.io/free/ch/",    // requires signup for full zone; skip if 401
  ee: "https://www.internet.ee/zone/ee.zone",
};

async function fetchZone(url) {
  const r = await fetch(url, { headers: { "user-agent": UA }, signal: AbortSignal.timeout(120_000) });
  if (!r.ok) throw new Error(`${url} → ${r.status}`);
  return Buffer.from(await r.arrayBuffer());
}

// Parse a BIND-format zone: pick lines that look like "domain.tld. IN NS ..."
// Also handles gzipped input transparently.
function extractDomains(buf, tldSuffix) {
  let text;
  if (buf[0] === 0x1f && buf[1] === 0x8b) text = gunzipSync(buf).toString("utf8");
  else text = buf.toString("utf8");
  const seen = new Set();
  const out = [];
  for (const line of text.split("\n")) {
    const m = line.match(/^([a-z0-9-]+(?:\.[a-z0-9-]+)*)\.\s+(?:\d+\s+)?IN\s+NS\b/i);
    if (!m) continue;
    let d = m[1].toLowerCase();
    // Zone lines can be subdomains; keep only registrable (name.tld)
    const parts = d.split(".");
    if (parts.length < 2) continue;
    if (!d.endsWith(tldSuffix)) continue;
    // registrable = last 2 labels for these ccTLDs
    d = parts.slice(-2).join(".");
    if (seen.has(d)) continue;
    seen.add(d);
    out.push(d);
    if (out.length >= MAX_DOMAINS_PER_TLD) break;
  }
  return out;
}

// CZDS: authenticate → download zone → parse
async function fetchCzds(tld) {
  const token = process.env.CZDS_TOKEN;
  if (!token) return null;
  const url = `https://czds-api.icann.org/czds/downloads/${tld}.zone`;
  const r = await fetch(url, {
    headers: { authorization: `Bearer ${token}`, "user-agent": UA },
    signal: AbortSignal.timeout(600_000),
  });
  if (!r.ok) { console.warn(`czds ${tld}: ${r.status}`); return null; }
  return Buffer.from(await r.arrayBuffer());
}

async function ingestOne(tld, buf) {
  const domains = extractDomains(buf, tld);
  console.log(`${tld}: extracted ${domains.length} registrable domains`);
  if (!domains.length) return;
  const nd = domains.map(d => JSON.stringify({
    u: `https://${d}/`, t: d, d: `Seed from ${tld} zone: ${d}`, c: "", src: `zone-${tld}`
  })).join("\n") + "\n";
  const gz = gzipSync(Buffer.from(nd));
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const rel = `hot/zones-${tld}-${ts}.ndjson.gz`;
  await putObject(rel, gz, "application/gzip");
  console.log(`  wrote ${rel} (${(gz.length/1e6).toFixed(2)} MB)`);
}

async function main() {
  // Public zones (no auth)
  for (const [tld, url] of Object.entries(PUBLIC_ZONES)) {
    try {
      const buf = await fetchZone(url);
      await ingestOne(tld, buf);
    } catch (e) { console.warn(`skip public ${tld}:`, e.message); }
  }
  // CZDS (com, net, org, biz, info, ...) — configured via env
  const czdsTlds = (process.env.CZDS_TLDS || "").split(",").map(s => s.trim()).filter(Boolean);
  for (const tld of czdsTlds) {
    try {
      const buf = await fetchCzds(tld);
      if (buf) await ingestOne(tld, buf);
    } catch (e) { console.warn(`skip czds ${tld}:`, e.message); }
  }
}

main().catch(e => { console.error(e); process.exit(1); });
