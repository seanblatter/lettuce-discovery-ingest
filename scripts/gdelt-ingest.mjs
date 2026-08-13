// scripts/gdelt-ingest.mjs
// Pulls the last N hours of GDELT v2 news mentions (updates every 15 min).
// Free, no auth. Produces high-quality news URL seeds + entity/tone metadata.
//
// Cron: hourly. Budget: ~5-15 GB/mo (capped by MAX_URLS_PER_RUN).
// GDELT master list: http://data.gdeltproject.org/gdeltv2/masterfilelist.txt

import { putObject } from "./r2.mjs";
import { gunzipSync, gzipSync, inflateRawSync } from "node:zlib";

const UA = "LettuceVision/1.0 (+https://lettuce.vision)";
const HOURS = Number(process.env.HOURS || 2);
const MAX_URLS_PER_RUN = Number(process.env.MAX_URLS || 60000);
const MASTER = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt";

async function fetchBuf(url) {
  const r = await fetch(url, { headers: { "user-agent": UA }, signal: AbortSignal.timeout(60000) });
  if (!r.ok) throw new Error(`${url} → ${r.status}`);
  return Buffer.from(await r.arrayBuffer());
}

// GDELT mentions CSV has: GLOBALEVENTID, EventTimeDate, MentionTimeDate,
// MentionType, MentionSourceName, MentionIdentifier(URL), SentenceID, ...tone fields
function parseMentions(text) {
  const out = [];
  for (const line of text.split("\n")) {
    if (!line) continue;
    const cols = line.split("\t");
    const url = cols[5];
    const source = cols[4] || "";
    const tone = parseFloat(cols[13] || "0");
    if (!url || !/^https?:\/\//.test(url)) continue;
    out.push({ u: url, src: "gdelt", host: source, tone: isNaN(tone) ? 0 : tone });
  }
  return out;
}

async function main() {
  console.log(`gdelt-ingest: last ${HOURS} h, cap ${MAX_URLS_PER_RUN} URLs`);
  const master = (await fetchBuf(MASTER)).toString("utf8");
  // Each line: size md5 url — filter to .mentions.CSV.zip files within window
  const cutoff = Date.now() - HOURS * 3600_000;
  const lines = master.split("\n").filter(l => l.includes(".mentions.CSV.zip"));
  const jobs = [];
  for (const l of lines) {
    const url = l.trim().split(/\s+/).pop();
    const m = url.match(/\/(\d{14})\.mentions\.CSV\.zip$/);
    if (!m) continue;
    const y = +m[1].slice(0,4), mo = +m[1].slice(4,6)-1, d = +m[1].slice(6,8),
          h = +m[1].slice(8,10), mi = +m[1].slice(10,12);
    const ts = Date.UTC(y, mo, d, h, mi);
    if (ts >= cutoff) jobs.push(url);
  }
  console.log(`${jobs.length} mention files in window`);
  if (!jobs.length) return;

  const docs = [];
  const seen = new Set();
  // Files are .CSV.zip. Node has no unzip built-in; but GDELT also serves .CSV.gz mirrors on
  // some endpoints. Use their v2 raw endpoint via the `unzip` trick: fetch and try gunzip
  // fallback. If zip: we skip. To keep zero-dep, we handle the .gz variant only.
  // GDELT actually offers .zip only for mentions. So we use `node:stream/promises` + minimal
  // ZIP central-directory read. Keep dep-free by parsing local file header ourselves.
  for (const url of jobs) {
    try {
      const buf = await fetchBuf(url);
      const text = extractFromZip(buf);
      if (!text) continue;
      for (const doc of parseMentions(text)) {
        if (seen.has(doc.u)) continue;
        seen.add(doc.u);
        docs.push(doc);
        if (docs.length >= MAX_URLS_PER_RUN) break;
      }
      if (docs.length >= MAX_URLS_PER_RUN) break;
    } catch (e) { console.warn("skip", url, e.message); }
  }

  console.log(`extracted ${docs.length} unique news URLs`);
  if (!docs.length) return;

  const nd = docs.map(d => JSON.stringify({
    u: d.u, t: "", d: `News mention (GDELT): ${d.host || ""}`, c: "",
    src: "gdelt", host: d.host, tone: d.tone,
  })).join("\n") + "\n";
  const gz = gzipSync(Buffer.from(nd));
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const rel = `hot/gdelt-${ts}.ndjson.gz`;
  await putObject(rel, gz, "application/gzip");
  console.log(`wrote ${rel} (${(gz.length/1024).toFixed(1)} KB)`);
}

// Minimal ZIP extractor: reads first local file header, inflates DEFLATE payload.
// Sufficient for GDELT mention zips which contain exactly one .CSV entry.
function extractFromZip(buf) {
  if (buf.length < 30 || buf.readUInt32LE(0) !== 0x04034b50) return null;
  const nameLen = buf.readUInt16LE(26);
  const extraLen = buf.readUInt16LE(28);
  const method = buf.readUInt16LE(8);
  const compSize = buf.readUInt32LE(18);
  const start = 30 + nameLen + extraLen;
  const payload = buf.subarray(start, start + compSize);
  if (method === 0) return payload.toString("utf8");
  if (method === 8) {
    // Raw deflate → wrap for zlib.inflateRawSync
    
    return inflateRawSync(payload).toString("utf8");
  }
  return null;
}

main().catch(e => { console.error(e); process.exit(1); });
