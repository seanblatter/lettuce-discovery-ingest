// scripts/parse-warc.mjs
// Streams a Common Crawl WARC.gz, extracts one NDJSON doc per response record,
// filters to English + likely-useful pages, gzips and uploads to R2 at raw/{id}.ndjson.gz.
//
// Extraction is intentionally cheap: title, meta description, first ~2KB of text
// stripped of tags, canonical URL, language. No heavy DOM parsing.
//
// Input:  path (relative CC path) via env WARC_PATH  (or JSON on stdin from claim-next)
// Output: raw/{id}.ndjson.gz on R2, plus a summary line to stdout.

import { putObject } from "./r2.mjs";
import { extractDoc, isGood } from "./lib-doc.mjs";
import { gzipSync } from "node:zlib";

const CC_HOST = "https://data.commoncrawl.org/";
const ALLOWED_LANGS = (process.env.ALLOWED_LANGS || "en").split(",");

async function* iterWarcRecords(stream) {
  // Streaming WARC parser. Delegates to zlib for gzip decoding.
  const { Readable } = await import("node:stream");
  const { createGunzip } = await import("node:zlib");
  const gunzip = createGunzip();
  const src = stream instanceof Readable ? stream : Readable.fromWeb(stream);
  src.pipe(gunzip);

  let buf = Buffer.alloc(0);
  for await (const chunk of gunzip) {
    buf = Buffer.concat([buf, chunk]);
    while (true) {
      const headEnd = buf.indexOf("\r\n\r\n");
      if (headEnd < 0) break;
      const headerText = buf.slice(0, headEnd).toString("latin1");
      const lenM = headerText.match(/Content-Length:\s*(\d+)/i);
      const typeM = headerText.match(/WARC-Type:\s*(\S+)/i);
      const uriM  = headerText.match(/WARC-Target-URI:\s*(\S+)/i);
      if (!lenM) break;
      const bodyLen = parseInt(lenM[1], 10);
      const totalLen = headEnd + 4 + bodyLen + 4; // \r\n\r\n after body
      if (buf.length < totalLen) break;
      const body = buf.slice(headEnd + 4, headEnd + 4 + bodyLen);
      buf = buf.slice(totalLen);
      if (typeM && typeM[1] === "response" && uriM) {
        yield { uri: uriM[1], body };
      }
    }
  }
}

function extractHttpBody(recordBody) {
  // Record body starts with an HTTP response header block, then \r\n\r\n, then HTML.
  const s = recordBody.indexOf("\r\n\r\n");
  if (s < 0) return "";
  return recordBody.slice(s + 4).toString("utf8");
}

async function main() {
  let claim = null;
  if (!process.env.WARC_PATH) {
    // read stdin JSON
    const chunks = [];
    for await (const c of process.stdin) chunks.push(c);
    if (!chunks.length) throw new Error("Need WARC_PATH env or JSON claim on stdin");
    claim = JSON.parse(Buffer.concat(chunks).toString());
    process.env.WARC_PATH = claim.path;
  }
  const warcPath = process.env.WARC_PATH;
  const id = (warcPath.match(/([^/]+)\.warc\.gz$/) || [null, "unknown"])[1];
  const url = CC_HOST + warcPath;

  console.error(`[parse] fetching ${url}`);
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);

  const docs = [];
  let seen = 0, kept = 0;
  try {
    for await (const rec of iterWarcRecords(res.body)) {
      seen++;
      try {
        const html = extractHttpBody(rec.body);
        if (!html) continue;
        const doc = extractDoc(rec.uri, html);
        if (isGood(doc, ALLOWED_LANGS)) { docs.push(doc); kept++; }
      } catch {}
      if (kept >= 60000) break; // safety cap per WARC
    }
  } catch (e) {
    console.error(`[parse] iterate error after ${seen} records: ${e.message}`);
  }

  const ndjson = docs.map(d => JSON.stringify(d)).join("\n") + "\n";
  const gz = gzipSync(Buffer.from(ndjson));
  await putObject(`raw/${id}.ndjson.gz`, gz, "application/x-ndjson", { ContentEncoding: "gzip" });
  console.error(`[parse] ${id}: seen=${seen} kept=${kept} bytes=${gz.length}`);
  console.log(JSON.stringify({ id, seen, kept, bytes: gz.length }));
}

main().catch(e => { console.error(e); process.exit(1); });
