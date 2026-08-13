// scripts/httparchive-signals.mjs
// Pulls quality/ranking signals from the HTTP Archive public BigQuery dataset.
// Enriches ~50M top URLs monthly with: tech stack, page speed, Lighthouse, meta.
//
// Cron: monthly (1st of month, 05:00 UTC). Budget: ~5 GB/mo.
// Auth: needs GCP_SA_KEY (service account JSON, free tier: 1 TB queries/mo).
//
// Output: signals/httparchive-{yyyymm}.ndjson.gz — { u, tech, lcp, cls, https, mobile, ct }
// Used later by pagerank/quality stage as a ranking multiplier.

import { putObject } from "./r2.mjs";
import { gzipSync } from "node:zlib";

const MAX_ROWS = Number(process.env.MAX_ROWS || 50_000_000);
const KEY_JSON = process.env.GCP_SA_KEY;

if (!KEY_JSON) {
  console.log("httparchive-signals: no GCP_SA_KEY set — skipping (this is optional)");
  process.exit(0);
}

// Get an access token via JWT-signed OAuth2 (no gcloud dep needed).
import { createSign } from "node:crypto";
async function accessToken() {
  const sa = JSON.parse(KEY_JSON);
  const now = Math.floor(Date.now() / 1000);
  const header = Buffer.from(JSON.stringify({ alg: "RS256", typ: "JWT" })).toString("base64url");
  const payload = Buffer.from(JSON.stringify({
    iss: sa.client_email,
    scope: "https://www.googleapis.com/auth/bigquery.readonly",
    aud: "https://oauth2.googleapis.com/token",
    exp: now + 3600, iat: now,
  })).toString("base64url");
  const sig = createSign("RSA-SHA256").update(`${header}.${payload}`).sign(sa.private_key, "base64url");
  const jwt = `${header}.${payload}.${sig}`;
  const r = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=${jwt}`,
  });
  const j = await r.json();
  if (!j.access_token) throw new Error("token: " + JSON.stringify(j));
  return { token: j.access_token, project: sa.project_id };
}

// Query most-recent httparchive.summary_pages_mobile table.
// (Public dataset; you're billed for scan bytes against your own project — <1 TB/mo free.)
const SQL = `
  SELECT
    url,
    APPROX_TOP_COUNT(tech, 8) AS tech_arr,
    AVG(lighthouse_performance) AS perf,
    AVG(lighthouse_seo) AS seo,
    ANY_VALUE(https) AS https,
    ANY_VALUE(mobile) AS mobile
  FROM \`httparchive.crawl.pages\`
  WHERE date = (SELECT MAX(date) FROM \`httparchive.crawl.pages\`)
  GROUP BY url
  LIMIT @lim
`;

async function main() {
  const { token, project } = await accessToken();
  console.log(`httparchive-signals: project=${project}, limit=${MAX_ROWS}`);

  const jobRes = await fetch(`https://bigquery.googleapis.com/bigquery/v2/projects/${project}/queries`, {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify({
      query: SQL,
      useLegacySql: false,
      parameterMode: "NAMED",
      queryParameters: [{ name: "lim", parameterType: { type: "INT64" }, parameterValue: { value: String(MAX_ROWS) } }],
      timeoutMs: 60000,
      maxResults: 100000,
    }),
  });
  const job = await jobRes.json();
  if (job.error) throw new Error(JSON.stringify(job.error));
  const jobId = job.jobReference.jobId;

  const rows = [];
  let pageToken;
  do {
    const url = new URL(`https://bigquery.googleapis.com/bigquery/v2/projects/${project}/queries/${jobId}`);
    url.searchParams.set("maxResults", "100000");
    if (pageToken) url.searchParams.set("pageToken", pageToken);
    const r = await fetch(url, { headers: { authorization: `Bearer ${token}` } });
    const j = await r.json();
    if (!j.jobComplete) { await new Promise(x => setTimeout(x, 3000)); continue; }
    for (const row of j.rows || []) {
      const f = row.f.map(x => x.v);
      rows.push({ u: f[0], tech: f[1], perf: +f[2] || null, seo: +f[3] || null, https: !!f[4], mobile: !!f[5] });
      if (rows.length >= MAX_ROWS) break;
    }
    pageToken = j.pageToken;
    if (rows.length % 500000 < 100000) console.log(`  ${rows.length} rows…`);
  } while (pageToken && rows.length < MAX_ROWS);

  console.log(`fetched ${rows.length} rows`);
  if (!rows.length) return;

  const yyyymm = new Date().toISOString().slice(0,7).replace("-", "");
  const nd = rows.map(r => JSON.stringify(r)).join("\n") + "\n";
  const gz = gzipSync(Buffer.from(nd));
  const rel = `signals/httparchive-${yyyymm}.ndjson.gz`;
  await putObject(rel, gz, "application/gzip");
  console.log(`wrote ${rel} (${(gz.length/1e6).toFixed(1)} MB)`);
}

main().catch(e => { console.error(e); process.exit(1); });
