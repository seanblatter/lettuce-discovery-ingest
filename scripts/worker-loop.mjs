// scripts/worker-loop.mjs
// Long-running worker: claim → parse → release-on-fail → repeat until time budget exhausted.
// Designed to fit inside a 6-hour GitHub Actions runner (defaults to 5h20m budget).

import { claimNext, releaseClaim } from "./claim-next.mjs";
import { spawn } from "node:child_process";

const BUDGET_MIN = Number(process.env.BUDGET_MIN || 320); // 5h20m
const deadline = Date.now() + BUDGET_MIN * 60 * 1000;

function runParse(claim) {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, ["scripts/parse-warc.mjs"], {
      stdio: ["pipe", "inherit", "inherit"],
      env: { ...process.env },
    });
    child.stdin.end(JSON.stringify(claim));
    const t = setTimeout(() => { try { child.kill("SIGKILL"); } catch {} }, 40 * 60 * 1000); // 40m per WARC hard cap
    child.on("exit", (code) => { clearTimeout(t); resolve(code === 0); });
  });
}

let processed = 0, failed = 0;
while (Date.now() < deadline) {
  const claim = await claimNext();
  if (!claim) {
    console.log("[worker] no free WARCs, sleeping 30s");
    await new Promise(r => setTimeout(r, 30_000));
    continue;
  }
  console.log(`[worker] claimed ${claim.id}`);
  const ok = await runParse(claim);
  if (ok) {
    processed++;
    // Success: raw/{id}.ndjson.gz now exists, claim marker can be removed
    await releaseClaim(claim.claimKey);
  } else {
    failed++;
    console.error(`[worker] parse failed for ${claim.id}, releasing claim`);
    await releaseClaim(claim.claimKey);
  }
}

console.log(`[worker] done. processed=${processed} failed=${failed}`);
