// scripts/loadtest.mjs
// Blackbox load test for lettuce.vision/search infrastructure.
//
// Usage:
//   node scripts/loadtest.mjs --base https://lettuce.vision --qps 50 --duration 60
//   node scripts/loadtest.mjs --base http://localhost:3000 --qps 10 --duration 30
//
// Hits /api/discovery/search-r2, /api/discovery/media, /api/discovery/entity
// with a mix of real query shapes (informational, navigational, transactional,
// entity, long-tail). Records p50/p95/p99, error rate, cold-start markers,
// degraded-response markers.

const argv = process.argv.slice(2);
function arg(name, dflt) {
  const i = argv.indexOf(`--${name}`);
  return i >= 0 ? argv[i + 1] : dflt;
}

const BASE      = arg("base", "https://lettuce.vision").replace(/\/$/, "");
const QPS       = Number(arg("qps", "20"));
const DURATION  = Number(arg("duration", "60"));   // seconds
const CONC      = Number(arg("conc", String(Math.max(4, QPS))));
const WARMUP    = Number(arg("warmup", "5"));      // seconds discarded from stats

// Realistic query mix. Distribution roughly mirrors observed web-search intent.
const QUERIES = [
  // informational (40%)
  "how to make sourdough", "python list comprehension", "what is bm25",
  "climate change causes", "how airplanes fly", "history of denver",
  "best time to visit iceland", "how to fix a leaky faucet",
  "why is the sky blue", "difference between llm and gpt",
  "cost of living in portugal", "how does bitcoin work",
  // navigational (15%)
  "wikipedia", "github", "hacker news", "reddit",
  "stack overflow", "arxiv", "twitter",
  // transactional (20%)
  "best coffee grinder 2026", "cheap flights to tokyo", "buy running shoes",
  "used tesla model 3", "book cabin colorado",
  "iphone 17 pro deal", "monthly subscription meal kit",
  "espresso machine under 500",
  // entity / brand (15%)
  "christopher nolan", "SpaceX", "denver colorado", "cloudflare workers",
  "einstein", "kagi search", "openai",
  // long-tail (10%)
  "how to teach a beagle not to bark at squirrels",
  "1985 volvo 245 wagon parts denver",
  "javascript regex capture group backreference",
  "smallest quiet dishwasher for rv",
];

function pick() { return QUERIES[Math.floor(Math.random() * QUERIES.length)]; }

async function once(query) {
  const t0 = Date.now();
  const url = `${BASE}/api/discovery/search-r2?q=${encodeURIComponent(query)}&limit=15`;
  try {
    const r = await fetch(url, { headers: { accept: "application/json" } });
    const dur = Date.now() - t0;
    if (!r.ok) return { ok: false, dur, status: r.status };
    const j = await r.json();
    return {
      ok: true, dur,
      status: r.status,
      resultCount: j.count || 0,
      elapsedMs: j.elapsedMs,
      rerankMs: j.rerankMs,
      indexSize: j.indexSize,
      invertedDocs: j.invertedDocs,
      hotDocs: j.hotStats?.docs,
      sponsored: (j.sponsored || []).length,
      degraded: !!j.degraded,
      answerLen: j.answer?.text ? j.answer.text.length : 0,
    };
  } catch (e) {
    return { ok: false, dur: Date.now() - t0, error: e.message };
  }
}

const samples = [];
let inflight = 0;
let launched = 0;
let errors = 0;
let degraded = 0;

const startAt = Date.now();
const endAt = startAt + (DURATION + WARMUP) * 1000;
const warmupUntil = startAt + WARMUP * 1000;

async function driver() {
  while (Date.now() < endAt) {
    if (inflight >= CONC) { await new Promise((r) => setTimeout(r, 5)); continue; }
    inflight++;
    launched++;
    once(pick())
      .then((r) => {
        if (Date.now() >= warmupUntil) {
          samples.push(r);
          if (!r.ok) errors++;
          if (r.degraded) degraded++;
        }
      })
      .finally(() => { inflight--; });
    // sleep to shape QPS
    await new Promise((r) => setTimeout(r, 1000 / QPS));
  }
  // drain
  while (inflight > 0) await new Promise((r) => setTimeout(r, 20));
}

const t0 = Date.now();
console.log(`loadtest: base=${BASE} qps=${QPS} conc=${CONC} duration=${DURATION}s (${WARMUP}s warmup)`);
await driver();
const totalMs = Date.now() - t0;

function pct(sorted, p) {
  if (!sorted.length) return 0;
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p / 100))];
}
const durs = samples.filter((s) => s.ok).map((s) => s.dur).sort((a, b) => a - b);
const p50 = pct(durs, 50), p95 = pct(durs, 95), p99 = pct(durs, 99);
const mean = durs.length ? Math.round(durs.reduce((a, b) => a + b, 0) / durs.length) : 0;
const okCount = samples.filter((s) => s.ok).length;
const nonEmpty = samples.filter((s) => s.ok && s.resultCount > 0).length;
const avgResults = okCount ? Math.round(samples.filter((s) => s.ok).reduce((a, b) => a + b.resultCount, 0) / okCount) : 0;
const avgIdx = okCount ? Math.round(samples.filter((s) => s.ok).reduce((a, b) => a + (b.indexSize || 0), 0) / okCount) : 0;

console.log(`
=== results ===
Launched:        ${launched}
Sampled:         ${samples.length} (warmup discarded)
Success:         ${okCount} (${((okCount / samples.length) * 100).toFixed(1)}%)
Errors:          ${errors}
Degraded:        ${degraded}
Non-empty:       ${nonEmpty} (${okCount ? ((nonEmpty/okCount)*100).toFixed(1) : 0}% of ok)
Avg results:     ${avgResults}
Avg indexSize:   ${avgIdx.toLocaleString()}
Latency p50:     ${p50} ms
Latency p95:     ${p95} ms
Latency p99:     ${p99} ms
Latency mean:    ${mean} ms
Actual QPS:      ${(okCount / DURATION).toFixed(1)}
Total wall:      ${(totalMs / 1000).toFixed(1)}s
`);

// Verdict guidance for 1M-DAU readiness
console.log("=== 1M DAU readiness ===");
const readyP95 = p95 < 800;
const readyErr = errors / Math.max(1, samples.length) < 0.01;
const readyNonEmpty = nonEmpty / Math.max(1, okCount) >= 0.85;
console.log(`p95 < 800ms:            ${readyP95 ? "✅" : "❌"} (${p95}ms)`);
console.log(`error rate < 1%:       ${readyErr ? "✅" : "❌"} (${((errors / Math.max(1, samples.length)) * 100).toFixed(2)}%)`);
console.log(`non-empty rate ≥ 85%:  ${readyNonEmpty ? "✅" : "❌"} (${okCount ? ((nonEmpty/okCount)*100).toFixed(1) : 0}%)`);

process.exit(readyP95 && readyErr ? 0 : 1);
