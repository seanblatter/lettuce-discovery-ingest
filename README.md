# lettuce-discovery-ingest

Public ingest pipeline for [Lettuce Discovery](https://lettuce.vision) — a search engine.

Parses [Common Crawl](https://commoncrawl.org/) WARC files → filtered NDJSON → indexed shards on Cloudflare R2.

## Architecture

- **WARC list** — `data/warcs.txt` (~9K URLs, one WARC segment each)
- **Claim-based coordinator** — `scripts/claim-next.mjs` atomically claims next unprocessed WARC via R2 conditional PUT
- **Streaming parser** — `scripts/parse-warc.mjs` reads WARC, extracts title/desc/text/lang, filters English + non-adult + Tranco-worthy, writes NDJSON
- **Matrix workflow** — `.github/workflows/ingest.yml` — 20 parallel jobs, each processes as many WARCs as it can in 5.5 hours, then exits cleanly
- **Watchdog** — `.github/workflows/watchdog.yml` — every 6 hours, re-triggers `ingest.yml` if there are WARCs left
- **Reshard** — `.github/workflows/reshard.yml` — after all WARCs done, merges raw NDJSON into 128 gzipped+brotli shards, uploads to R2 under `discovery/shards/`

## Idempotency guarantees

- Every WARC has a stable ID → NDJSON is written to `discovery/raw/{warc_id}.ndjson.gz`
- Re-running never duplicates work: only unclaimed WARCs are picked up
- Failures roll back the claim (via R2 delete of the `.claim` marker) so the WARC is re-processable
- Watchdog + retry policy means the job **cannot get stuck** — it will always resume until done

## Cost

- Repo is **public** → unlimited GitHub Actions minutes
- R2 storage for 1 B docs ≈ 100 GB × $0.015/GB = **~$1.35/mo**
- R2 egress: **free**
- Total: **~$0 one-time, ~$1.35/mo**

## Runtime

~48 hours end-to-end for 1 B docs (20 parallel workers).
