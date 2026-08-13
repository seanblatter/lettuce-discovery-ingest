# ct-tailer

Long-running Certificate Transparency log tailer. Captures **every TLS certificate issued on the public internet** in near-real-time (~250-300M certs/day, ~40-60M unique content-bearing hostnames/day after filtering).

Uploads batched, gzipped NDJSON to R2 under `discovery/ct-live/{yyyy}/{mm}/{dd}/{hh}-{seq}.ndjson.gz`. Picked up by the existing nightly reshard.

## Cost

- **Compute:** $0 (runs on Oracle Cloud Free ARM VM, 4c/24GB, forever free)
- **Egress:** $0 (CT logs are free public HTTP; R2 egress is free)
- **R2 storage:** ~$2.25/mo (~150 GB/mo new hostnames after dedup)
- **R2 writes:** ~$0.20/mo (batched to ~50k writes/mo)
- **Total: ~$2.50/mo**

## Deploy (Oracle Cloud Free VM)

1. Create free tier Ampere ARM VM: **4 OCPU / 24 GB RAM / 200 GB boot / Ubuntu 24.04**
2. SSH in, then:

```bash
curl -fsSL https://raw.githubusercontent.com/seanblatter/lettuce-discovery-ingest/main/ct-tailer/bootstrap.sh | sudo bash
sudo nano /etc/ct-tailer.env    # fill R2 creds
sudo systemctl start ct-tailer
sudo journalctl -u ct-tailer -f
```

## How it works

- Fetches Google's canonical CT log list (`gstatic.com/ct/log_list/v3/log_list.json`) — ~40 active logs (Argon, Xenon, Nimbus, Oak, Yeti, Sabre, …)
- One async tail loop per log (`get-sth` → `get-entries` → advance offset)
- Each cert's SANs/CN are extracted via a lightweight DER scan for `[2] dNSName` tags
- In-mem 24 h dedup + wildcard/adult/bad-TLD filters
- Batches to 100 k hostnames or 5 min, whichever first
- Writes atomic per-log offset to `./state/{log-slug}.offset` so restarts resume cleanly
- systemd `Restart=always` keeps it up forever

## Filters (applied inline before R2 write)

- Wildcards (`*.foo.com`) dropped — they don't reveal a host
- Bad TLDs: `.onion`, `.zip`, `.invalid`, `.local`, `.test`, `.example`
- Adult keyword regex
- 24 h in-memory rolling dedup per host

Long-term dedup + LE-renewal suppression happens downstream in `scripts/dedup-and-cap.mjs`.

## What you'll see in R2

```
discovery/ct-live/2026/08/13/14-a1b2c3d4e5.ndjson.gz
discovery/ct-live/2026/08/13/14-f6e7d8c9b0.ndjson.gz
...
```

Each line:
```json
{"u":"https://blog.acme.com/","t":"blog.acme.com","d":"CT:google-argon2026","c":"","src":"ct","log":"google-argon2026","ts":1723556789012}
```

## Restart / redeploy

```bash
cd /opt/ct-tailer && sudo -u ct git pull   # if you clone directly
sudo systemctl restart ct-tailer
```

## Monitor

```bash
sudo journalctl -u ct-tailer -f            # live logs
tail -f /var/log/ct-tailer.log             # or the file
```

Expected output at steady state:
```
[flush] 100000 hosts → discovery/ct-live/... (2453.1 KB)
[google-argon2026] err @412335: entries: 429     # occasional rate-limit, auto-retried
[dedup] pruned 45210, size=1823410
```
