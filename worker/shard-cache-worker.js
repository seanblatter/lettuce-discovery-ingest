// worker/shard-cache-worker.js (item 12)
// Cloudflare Worker: sits in front of R2, serves shards from Cache API +
// stale-while-revalidate. Zero R2 Class B ops on cache hit.
//
// Deploy with wrangler:
//   cd worker && npx wrangler deploy
//
// Route: shards.lettuce.vision/*  →  this worker  →  R2 (fallback)

const R2_PUBLIC_BASE = "https://pub-XXXX.r2.dev/discovery"; // set to your public bucket URL
const CACHE_TTL_SECONDS = 6 * 3600; // 6h; deltas ensure freshness

export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);
    const cacheKey = new Request(url.toString(), req);
    const cache = caches.default;

    let response = await cache.match(cacheKey);
    if (response) {
      // Stale-while-revalidate: if age > half TTL, refresh in background
      const age = Number(response.headers.get("age") || 0);
      if (age > CACHE_TTL_SECONDS / 2) {
        ctx.waitUntil(refreshAndCache(url, cacheKey, cache));
      }
      return response;
    }

    response = await fetchFromR2(url);
    if (response.ok) {
      const cacheable = new Response(response.body, response);
      cacheable.headers.set("Cache-Control", `public, max-age=${CACHE_TTL_SECONDS}, s-maxage=${CACHE_TTL_SECONDS}`);
      ctx.waitUntil(cache.put(cacheKey, cacheable.clone()));
      return cacheable;
    }
    return response;
  },
};

async function fetchFromR2(url) {
  const target = R2_PUBLIC_BASE + url.pathname;
  return fetch(target, { cf: { cacheEverything: true, cacheTtl: 3600 } });
}

async function refreshAndCache(url, cacheKey, cache) {
  const fresh = await fetchFromR2(url);
  if (fresh.ok) await cache.put(cacheKey, fresh);
}
