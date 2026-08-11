// scripts/lib-doc.mjs — shared doc extraction (used by parse-warc, sitemap, rss)
import { createHash } from "node:crypto";

const BAD_TLDS = /\.(zip|onion)$/i;
const ADULT_WORDS = /\b(porn|xxx|escort|nsfw|hentai|cams?)\b/i;
const MIN_TEXT_LEN = Number(process.env.MIN_TEXT_LEN || 120);
const MAX_TEXT_LEN = 2000;

export function stripTags(html) {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ").replace(/&amp;/g, "&").replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">").replace(/&quot;/g, '"').replace(/&#39;/g, "'")
    .replace(/\s+/g, " ").trim();
}

export function extractDoc(url, html) {
  const titleM = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  const descM = html.match(/<meta[^>]+name=["']description["'][^>]+content=["']([^"']+)["']/i)
             || html.match(/<meta[^>]+content=["']([^"']+)["'][^>]+name=["']description["']/i);
  const canonM = html.match(/<link[^>]+rel=["']canonical["'][^>]+href=["']([^"']+)["']/i);
  const langM = html.match(/<html[^>]+lang=["']([a-zA-Z-]+)["']/i);
  const title = titleM ? stripTags(titleM[1]).slice(0, 200) : "";
  const desc  = descM  ? descM[1].slice(0, 400) : "";
  const canon = canonM ? canonM[1] : url;
  const lang  = langM  ? langM[1].toLowerCase().split("-")[0] : "";
  const bodyStart = html.search(/<body[\s>]/i);
  const body = bodyStart >= 0 ? html.slice(bodyStart, bodyStart + 40000) : html.slice(0, 40000);
  const text = stripTags(body).slice(0, MAX_TEXT_LEN);
  return { u: canon, t: title, d: desc, c: text, l: lang, s: simhash(title + " " + text) };
}

export function isGood(doc, allowedLangs = ["en"]) {
  if (!doc.t || doc.t.length < 5) return false;
  if (!doc.c || doc.c.length < MIN_TEXT_LEN) return false;
  if (doc.l && !allowedLangs.includes(doc.l)) return false;
  if (BAD_TLDS.test(doc.u)) return false;
  if (ADULT_WORDS.test(doc.u) || ADULT_WORDS.test(doc.t)) return false;
  return true;
}

// 64-bit simhash for near-dup detection
export function simhash(text) {
  const tokens = text.toLowerCase().split(/\W+/).filter(w => w.length > 2);
  const v = new Int32Array(64);
  for (const tok of tokens) {
    const h = createHash("md5").update(tok).digest();
    // read low 8 bytes as 64 bits
    for (let bit = 0; bit < 64; bit++) {
      const byte = h[bit >> 3];
      const set = (byte >> (bit & 7)) & 1;
      v[bit] += set ? 1 : -1;
    }
  }
  const hi = [], lo = [];
  for (let i = 0; i < 32; i++) hi.push(v[i] > 0 ? "1" : "0");
  for (let i = 32; i < 64; i++) lo.push(v[i] > 0 ? "1" : "0");
  const hex = (parseInt(hi.join(""), 2)).toString(16).padStart(8, "0")
            + (parseInt(lo.join(""), 2)).toString(16).padStart(8, "0");
  return hex;
}
