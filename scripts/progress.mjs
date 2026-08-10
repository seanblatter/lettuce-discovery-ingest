// scripts/progress.mjs
// Prints ingest progress: total WARCs, done, claimed (in-flight), remaining.
import { getObjectBuffer, listPrefix } from "./r2.mjs";

const list = (await getObjectBuffer("ingest/warcs.txt")).toString().split("\n").filter(Boolean);
const raws = await listPrefix("raw/");
const claims = await listPrefix("ingest/claims/");

const done = raws.length;
const claimed = claims.length;
const total = list.length;
const remaining = total - done;

console.log(JSON.stringify({
  total,
  done,
  claimed,
  remaining,
  pct: ((done / total) * 100).toFixed(2) + "%",
  raw_bytes: raws.reduce((s, o) => s + (o.Size || 0), 0),
}, null, 2));

// exit code 0 = still work to do, 3 = all done
process.exit(remaining === 0 ? 3 : 0);
