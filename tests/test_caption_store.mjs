// Unit tests for client/caption_store.js. Run: node --test tests/
import { test } from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { CaptionStore } = require("../client/caption_store.js");

const u = (segment, revision, final = false, ts = 1000 + segment, text) =>
  ({ type: "CAPTION_UPDATE", session: "L1", segment, revision, final, source_ts: ts,
     text: text ?? `s${segment} r${revision}` });

test("newer revision replaces older; finals are immutable", () => {
  const s = new CaptionStore();
  assert.equal(s.apply(u(1, 1), "live").status, "accepted");
  assert.equal(s.apply(u(1, 2), "live").status, "accepted");
  assert.equal(s.apply(u(1, 1), "live").status, "stale");
  assert.equal(s.apply(u(1, 2), "live").status, "duplicate");
  assert.equal(s.apply(u(1, 3, true), "live").status, "accepted");
  assert.equal(s.apply(u(1, 4), "live").status, "after_final");
  assert.equal(s.segments.size, 1);
  assert.equal(s.segments.get(1).text, "s1 r3");
});

test("resume state tracks the latest FINAL, not provisional", () => {
  const s = new CaptionStore();
  s.apply(u(1, 3, true), "live");
  s.apply(u(2, 1), "live");
  assert.deepEqual(s.resumeState(), { last_segment: 1, last_revision: 3 });
});

test("HISTORY merge recovers missing finals without duplicates", () => {
  const s = new CaptionStore();
  s.apply(u(1, 3, true), "live");
  s.apply(u(2, 3, true), "live");
  const n = s.applyHistory([u(2, 3, true), u(3, 3, true), u(4, 3, true), u(5, 1, false)]);
  assert.equal(n, 2);                       // 2 dup, 5 provisional rejected
  assert.equal(s.segments.has(5), false);
  assert.deepEqual(s.resumeState(), { last_segment: 4, last_revision: 3 });
});

test("recent(seconds) uses lecture time and marks gaps", () => {
  const s = new CaptionStore();
  for (let seg = 1; seg <= 70; seg++) if (seg < 40 || seg > 42) s.apply(u(seg, 1, true, 1000 + seg), "live");
  const last30 = s.recent(30).filter((x) => !x.gap);
  assert.equal(last30[0].segment, 43);       // 40..42 missing
  assert.equal(last30.at(-1).segment, 70);
  const last60 = s.recent(60);
  assert.ok(last60.some((x) => x.gap === 3));
  assert.equal(s.recent(15).length, 16);    // 55..70 inclusive
});

test("local history is bounded but keeps >= 60 s", () => {
  const s = new CaptionStore({ retainSeconds: 120 });
  for (let seg = 1; seg <= 1000; seg++) s.apply(u(seg, 1, true, 1000 + seg), "live");
  assert.ok(s.segments.size <= 122);
  assert.ok(s.recent(60).length >= 60);
  assert.equal(s.apply(u(3, 1, true), "history").status, "expired");
});

test("invalid messages rejected", () => {
  const s = new CaptionStore();
  assert.equal(s.apply({ segment: "1", revision: 1, final: true, text: "x" }).status, "invalid");
  assert.equal(s.apply({ segment: 1, revision: 0, final: true, text: "x" }).status, "invalid");
  assert.equal(s.apply({ segment: 1, revision: 1, final: "yes", text: "x" }).status, "invalid");
});
