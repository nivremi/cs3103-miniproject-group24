/* LiveRecall client-side caption state. No DOM; unit-tested with Node. */
(function (root) {
  "use strict";

  const DEFAULT_RETAIN_SECONDS = 120; // >= 60 s required; extra margin
  const MAX_SEGMENTS = 3000;          // hard cap on local memory

  function isPosInt(v) {
    return Number.isInteger(v) && v >= 1;
  }

  class CaptionStore {
    constructor(options) {
      const opts = options || {};
      this.retainSeconds = opts.retainSeconds || DEFAULT_RETAIN_SECONDS;
      this.segments = new Map();   // segment -> entry
      this.lastFinalSegment = 0;
      this.lastFinalRevision = 0;
      this.latestTs = 0;           // newest source_ts seen (lecture clock)
      this.prunedFloor = 0;        // segments <= this were pruned as old
    }

    // Apply one CAPTION_UPDATE. Returns {status, entry}.
    apply(msg, origin) {
      if (!msg || !isPosInt(msg.segment) || !isPosInt(msg.revision) ||
          typeof msg.final !== "boolean" || typeof msg.text !== "string") {
        return { status: "invalid" };
      }
      if (origin === "history" && !msg.final) {
        return { status: "invalid" };   // HISTORY must only carry finals
      }
      const seg = msg.segment;
      const existing = this.segments.get(seg);
      if (!existing && seg <= this.prunedFloor) {
        return { status: "expired" };
      }
      if (existing) {
        if (existing.final) {
          return { status: msg.revision === existing.revision ? "duplicate" : "after_final", entry: existing };
        }
        if (msg.revision === existing.revision) return { status: "duplicate", entry: existing };
        if (msg.revision < existing.revision) return { status: "stale", entry: existing };
      }

      const ts = typeof msg.source_ts === "number" ? msg.source_ts : Date.now() / 1000;
      const entry = {
        segment: seg,
        revision: msg.revision,
        final: msg.final,
        text: msg.text,
        source_ts: ts,
        received_at: Date.now(),
        origin: origin || "live",
      };
      this.segments.set(seg, entry);
      if (ts > this.latestTs) this.latestTs = ts;
      if (entry.final && (seg > this.lastFinalSegment ||
          (seg === this.lastFinalSegment && entry.revision > this.lastFinalRevision))) {
        this.lastFinalSegment = seg;
        this.lastFinalRevision = entry.revision;
      }
      this._prune();
      return { status: "accepted", entry };
    }

    /** Merge a HISTORY message; returns number of newly recovered finals. */
    applyHistory(items) {
      let recovered = 0;
      for (const item of Array.isArray(items) ? items : []) {
        if (this.apply(item, "history").status === "accepted") recovered += 1;
      }
      return recovered;
    }

    /** §4.2: resume from the latest FINALIZED state, never a provisional. */
    resumeState() {
      return { last_segment: this.lastFinalSegment, last_revision: this.lastFinalRevision };
    }

    /** All known segments, oldest first. */
    ordered() {
      return Array.from(this.segments.values()).sort((a, b) => a.segment - b.segment);
    }

    // Finalized captions from the last `seconds`, oldest first,
    // with {gap: n} markers for locally missing segments.
    recent(seconds) {
      const cutoff = this.latestTs - seconds;
      const finals = this.ordered().filter((e) => e.final && e.source_ts >= cutoff);
      const out = [];
      let prev = null;
      for (const e of finals) {
        if (prev !== null && e.segment - prev > 1) {
          let missing = 0;
          for (let s = prev + 1; s < e.segment; s++) {
            const m = this.segments.get(s);
            if (!m || !m.final) missing += 1;
          }
          if (missing > 0) out.push({ gap: missing });
        }
        out.push(e);
        prev = e.segment;
      }
      return out;
    }

    _prune() {
      const cutoff = this.latestTs - this.retainSeconds;
      const overflow = this.segments.size - MAX_SEGMENTS;
      if (overflow <= 0) {
        // O(n) with n ~ a few hundred: cheap, and correct even when HISTORY
        // inserted older segments after newer live ones.
        for (const [seg, e] of this.segments) {
          if (e.final && e.source_ts < cutoff) {
            this.segments.delete(seg);
            if (seg > this.prunedFloor) this.prunedFloor = seg;
          }
        }
        return;
      }
      const sorted = Array.from(this.segments.keys()).sort((a, b) => a - b);
      for (const seg of sorted.slice(0, overflow)) {
        this.segments.delete(seg);
        if (seg > this.prunedFloor) this.prunedFloor = seg;
      }
    }
  }

  root.CaptionStore = CaptionStore;
  if (typeof module !== "undefined" && module.exports) {
    module.exports = { CaptionStore };
  }
})(typeof window !== "undefined" ? window : globalThis);
