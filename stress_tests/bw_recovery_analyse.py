#!/usr/bin/env python3
"""Timeline for stress_tests/bw_recovery.sh.

Per 5 s bucket of source time: revision coverage (every client should get
revisions 1..3 of every segment), finals coverage, and P50/P95 caption age,
plus the server's degraded_now count. Recovery = how long after the limit is
removed until coverage is back at ~100% and degraded_now is 0.

Usage: python stress_tests/bw_recovery_analyse.py stress_tests/results/bw_recovery
"""

from __future__ import annotations

import csv
import re
import sys
from collections import defaultdict
from datetime import datetime

BUCKET_S = 5.0
REVISIONS_PER_SEGMENT = 3


def pct(xs, q):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))]


def main(stem: str) -> None:
    t0, on_at, off_at, segments, nclients = open(f"{stem}.meta").read().split()
    t0, on_at, off_at = float(t0), float(on_at), float(off_at)

    got = set()                         # (client, segment, revision), live only
    finals = set()                      # (client, segment), live or HISTORY
    seg_t: dict[int, float] = {}        # segment -> source time of revision 1
    ages = defaultdict(list)            # bucket -> caption ages (ms)
    clients = set()
    with open(f"{stem}.csv", newline="") as f:
        for r in csv.DictReader(f):
            clients.add(r["client_id"])
            if r["event_type"] == "CAPTION_UPDATE" and r["status"] == "accepted":
                seg, rev = int(r["segment"]), int(r["revision"])
                ts = float(r["source_ts"]) - t0
                seg_t[seg] = min(seg_t.get(seg, ts), ts)
                got.add((r["client_id"], seg, rev))
                if r["final"] in ("1", "True", "true"):
                    finals.add((r["client_id"], seg))
                ages[int(ts // BUCKET_S)].append(float(r["latency_ms"]))
            elif r["event_type"] == "HISTORY_ITEM":
                finals.add((r["client_id"], int(r["segment"])))

    # degraded_now from the server's periodic stats lines
    degraded = defaultdict(int)
    for line in open(f"{stem}.server.log"):
        m = re.search(r"^(\S+ \S+),\d+ .*degraded_now=(\d+)", line)
        if m:
            t = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").timestamp() - t0
            if t >= 0:
                b = int(t // BUCKET_S)
                degraded[b] = max(degraded[b], int(m.group(2)))

    n = len(clients)
    by_bucket = defaultdict(list)
    for seg, ts in seg_t.items():
        by_bucket[int(ts // BUCKET_S)].append(seg)

    print(f"clients={n}  limit ON at {on_at:.0f}s, OFF at {off_at:.0f}s")
    print(f"{'t (s)':>9} {'phase':>6} {'rev cov':>8} {'final cov':>9} "
          f"{'P50 ms':>8} {'P95 ms':>8} {'degraded':>8}")
    for b in sorted(by_bucket):
        segs = by_bucket[b]
        rev = sum((c, s, r) in got for c in clients for s in segs
                  for r in range(1, REVISIONS_PER_SEGMENT + 1))
        fin = sum((c, s) in finals for c in clients for s in segs)
        lo = b * BUCKET_S
        phase = "OFF" if lo < on_at else ("ON" if lo < off_at else "after")
        print(f"{lo:4.0f}-{lo + BUCKET_S:<4.0f} {phase:>6} "
              f"{100 * rev / (n * len(segs) * REVISIONS_PER_SEGMENT):7.1f}% "
              f"{100 * fin / (n * len(segs)):8.1f}% "
              f"{pct(ages[b], 0.5):8.0f} {pct(ages[b], 0.95):8.0f} "
              f"{degraded.get(b, 0):8d}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "stress_tests/results/bw_recovery")
