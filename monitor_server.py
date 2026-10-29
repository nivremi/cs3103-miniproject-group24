#!/usr/bin/env python3
"""Small Ubuntu helper for observing solution_server.py RSS memory over time.

Examples:
  python monitor_server.py --match solution_server.py --duration 60
  python monitor_server.py --pid 12345 --duration 60 --output memory.csv

This is an observation aid, not part of the grading protocol.
"""

from __future__ import annotations

import argparse
import csv
import os
import time
from pathlib import Path


def find_pid(match: str) -> int | None:
    me = os.getpid()
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit() or int(entry.name) == me:
            continue
        try:
            cmd = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode(errors="ignore")
        except (OSError, PermissionError):
            continue
        if match in cmd:
            return int(entry.name)
    return None


def rss_kb(pid: int) -> int | None:
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except OSError:
        return None
    return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--pid", type=int)
    p.add_argument("--match", default="solution_server.py")
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("--duration", type=float, default=60.0)
    p.add_argument("--output", default="memory_rss.csv")
    args = p.parse_args()

    pid = args.pid or find_pid(args.match)
    if pid is None:
        raise SystemExit(f"No process found matching: {args.match}")

    print(f"Monitoring PID {pid}; output={args.output}")
    start = time.monotonic()
    rows = []
    while time.monotonic() - start <= args.duration:
        value = rss_kb(pid)
        if value is None:
            print("Process ended.")
            break
        elapsed = time.monotonic() - start
        mb = value / 1024.0
        rows.append((elapsed, mb))
        print(f"{elapsed:7.1f}s  {mb:8.2f} MB")
        time.sleep(args.interval)

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["elapsed_s", "rss_mb"])
        w.writerows((f"{a:.3f}", f"{b:.3f}") for a, b in rows)


if __name__ == "__main__":
    main()
