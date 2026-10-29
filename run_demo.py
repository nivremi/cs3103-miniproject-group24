#!/usr/bin/env python3
"""One-command LiveRecall public experiment runner.

`--runs N` means N independent experiments. The server is restarted for every
run and the median of key metrics is reported. `--repeat` is accepted as a
backward-compatible alias for `--runs`.
"""

from __future__ import annotations

import argparse
import json
import math
import socket
import statistics
import subprocess
import sys
import time
from pathlib import Path

from analyse_results import analyse_file, print_summary


def terminate_process(proc, name):
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def wait_for_port(port, proc, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("solution_server.py exited before becoming ready")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                return
        except OSError:
            time.sleep(0.15)
    raise RuntimeError("server did not listen within 10 seconds")


def default_source_segments(duration, source_start_delay):
    # Public generator is ~0.75 s/segment (2*0.20 revision delay + 0.35 gap).
    # Leave a small tail at the end so clients can drain/settle.
    active = max(3.0, duration - source_start_delay - 2.0)
    return max(4, math.floor(active / 0.75))


def one_run(args, run_no, here):
    uri = f"ws://127.0.0.1:{args.port}"
    suffix = f"_run{run_no:02d}" if args.runs > 1 else ""
    output_path = here / f"{args.output_stem}{suffix}.csv"
    json_path = here / f"{args.output_stem}{suffix}.json"

    segments = args.source_segments or default_source_segments(
        args.duration, args.source_start_delay
    )

    server = clients = None
    try:
        print(f"\n=== Run {run_no}/{args.runs} ===")
        print(f"Clients={args.clients}, duration={args.duration}s, source segments={segments}")

        server = subprocess.Popen(
            [sys.executable, "-u", "solution_server.py", "--port", str(args.port)],
            cwd=here,
            stdout=subprocess.DEVNULL if args.quiet_server else None,
            stderr=subprocess.STDOUT if args.quiet_server else None,
        )
        wait_for_port(args.port, server)

        client_cmd = [
            sys.executable, "-u", "load_clients.py",
            "--uri", uri,
            "--clients", str(args.clients),
            "--duration", str(args.duration),
            "--output", str(output_path),
            "--history-seconds", str(args.history_seconds),
            "--slow-percent", str(args.slow_percent),
            "--slow-ms", str(args.slow_ms),
            "--disconnect-percent", str(args.disconnect_percent),
            "--disconnect-at", str(args.disconnect_at),
            "--disconnect-seconds", str(args.disconnect_seconds),
            "--late-percent", str(args.late_percent),
            "--late-seconds", str(args.late_seconds),
            "--seed", str(args.seed),
        ]
        clients = subprocess.Popen(client_cmd, cwd=here)
        time.sleep(args.source_start_delay)
        if clients.poll() is not None:
            raise RuntimeError("load_clients.py exited early")

        source_rc = subprocess.run(
            [
                sys.executable, "-u", "caption_source.py",
                "--uri", uri,
                "--segments", str(segments),
                "--quiet",
            ],
            cwd=here,
        ).returncode
        if source_rc != 0:
            raise RuntimeError("caption_source.py failed")

        client_rc = clients.wait()
        if client_rc != 0:
            raise RuntimeError("load_clients.py failed")

        summary = analyse_file(
            output_path,
            expected_finals=segments,
            expected_updates=segments * 3,
            fresh_threshold_ms=args.fresh_threshold_ms,
        )
        print_summary(summary, args.fresh_threshold_ms)
        json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        return summary
    finally:
        terminate_process(clients, "load_clients.py")
        terminate_process(server, "solution_server.py")
        time.sleep(0.25)


def nested_value(d, path):
    cur = d
    for key in path.split("."):
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return cur


def print_medians(summaries):
    if len(summaries) < 2:
        return
    metrics = [
        ("P50 caption age", "p50_ms", "ms"),
        ("P95 caption age", "p95_ms", "ms"),
        ("P99 caption age", "p99_ms", "ms"),
        ("Fresh Caption Ratio", "fresh_pct", "%"),
        ("Fresh Final Caption Ratio", "fresh_final_pct", "%"),
        ("Final completeness", "final_completeness_pct", "%"),
        ("Normal Update Coverage", "normal_update_coverage_pct", "%"),
        ("Normal-client P95", "normal_clients.p95_ms", "ms"),
        ("Slow-client P95", "slow_clients.p95_ms", "ms"),
        ("Recovery P95", "recovery_p95_ms", "ms"),
    ]
    print("\n=== Median across independent runs ===")
    for label, path, unit in metrics:
        vals = [nested_value(s, path) for s in summaries]
        vals = [float(v) for v in vals if v is not None and not math.isnan(float(v))]
        if vals:
            print(f"{label:29s} {statistics.median(vals):10.2f} {unit}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--clients", type=int, default=20)
    p.add_argument("--duration", type=float, default=20)
    p.add_argument("--runs", "--repeat", dest="runs", type=int, default=1,
                   help="Independent experiment runs (use 3 for reported public tests)")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--output-stem", default="demo_results")
    p.add_argument("--source-segments", type=int,
                   help="Override automatically chosen number of source segments")
    p.add_argument("--history-seconds", type=int, default=30)
    p.add_argument("--fresh-threshold-ms", type=float, default=500.0)
    p.add_argument("--seed", type=int, default=3103)

    p.add_argument("--slow-percent", type=float, default=0)
    p.add_argument("--slow-ms", type=float, default=500)
    p.add_argument("--disconnect-percent", type=float, default=0)
    p.add_argument("--disconnect-at", type=float, default=10)
    p.add_argument("--disconnect-seconds", type=float, default=5)
    p.add_argument("--late-percent", type=float, default=0)
    p.add_argument("--late-seconds", type=float, default=5)
    p.add_argument("--source-start-delay", type=float, default=1.0)
    p.add_argument("--quiet-server", action="store_true")
    args = p.parse_args()

    if args.runs < 1:
        p.error("--runs must be >= 1")
    if args.duration <= 2:
        p.error("--duration should be > 2 seconds")

    here = Path(__file__).resolve().parent
    for name in ["solution_server.py", "load_clients.py", "caption_source.py", "analyse_results.py"]:
        if not (here / name).exists():
            raise SystemExit(f"Required file missing: {name}")

    summaries = []
    try:
        for run_no in range(1, args.runs + 1):
            summaries.append(one_run(args, run_no, here))
    except KeyboardInterrupt:
        raise SystemExit("Interrupted")
    except RuntimeError as e:
        raise SystemExit(str(e))

    print_medians(summaries)
    print("\nPublic run complete. CSV and JSON result files were written beside run_demo.py.")


if __name__ == "__main__":
    main()
