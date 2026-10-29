#!/usr/bin/env python3
"""Analyse CSV logs produced by the supplied LiveRecall load client."""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import Counter, defaultdict
from pathlib import Path


def percentile(values, p):
    if not values:
        return float("nan")
    xs = sorted(values)
    if len(xs) == 1:
        return xs[0]
    k = (len(xs) - 1) * p / 100.0
    lo = math.floor(k)
    hi = math.ceil(k)
    if lo == hi:
        return xs[lo]
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def pct(n, d):
    return (100.0 * n / d) if d else float("nan")


def safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def role_stats(rows, slow_value, threshold_ms):
    subset = [
        r for r in rows
        if r["event_type"] == "CAPTION_UPDATE"
        and r.get("slow", "0") == str(slow_value)
        and r.get("status") == "accepted"
        and r.get("latency_ms")
    ]
    lat = [float(r["latency_ms"]) for r in subset]
    if not lat:
        return None
    return {
        "messages": len(lat),
        "p50_ms": percentile(lat, 50),
        "p95_ms": percentile(lat, 95),
        "fresh_pct": pct(sum(x <= threshold_ms for x in lat), len(lat)),
    }


def analyse_file(csv_file, expected_finals=None, expected_updates=None,
                 fresh_threshold_ms=500.0):
    with open(csv_file, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    caption_rows = [r for r in rows if r["event_type"] in ("CAPTION_UPDATE", "HISTORY_ITEM")]
    accepted_live = [
        r for r in rows
        if r["event_type"] == "CAPTION_UPDATE" and r["status"] == "accepted"
    ]
    accepted_or_recovered = [
        r for r in caption_rows if r["status"] in ("accepted", "recovered")
    ]
    live_finals = [r for r in accepted_live if r["final"] == "1"]

    latencies = [float(r["latency_ms"]) for r in accepted_live if r["latency_ms"]]
    # Earliest correct delivery time for each generated final state, per client.
    # Using unique (client, segment) keys prevents duplicates from inflating the metric.
    final_first_latency = {}
    for r in accepted_or_recovered:
        if r["final"] != "1" or not r["latency_ms"]:
            continue
        key = (r["client_id"], int(r["segment"]))
        value = float(r["latency_ms"])
        if key not in final_first_latency or value < final_first_latency[key]:
            final_first_latency[key] = value

    clients = sorted({r["client_id"] for r in rows if r["client_id"] != ""})
    status_counts = Counter(r["status"] for r in caption_rows)

    final_by_client = defaultdict(set)
    live_update_by_client = defaultdict(set)
    for r in accepted_or_recovered:
        if r["final"] == "1":
            final_by_client[r["client_id"]].add(int(r["segment"]))
    for r in accepted_live:
        live_update_by_client[r["client_id"]].add((int(r["segment"]), int(r["revision"])))

    summary = {
        "clients": len(clients),
        "live_caption_messages": sum(r["event_type"] == "CAPTION_UPDATE" for r in rows),
        "recovered_history_items": sum(r["event_type"] == "HISTORY_ITEM" for r in rows),
        "accepted": status_counts["accepted"],
        "recovered": status_counts["recovered"],
        "duplicates": status_counts["duplicate"],
        "stale": status_counts["stale"],
        "invalid_history_nonfinal": status_counts["invalid_history_nonfinal"],
        "connections": sum(r["event_type"] == "CONNECT" for r in rows),
        "connection_errors": sum(r["event_type"] == "CONNECT_ERROR" for r in rows),
    }

    if latencies:
        summary.update({
            "p50_ms": percentile(latencies, 50),
            "p95_ms": percentile(latencies, 95),
            "p99_ms": percentile(latencies, 99),
            "fresh_pct": pct(sum(x <= fresh_threshold_ms for x in latencies), len(latencies)),
        })

    if final_first_latency:
        fresh_final_count = sum(
            x <= fresh_threshold_ms for x in final_first_latency.values()
        )
        if expected_finals is not None and clients:
            fresh_final_denominator = expected_finals * len(clients)
        else:
            fresh_final_denominator = len(final_first_latency)
        summary["fresh_final_pct"] = pct(fresh_final_count, fresh_final_denominator)
        summary["fresh_final_count"] = fresh_final_count
        summary["fresh_final_denominator"] = fresh_final_denominator

    if expected_finals is not None and clients:
        total_expected = expected_finals * len(clients)
        total_unique = sum(len(final_by_client[c]) for c in clients)
        summary["unique_finals"] = total_unique
        summary["expected_finals_total"] = total_expected
        summary["final_completeness_pct"] = pct(total_unique, total_expected)

    if expected_updates is not None and clients:
        total_expected_updates = expected_updates * len(clients)
        total_unique_updates = sum(len(live_update_by_client[c]) for c in clients)
        summary["unique_live_revisions"] = total_unique_updates
        summary["expected_updates_total"] = total_expected_updates
        summary["normal_update_coverage_pct"] = pct(total_unique_updates, total_expected_updates)

    normal_stats = role_stats(rows, 0, fresh_threshold_ms)
    slow_stats = role_stats(rows, 1, fresh_threshold_ms)
    if normal_stats:
        summary["normal_clients"] = normal_stats
    if slow_stats:
        summary["slow_clients"] = slow_stats

    recovery_values = []
    for r in rows:
        if r["event_type"] != "HISTORY" or not r.get("recovery_ms"):
            continue
        connection_no = int(r.get("connection_no") or 0)
        is_late = r.get("late", "0") == "1"
        if connection_no > 1 or is_late:
            value = safe_float(r["recovery_ms"])
            if value is not None:
                recovery_values.append(value)
    if recovery_values:
        summary["recovery_count"] = len(recovery_values)
        summary["recovery_p50_ms"] = percentile(recovery_values, 50)
        summary["recovery_p95_ms"] = percentile(recovery_values, 95)

    return summary


def print_summary(s, threshold_ms=500.0):
    print("LiveRecall Results")
    print("==================")
    print(f"Clients observed:              {s.get('clients', 0)}")
    print(f"Live caption messages:         {s.get('live_caption_messages', 0)}")
    print(f"Recovered history items:       {s.get('recovered_history_items', 0)}")
    print(f"Accepted messages:             {s.get('accepted', 0)}")
    print(f"Recovered messages:            {s.get('recovered', 0)}")
    print(f"Duplicate messages observed:   {s.get('duplicates', 0)}")
    print(f"Stale messages observed:       {s.get('stale', 0)}")
    if s.get("invalid_history_nonfinal", 0):
        print(f"Invalid non-final HISTORY:     {s['invalid_history_nonfinal']}")

    if "p50_ms" in s:
        print(f"P50 caption age:               {s['p50_ms']:.2f} ms")
        print(f"P95 caption age:               {s['p95_ms']:.2f} ms")
        print(f"P99 caption age:               {s['p99_ms']:.2f} ms")
        print(f"Fresh Caption Ratio (<={threshold_ms:.0f} ms): {s['fresh_pct']:.2f}%")
    if "fresh_final_pct" in s:
        print(f"Fresh Final Caption Ratio (<={threshold_ms:.0f} ms): {s['fresh_final_pct']:.2f}%")

    print(f"Successful connections:        {s.get('connections', 0)}")
    print(f"Connection errors:             {s.get('connection_errors', 0)}")

    if "final_completeness_pct" in s:
        print(f"Unique final captions:         {s['unique_finals']}/{s['expected_finals_total']}")
        print(f"Final-caption completeness:    {s['final_completeness_pct']:.2f}%")
    if "normal_update_coverage_pct" in s:
        print(f"Unique live revisions:         {s['unique_live_revisions']}/{s['expected_updates_total']}")
        print(f"Normal Update Coverage:        {s['normal_update_coverage_pct']:.2f}%")

    if "slow_clients" in s:
        print("\nSlow-reader isolation")
        print("---------------------")
        n = s.get("normal_clients")
        q = s.get("slow_clients")
        if n:
            print(f"Normal clients: P50={n['p50_ms']:.2f} ms, P95={n['p95_ms']:.2f} ms, "
                  f"Fresh={n['fresh_pct']:.2f}% ({n['messages']} msgs)")
        if q:
            print(f"Slow clients:   P50={q['p50_ms']:.2f} ms, P95={q['p95_ms']:.2f} ms, "
                  f"Fresh={q['fresh_pct']:.2f}% ({q['messages']} msgs)")

    if "recovery_p50_ms" in s:
        print("\nReconnect / late-join HISTORY response")
        print("--------------------------------------")
        print(f"Recovery P50: {s['recovery_p50_ms']:.2f} ms")
        print(f"Recovery P95: {s['recovery_p95_ms']:.2f} ms")
        print("Note: fast HISTORY response alone does not prove correct recovery; "
              "check final completeness and duplicate/stale counts too.")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("csv_file")
    p.add_argument("--expected-finals", type=int, default=None)
    p.add_argument("--expected-updates", type=int, default=None)
    p.add_argument("--fresh-threshold-ms", type=float, default=500.0)
    p.add_argument("--json-output")
    args = p.parse_args()

    summary = analyse_file(
        args.csv_file,
        expected_finals=args.expected_finals,
        expected_updates=args.expected_updates,
        fresh_threshold_ms=args.fresh_threshold_ms,
    )
    print_summary(summary, args.fresh_threshold_ms)

    if args.json_output:
        Path(args.json_output).write_text(json.dumps(summary, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
