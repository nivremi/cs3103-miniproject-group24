#!/usr/bin/env python3
"""Scalable simulated WebSocket clients for LiveRecall public tests.

This harness records client roles, classifies duplicate/stale state across both
live and HISTORY deliveries, and resumes using the latest FINALIZED state.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import random
import time
from pathlib import Path

import websockets

CSV_FIELDS = [
    "client_id", "slow", "disconnecting", "late", "connection_no",
    "event_type", "segment", "revision", "final", "source_ts",
    "receive_ts", "latency_ms", "status", "recovery_ms",
]


class CSVLogger:
    def __init__(self, path: str):
        self.path = Path(path)
        self.lock = asyncio.Lock()
        self.fp = None
        self.writer = None

    async def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fp = self.path.open("w", newline="", encoding="utf-8")
        self.writer = csv.DictWriter(self.fp, fieldnames=CSV_FIELDS)
        self.writer.writeheader()

    async def write(self, row: dict) -> None:
        async with self.lock:
            self.writer.writerow({k: row.get(k, "") for k in CSV_FIELDS})

    async def close(self) -> None:
        if self.fp:
            self.fp.flush()
            self.fp.close()


async def simulated_client(client_id, args, logger, slow, disconnecting, late):
    rng = random.Random(args.seed + client_id)
    role = {
        "client_id": client_id,
        "slow": int(slow),
        "disconnecting": int(disconnecting),
        "late": int(late),
    }

    if late:
        await asyncio.sleep(args.late_seconds + rng.uniform(0, 0.25))

    # All clients share one global test end time. Late clients therefore run
    # only for the remaining portion of the experiment.
    global_start = args._global_start
    end_time = global_start + args.duration

    highest_revision: dict[int, int] = {}
    final_seen: set[int] = set()
    last_final_segment = 0
    last_final_revision = 0
    disconnected_once = False
    connection_no = 0

    def classify(seg: int, rev: int) -> str:
        previous = highest_revision.get(seg)
        if previous is None:
            highest_revision[seg] = rev
            return "accepted"
        if rev < previous:
            return "stale"
        if rev == previous:
            return "duplicate"
        highest_revision[seg] = rev
        return "accepted"

    while time.monotonic() < end_time:
        try:
            async with websockets.connect(
                args.uri,
                max_size=2**20,
                ping_interval=20,
                ping_timeout=20,
            ) as ws:
                connection_no += 1
                connected_epoch = time.time()
                await logger.write({
                    **role,
                    "connection_no": connection_no,
                    "event_type": "CONNECT",
                    "receive_ts": f"{connected_epoch:.6f}",
                    "status": "reconnect" if connection_no > 1 else "initial",
                })

                await ws.send(json.dumps({
                    "type": "CLIENT_HELLO",
                    "client_id": client_id,
                    "session": args.session,
                    "last_segment": last_final_segment,
                    "last_revision": last_final_revision,
                    "history_seconds": args.history_seconds,
                }))

                while time.monotonic() < end_time:
                    elapsed_global = time.monotonic() - global_start
                    if (disconnecting and not disconnected_once
                            and elapsed_global >= args.disconnect_at):
                        disconnected_once = True
                        await logger.write({
                            **role,
                            "connection_no": connection_no,
                            "event_type": "FORCED_DISCONNECT",
                            "receive_ts": f"{time.time():.6f}",
                            "status": "test",
                        })
                        await ws.close()
                        await asyncio.sleep(args.disconnect_seconds)
                        break

                    timeout = min(1.0, max(0.05, end_time - time.monotonic()))
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                    except asyncio.TimeoutError:
                        continue

                    recv_ts = time.time()
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        await logger.write({
                            **role,
                            "connection_no": connection_no,
                            "event_type": "INVALID_JSON",
                            "receive_ts": f"{recv_ts:.6f}",
                            "status": "invalid",
                        })
                        continue

                    if msg.get("type") == "CAPTION_UPDATE":
                        seg = int(msg["segment"])
                        rev = int(msg["revision"])
                        src = float(msg["source_ts"])
                        is_final = bool(msg.get("final"))
                        status = classify(seg, rev)

                        if status == "accepted" and is_final:
                            final_seen.add(seg)
                            if (seg > last_final_segment or
                                    (seg == last_final_segment and rev > last_final_revision)):
                                last_final_segment = seg
                                last_final_revision = rev

                        await logger.write({
                            **role,
                            "connection_no": connection_no,
                            "event_type": "CAPTION_UPDATE",
                            "segment": seg,
                            "revision": rev,
                            "final": int(is_final),
                            "source_ts": f"{src:.6f}",
                            "receive_ts": f"{recv_ts:.6f}",
                            "latency_ms": f"{(recv_ts-src)*1000.0:.3f}",
                            "status": status,
                        })

                        if slow:
                            await asyncio.sleep(args.slow_ms / 1000.0)

                    elif msg.get("type") == "HISTORY":
                        items = msg.get("items", [])
                        recovery_ms = (recv_ts - connected_epoch) * 1000.0
                        await logger.write({
                            **role,
                            "connection_no": connection_no,
                            "event_type": "HISTORY",
                            "receive_ts": f"{recv_ts:.6f}",
                            "status": f"{len(items)} items",
                            "recovery_ms": f"{recovery_ms:.3f}",
                        })

                        for item in items:
                            seg = int(item["segment"])
                            rev = int(item["revision"])
                            src = float(item["source_ts"])
                            is_final = bool(item.get("final"))

                            if not is_final:
                                status = "invalid_history_nonfinal"
                            else:
                                base = classify(seg, rev)
                                status = "recovered" if base == "accepted" else base

                            if status == "recovered":
                                final_seen.add(seg)
                                if (seg > last_final_segment or
                                        (seg == last_final_segment and rev > last_final_revision)):
                                    last_final_segment = seg
                                    last_final_revision = rev

                            await logger.write({
                                **role,
                                "connection_no": connection_no,
                                "event_type": "HISTORY_ITEM",
                                "segment": seg,
                                "revision": rev,
                                "final": int(is_final),
                                "source_ts": f"{src:.6f}",
                                "receive_ts": f"{recv_ts:.6f}",
                                "latency_ms": f"{(recv_ts-src)*1000.0:.3f}",
                                "status": status,
                                "recovery_ms": f"{recovery_ms:.3f}",
                            })

                        if slow and items:
                            await asyncio.sleep(args.slow_ms / 1000.0)

        except (OSError, websockets.exceptions.WebSocketException) as e:
            await logger.write({
                **role,
                "connection_no": connection_no,
                "event_type": "CONNECT_ERROR",
                "receive_ts": f"{time.time():.6f}",
                "status": type(e).__name__,
            })
            await asyncio.sleep(0.4)


async def main_async(args):
    rng = random.Random(args.seed)
    n = args.clients
    ids = list(range(n))
    rng.shuffle(ids)

    n_slow = round(n * args.slow_percent / 100.0)
    n_disc = round(n * args.disconnect_percent / 100.0)
    n_late = round(n * args.late_percent / 100.0)

    # Roles are disjoint in the public harness to make results easy to interpret.
    slow_ids = set(ids[:n_slow])
    disc_ids = set(ids[n_slow:n_slow+n_disc])
    late_ids = set(ids[n_slow+n_disc:n_slow+n_disc+n_late])

    logger = CSVLogger(args.output)
    await logger.open()
    args._global_start = time.monotonic()

    print(
        f"Starting {n} clients; slow={len(slow_ids)}, "
        f"disconnecting={len(disc_ids)}, late={len(late_ids)}"
    )

    tasks = [
        asyncio.create_task(simulated_client(
            i, args, logger,
            slow=i in slow_ids,
            disconnecting=i in disc_ids,
            late=i in late_ids,
        ))
        for i in range(n)
    ]
    await asyncio.gather(*tasks)
    await logger.close()
    print(f"Results written to {args.output}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--uri", default="ws://127.0.0.1:8765")
    p.add_argument("--session", default="L1")
    p.add_argument("--clients", type=int, default=100)
    p.add_argument("--duration", type=float, default=60)
    p.add_argument("--output", default="results.csv")
    p.add_argument("--seed", type=int, default=3103)
    p.add_argument("--history-seconds", type=int, default=30)

    p.add_argument("--slow-percent", type=float, default=0)
    p.add_argument("--slow-ms", type=float, default=500)

    p.add_argument("--disconnect-percent", type=float, default=0)
    p.add_argument("--disconnect-at", type=float, default=30)
    p.add_argument("--disconnect-seconds", type=float, default=5)

    p.add_argument("--late-percent", type=float, default=0)
    p.add_argument("--late-seconds", type=float, default=10)

    args = p.parse_args()
    if not (0 <= args.history_seconds <= 60):
        p.error("--history-seconds must be between 0 and 60")
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
