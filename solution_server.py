#!/usr/bin/env python3
"""CS3103 Assignment 4 - LiveRecall server (iteration 2).

Entry point: python solution_server.py --port 8765

WebSocket glue; graded logic is in liverecall_core.py so it can be
unit-tested without a network. Experiment flags (--no-degrade etc.) are for
the before/after comparison; defaults are the submitted config.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from typing import Any

import websockets

from liverecall_core import (
    DEFAULT_HISTORY_SECONDS,
    ClientConn,
    ServerConfig,
    SessionState,
    history_message,
    parse_caption_update,
)

log = logging.getLogger("liverecall")

HELLO_TIMEOUT_S = 5.0
MAX_SESSIONS = 16
MAX_INCOMING_MESSAGE = 2**20

CONFIG = ServerConfig()
SESSIONS: dict[str, SessionState] = {}
TOTALS = {"clients_served": 0, "coalesced": 0, "overflow_closes": 0,
          "gate_events": 0, "max_pending_seen": 0, "degrade_events": 0}


def get_session(name: str) -> SessionState | None:
    state = SESSIONS.get(name)
    if state is None:
        if len(SESSIONS) >= MAX_SESSIONS:
            return None
        state = SessionState(name, CONFIG)
        SESSIONS[name] = state
        log.info("session %s created", name)
    return state


def _int_field(msg: dict, name: str, default: int = 0) -> int:
    v = msg.get(name, default)
    if isinstance(v, bool) or not isinstance(v, int) or v < 0:
        raise ValueError(f"bad {name}")
    return v


async def handle_source(ws, hello: dict[str, Any]) -> None:
    default_session = hello.get("session") if isinstance(hello.get("session"), str) else "L1"
    log.info("source connected: session=%s", default_session)
    rejected = 0
    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                rejected += 1
                continue
            if not isinstance(msg, dict):
                rejected += 1
                continue

            msg_type = msg.get("type")
            if msg_type == "CAPTION_UPDATE":
                try:
                    cap = parse_caption_update(msg, default_session)
                except ValueError as e:
                    rejected += 1
                    log.debug("rejected update: %s", e)
                    continue
                state = get_session(cap.session)
                if state is not None and state.apply(cap):
                    state.broadcast(cap)
            elif msg_type == "SOURCE_DONE":
                # Test-control message. The specification does not require it
                # to be forwarded to clients.
                log.info("source finished; %s", summary_line())
            else:
                # Forward compatibility: ignore fields/messages this baseline
                # doesn't understand rather than crashing.
                pass
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        log.info("source disconnected (rejected %d malformed messages)", rejected)


async def handle_client(ws, hello: dict[str, Any]) -> None:
    try:
        session_name = hello.get("session", "L1")
        if not isinstance(session_name, str):
            raise ValueError("bad session")
        last_segment = _int_field(hello, "last_segment")
        last_revision = _int_field(hello, "last_revision")
        history_seconds = hello.get("history_seconds", DEFAULT_HISTORY_SECONDS)
        if isinstance(history_seconds, bool) or not isinstance(history_seconds, (int, float)):
            raise ValueError("bad history_seconds")
    except ValueError as e:
        await ws.close(code=1008, reason=str(e))
        return

    state = get_session(session_name)
    if state is None:
        await ws.close(code=1013, reason="too many sessions")
        return

    client_id = hello.get("client_id")
    conn = ClientConn(ws, state, client_id, last_segment, CONFIG)

    # No await between building HISTORY and registering for live updates:
    # asyncio can't run the source handler in between, so no final can fall
    # into the gap or be delivered twice.
    items, gap = state.recovery_items(last_segment, last_revision, float(history_seconds))
    conn.send_control(history_message(state.name, items, gap))
    state.clients.add(conn)
    conn.start()
    TOTALS["clients_served"] += 1
    log.debug("client %s connected (resume=%d, history=%d items, gap=%s); clients=%d",
              client_id, last_segment, len(items), gap, len(state.clients))

    try:
        # Student clients normally don't send further messages after hello.
        # Keep the WebSocket open until the client disconnects.
        async for _ in ws:
            pass
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        state.clients.discard(conn)
        conn.stop()
        TOTALS["coalesced"] += conn.coalesced
        TOTALS["gate_events"] += conn.gate_events
        TOTALS["degrade_events"] += conn.degrade_events
        TOTALS["max_pending_seen"] = max(TOTALS["max_pending_seen"], conn.max_npending)
        if conn.overflowed:
            TOTALS["overflow_closes"] += 1
        log.debug("client %s disconnected; clients=%d", client_id, len(state.clients))


async def handler(ws) -> None:
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=HELLO_TIMEOUT_S)
        hello = json.loads(raw)
        if not isinstance(hello, dict):
            raise ValueError
    except (asyncio.TimeoutError, json.JSONDecodeError, TypeError, ValueError):
        await ws.close(code=1008, reason="Expected a valid hello message")
        return
    except websockets.exceptions.ConnectionClosed:
        return

    msg_type = hello.get("type")
    if msg_type == "SOURCE_HELLO":
        await handle_source(ws, hello)
    elif msg_type == "CLIENT_HELLO":
        await handle_client(ws, hello)
    else:
        await ws.close(code=1008, reason="Expected SOURCE_HELLO or CLIENT_HELLO")


def summary_line() -> str:
    live = [c for s in SESSIONS.values() for c in s.clients]
    coalesced = TOTALS["coalesced"] + sum(c.coalesced for c in live)
    max_pending = max([TOTALS["max_pending_seen"]] + [c.max_npending for c in live])
    gated_now = sum(c.gated for c in live)
    degraded_now = sum(c.degraded for c in live)
    degrade_events = TOTALS["degrade_events"] + sum(c.degrade_events for c in live)
    hist = sum(len(s.history) for s in SESSIONS.values())
    counters = {}
    for s in SESSIONS.values():
        for k, v in s.counters.items():
            counters[k] = counters.get(k, 0) + v
    return (f"clients={len(live)} gated_now={gated_now} degraded_now={degraded_now} "
            f"degrade_events={degrade_events} history_items={hist} "
            f"coalesced={coalesced} max_pending={max_pending} "
            f"overflow_closes={TOTALS['overflow_closes']} "
            f"source={counters}")


async def stats_loop(interval: float) -> None:
    while True:
        await asyncio.sleep(interval)
        log.info("stats %s", summary_line())


class _ProbeNoiseFilter(logging.Filter):
    """Suppress 'opening handshake failed' for the TCP readiness probe.

    run_demo.py probes with a plain TCP connect; websockets logs it as a
    failed handshake. Only the 0-byte case is dropped; real errors still log.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if record.getMessage() != "opening handshake failed" or not record.exc_info:
            return True
        exc = record.exc_info[1]
        while exc is not None:
            if isinstance(exc, EOFError) and "after 0 bytes" in str(exc):
                return False
            exc = exc.__cause__
        return True


async def main_async(args) -> None:
    async with websockets.serve(
        handler,
        args.host,
        args.port,
        max_size=MAX_INCOMING_MESSAGE,
        ping_interval=20,
        ping_timeout=40,
    ):
        log.info("LiveRecall listening on ws://%s:%d (window=%d, coalesce=%s, "
                 "degrade=%s, max_pending=%d, history=%.0fs)", args.host, args.port,
                 CONFIG.window, CONFIG.coalesce, CONFIG.degrade, CONFIG.max_pending,
                 CONFIG.history_retain_s)
        if args.stats_interval > 0:
            asyncio.create_task(stats_loop(args.stats_interval))
        await asyncio.Future()


def main() -> None:
    p = argparse.ArgumentParser(description="LiveRecall caption server")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--verbose", action="store_true", help="log every client connect/disconnect")
    p.add_argument("--stats-interval", type=float, default=0,
                   help="print delivery statistics every N seconds (0 = off)")
    exp = p.add_argument_group("experiment switches (defaults = submitted design)")
    exp.add_argument("--window", type=int, default=CONFIG.window,
                     help="max unacknowledged messages per client; 0 disables the send window")
    exp.add_argument("--ping-after", type=int, default=CONFIG.ping_after)
    exp.add_argument("--max-pending", type=int, default=CONFIG.max_pending)
    exp.add_argument("--no-coalesce", action="store_true",
                     help="never drop superseded provisional revisions")
    exp.add_argument("--coalesce-age-ms", type=float, default=CONFIG.coalesce_age_s * 1000)
    exp.add_argument("--no-degrade", action="store_true",
                     help="disable the finals-only degraded mode (iteration-1 behaviour)")
    exp.add_argument("--degrade-rtt-ms", type=float, default=CONFIG.degrade_rtt_s * 1000,
                     help="ping RTT that marks a client as too slow")
    exp.add_argument("--degrade-hold", type=float, default=CONFIG.degrade_hold_s,
                     help="seconds to stay degraded after the last slow ping (doubles on relapse)")
    exp.add_argument("--degrade-hold-max", type=float, default=CONFIG.degrade_hold_max_s)
    exp.add_argument("--slow-reader-rtt-ms", type=float, default=CONFIG.slow_reader_rtt_s * 1000,
                     help="ping RTT that marks a persistent slow reader (degraded for --degrade-hold-max)")
    exp.add_argument("--degraded-idle-ms", type=float, default=CONFIG.degraded_idle_s * 1000,
                     help="degraded clients get a provisional only after this idle time")
    exp.add_argument("--history-retain", type=float, default=CONFIG.history_retain_s,
                     help="seconds of finalized captions kept for recovery (>= 60)")
    exp.add_argument("--history-max-items", type=int, default=CONFIG.history_max_items)
    args = p.parse_args()

    if args.history_retain < 60:
        p.error("--history-retain must be >= 60 (§3 row 8)")
    if args.window and args.ping_after >= args.window:
        p.error("--ping-after must be smaller than --window")

    CONFIG.window = max(0, args.window)
    CONFIG.ping_after = max(1, args.ping_after)
    CONFIG.max_pending = max(1, args.max_pending)
    CONFIG.coalesce = not args.no_coalesce
    CONFIG.coalesce_age_s = max(0.0, args.coalesce_age_ms / 1000.0)
    CONFIG.degrade = not args.no_degrade
    CONFIG.degrade_rtt_s = max(0.0, args.degrade_rtt_ms / 1000.0)
    CONFIG.degrade_hold_s = max(0.0, args.degrade_hold)
    CONFIG.degrade_hold_max_s = max(CONFIG.degrade_hold_s, args.degrade_hold_max)
    CONFIG.slow_reader_rtt_s = max(CONFIG.degrade_rtt_s, args.slow_reader_rtt_ms / 1000.0)
    CONFIG.degraded_idle_s = max(0.0, args.degraded_idle_ms / 1000.0)
    CONFIG.history_retain_s = args.history_retain
    CONFIG.history_max_items = max(1, args.history_max_items)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    # The websockets library is chatty at DEBUG; keep our own logs readable.
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("websockets.server").addFilter(_ProbeNoiseFilter())

    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
