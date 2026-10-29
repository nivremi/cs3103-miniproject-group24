#!/usr/bin/env python3
"""CS3103 Assignment 4 - LiveRecall server (iteration 1, step 2).

Entry point: python solution_server.py --port 8765

Updates are validated and revision-checked in liverecall_core.py before being
forwarded in canonical form. HISTORY is still empty and live delivery is
still the starter's naive broadcast.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from typing import Any

import websockets

from liverecall_core import SessionState, encode, parse_caption_update

log = logging.getLogger("liverecall")

HELLO_TIMEOUT_S = 5.0
MAX_SESSIONS = 16
MAX_INCOMING_MESSAGE = 2**20

SESSIONS: dict[str, SessionState] = {}
CLIENTS: dict[str, set[Any]] = {}        # session -> connected client sockets
TOTALS = {"clients_served": 0}


async def broadcast_naively(session: str, text: str) -> None:
    """Starter-kit broadcast: the source handler waits for every send."""
    clients = CLIENTS.get(session)
    if not clients:
        return
    snapshot = list(clients)
    results = await asyncio.gather(*(c.send(text) for c in snapshot),
                                   return_exceptions=True)
    for client, result in zip(snapshot, results):
        if isinstance(result, Exception):
            clients.discard(client)


def get_session(name: str) -> SessionState | None:
    state = SESSIONS.get(name)
    if state is None:
        if len(SESSIONS) >= MAX_SESSIONS:
            return None
        state = SessionState(name)
        SESSIONS[name] = state
        log.info("session %s created", name)
    return state


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
                    await broadcast_naively(state.name, cap.wire)
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
    session_name = hello.get("session", "L1")
    if not isinstance(session_name, str):
        await ws.close(code=1008, reason="bad session")
        return

    state = get_session(session_name)
    if state is None:
        await ws.close(code=1013, reason="too many sessions")
        return

    client_id = hello.get("client_id")
    # No recovery state yet: HISTORY is always empty (as in the starter).
    await ws.send(encode({"type": "HISTORY", "session": state.name, "items": []}))
    clients = CLIENTS.setdefault(state.name, set())
    clients.add(ws)
    TOTALS["clients_served"] += 1
    log.debug("client %s connected; clients=%d", client_id, len(clients))

    try:
        # Student clients normally don't send further messages after hello.
        # Keep the WebSocket open until the client disconnects.
        async for _ in ws:
            pass
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        clients.discard(ws)
        log.debug("client %s disconnected; clients=%d", client_id, len(clients))


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
    nclients = sum(len(c) for c in CLIENTS.values())
    counters = {}
    for s in SESSIONS.values():
        for k, v in s.counters.items():
            counters[k] = counters.get(k, 0) + v
    return f"clients={nclients} source={counters}"


async def stats_loop(interval: float) -> None:
    while True:
        await asyncio.sleep(interval)
        log.info("stats %s", summary_line())


async def main_async(args) -> None:
    async with websockets.serve(
        handler,
        args.host,
        args.port,
        max_size=MAX_INCOMING_MESSAGE,
        ping_interval=20,
        ping_timeout=40,
    ):
        log.info("LiveRecall listening on ws://%s:%d", args.host, args.port)
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
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    # The websockets library is chatty at DEBUG; keep our own logs readable.
    logging.getLogger("websockets").setLevel(logging.WARNING)

    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
