"""Fake WebSocket connections for unit tests (no network, no websockets package).

FakeClientWS consumes one message every period seconds. A ping is answered
once every message sent before it has been consumed.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
import types


class FakeClientWS:
    def __init__(self, period: float = 0.0):
        self.period = period          # seconds per consumed message; 0 = instant
        self.inbox: list[tuple[str, float]] = []   # (text, send_time)
        self.received: list[tuple[dict, float]] = []  # (msg, consume_time)
        self.sent_count = 0
        self.close_code = None
        self._pings: list[tuple[int, asyncio.Future]] = []
        self._consumer: asyncio.Task | None = None
        self._incoming: asyncio.Queue = asyncio.Queue()   # client -> server

    # ---- server-facing API (subset of websockets' connection) ----
    async def send(self, text: str) -> None:
        if self.close_code is not None:
            raise ConnectionError("closed")
        self.sent_count += 1
        self.inbox.append((text, time.monotonic()))
        self._ensure_consumer()
        self._resolve_pings()

    async def ping(self):
        fut = asyncio.get_running_loop().create_future()
        self._pings.append((self.sent_count, fut))
        self._ensure_consumer()
        self._resolve_pings()
        return fut

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.close_code = code
        await self._incoming.put(None)

    async def recv(self):
        item = await self._incoming.get()
        if item is None:
            raise FakeConnectionClosed()
        return item

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self._incoming.get()
        if item is None:
            raise StopAsyncIteration
        return item

    # ---- test-facing helpers ----
    async def client_send(self, obj) -> None:
        await self._incoming.put(json.dumps(obj))

    async def disconnect(self) -> None:
        await self._incoming.put(None)

    def unread(self) -> int:
        return self.sent_count - len(self.received)

    def messages(self, mtype: str | None = None) -> list[dict]:
        return [m for m, _ in self.received if mtype is None or m.get("type") == mtype]

    def _ensure_consumer(self) -> None:
        if self._consumer is None or self._consumer.done():
            self._consumer = asyncio.create_task(self._consume())

    def _resolve_pings(self) -> None:
        consumed = len(self.received)
        still = []
        for marker, fut in self._pings:
            if consumed >= marker and not fut.done():
                fut.set_result(0.0)
            elif not fut.done():
                still.append((marker, fut))
        self._pings = still

    async def _consume(self) -> None:
        while self.inbox:
            text, _ = self.inbox.pop(0)
            if self.period:
                await asyncio.sleep(self.period)
            self.received.append((json.loads(text), time.time()))
            self._resolve_pings()
        self._resolve_pings()


class FakeConnectionClosed(Exception):
    pass


def install_fake_websockets_module() -> None:
    """Let solution_server import without the real websockets package."""
    try:
        import websockets  # noqa: F401  (real package available: use it)
        return
    except ImportError:
        pass
    ws_mod = types.ModuleType("websockets")
    exc_mod = types.ModuleType("websockets.exceptions")
    exc_mod.ConnectionClosed = FakeConnectionClosed
    ws_mod.exceptions = exc_mod
    ws_mod.serve = None
    sys.modules["websockets"] = ws_mod
    sys.modules["websockets.exceptions"] = exc_mod
