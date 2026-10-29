"""LiveRecall server core (iteration 1).

Network-free; unit-tested via tests/test_core.py.
§ references are to the CS3103 A4 brief.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections import OrderedDict, deque
from dataclasses import dataclass
from typing import Any, Iterable

log = logging.getLogger("liverecall")

# §4.2: clients may request up to 60 s of recovery context.
MAX_HISTORY_SECONDS_REQUEST = 60.0
DEFAULT_HISTORY_SECONDS = 30.0

# §3 row 8: server history must cover >= 60 s; we keep a little more so a
# client that was away for ~60 s plus reconnect time can still be repaired.
DEFAULT_HISTORY_RETAIN_S = 90.0
# Hard cap independent of timestamps, in case source_ts is unusual.
DEFAULT_HISTORY_MAX_ITEMS = 2000
# Unfinished segments tracked at once (normally 1). Protects memory if a
# source abandons segments without finalizing them.
MAX_ACTIVE_SEGMENTS = 64

MAX_TEXT_CHARS = 4000
MAX_SESSION_CHARS = 64

# Per-client delivery defaults (§4.3). See ClientConn for the reasoning.
DEFAULT_WINDOW = 8          # max messages sent but not yet ping-acknowledged
DEFAULT_PING_AFTER = 4      # send a marker ping once this many are unacked
DEFAULT_MAX_PENDING = 400   # hard bound on queued items per client

# WebSocket close code 1013 = "try again later": client reconnects and
# resumes with its last finalized state (§4.2).
CLOSE_TOO_SLOW = 1013


@dataclass
class ServerConfig:
    history_retain_s: float = DEFAULT_HISTORY_RETAIN_S
    history_max_items: int = DEFAULT_HISTORY_MAX_ITEMS
    window: int = DEFAULT_WINDOW            # 0 disables ping-window gating
    ping_after: int = DEFAULT_PING_AFTER
    max_pending: int = DEFAULT_MAX_PENDING


@dataclass(frozen=True, slots=True)
class Caption:
    session: str
    segment: int
    revision: int
    source_ts: float
    final: bool
    text: str
    wire: str  # canonical CAPTION_UPDATE JSON, encoded once

    @property
    def key(self) -> tuple[int, int]:
        return (self.segment, self.revision)


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _is_number(v: Any) -> bool:
    return (isinstance(v, (int, float)) and not isinstance(v, bool)
            and math.isfinite(v))


def encode(obj: dict) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


def parse_caption_update(msg: dict, default_session: str | None = None) -> Caption:
    """Validate a CAPTION_UPDATE (§4.1) and return a Caption. Raises ValueError on bad input."""
    session = msg.get("session", default_session)
    segment = msg.get("segment")
    revision = msg.get("revision")
    source_ts = msg.get("source_ts")
    final = msg.get("final")
    text = msg.get("text")

    if not isinstance(session, str) or not 0 < len(session) <= MAX_SESSION_CHARS:
        raise ValueError("bad session")
    if not _is_int(segment) or segment < 1:
        raise ValueError("bad segment")
    if not _is_int(revision) or revision < 1:
        raise ValueError("bad revision")
    if not _is_number(source_ts):
        raise ValueError("bad source_ts")
    if not isinstance(final, bool):
        raise ValueError("bad final")
    if not isinstance(text, str) or len(text) > MAX_TEXT_CHARS:
        raise ValueError("bad text")

    # Only the required fields are forwarded: smaller messages and no
    # test-control fields leaking to clients.
    wire = encode({
        "type": "CAPTION_UPDATE",
        "session": session,
        "segment": segment,
        "revision": revision,
        "source_ts": source_ts,
        "final": final,
        "text": text,
    })
    return Caption(session, segment, revision, float(source_ts), final, text, wire)


def history_message(session: str, items: Iterable[Caption], gap: bool) -> str:
    """Build a HISTORY message (§4.2) by splicing pre-encoded items.

    gap=True when the client's resume point is older than our retained window.
    """
    body = ",".join(c.wire for c in items)
    return ('{"type":"HISTORY","session":' + encode(session)
            + ',"items":[' + body + '],"gap":' + ("true" if gap else "false") + "}")


class SessionState:
    """Authoritative caption state for one session.

    Finals are immutable (§4.1). Provisionals only advance. History is bounded
    by source time and by count.
    """

    def __init__(self, name: str, cfg: ServerConfig):
        self.name = name
        self.cfg = cfg
        self.active: dict[int, Caption] = {}          # unfinished segments
        self.history: deque[Caption] = deque()        # finalized, by segment
        self.final_segments: set[int] = set()         # segments in history
        self.expired_floor = 0   # highest segment evicted from history
        self.newest_ts = 0.0     # newest source_ts seen (history clock)
        self.clients: set[ClientConn] = set()
        self.counters = {"accepted": 0, "stale_or_duplicate": 0,
                         "after_final": 0, "expired": 0}

    # -- source side -------------------------------------------------------

    def apply(self, cap: Caption) -> bool:
        """Apply one update. Returns True if it is new state to forward."""
        seg = cap.segment
        if seg in self.final_segments:
            self.counters["after_final"] += 1
            return False
        if seg <= self.expired_floor and seg not in self.active:
            # Older than anything we still retain; it can't be a valid update.
            self.counters["expired"] += 1
            return False
        current = self.active.get(seg)
        if current is not None and cap.revision <= current.revision:
            self.counters["stale_or_duplicate"] += 1
            return False

        self.counters["accepted"] += 1
        if cap.source_ts > self.newest_ts:
            self.newest_ts = cap.source_ts
        if cap.final:
            self.active.pop(seg, None)
            self._add_final(cap)
        else:
            self.active[seg] = cap
            if len(self.active) > MAX_ACTIVE_SEGMENTS:
                oldest = min(self.active)
                del self.active[oldest]
        return True

    def broadcast(self, cap: Caption) -> None:
        """Hand an accepted update to every client. Never blocks (O(clients))."""
        for client in tuple(self.clients):
            client.enqueue(cap)

    def _add_final(self, cap: Caption) -> None:
        h = self.history
        if not h or cap.segment > h[-1].segment:
            h.append(cap)
        else:  # out-of-order final (not produced by the public source)
            i = len(h)
            while i > 0 and h[i - 1].segment > cap.segment:
                i -= 1
            h.insert(i, cap)
        self.final_segments.add(cap.segment)
        self._trim()

    def _trim(self) -> None:
        h = self.history
        cutoff = self.newest_ts - self.cfg.history_retain_s
        while h and (len(h) > self.cfg.history_max_items or h[0].source_ts < cutoff):
            old = h.popleft()
            self.final_segments.discard(old.segment)
            if old.segment > self.expired_floor:
                self.expired_floor = old.segment

    # -- client side -------------------------------------------------------

    def recovery_items(self, last_segment: int, last_revision: int,
                       history_seconds: float) -> tuple[list[Caption], bool]:
        """Select HISTORY items for a CLIENT_HELLO (§4.2).

        Resume (last_segment > 0): all retained finals newer than the client's
        last state, ignoring history_seconds (§3 rows 5, 7).
        Late join: finals from the last history_seconds (clamped to 60) of
        source time. Returns (items, gap); gap means some expired (§3 row 6).
        """
        if last_segment > 0:
            floor = (last_segment, last_revision)
            items = [c for c in self.history if c.key > floor]
            # Finals between last_segment and expired_floor were evicted.
            return items, last_segment < self.expired_floor
        secs = min(max(history_seconds, 0.0), MAX_HISTORY_SECONDS_REQUEST)
        cutoff = self.newest_ts - secs
        return [c for c in self.history if c.source_ts >= cutoff], False


class ClientConn:
    """Outbound path for one client.

    Direct ws.send() pushes messages into the TCP buffer where a superseded
    revision can't be dropped. We keep the backlog here instead:

    1. Send window: at most ``window`` messages unacknowledged. Marker pings
       track acknowledgement -- the client library processes frames in order,
       so a pong means everything before that ping was consumed.
    2. Pending buffer keyed by segment, oldest segment first. Every
       revision is kept for now (no dropping yet).
    3. Hard bound: if the buffer exceeds max_pending, close with 1013; the
       client resumes via HISTORY.
    """

    def __init__(self, ws: Any, session: SessionState, client_id: Any,
                 last_segment: int, cfg: ServerConfig):
        self.ws = ws
        self.session = session
        self.client_id = client_id
        self.cfg = cfg
        # Client already holds finals up to here; never send it older state.
        self.floor_segment = last_segment

        # segment -> list of [Caption, enqueue_time]; usually 1 entry.
        self.pending: OrderedDict[int, list[list[Any]]] = OrderedDict()
        self.npending = 0
        self.control: deque[str] = deque()   # HISTORY etc., sent first
        self.wake = asyncio.Event()
        self.closed = False

        self.sent = 0
        self.acked = 0
        self._ping_task: asyncio.Task | None = None
        self._sender: asyncio.Task | None = None

        # statistics for logs/report
        self.max_npending = 0
        self.gated = False
        self.gate_events = 0
        self.overflowed = False
        self.rtt_ms: float | None = None

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> asyncio.Task:
        self._sender = asyncio.create_task(self._run(), name=f"send-{self.client_id}")
        return self._sender

    def stop(self) -> None:
        self.closed = True
        for t in (self._sender, self._ping_task):
            if t is not None and not t.done():
                t.cancel()
        self.pending.clear()
        self.control.clear()
        self.npending = 0

    # -- producer side (called synchronously from the source handler) ----

    def send_control(self, text: str) -> None:
        if not self.closed:
            self.control.append(text)
            self.wake.set()

    def enqueue(self, cap: Caption) -> None:
        if self.closed or cap.segment <= self.floor_segment:
            return
        now = time.monotonic()
        slots = self.pending.get(cap.segment)
        if slots is None:
            self.pending[cap.segment] = [[cap, now]]
            self.npending += 1
        else:
            last = slots[-1]
            if cap.revision <= last[0].revision:
                return
            slots.append([cap, now])
            self.npending += 1

        if self.npending > self.max_npending:
            self.max_npending = self.npending
        if self.npending > self.cfg.max_pending:
            self._overflow()
            return
        self.wake.set()

    # -- overload helpers --------------------------------------------------

    def _window_full(self) -> bool:
        return self.cfg.window > 0 and self.sent - self.acked >= self.cfg.window

    def _overflow(self) -> None:
        log.warning("client %s too slow (%d pending); closing with %d to force resume",
                    self.client_id, self.npending, CLOSE_TOO_SLOW)
        self.overflowed = True
        self.stop()
        asyncio.ensure_future(self._close_quietly(CLOSE_TOO_SLOW, "too slow; resume"))

    async def _close_quietly(self, code: int, reason: str) -> None:
        try:
            await self.ws.close(code, reason)
        except Exception:
            pass

    # -- consumer side -----------------------------------------------------

    def _pop_next(self) -> str | None:
        """Next message to send, None if nothing is sendable right now."""
        if self.control:
            return self.control.popleft()
        if not self.pending:
            return None
        seg, slots = next(iter(self.pending.items()))
        cap, _ = slots.pop(0)
        self.npending -= 1
        if not slots:
            del self.pending[seg]
        return cap.wire

    async def _run(self) -> None:
        try:
            while not self.closed:
                if not self.control and not self.pending:
                    self.wake.clear()
                    await self.wake.wait()
                    continue
                if self._window_full():
                    if not self.gated:
                        self.gated = True
                        self.gate_events += 1
                    self._maybe_ping(force=True)
                    self.wake.clear()
                    await self.wake.wait()
                    continue
                self.gated = False
                text = self._pop_next()
                if text is None:
                    continue
                await self.ws.send(text)
                self.sent += 1
                self._maybe_ping()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # connection closed / reset
            log.debug("client %s sender stopped: %r", self.client_id, e)
        finally:
            self.closed = True

    def _maybe_ping(self, force: bool = False) -> None:
        if self.cfg.window <= 0 or self.closed or self._ping_task is not None:
            return
        unacked = self.sent - self.acked
        if unacked >= self.cfg.ping_after or (force and unacked > 0):
            self._ping_task = asyncio.create_task(self._ping(self.sent))

    async def _ping(self, marker: int) -> None:
        started = time.monotonic()
        ok = False
        try:
            waiter = await self.ws.ping()
            await waiter
            ok = True
        except asyncio.CancelledError:
            raise
        except Exception:
            pass  # connection closing; the handler cleans up
        finally:
            self._ping_task = None
        if ok:
            rtt = (time.monotonic() - started) * 1000.0
            self.rtt_ms = rtt if self.rtt_ms is None else 0.8 * self.rtt_ms + 0.2 * rtt
            if marker > self.acked:
                self.acked = marker
            self.wake.set()
            self._maybe_ping()
