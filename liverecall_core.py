"""LiveRecall server core: protocol state (iteration 1, step 3).

Network-free; unit-tested via tests/test_core.py.
§ references are to the CS3103 A4 brief.
"""

from __future__ import annotations

import json
import logging
import math
import time
from collections import deque
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


@dataclass
class ServerConfig:
    history_retain_s: float = DEFAULT_HISTORY_RETAIN_S
    history_max_items: int = DEFAULT_HISTORY_MAX_ITEMS


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
