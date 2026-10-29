"""LiveRecall server core: validation and revision state (iteration 1, step 2).

Network-free; unit-tested via tests/test_core.py.
§ references are to the CS3103 A4 brief.
"""

from __future__ import annotations

import json
import logging
import math
import time
from dataclasses import dataclass
from typing import Any

log = logging.getLogger("liverecall")

# Unfinished segments tracked at once (normally 1). Protects memory if a
# source abandons segments without finalizing them.
MAX_ACTIVE_SEGMENTS = 64

MAX_TEXT_CHARS = 4000
MAX_SESSION_CHARS = 64


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


class SessionState:
    """Authoritative caption state for one session.

    Finals are immutable (§4.1). Provisionals only advance.
    """

    def __init__(self, name: str):
        self.name = name
        self.active: dict[int, Caption] = {}          # unfinished segments
        self.finals: dict[int, Caption] = {}          # segment -> final (no bound yet)
        self.counters = {"accepted": 0, "stale_or_duplicate": 0,
                         "after_final": 0}

    # -- source side -------------------------------------------------------

    def apply(self, cap: Caption) -> bool:
        """Apply one update. Returns True if it is new state to forward."""
        seg = cap.segment
        if seg in self.finals:
            self.counters["after_final"] += 1
            return False
        current = self.active.get(seg)
        if current is not None and cap.revision <= current.revision:
            self.counters["stale_or_duplicate"] += 1
            return False

        self.counters["accepted"] += 1
        if cap.final:
            self.active.pop(seg, None)
            self.finals[seg] = cap
        else:
            self.active[seg] = cap
            if len(self.active) > MAX_ACTIVE_SEGMENTS:
                oldest = min(self.active)
                del self.active[oldest]
        return True
