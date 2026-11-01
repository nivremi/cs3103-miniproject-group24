"""LiveRecall server core (iteration 2).

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
from typing import Any, Callable, Iterable

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
DEFAULT_COALESCE_AGE_S = 0.15  # a pending item this old means "client is behind"

# Finals-only degraded mode (iteration 2). See ClientConn for the reasoning.
DEFAULT_DEGRADE_RTT_S = 1.0        # ping RTT (or outstanding ping age) = "slow"
DEFAULT_DEGRADE_HOLD_S = 10.0      # first hold time after the last slow signal
DEFAULT_DEGRADE_HOLD_MAX_S = 120.0  # cap for the exponential hold back-off
DEFAULT_SLOW_READER_RTT_S = 3.0    # ping this slow = the client's own queue overflowed
DEFAULT_DEGRADED_IDLE_S = 1.0      # degraded: provisional only after this idle time
DEFAULT_DEGRADE_HEADROOM = 0.8     # leave only if full rate <= 80% of read rate
DRAIN_FACTOR = 6.0                 # hold >= 6 x slow RTT (time to drain the backlog)
RATE_PERIOD_S = 2.0                # offered-rate measurement period
READ_RATE_WINDOW_S = 10.0          # read-rate measurement period
SATURATED_FRACTION = 0.8           # ...counted only if blocked on the window this often

# Slow-consumer resync (iteration 2). See ClientConn point 5.
DEFAULT_RESYNC_LAG_S = 6.0         # estimated lag that triggers a resync
SLOW_READER_MEMORY = 4096          # bounded record of known slow client_ids

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
    coalesce: bool = True                   # False = keep every revision (ablation)
    coalesce_age_s: float = DEFAULT_COALESCE_AGE_S
    degrade: bool = True                    # False = iteration-1 behaviour (ablation)
    degrade_rtt_s: float = DEFAULT_DEGRADE_RTT_S
    degrade_hold_s: float = DEFAULT_DEGRADE_HOLD_S
    degrade_hold_max_s: float = DEFAULT_DEGRADE_HOLD_MAX_S
    slow_reader_rtt_s: float = DEFAULT_SLOW_READER_RTT_S
    degraded_idle_s: float = DEFAULT_DEGRADED_IDLE_S
    resync: bool = True                     # False = never force a resume
    resync_lag_s: float = DEFAULT_RESYNC_LAG_S
    net_check: bool = True                  # False = any slow ping degrades (ablation)


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
        # Finals per second (EWMA over source_ts gaps); used by resync.
        self.final_rate = 0.0
        self._last_final_ts: float | None = None
        # client_id -> (read_rate msgs/s, expires_at): clients we had to
        # resync, so their next connection starts degraded (bounded, LRU).
        self.slow_readers: OrderedDict[Any, tuple[float, float]] = OrderedDict()

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
            self._update_final_rate(cap.source_ts)
        else:  # out-of-order final (not produced by the public source)
            i = len(h)
            while i > 0 and h[i - 1].segment > cap.segment:
                i -= 1
            h.insert(i, cap)
        self.final_segments.add(cap.segment)
        self._trim()

    def _update_final_rate(self, ts: float) -> None:
        last, self._last_final_ts = self._last_final_ts, ts
        if last is None or not 0 < ts - last < 60:
            return
        sample = 1.0 / (ts - last)
        self.final_rate = (sample if self.final_rate == 0.0
                           else 0.8 * self.final_rate + 0.2 * sample)

    def remember_slow(self, client_id: Any, read_rate: float, now: float) -> None:
        if not isinstance(client_id, (str, int)):
            return
        self.slow_readers.pop(client_id, None)
        self.slow_readers[client_id] = (read_rate, now + self.cfg.degrade_hold_max_s)
        while len(self.slow_readers) > SLOW_READER_MEMORY:
            self.slow_readers.popitem(last=False)

    def known_slow_rate(self, client_id: Any, now: float) -> float | None:
        if not isinstance(client_id, (str, int)):
            return None
        entry = self.slow_readers.get(client_id)
        if entry is None:
            return None
        if entry[1] <= now:
            del self.slow_readers[client_id]
            return None
        return entry[0]

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
    2. Pending buffer keyed by segment. A provisional is replaced by a newer
       revision when the window is full or it has waited >= coalesce_age_s.
       A burst to a healthy client is not coalesced.
    3. Hard bound: if the buffer exceeds max_pending after compaction, close
       with 1013; the client resumes via HISTORY.
    4. Degraded mode (finals only). The websockets library (max_queue~16)
       answers pings until its queue fills, so a slightly-slow reader hides
       ~8 s of backlog before pings slow down. Once a ping takes >=
       degrade_rtt_s we switch to finals only, letting that backlog drain.
       Hold = max(degrade_hold_s, DRAIN_FACTOR * RTT). DRAIN_FACTOR=6:
       websockets pauses at 16 messages, so a T-second ping implies ~1.8T of
       actual lag, draining at ~1/3 rate in T3 => ~5.4T to clear.
       RTT >= slow_reader_rtt_s means the client's queue overflowed; it stays
       degraded for degrade_hold_max_s. Leave when hold expires and offered
       rate fits the measured read rate.
    5. Slow-consumer resync: if read rate < finals rate, close with 1013 when
       estimated lag >= resync_lag_s. CLIENT_HELLO -> HISTORY handles
       recovery. client_id is remembered so the next connection starts
       degraded.
    6. Network stall vs slow reader (iteration 3). Under packet loss a TCP
       retransmission (RTO >= 200 ms, doubling on repeat loss) can hold a
       ping for over a second although the client reads instantly. In T5
       that put ~1 in 3 healthy clients into finals-only mode (coverage
       94%). ``net_busy()`` (from the server: our bytes not yet ACKed by the
       client's kernel) tells them apart: a slow reader's kernel has ACKed
       everything and only the app hasn't read it. It is checked while the
       ping is outstanding, every degrade_rtt_s (by the time the pong
       arrives the retransmission has been ACKed). A ping the network was
       holding is not a slow signal; window-full coalescing (point 2) still
       keeps a congested link to the newest revision of each segment.
    """

    def __init__(self, ws: Any, session: SessionState, client_id: Any,
                 last_segment: int, cfg: ServerConfig,
                 net_busy: Callable[[], bool] | None = None):
        self.ws = ws
        self.net_busy = net_busy if cfg.net_check else None
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
        self._ping_started: float | None = None  # monotonic time of outstanding ping
        self.last_send = 0.0

        # degraded mode state (see class docstring, point 4)
        self.degraded = False
        self.degraded_until = 0.0
        self.hold_s = cfg.degrade_hold_s
        self.left_degraded_at: float | None = None
        self.entered_degraded_at = 0.0
        self.last_episode_s = 0.0
        self._last_slow_rtt = 0.0
        self.read_rate: float | None = None   # msgs/s, measured while saturated
        self.read_rate_at = 0.0
        self.offered_rate = 0.0               # msgs/s handed to enqueue()
        self._rate_count = 0
        self._rate_t0 = time.monotonic()
        self._sender: asyncio.Task | None = None
        # time spent blocked on the window, for the read-rate measurement
        self._gated_total = 0.0
        self._gated_t = 0.0
        self._meas_t0 = self._rate_t0
        self._meas_acked0 = 0
        self._meas_gated0 = 0.0

        # resync state (see class docstring, point 5)
        self._bucket = 0.0        # estimated messages queued at the client
        self._bucket_t = time.monotonic()
        self.resynced = False
        known = session.known_slow_rate(client_id, self._bucket_t) if cfg.resync else None
        if known is not None and cfg.degrade:
            # Resumed after a resync: start degraded with the known read rate.
            self.read_rate = known
            self.read_rate_at = self._bucket_t
            self.degraded = True
            self.entered_degraded_at = self._bucket_t
            self.degraded_until = self._bucket_t + cfg.degrade_hold_max_s

        # statistics for logs/report
        self.coalesced = 0
        self.max_npending = 0
        self.gated = False
        self.gate_events = 0
        self.overflowed = False
        self.rtt_ms: float | None = None
        self.degrade_events = 0
        self.degraded_dropped = 0
        self.net_stalls = 0       # slow-ping checks blamed on the network (point 6)
        self._ping_net_stalled: bool | None = None  # verdict for the outstanding ping
        self.started_degraded = self.degraded

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
        self._count_offered(now)
        self._update_mode(now)
        if self.closed:          # _update_mode may have resynced the client
            return
        slots = self.pending.get(cap.segment)
        if slots is None:
            self.pending[cap.segment] = [[cap, now]]
            self.npending += 1
        else:
            last = slots[-1]
            if cap.revision <= last[0].revision:
                return
            if (self.cfg.coalesce and not last[0].final
                    and (self.degraded or self._window_full()
                         or now - last[1] >= self.cfg.coalesce_age_s)):
                last[0] = cap            # replace in place; keep queue position
                self.coalesced += 1
            else:
                slots.append([cap, now])
                self.npending += 1

        if self.npending > self.max_npending:
            self.max_npending = self.npending
        if self.npending > self.cfg.max_pending:
            self._compact()
            if self.npending > self.cfg.max_pending:
                self._overflow()
                return
        self.wake.set()

    # -- overload helpers --------------------------------------------------

    def _window_full(self) -> bool:
        return self.cfg.window > 0 and self.sent - self.acked >= self.cfg.window

    def _slow_signal(self, now: float) -> bool:
        """True if the current ping has been outstanding for too long."""
        return (self._ping_started is not None
                and now - self._ping_started >= self.cfg.degrade_rtt_s
                and self._ping_net_stalled is False)

    def _network_stalled(self) -> bool:
        """Slow ping caused by TCP loss recovery, not the reader (point 6)."""
        if self.net_busy is None or not self.net_busy():
            return False
        self.net_stalls += 1
        return True

    def _count_offered(self, now: float) -> None:
        """Rate of updates offered to this client (what normal mode sends)."""
        self._rate_count += 1
        elapsed = now - self._rate_t0
        if elapsed >= RATE_PERIOD_S:
            sample = self._rate_count / elapsed
            self.offered_rate = (sample if self.offered_rate == 0.0
                                 else 0.5 * self.offered_rate + 0.5 * sample)
            self._rate_count = 0
            self._rate_t0 = now

    def _too_slow_for_full_rate(self, now: float) -> bool:
        return (self.read_rate is not None
                and now - self.read_rate_at < self.cfg.degrade_hold_max_s
                and self.offered_rate > DEFAULT_DEGRADE_HEADROOM * self.read_rate)

    def _update_mode(self, now: float) -> None:
        if not self.cfg.degrade:
            return
        if self._should_resync(now):
            self._resync(now)
            return
        if self._slow_signal(now):
            self._note_slow(now, now - self._ping_started)
        elif (self.degraded and now >= self.degraded_until
              and not self._too_slow_for_full_rate(now)):
            self.degraded = False
            self.left_degraded_at = now
            self.last_episode_s = now - self.entered_degraded_at
            log.debug("client %s leaves degraded mode", self.client_id)

    def _estimated_lag(self, now: float) -> float:
        """Seconds of messages the client still has to read (leaky bucket)."""
        if not self.read_rate:
            return 0.0
        queued = max(0.0, self._bucket - self.read_rate * (now - self._bucket_t))
        return queued / self.read_rate

    def _bucket_add(self, now: float) -> None:
        if self.read_rate:
            self._bucket = self._estimated_lag(now) * self.read_rate + 1
        self._bucket_t = now

    def _should_resync(self, now: float) -> bool:
        return (self.cfg.resync and self.degraded and self.read_rate is not None
                and self.session.final_rate > self.read_rate
                and self._estimated_lag(now) >= self.cfg.resync_lag_s)

    def _resync(self, now: float) -> None:
        log.info("client %s cannot keep up with finals (reads %.2f/s, finals %.2f/s, "
                 "lag ~%.1fs); closing with %d to resume via HISTORY", self.client_id,
                 self.read_rate, self.session.final_rate, self._estimated_lag(now),
                 CLOSE_TOO_SLOW)
        self.resynced = True
        self.session.remember_slow(self.client_id, self.read_rate, now)
        self.stop()
        asyncio.ensure_future(self._close_quietly(CLOSE_TOO_SLOW, "too slow; resume"))

    def _note_slow(self, now: float, rtt: float) -> None:
        """Enter or extend degraded mode. rtt is the slow ping's wait time."""
        if not self.cfg.degrade:
            return
        if not self.degraded:
            relapsed = (self.left_degraded_at is not None
                        and now - self.left_degraded_at < self.last_episode_s)
            self.hold_s = (min(self.hold_s * 2, self.cfg.degrade_hold_max_s)
                           if relapsed else self.cfg.degrade_hold_s)
            self.degraded = True
            self.entered_degraded_at = now
            self.degrade_events += 1
            log.debug("client %s enters degraded mode (hold %.0fs)",
                      self.client_id, self.hold_s)
            self._compact()
        if rtt >= self.cfg.slow_reader_rtt_s:
            hold = self.cfg.degrade_hold_max_s     # persistent slow reader
        else:
            hold = max(self.hold_s, DRAIN_FACTOR * rtt)
        self.degraded_until = max(self.degraded_until, now + hold)

    def _compact(self) -> None:
        """Keep only finals and the newest revision of each segment."""
        if not self.cfg.coalesce:
            return
        for seg, slots in self.pending.items():
            if len(slots) > 1:
                removed = len(slots) - 1
                self.pending[seg] = [slots[-1]]
                self.npending -= removed
                self.coalesced += removed

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
        now = time.monotonic()
        self._update_mode(now)
        if self.closed:
            return None
        while self.pending:
            seg, slots = next(iter(self.pending.items()))
            if self.degraded and not slots[0][0].final:
                if self.npending > 1:
                    # Something newer is queued behind this provisional (a
                    # newer revision, a final, or a later segment): drop it.
                    slots.pop(0)
                    self.npending -= 1
                    if not slots:
                        del self.pending[seg]
                    self.coalesced += 1
                    self.degraded_dropped += 1
                    continue
                if self.sent > self.acked or now - self.last_send < self.cfg.degraded_idle_s:
                    return None   # newest provisional waits for an idle client
            cap, enq = slots.pop(0)
            self.npending -= 1
            if not slots:
                del self.pending[seg]
            # Dequeue-time coalescing: a provisional that has a newer
            # revision queued behind it and has waited too long is obsolete.
            if (self.cfg.coalesce and slots and not cap.final
                    and now - enq >= self.cfg.coalesce_age_s):
                self.coalesced += 1
                continue
            return cap.wire
        return None

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
                        self._gated_t = time.monotonic()
                        self.gate_events += 1
                    self._maybe_ping(force=True)
                    self.wake.clear()
                    await self.wake.wait()
                    continue
                if self.gated:
                    self.gated = False
                    self._gated_total += time.monotonic() - self._gated_t
                text = self._pop_next()
                if text is None:
                    if self.pending:
                        # Degraded and only a provisional is left: make sure
                        # a pong will wake us, and re-check after the idle time.
                        self._maybe_ping(force=True)
                        self.wake.clear()
                        try:
                            await asyncio.wait_for(self.wake.wait(),
                                                   self.cfg.degraded_idle_s / 4)
                        except TimeoutError:
                            pass
                    continue
                await self.ws.send(text)
                self.sent += 1
                self.last_send = time.monotonic()
                self._bucket_add(self.last_send)
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
            self._ping_started = time.monotonic()
            self._ping_net_stalled = None
            self._ping_task = asyncio.create_task(self._ping(self.sent))

    async def _ping(self, marker: int) -> None:
        started = time.monotonic()
        ok = False
        try:
            waiter = await self.ws.ping()
            # Every degrade_rtt_s the ping is still out, decide whether the
            # network or the reader is holding it (point 6).
            check_s = max(self.cfg.degrade_rtt_s, 0.05)
            while True:
                try:
                    await asyncio.wait_for(asyncio.shield(waiter), check_s)
                    break
                except TimeoutError:
                    self._ping_net_stalled = self._network_stalled()
            ok = True
        except asyncio.CancelledError:
            raise
        except Exception:
            pass  # connection closing; the handler cleans up
        finally:
            self._ping_task = None
            self._ping_started = None
        if ok:
            now = time.monotonic()
            slow = (now - started >= self.cfg.degrade_rtt_s
                    and not self._ping_net_stalled)
            if slow:
                self._last_slow_rtt = now - started
                self._note_slow(now, now - started)
            rtt = (now - started) * 1000.0
            self.rtt_ms = rtt if self.rtt_ms is None else 0.8 * self.rtt_ms + 0.2 * rtt
            if marker > self.acked:
                self.acked = marker
            self._measure_read_rate(now)
            self.wake.set()
            self._maybe_ping()

    def _measure_read_rate(self, now: float) -> None:
        """Read rate = acks per second over a period the client was the
        bottleneck (sender blocked on the window most of the time)."""
        elapsed = now - self._meas_t0
        if elapsed < READ_RATE_WINDOW_S:
            return
        gated = self._gated_total + (now - self._gated_t if self.gated else 0.0)
        if gated - self._meas_gated0 >= SATURATED_FRACTION * elapsed:
            sample = (self.acked - self._meas_acked0) / elapsed
            first = self.read_rate is None
            self.read_rate = sample if first else 0.5 * self.read_rate + 0.5 * sample
            self.read_rate_at = now
            if first:
                # The client is at least as far behind as its last slow ping.
                self._bucket = self._last_slow_rtt * self.read_rate
                self._bucket_t = now
        self._meas_t0, self._meas_acked0, self._meas_gated0 = now, self.acked, gated
