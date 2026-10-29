"""Unit tests for liverecall_core.py and solution_server.py.

Run: python -m unittest discover -s tests -v
Uses FakeClientWS; no network needed.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.fakes import FakeClientWS, install_fake_websockets_module  # noqa: E402

install_fake_websockets_module()

import liverecall_core as core  # noqa: E402
import solution_server as server  # noqa: E402


def upd(seg, rev, final=False, ts=None, text=None, session="L1", **extra):
    msg = {"type": "CAPTION_UPDATE", "session": session, "segment": seg,
           "revision": rev, "source_ts": time.time() if ts is None else ts,
           "final": final, "text": text or f"s{seg} r{rev}"}
    msg.update(extra)
    return msg


def cap(*a, **kw):
    return core.parse_caption_update(upd(*a, **kw))


def keys(msgs):
    return [(m["segment"], m["revision"]) for m in msgs]


# ---------------------------------------------------------------------------
# §4.1 validation
# ---------------------------------------------------------------------------

class ParseTests(unittest.TestCase):
    def test_valid_update_is_canonicalised(self):
        c = core.parse_caption_update(upd(3, 2, text="TCP ✓ 中文", phase="warmup"))
        wire = json.loads(c.wire)
        self.assertEqual(wire["text"], "TCP ✓ 中文")
        self.assertNotIn("phase", wire)              # extra fields not forwarded
        self.assertEqual(set(wire), {"type", "session", "segment", "revision",
                                     "source_ts", "final", "text"})

    def test_invalid_fields_rejected(self):
        bad = [upd(True, 1), upd(0, 1), upd(1, -1), upd(1, 1, final="yes"),
               upd(1, 1, ts=float("nan")), upd("1", 1), upd(1, 1, text=5)]
        for m in bad:
            with self.assertRaises(ValueError, msg=m):
                core.parse_caption_update(m)
        m = upd(1, 1)
        m["text"] = 7
        with self.assertRaises(ValueError):
            core.parse_caption_update(m)


# ---------------------------------------------------------------------------
# §3 rows 2, 5-8 / §4.1-4.2 session state
# ---------------------------------------------------------------------------

class SessionStateTests(unittest.TestCase):
    def setUp(self):
        self.s = core.SessionState("L1", core.ServerConfig())

    def test_revision_rules(self):
        s = self.s
        self.assertTrue(s.apply(cap(1, 1)))
        self.assertFalse(s.apply(cap(1, 1)))     # duplicate
        self.assertTrue(s.apply(cap(1, 3)))
        self.assertFalse(s.apply(cap(1, 2)))     # stale
        self.assertTrue(s.apply(cap(1, 4, final=True)))
        self.assertFalse(s.apply(cap(1, 9)))     # nothing overwrites a final
        self.assertFalse(s.apply(cap(1, 9, final=True)))
        self.assertEqual(s.active, {})
        self.assertEqual([c.key for c in s.history], [(1, 4)])

    def test_history_bounded_by_time_and_count(self):
        cfg = core.ServerConfig(history_retain_s=60, history_max_items=1000)
        s = core.SessionState("L1", cfg)
        t0 = 1_000_000.0
        for seg in range(1, 301):                # 300 finals, one per second
            s.apply(cap(seg, 1, final=True, ts=t0 + seg))
        self.assertLessEqual(len(s.history), 61)
        self.assertEqual(s.history[-1].segment, 300)
        self.assertGreaterEqual(s.history[-1].source_ts - s.history[0].source_ts, 59)
        # An update for an expired segment is rejected.
        self.assertFalse(s.apply(cap(5, 1, ts=t0 + 301)))

        cfg2 = core.ServerConfig(history_retain_s=10_000, history_max_items=50)
        s2 = core.SessionState("L1", cfg2)
        for seg in range(1, 301):
            s2.apply(cap(seg, 1, final=True, ts=t0 + seg))
        self.assertEqual(len(s2.history), 50)

    def test_recovery_resume_and_late_join(self):
        s = self.s
        t0 = 1_000_000.0
        for seg in range(1, 41):                 # finals every second
            s.apply(cap(seg, 3, final=True, ts=t0 + seg))
        s.apply(cap(41, 1, ts=t0 + 41))          # provisional, active

        items, gap = s.recovery_items(38, 3, 30)
        self.assertEqual(keys(json.loads(c.wire) for c in items), [(39, 3), (40, 3)])
        self.assertFalse(gap)

        items, _ = s.recovery_items(0, 0, 30)   # late join: last 30 s of finals
        segs = [c.segment for c in items]
        self.assertEqual(segs, list(range(11, 41)))
        self.assertTrue(all(c.final for c in items))   # never provisional

        items, _ = s.recovery_items(0, 0, 999)  # clamped to 60 s
        self.assertEqual(items[0].segment, 1)

        items, _ = s.recovery_items(5, 3, 30)   # resume ignores history_seconds
        self.assertEqual(items[0].segment, 6)

    def test_gap_flag_when_resume_point_expired(self):
        s = core.SessionState("L1", core.ServerConfig(history_retain_s=60))
        t0 = 1_000_000.0
        for seg in range(1, 201):
            s.apply(cap(seg, 1, final=True, ts=t0 + seg))
        items, gap = s.recovery_items(10, 1, 30)
        self.assertTrue(gap)
        self.assertEqual(items[0].segment, s.history[0].segment)

    def test_history_message_format(self):
        s = self.s
        s.apply(cap(1, 2, final=True, ts=10.0))
        msg = json.loads(core.history_message("L1", list(s.history), False))
        self.assertEqual(msg["type"], "HISTORY")
        self.assertEqual(msg["items"][0]["type"], "CAPTION_UPDATE")
        self.assertEqual(msg["items"][0]["segment"], 1)


# ---------------------------------------------------------------------------
# §3 rows 9-11, §4.3 per-client delivery
# ---------------------------------------------------------------------------

async def run_source(state, segments, revisions=3, rev_delay=0.0, gap=0.0, start=1):
    """Feed a state like caption_source.py does, broadcasting accepted updates."""
    for seg in range(start, start + segments):
        for rev in range(1, revisions + 1):
            c = cap(seg, rev, final=(rev == revisions))
            if state.apply(c):
                state.broadcast(c)
            if rev != revisions and rev_delay:
                await asyncio.sleep(rev_delay)
        if gap:
            await asyncio.sleep(gap)


def check_client_invariants(test, ws):
    """No stale/duplicate revision and every final at most once."""
    highest = {}
    for m in ws.messages("CAPTION_UPDATE"):
        seg, rev = m["segment"], m["revision"]
        test.assertGreater(rev, highest.get(seg, 0), f"stale/duplicate {seg}/{rev}")
        highest[seg] = rev


class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def _client(self, state, ws, cfg, last_segment=0):
        conn = core.ClientConn(ws, state, 1, last_segment, cfg)
        state.clients.add(conn)
        conn.start()
        return conn

    async def test_fast_client_gets_every_revision_even_in_bursts(self):
        cfg = core.ServerConfig()
        state = core.SessionState("L1", cfg)
        ws = FakeClientWS()
        conn = await self._client(state, ws, cfg)
        await run_source(state, 30)               # 90 updates in one burst
        await asyncio.sleep(0.05)
        self.assertEqual(len(ws.messages()), 90)
        self.assertEqual(conn.coalesced, 0)
        check_client_invariants(self, ws)
        conn.stop()

    async def test_slow_reader_coalesced_bounded_and_complete(self):
        # Time-scaled public T3: source 3 revs per 0.075 s, reader 0.05 s/msg
        # (same ratios as 0.2 s / 0.35 s gap / 500 ms slow reader).
        cfg = core.ServerConfig(coalesce_age_s=0.015)
        state = core.SessionState("L1", cfg)
        slow = FakeClientWS(period=0.05)
        fast = FakeClientWS()
        c_slow = await self._client(state, slow, cfg)
        c_fast = await self._client(state, fast, cfg)
        segments = 60
        await run_source(state, segments, rev_delay=0.02, gap=0.035)
        await asyncio.sleep(1.5)                  # let the slow reader drain

        finals_slow = {m["segment"] for m in slow.messages() if m["final"]}
        self.assertEqual(finals_slow, set(range(1, segments + 1)))  # complete
        self.assertEqual(len(fast.messages()), segments * 3)        # unaffected
        self.assertGreater(c_slow.coalesced, 0)
        self.assertLessEqual(c_slow.max_npending, 10)               # bounded
        check_client_invariants(self, slow)
        # In-flight data is bounded by the window, so the slow client is at
        # most ~window messages behind, not the whole backlog.
        self.assertLessEqual(slow.sent_count - len(slow.received), cfg.window)
        c_slow.stop()
        c_fast.stop()

    async def test_without_window_backlog_grows(self):
        """Ablation: window off + no coalescing = every revision goes into
        the transport, so the slow reader falls further and further behind."""
        cfg = core.ServerConfig(window=0, coalesce=False)
        state = core.SessionState("L1", cfg)
        slow = FakeClientWS(period=0.05)
        conn = await self._client(state, slow, cfg)
        await run_source(state, 60, rev_delay=0.02, gap=0.035)
        backlog = slow.sent_count - len(slow.received)
        self.assertGreater(backlog, 50)
        conn.stop()
        slow._consumer.cancel()

    async def test_overflow_closes_with_1013(self):
        cfg = core.ServerConfig(max_pending=20)
        state = core.SessionState("L1", cfg)
        stuck = FakeClientWS(period=3600)         # never drains
        conn = await self._client(state, stuck, cfg)
        await run_source(state, 40)
        await asyncio.sleep(0.01)
        self.assertTrue(conn.overflowed)
        self.assertEqual(stuck.close_code, core.CLOSE_TOO_SLOW)
        self.assertEqual(conn.npending, 0)        # memory released
        if stuck._consumer:
            stuck._consumer.cancel()

    async def test_floor_skips_finals_client_already_has(self):
        cfg = core.ServerConfig()
        state = core.SessionState("L1", cfg)
        ws = FakeClientWS()
        conn = await self._client(state, ws, cfg, last_segment=5)
        await run_source(state, 7)
        await asyncio.sleep(0.02)
        self.assertEqual({m["segment"] for m in ws.messages()}, {6, 7})
        conn.stop()


# ---------------------------------------------------------------------------
# Handler-level flow (§4.2): HISTORY first, resume, no duplicate finals
# ---------------------------------------------------------------------------

class HandlerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        server.SESSIONS.clear()

    async def _source(self):
        src = FakeClientWS()
        task = asyncio.create_task(server.handler(src))
        await src.client_send({"type": "SOURCE_HELLO", "session": "L1"})
        await asyncio.sleep(0.01)                 # let the handler register
        return src, task

    async def _student(self, last_segment=0, last_revision=0, history_seconds=30):
        ws = FakeClientWS()
        task = asyncio.create_task(server.handler(ws))
        await ws.client_send({"type": "CLIENT_HELLO", "client_id": 7, "session": "L1",
                              "last_segment": last_segment, "last_revision": last_revision,
                              "history_seconds": history_seconds})
        await asyncio.sleep(0.01)
        return ws, task

    async def test_reconnect_recovers_missed_finals_without_duplicates(self):
        src, src_task = await self._source()
        a, a_task = await self._student()
        for seg in range(1, 6):
            for rev in (1, 2, 3):
                await src.client_send(upd(seg, rev, final=rev == 3))
        await asyncio.sleep(0.05)
        first = a.messages()
        self.assertEqual(first[0]["type"], "HISTORY")
        self.assertEqual(len(a.messages("CAPTION_UPDATE")), 15)

        await a.disconnect()                      # client goes away
        await a_task
        for seg in range(6, 10):                  # missed while offline
            for rev in (1, 2, 3):
                await src.client_send(upd(seg, rev, final=rev == 3))
        await asyncio.sleep(0.02)

        b, b_task = await self._student(last_segment=5, last_revision=3)
        await src.client_send(upd(10, 1))
        await src.client_send(upd(10, 2, final=True))
        await asyncio.sleep(0.05)
        hist = b.messages("HISTORY")[0]
        self.assertEqual(keys(hist["items"]), [(6, 3), (7, 3), (8, 3), (9, 3)])
        self.assertEqual(keys(b.messages("CAPTION_UPDATE")), [(10, 1), (10, 2)])

        # late joiner gets recent finals then live
        c, c_task = await self._student()
        await asyncio.sleep(0.02)
        late = c.messages("HISTORY")[0]
        self.assertEqual([i["segment"] for i in late["items"]], list(range(1, 11)))

        for ws in (b, c, src):
            await ws.disconnect()
        await asyncio.gather(b_task, c_task, src_task)

    async def test_bad_hello_and_bad_messages_do_not_crash(self):
        ws = FakeClientWS()
        task = asyncio.create_task(server.handler(ws))
        await ws._incoming.put("not json")
        await task
        self.assertEqual(ws.close_code, 1008)

        src, src_task = await self._source()
        await src._incoming.put("{broken")
        await src.client_send({"type": "CAPTION_UPDATE", "segment": "x"})
        await src.client_send({"type": "SOMETHING_NEW"})
        await src.client_send(upd(1, 1, phase="extra field"))
        st, st_task = await self._student()
        await src.client_send(upd(1, 2, final=True))
        await asyncio.sleep(0.02)
        self.assertEqual(keys(st.messages("CAPTION_UPDATE")), [(1, 2)])
        await st.disconnect()
        await src.disconnect()
        await asyncio.gather(st_task, src_task)


if __name__ == "__main__":
    unittest.main()
