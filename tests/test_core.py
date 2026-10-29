"""Unit tests for liverecall_core.py.

Run: python -m unittest discover -s tests -v
"""

from __future__ import annotations

import json
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import liverecall_core as core  # noqa: E402


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
# §3 row 2 / §4.1 revision state
# ---------------------------------------------------------------------------

class SessionStateTests(unittest.TestCase):
    def setUp(self):
        self.s = core.SessionState("L1")

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
        self.assertEqual(s.finals[1].key, (1, 4))

if __name__ == "__main__":
    unittest.main()
