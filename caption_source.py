#!/usr/bin/env python3
"""Deterministic caption/revision generator for LiveRecall.

Examples:
  python caption_source.py
  python caption_source.py --segments 40
  python caption_source.py --duration 120
  python caption_source.py --transcript sample_transcript.txt --repeat 3

Each segment is completed before the next segment starts. Segment numbers are
monotonically increasing from 1. Each segment has three revisions in this
public generator; student solutions must not rely on exactly three revisions.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path

import websockets

DEFAULT_SENTENCES = [
    "TCP provides reliable byte stream delivery between application processes.",
    "The receiver acknowledges data so that the sender can detect missing information.",
    "Round trip time measurements are used to estimate a retransmission timeout.",
    "HTTP version three uses QUIC over UDP rather than TCP.",
    "A router decrements the IPv4 time to live field before forwarding a packet.",
    "Network address translation changes addressing information at a network boundary.",
    "Packet loss may increase application delay when retransmission is required.",
    "A good network experiment should report both average and tail latency.",
    "Live captions may be revised several times before the final text is available.",
    "A reconnecting LiveRecall client should recover only the recent context that it missed.",
]


def load_sentences(path: str | None) -> list[str]:
    if not path:
        return DEFAULT_SENTENCES
    lines = [x.strip() for x in Path(path).read_text(encoding="utf-8").splitlines()]
    sentences = [x for x in lines if x and not x.startswith("#")]
    if not sentences:
        raise SystemExit("Transcript contains no usable sentences.")
    return sentences


def make_revisions(sentence: str) -> list[str]:
    words = sentence.split()
    if len(words) < 4:
        return [sentence]
    a = max(1, len(words) // 3)
    b = max(a + 1, (2 * len(words)) // 3)
    return [" ".join(words[:a]), " ".join(words[:b]), sentence]


async def run(args) -> None:
    sentences = load_sentences(args.transcript)
    start_mono = time.monotonic()
    emitted_segments = 0

    async with websockets.connect(args.uri, max_size=2**20) as ws:
        await ws.send(json.dumps({"type": "SOURCE_HELLO", "session": args.session}))

        segment = 1
        sentence_index = 0
        max_segments = args.segments
        if max_segments is None and args.duration is None:
            max_segments = len(sentences) * args.repeat

        while True:
            if max_segments is not None and emitted_segments >= max_segments:
                break
            if args.duration is not None and emitted_segments > 0:
                if time.monotonic() - start_mono >= args.duration:
                    break

            sentence = sentences[sentence_index % len(sentences)]
            sentence_index += 1
            revs = make_revisions(sentence)

            for revision, text in enumerate(revs, start=1):
                event = {
                    "type": "CAPTION_UPDATE",
                    "session": args.session,
                    "segment": segment,
                    "revision": revision,
                    "source_ts": time.time(),
                    "final": revision == len(revs),
                    "text": text,
                }
                await ws.send(json.dumps(event))
                if not args.quiet:
                    print(json.dumps(event), flush=True)
                if revision != len(revs):
                    await asyncio.sleep(args.revision_delay)

            emitted_segments += 1
            segment += 1
            await asyncio.sleep(args.segment_gap)

        await ws.send(json.dumps({
            "type": "SOURCE_DONE",
            "session": args.session,
            "source_ts": time.time(),
            "segments": emitted_segments,
        }))

    print(f"SOURCE_DONE: emitted {emitted_segments} finalized segments", flush=True)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--uri", default="ws://127.0.0.1:8765")
    p.add_argument("--session", default="L1")
    p.add_argument("--transcript", help="One sentence per line")
    p.add_argument("--segments", type=int,
                   help="Emit exactly this many finalized segments, cycling the transcript")
    p.add_argument("--duration", type=float,
                   help="Long/demo mode: keep cycling the transcript for about this many seconds")
    p.add_argument("--repeat", type=int, default=1,
                   help="Transcript cycles when --segments/--duration aren't supplied")
    p.add_argument("--revision-delay", type=float, default=0.20,
                   help="Seconds between provisional revisions")
    p.add_argument("--segment-gap", type=float, default=0.35,
                   help="Seconds after a final revision before the next segment")
    p.add_argument("--quiet", action="store_true")
    args = p.parse_args()

    if args.segments is not None and args.segments < 1:
        p.error("--segments must be >= 1")
    if args.duration is not None and args.duration <= 0:
        p.error("--duration must be > 0")
    if args.repeat < 1:
        p.error("--repeat must be >= 1")

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
