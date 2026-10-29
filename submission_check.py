#!/usr/bin/env python3
"""Compatibility check for the LiveRecall submission structure and startup path."""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
import time
from pathlib import Path

import websockets

DEV_REQUIRED = [
    "solution_server.py",
    "requirements.txt",
    "client/index.html",
    "client/app.js",
]
FINAL_REQUIRED = [
    "README.md",
    "AI_USAGE.md",
    "CONTRIBUTIONS.md",
    "report.pdf",
]


async def websocket_probe(port: int) -> None:
    uri = f"ws://127.0.0.1:{port}"
    async with websockets.connect(uri, open_timeout=2) as ws:
        await ws.send(json.dumps({
            "type": "CLIENT_HELLO",
            "client_id": 999999,
            "session": "L1",
            "last_segment": 0,
            "last_revision": 0,
            "history_seconds": 30,
        }))
        # A server may or may not immediately send HISTORY under the current
        # minimum interface. The important compatibility check here is a real
        # WebSocket handshake + successful CLIENT_HELLO send without crashing.
        try:
            await asyncio.wait_for(ws.recv(), timeout=0.4)
        except asyncio.TimeoutError:
            pass


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--final", action="store_true",
                   help="also require README.md, AI_USAGE.md, CONTRIBUTIONS.md and report.pdf")
    p.add_argument("--port", type=int, default=9876)
    args = p.parse_args()

    root = Path.cwd()
    required = list(DEV_REQUIRED) + (FINAL_REQUIRED if args.final else [])
    missing = [name for name in required if not (root / name).exists()]
    if missing:
        print("Missing required files:")
        for name in missing:
            print(" -", name)
        raise SystemExit(1)

    print("Required files found.")
    print(f"Starting: python solution_server.py --port {args.port}")
    proc = subprocess.Popen(
        [sys.executable, "-u", "solution_server.py", "--port", str(args.port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        deadline = time.time() + 10
        last_error = None
        while time.time() < deadline:
            if proc.poll() is not None:
                out = proc.stdout.read() if proc.stdout else ""
                print(out)
                raise SystemExit("Server exited before becoming ready.")
            try:
                asyncio.run(websocket_probe(args.port))
                print("PASS: WebSocket connection and CLIENT_HELLO succeeded.")
                print("This check does not validate revision, recovery, queue, or performance correctness.")
                return
            except Exception as e:
                last_error = e
                time.sleep(0.2)
        raise SystemExit(f"Server was not WebSocket-ready within 10 seconds: {last_error}")
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()


if __name__ == "__main__":
    main()
