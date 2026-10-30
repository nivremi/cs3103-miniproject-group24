# Team XX - LiveRecall

> Iteration 1. Team details, final experiment results and the report still need to be filled in.

## Team members

- Name / student ID / responsibility

## Ubuntu setup

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip iproute2
```

Python 3.10 or newer is required (Ubuntu 22.04+ ships it).

## Install

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python submission_check.py
```

## Required server

```bash
python solution_server.py --port 8765
```

Optional flags (the defaults are the submitted design):

| Flag | Purpose |
|---|---|
| `--stats-interval 5` | print delivery statistics every 5 s (coalesced revisions, gated clients, max pending, history size) |
| `--verbose` | log every client connect/disconnect |
| `--window 0` | disable the ping-acknowledged send window (experiment) |
| `--no-coalesce` | never drop superseded provisional revisions (experiment) |
| `--max-pending N` | per-client queued-item bound (default 400) |
| `--history-retain S` | seconds of finals kept for recovery (default 90, minimum 60) |

## Browser client

```bash
python -m http.server 8000 --directory client
```

Open `http://127.0.0.1:8000` in two or more tabs. The page connects to `ws://<page host>:8765` by default; override with `?server=ws://HOST:PORT&session=L1`.

- Provisional captions are italic on an amber background with a cursor; final captions are plain. A newer revision replaces the row in place.
- **What did I miss?** (button or `M` key) shows finalized captions from the last 30 s, with 15/30/60 s tabs. It reads local state only.
- On disconnect the status pill shows a countdown; the client reconnects with backoff and jitter, resumes from its latest final, and reports how many captions were recovered.

## Mandatory deterministic demo

```bash
# terminal 1
python solution_server.py --port 8765
# terminal 2
python -m http.server 8000 --directory client
# terminal 3 (long mode so the 60 s recall view is meaningful)
python caption_source.py --duration 120
```

- **Disconnect demo:** stop and restart the server, or in DevTools → Network set *Offline* for ~10 s and then back to *No throttling*.
- **Late join:** open a new tab after ~40 s.

## Tests

```bash
# unit tests (no network needed)
python -m unittest discover -s tests -t . -v
node --test tests/test_caption_store.mjs      # optional, needs Node 18+

# public tests (three independent runs each; report the median)
python run_demo.py --clients 20  --duration 20 --runs 3 --output-stem t1
python run_demo.py --clients 100 --duration 40 --runs 3 --output-stem t2
python run_demo.py --clients 100 --duration 60 --runs 3 --slow-percent 10 --slow-ms 500 --output-stem t3
python run_demo.py --clients 100 --duration 60 --runs 3 --disconnect-percent 20 --disconnect-at 15 --disconnect-seconds 5 --output-stem t4
sudo bash network_profiles/delay_loss.sh lo && python run_demo.py --clients 100 --duration 40 --runs 3 --output-stem t5; sudo bash network_profiles/reset.sh lo
```

## Design notes (iteration 1)

Code: `solution_server.py` (WebSocket glue) and `liverecall_core.py` (all graded logic; no network imports, unit-tested).

**Revision state.** `SessionState.apply` accepts an update only if it is newer than the current revision of an unfinished segment. A final is never replaced. Updates for segments older than the retained history are rejected. Only accepted updates are forwarded, so clients never receive stale or duplicate revisions from the server. Each update is validated and re-encoded once, with only the required fields, and that string is shared by every client.

**Bounded history.** Finals are kept in segment order for 90 s of source time, with a hard cap of 2000 items.

**HISTORY.**

- **Resume** (`last_segment > 0`): every retained final newer than `(last_segment, last_revision)`. `history_seconds` is ignored here because completeness matters more than the window size.
- **Late join**: finals from the last `history_seconds` (clamped to 60 s).
- HISTORY never contains provisional captions. It carries an extension field `gap: true` when some missed finals have already expired.
- The HISTORY snapshot and registration for live updates happen with no `await` in between, so no final can be missed or delivered twice.

**Per-client delivery (§4.3).** Each client has its own sender task. The source handler only does O(1) in-memory enqueues and never waits on a client.

1. **Send window.** At most 8 messages may be in flight beyond the last acknowledged WebSocket ping. The client's WebSocket stack answers a ping only after reading the data in front of it, so a slow reader stops receiving new data until it catches up. The backlog then stays in our buffer, where it can still be coalesced, instead of in TCP buffers, where it cannot. Fast clients answer within milliseconds and are never limited.
2. **Semantic coalescing.** The pending buffer is keyed by segment. A provisional revision is replaced by a newer revision of the same segment only when the client is behind: the window is full, or the item has waited at least 150 ms. Healthy clients therefore still receive every revision, even in bursts (Normal Update Coverage). Finals are never dropped.
3. **Hard bound.** If a client still has more than 400 queued items after compaction, it is closed with code 1013. It reconnects and repairs itself from the bounded history.

Simulated slow reader (unit-test model, time scaled ×10; source 4 msg/s, reader 2 msg/s, 120 segments):

| Variant | Slow client: finals by end | Slow client: median lag | Fast client |
|---|---|---|---|
| Final design | 120/120 | ~2.3 s (real-time equivalent) | every revision, ~0 ms |
| No window or coalescing | 63/120, falling further behind | ~23 s and growing | every revision |

These figures come from a model and still need confirming with `run_demo.py` T3 on Ubuntu. The real client library also buffers about 16 messages, which adds lag that the model does not include.

## Optional ASR bonus

Not attempted yet.
