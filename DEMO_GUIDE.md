# Mandatory Deterministic Application Demo

No ASR or speech-recognition setup is required.

## Suggested setup

Terminal 1:

```bash
python solution_server.py --port 8765
```

Terminal 2 (serve the browser files):

```bash
python -m http.server 8000 --directory client
```

Open `http://127.0.0.1:8000` in at least two browser tabs/windows.

Terminal 3, use long/demo mode so the 60-second recall view is meaningful:

```bash
python caption_source.py --duration 120
```

## Demonstrate

- provisional versus final captions are visually distinguishable;
- newer revisions replace older revisions of the same segment;
- `What did I miss?` defaults to 30 seconds and supports 15/30/60 seconds using local state under normal operation;
- one client temporarily disconnects, reconnects automatically, and recovers recent missed finalized captions without duplicate finals;
- a newly opened late client receives recent context and then continues with live updates.

The supplied browser is deliberately naive. It appends every revision so that the first revision-handling problem is visible immediately. Replace that behavior as part of your solution.
