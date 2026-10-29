# Package Contents

- `solution_server.py` - working naive broadcaster baseline; graded mechanisms intentionally absent.
- `caption_source.py` - deterministic revision source; exact-segment and long/demo modes.
- `load_clients.py` - scalable clients with role labels, finalized resume state, and history duplicate/stale checks.
- `run_demo.py` - independent public runs + median summary.
- `analyse_results.py` - correctness/freshness/coverage, slow-reader split, and recovery timing.
- `monitor_server.py` - optional Ubuntu RSS memory observation helper.
- `client/` - working naive visual browser skeleton.
- `network_profiles/` - public `tc/netem` examples.
- `START_HERE.md`, `DEMO_GUIDE.md`, `PUBLIC_TESTS.md`, `PROTOCOL_EXAMPLES.md`, `NETWORKING_PRIMER.md`, `UBUNTU_SETUP.md` - student guides.
- `submission_check.py` - startup/WebSocket compatibility checker; `--final` checks final deliverables.
- `README_TEMPLATE.md`, `AI_USAGE_TEMPLATE.md`, `CONTRIBUTIONS_TEMPLATE.md` - submission templates.
- `bonus_asr/` - optional-only bonus guidance.
