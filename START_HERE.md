# CS3103 Assignment 4 - LiveRecall Starter Package

This package gives you a **working naive baseline** plus public test tools. It is not a complete graded solution.

The baseline is intended to let you see `caption_source.py -> solution_server.py -> browser/load clients` working immediately. The networking mechanisms that distinguish strong submissions remain TODOs.

## What is already provided

- required `python solution_server.py --port <port>` startup path;
- basic WebSocket source/client connection handling;
- naive direct broadcast of caption updates;
- deterministic caption generator;
- scalable simulated clients and CSV logs;
- public metrics and independent-run support;
- minimal visible browser UI;
- Ubuntu `tc/netem` examples.

## What is deliberately NOT solved

- revision replacement / deduplication;
- bounded recovery history;
- correct late join and reconnect recovery;
- bounded per-client application queues;
- slow-client isolation;
- semantic coalescing and backpressure;
- constrained-bandwidth overload policy;
- final usable browser behavior.

## Quick start

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip iproute2
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python submission_check.py
```

Basic public experiment:

```bash
python run_demo.py --clients 20 --duration 20 --runs 1
```

For results used in the report, use three independent runs:

```bash
python run_demo.py --clients 100 --duration 40 --runs 3 --output-stem t2
```

See `PUBLIC_TESTS.md` for T1-T5.

## Browser demo

See `DEMO_GUIDE.md`. The starter browser intentionally appends each revision as a new line; this makes the revision-replacement TODO obvious.

## Short background references

- `PROTOCOL_EXAMPLES.md` - normal, reconnect, and late-join message flow.
- `NETWORKING_PRIMER.md` - WebSocket/TCP, asyncio, per-client queues, bounded state, and backpressure terminology.

These guides explain mechanics, not the optimal design.

## Submission compatibility

During development:

```bash
python submission_check.py
```

Before submission, after you have created the required final files:

```bash
python submission_check.py --final
```

The instructor reference implementation and hidden evaluator are not included. Do not modify the public measurement logic when reporting the required public-test results.
