#!/usr/bin/env bash
set -euo pipefail
IFACE="${1:-lo}"
sudo tc qdisc del dev "$IFACE" root 2>/dev/null || true
sudo tc qdisc add dev "$IFACE" root netem delay 100ms 20ms distribution normal
echo "Applied ~100 ms delay with jitter on $IFACE"
