#!/usr/bin/env bash
set -euo pipefail
IFACE="${1:-lo}"
echo "NOTE: this reorders IP packets, not WebSocket messages. TCP restores byte-stream order; expect retransmission/head-of-line effects."
sudo tc qdisc del dev "$IFACE" root 2>/dev/null || true
sudo tc qdisc add dev "$IFACE" root netem delay 20ms reorder 20% 50%
echo "Applied IP packet reordering profile on $IFACE"
