#!/usr/bin/env bash
set -euo pipefail
IFACE="${1:-lo}"
sudo tc qdisc del dev "$IFACE" root 2>/dev/null || true
sudo tc qdisc add dev "$IFACE" root netem delay 120ms 30ms distribution normal loss 3%
echo "Applied delay+jitter+3% IP packet loss on $IFACE"
echo "Reminder: WebSocket uses TCP; loss/reordering appears mainly as retransmission/head-of-line delay, not missing application messages."
