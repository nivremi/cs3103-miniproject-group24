#!/usr/bin/env bash
set -euo pipefail
IFACE="${1:-lo}"
sudo tc qdisc del dev "$IFACE" root 2>/dev/null || true
sudo tc qdisc add dev "$IFACE" root netem loss 3%
echo "Applied 3% IP packet loss on $IFACE (TCP will retransmit; WebSocket messages remain reliable/ordered)"
