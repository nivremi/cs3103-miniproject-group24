#!/usr/bin/env bash
set -euo pipefail
IFACE="${1:-lo}"
sudo tc qdisc del dev "$IFACE" root 2>/dev/null || true
sudo tc qdisc add dev "$IFACE" root tbf rate 2mbit burst 32kbit latency 400ms
echo "Applied 2 Mbit/s rate limit on $IFACE"
