#!/usr/bin/env bash
set -euo pipefail
IFACE="${1:-lo}"
sudo tc qdisc del dev "$IFACE" root 2>/dev/null || true
echo "Reset qdisc on $IFACE"
