#!/usr/bin/env bash
# Bandwidth-recovery stress test (not a public test; harness files unchanged).
#
# Brief §10: the overload policy should "return to full-update behavior when
# the client/network catches up". Timeline (seconds after the source starts):
#   0 .. ON_AT        unimpaired
#   ON_AT .. OFF_AT   network_profiles/bandwidth_2mbit.sh on lo (overload)
#   OFF_AT .. end     unimpaired again: how fast do clients get every revision?
#
# Usage (from submission/, in a real terminal: sudo asks for a password once):
#   ./stress_tests/bw_recovery.sh [clients] [duration] [on_at] [off_at]
# Then: cs3103/bin/python stress_tests/bw_recovery_analyse.py stress_tests/results/bw_recovery
set -euo pipefail
cd "$(dirname "$0")/.."

CLIENTS="${1:-400}"     # ~100 clients fit in 2 Mbit/s; 400 overload it
DURATION="${2:-90}"
ON_AT="${3:-20}"
OFF_AT="${4:-50}"
PORT=8765
PY=cs3103/bin/python
OUT=stress_tests/results/bw_recovery
SEGMENTS=$(( (DURATION - 3) * 4 / 3 ))   # ~0.75 s/segment, as run_demo.py

sudo -v                                  # ask for the password before starting
cleanup() {
    sudo ./network_profiles/reset.sh lo >/dev/null 2>&1 || true
    kill "${SERVER:-}" "${CLIENTS_PID:-}" 2>/dev/null || true
}
trap cleanup EXIT
sudo ./network_profiles/reset.sh lo

"$PY" -u solution_server.py --port "$PORT" --stats-interval 2 > "$OUT.server.log" 2>&1 &
SERVER=$!
sleep 2
"$PY" -u load_clients.py --uri "ws://127.0.0.1:$PORT" --clients "$CLIENTS" \
    --duration "$DURATION" --output "$OUT.csv" &
CLIENTS_PID=$!
sleep 1

T0=$(date +%s.%N)
echo "$T0 $ON_AT $OFF_AT $SEGMENTS $CLIENTS" > "$OUT.meta"
"$PY" -u caption_source.py --uri "ws://127.0.0.1:$PORT" --segments "$SEGMENTS" --quiet &
SOURCE=$!

sleep "$ON_AT"
sudo ./network_profiles/bandwidth_2mbit.sh lo
echo ">>> t=${ON_AT}s bandwidth limit ON"
sleep $(( OFF_AT - ON_AT ))
sudo ./network_profiles/reset.sh lo
echo ">>> t=${OFF_AT}s bandwidth limit OFF"

wait "$SOURCE" || true
wait "$CLIENTS_PID" || true
echo "Done. Results: $OUT.csv, server log: $OUT.server.log"
