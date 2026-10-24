#!/usr/bin/env bash
# ChronoSaga Benchmark Suite (Bash)
# Runs the full benchmark workload and records measurements into results/
set -euo pipefail

echo "=========================================================="
echo "        ChronoSaga Benchmark Suite Protocol               "
echo "=========================================================="

BINARY="bin/chronosaga"
if [ ! -f "$BINARY" ]; then
    echo "Building $BINARY..."
    go build -o "$BINARY" ./cmd/chronosaga
fi

exec "$BINARY" benchmark "$@"
