#!/usr/bin/env bash
# ChronoSaga Saga Failure Demo (Bash)
# Demonstrates forward activity execution, downstream failure, and strict reverse LIFO compensation.
set -euo pipefail

echo "=========================================================="
echo "     ChronoSaga Demo: Saga Reverse LIFO Compensation      "
echo "=========================================================="

BINARY="bin/chronosaga"
if [ ! -f "$BINARY" ]; then
    echo "Building $BINARY..."
    go build -o "$BINARY" ./cmd/chronosaga
fi

exec "$BINARY" demo saga-failure
