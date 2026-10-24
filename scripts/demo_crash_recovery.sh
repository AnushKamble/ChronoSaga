#!/usr/bin/env bash
# ChronoSaga Crash Recovery Demo (Bash)
# Demonstrates process interruption mid-workflow, offline time elapsed, and deterministic replay resumption.
set -euo pipefail

echo "=========================================================="
echo "  ChronoSaga Demo: Crash Recovery & Deterministic Replay  "
echo "=========================================================="

BINARY="bin/chronosaga"
if [ ! -f "$BINARY" ]; then
    echo "Building $BINARY..."
    go build -o "$BINARY" ./cmd/chronosaga
fi

exec "$BINARY" demo crash-recovery
