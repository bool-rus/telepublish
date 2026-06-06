#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

set -a && source .env && set +a

pkill -f 'target/debug/telepublish' 2>/dev/null || true
sleep 1

setsid sh -c 'target/debug/telepublish > /tmp/telepublish.log 2>&1' &
echo "Started, PID: $!"
