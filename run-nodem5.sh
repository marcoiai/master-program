#!/usr/bin/env bash
set -euo pipefail

pkill -f master_program 2>/dev/null || true
cd "$(dirname "$0")"
nohup env MASTER_PROGRAM_HOST=0.0.0.0 MASTER_PROGRAM_NODE_ID=m5 MASTER_PROGRAM_PORT=17321 cargo run --quiet > /tmp/master_program-m5.log 2>&1 &
