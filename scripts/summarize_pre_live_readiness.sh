#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: scripts/summarize_pre_live_readiness.sh /path/to/pre_live_readiness.json" >&2
  exit 64
fi

PYTHONPATH="${PYTHONPATH:-python-service}" \
  python3 -m src.research.pre_live_readiness \
  --input "$1" \
  --format summary
