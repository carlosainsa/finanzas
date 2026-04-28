#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage: scripts/analyze_pre_live_blockers.sh /path/to/report-root [output-dir]" >&2
  exit 64
fi

REPORT_ROOT="$1"
OUTPUT_DIR="${2:-$REPORT_ROOT/blocker_diagnostics}"

PYTHONPATH="${PYTHONPATH:-python-service}" \
  python3 -m src.research.pre_live_blocker_analysis \
  --report-root "$REPORT_ROOT" \
  --output-dir "$OUTPUT_DIR"
