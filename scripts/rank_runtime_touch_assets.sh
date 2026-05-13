#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKDB_PATH=""
OUTPUT_DIR=""
LOOKBACK_MS="${EXECUTION_PROBE_RUNTIME_TOUCH_LOOKBACK_MS:-900000}"
MIN_SNAPSHOTS="${EXECUTION_PROBE_MIN_RUNTIME_TOUCH_SNAPSHOTS:-10}"
MIN_TOUCH_CHANGE_RATE="${EXECUTION_PROBE_MIN_RUNTIME_TOUCH_CHANGE_RATE:-0.01}"
LIMIT="${EXECUTION_PROBE_UNIVERSE_LIMIT:-20}"

usage() {
  cat <<'EOF'
Usage: scripts/rank_runtime_touch_assets.sh --duckdb PATH --output-dir PATH [--lookback-ms N] [--min-snapshots N] [--min-touch-change-rate X] [--limit N]

Ranks assets by fresh runtime top-of-book changes from orderbook snapshots.
The output is research-only and cannot enable live trading.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --duckdb)
      DUCKDB_PATH="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --lookback-ms)
      LOOKBACK_MS="$2"
      shift 2
      ;;
    --min-snapshots)
      MIN_SNAPSHOTS="$2"
      shift 2
      ;;
    --min-touch-change-rate)
      MIN_TOUCH_CHANGE_RATE="$2"
      shift 2
      ;;
    --limit)
      LIMIT="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 64
      ;;
  esac
done

if [[ -z "$DUCKDB_PATH" || ! -f "$DUCKDB_PATH" ]]; then
  echo "--duckdb must point to an existing DuckDB file" >&2
  exit 64
fi
if [[ -z "$OUTPUT_DIR" ]]; then
  echo "--output-dir is required" >&2
  exit 64
fi

cd "$ROOT_DIR"
PYTHONPATH=python-service python3 -m src.research.runtime_touch_ranking \
  --duckdb "$DUCKDB_PATH" \
  --output-dir "$OUTPUT_DIR" \
  --lookback-ms "$LOOKBACK_MS" \
  --min-snapshots "$MIN_SNAPSHOTS" \
  --min-touch-change-rate "$MIN_TOUCH_CHANGE_RATE" \
  --limit "$LIMIT"
