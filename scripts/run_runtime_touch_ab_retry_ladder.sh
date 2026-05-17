#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LADDER_TIMESTAMP="${LADDER_TIMESTAMP:-runtime-touch-ab-retry-ladder-$(date -u +%Y%m%dT%H%M%SZ)}"
OUTPUT_DIR="${OUTPUT_DIR:-${ROOT_DIR}/.tmp/operational/${LADDER_TIMESTAMP}/runtime_touch_ab_retry_ladder}"
FRESH_DUCKDB="${FRESH_DUCKDB:-}"
FRESH_REPORT_ROOT="${FRESH_REPORT_ROOT:-}"
OBSERVATION_SECONDS="${REAL_DRY_RUN_SECONDS:-3600}"
FRESH_CAPTURE_SECONDS="${FRESH_CAPTURE_SECONDS:-1800}"
MIN_ASSETS="${EXECUTION_PROBE_UNIVERSE_MIN_ASSETS:-2}"
PROFILE_A="${PROFILE_A:-execution_probe_v11}"
PROFILE_B="${PROFILE_B:-execution_probe_v12}"
MIN_RUNTIME_TOUCH_SNAPSHOTS="${EXECUTION_PROBE_MIN_RUNTIME_TOUCH_SNAPSHOTS:-10}"
PRINT_PLAN=0

usage() {
  cat <<'EOF'
Usage: scripts/run_runtime_touch_ab_retry_ladder.sh --fresh-duckdb PATH [--fresh-report-root PATH] [--duration-seconds N] [--print-plan]

Evaluates a research-only retry ladder for runtime-touch v11/v12 A/B.
It does not start services and does not execute trades. The selected output
is a reproducible command for scripts/run_runtime_touch_ab_cycle.sh.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --fresh-duckdb|--universe-duckdb)
      FRESH_DUCKDB="$2"
      shift 2
      ;;
    --fresh-report-root)
      FRESH_REPORT_ROOT="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --duration-seconds)
      OBSERVATION_SECONDS="$2"
      shift 2
      ;;
    --fresh-capture-seconds)
      FRESH_CAPTURE_SECONDS="$2"
      shift 2
      ;;
    --min-assets)
      MIN_ASSETS="$2"
      shift 2
      ;;
    --profile-a)
      PROFILE_A="$2"
      shift 2
      ;;
    --profile-b)
      PROFILE_B="$2"
      shift 2
      ;;
    --print-plan)
      PRINT_PLAN=1
      shift
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

if ! [[ "$OBSERVATION_SECONDS" =~ ^[0-9]+$ ]] || (( OBSERVATION_SECONDS < 1800 || OBSERVATION_SECONDS > 5400 )); then
  echo "duration must be an integer between 1800 and 5400 seconds" >&2
  exit 64
fi
if ! [[ "$FRESH_CAPTURE_SECONDS" =~ ^[0-9]+$ ]] || (( FRESH_CAPTURE_SECONDS < 1800 || FRESH_CAPTURE_SECONDS > 5400 )); then
  echo "fresh capture duration must be an integer between 1800 and 5400 seconds" >&2
  exit 64
fi
if ! [[ "$MIN_ASSETS" =~ ^[0-9]+$ ]] || (( MIN_ASSETS < 2 )); then
  echo "min assets must be an integer >= 2 for runtime-touch A/B" >&2
  exit 64
fi
if [[ "$PRINT_PLAN" != "1" && ( -z "$FRESH_DUCKDB" || ! -f "$FRESH_DUCKDB" ) ]]; then
  echo "--fresh-duckdb must exist unless --print-plan is used" >&2
  exit 64
fi

ARGS=(
  -m src.research.runtime_touch_ab_retry_ladder
  --output-dir "$OUTPUT_DIR"
  --duration-seconds "$OBSERVATION_SECONDS"
  --fresh-capture-seconds "$FRESH_CAPTURE_SECONDS"
  --min-assets "$MIN_ASSETS"
  --profile-a "$PROFILE_A"
  --profile-b "$PROFILE_B"
  --min-runtime-touch-snapshots "$MIN_RUNTIME_TOUCH_SNAPSHOTS"
)
if [[ -n "$FRESH_DUCKDB" ]]; then
  ARGS+=(--fresh-duckdb "$FRESH_DUCKDB")
fi
if [[ -n "$FRESH_REPORT_ROOT" ]]; then
  ARGS+=(--fresh-report-root "$FRESH_REPORT_ROOT")
fi
if [[ "$PRINT_PLAN" == "1" ]]; then
  ARGS+=(--print-plan)
fi

cd "$ROOT_DIR"
PYTHONPATH=python-service python3 "${ARGS[@]}"
