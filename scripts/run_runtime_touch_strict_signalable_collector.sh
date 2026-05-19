#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COLLECTOR_TIMESTAMP="${COLLECTOR_TIMESTAMP:-runtime-touch-strict-signalable-collector-$(date -u +%Y%m%dT%H%M%SZ)}"
RUN_ROOT="${RUN_ROOT:-${ROOT_DIR}/.tmp/operational/${COLLECTOR_TIMESTAMP}}"
WINDOW_SECONDS="${WINDOW_SECONDS:-3600}"
OBSERVATION_SECONDS="${REAL_DRY_RUN_SECONDS:-1800}"
MAX_WINDOWS="${MAX_WINDOWS:-3}"
MIN_ASSETS="${EXECUTION_PROBE_UNIVERSE_MIN_ASSETS:-2}"
PRINT_PLAN=0

usage() {
  cat <<'EOF'
Usage: scripts/run_runtime_touch_strict_signalable_collector.sh [--window-seconds N] [--duration-seconds N] [--max-windows N] [--min-assets N] [--print-plan]

Research-only collector:
repeat fresh runtime windows -> retry ladder -> stop only when strict_signalable has enough assets.
It keeps EXECUTION_MODE=dry_run, never runs A/B, and never enables live trading.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-root)
      RUN_ROOT="$2"
      shift 2
      ;;
    --window-seconds|--fresh-capture-seconds)
      WINDOW_SECONDS="$2"
      shift 2
      ;;
    --duration-seconds)
      OBSERVATION_SECONDS="$2"
      shift 2
      ;;
    --max-windows)
      MAX_WINDOWS="$2"
      shift 2
      ;;
    --min-assets)
      MIN_ASSETS="$2"
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

if ! [[ "$WINDOW_SECONDS" =~ ^[0-9]+$ ]] || (( WINDOW_SECONDS < 3600 || WINDOW_SECONDS > 5400 )); then
  echo "window duration must be an integer between 3600 and 5400 seconds" >&2
  exit 64
fi
if ! [[ "$OBSERVATION_SECONDS" =~ ^[0-9]+$ ]] || (( OBSERVATION_SECONDS < 1800 || OBSERVATION_SECONDS > 5400 )); then
  echo "observation duration must be an integer between 1800 and 5400 seconds" >&2
  exit 64
fi
if ! [[ "$MAX_WINDOWS" =~ ^[0-9]+$ ]] || (( MAX_WINDOWS < 1 || MAX_WINDOWS > 12 )); then
  echo "max windows must be an integer between 1 and 12" >&2
  exit 64
fi
if ! [[ "$MIN_ASSETS" =~ ^[0-9]+$ ]] || (( MIN_ASSETS < 2 )); then
  echo "min assets must be an integer >= 2" >&2
  exit 64
fi
if [[ "${EXECUTION_MODE:-dry_run}" != "dry_run" ]]; then
  echo "Refusing to run: EXECUTION_MODE must be dry_run or unset." >&2
  exit 64
fi

SUMMARY_PATH="${RUN_ROOT}/runtime_touch_strict_signalable_collector_summary.json"
WINDOW_REPORTS_FILE="${RUN_ROOT}/runtime_touch_strict_signalable_window_reports.txt"

if [[ "$PRINT_PLAN" == "1" ]]; then
  python3 - "$RUN_ROOT" "$WINDOW_SECONDS" "$OBSERVATION_SECONDS" "$MAX_WINDOWS" "$MIN_ASSETS" "$SUMMARY_PATH" <<'PY'
import json
import sys

(
    run_root,
    window_seconds,
    observation_seconds,
    max_windows,
    min_assets,
    summary_path,
) = sys.argv[1:]
print(json.dumps({
    "script": "scripts/run_runtime_touch_strict_signalable_collector.sh",
    "can_execute_trades": False,
    "can_promote_live": False,
    "execution_mode": "dry_run",
    "decision_policy": "runtime_touch_strict_signalable_collection_only",
    "window_seconds": int(window_seconds),
    "observation_seconds": int(observation_seconds),
    "max_windows": int(max_windows),
    "min_assets": int(min_assets),
    "delegates_to": [
        "scripts/run_runtime_touch_ab_auto_route.sh",
        "src.research.runtime_touch_strict_signalable_collector",
    ],
    "stop_condition": "route_result == READY_FOR_STRICT_AB_RETRY",
    "outputs": {
        "run_root": run_root,
        "window_reports": f"{run_root}/runtime_touch_strict_signalable_window_reports.txt",
        "summary": summary_path,
    },
}, indent=2, sort_keys=True))
PY
  exit 0
fi

mkdir -p "$RUN_ROOT"
: > "$WINDOW_REPORTS_FILE"

for window_index in $(seq 1 "$MAX_WINDOWS"); do
  window_label="$(printf 'window-%02d' "$window_index")"
  window_run_root="${RUN_ROOT}/windows/${window_label}"
  fresh_timestamp="${COLLECTOR_TIMESTAMP}-${window_label}-fresh"
  fresh_data_lake_root="${ROOT_DIR}/.tmp/real-dry-run-data-lake/${fresh_timestamp}"
  fresh_duckdb="${fresh_data_lake_root}/research.duckdb"
  fresh_report_root="${fresh_data_lake_root}/reports/${fresh_timestamp}"

  RUN_ROOT="$window_run_root" \
    FRESH_TIMESTAMP="$fresh_timestamp" \
    FRESH_DATA_LAKE_ROOT="$fresh_data_lake_root" \
    FRESH_DUCKDB="$fresh_duckdb" \
    FRESH_REPORT_ROOT="$fresh_report_root" \
    EXECUTION_MODE=dry_run \
    "$ROOT_DIR/scripts/run_runtime_touch_ab_auto_route.sh" \
      --fresh-capture-seconds "$WINDOW_SECONDS" \
      --duration-seconds "$OBSERVATION_SECONDS"

  ladder_report="${window_run_root}/runtime_touch_ab_retry_ladder/runtime_touch_ab_retry_ladder.json"
  printf '%s\n' "$ladder_report" >> "$WINDOW_REPORTS_FILE"

  mapfile -t ladder_reports < "$WINDOW_REPORTS_FILE"
  collector_args=(--output "$SUMMARY_PATH" --max-windows "$MAX_WINDOWS" --min-assets "$MIN_ASSETS")
  for report_path in "${ladder_reports[@]}"; do
    collector_args+=(--ladder-report "$report_path")
  done
  PYTHONPATH=python-service python3 -m src.research.runtime_touch_strict_signalable_collector \
    "${collector_args[@]}" \
    > "$RUN_ROOT/runtime_touch_strict_signalable_collector.stdout.json"

  should_stop="$(python3 - "$SUMMARY_PATH" <<'PY'
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
print("1" if payload.get("status") == "ready" else "0")
PY
)"
  if [[ "$should_stop" == "1" ]]; then
    break
  fi
done

cat "$SUMMARY_PATH"
