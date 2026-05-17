#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROUTE_TIMESTAMP="${ROUTE_TIMESTAMP:-runtime-touch-ab-auto-route-$(date -u +%Y%m%dT%H%M%SZ)}"
RUN_ROOT="${RUN_ROOT:-${ROOT_DIR}/.tmp/operational/${ROUTE_TIMESTAMP}}"
FRESH_TIMESTAMP="${FRESH_TIMESTAMP:-${ROUTE_TIMESTAMP}-fresh}"
FRESH_DATA_LAKE_ROOT="${FRESH_DATA_LAKE_ROOT:-${ROOT_DIR}/.tmp/real-dry-run-data-lake/${FRESH_TIMESTAMP}}"
FRESH_DUCKDB="${FRESH_DUCKDB:-${FRESH_DATA_LAKE_ROOT}/research.duckdb}"
FRESH_REPORT_ROOT="${FRESH_REPORT_ROOT:-${FRESH_DATA_LAKE_ROOT}/reports/${FRESH_TIMESTAMP}}"
FRESH_CAPTURE_SECONDS="${FRESH_CAPTURE_SECONDS:-3600}"
OBSERVATION_SECONDS="${REAL_DRY_RUN_SECONDS:-3600}"
LADDER_OUTPUT_DIR="${LADDER_OUTPUT_DIR:-${RUN_ROOT}/runtime_touch_ab_retry_ladder}"
PRINT_PLAN=0
SKIP_FRESH_CAPTURE=0
RUN_SELECTED_AB=0

usage() {
  cat <<'EOF'
Usage: scripts/run_runtime_touch_ab_auto_route.sh [--skip-fresh-capture --fresh-duckdb PATH --fresh-report-root PATH] [--fresh-capture-seconds N] [--duration-seconds N] [--run-selected-ab] [--print-plan]

Research-only route:
fresh runtime capture -> runtime-touch A/B retry ladder -> optional selected A/B.
The route keeps EXECUTION_MODE=dry_run and never enables live trading.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-fresh-capture)
      SKIP_FRESH_CAPTURE=1
      shift
      ;;
    --fresh-duckdb|--universe-duckdb)
      FRESH_DUCKDB="$2"
      shift 2
      ;;
    --fresh-report-root)
      FRESH_REPORT_ROOT="$2"
      shift 2
      ;;
    --fresh-capture-seconds)
      FRESH_CAPTURE_SECONDS="$2"
      shift 2
      ;;
    --duration-seconds)
      OBSERVATION_SECONDS="$2"
      shift 2
      ;;
    --run-root)
      RUN_ROOT="$2"
      LADDER_OUTPUT_DIR="$2/runtime_touch_ab_retry_ladder"
      shift 2
      ;;
    --run-selected-ab)
      RUN_SELECTED_AB=1
      shift
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

if ! [[ "$FRESH_CAPTURE_SECONDS" =~ ^[0-9]+$ ]] || (( FRESH_CAPTURE_SECONDS < 3600 || FRESH_CAPTURE_SECONDS > 5400 )); then
  echo "fresh capture duration must be an integer between 3600 and 5400 seconds" >&2
  exit 64
fi
if ! [[ "$OBSERVATION_SECONDS" =~ ^[0-9]+$ ]] || (( OBSERVATION_SECONDS < 1800 || OBSERVATION_SECONDS > 5400 )); then
  echo "observation duration must be an integer between 1800 and 5400 seconds" >&2
  exit 64
fi
if [[ "${EXECUTION_MODE:-dry_run}" != "dry_run" ]]; then
  echo "Refusing to run: EXECUTION_MODE must be dry_run or unset." >&2
  exit 64
fi
if [[ "$PRINT_PLAN" != "1" && "$SKIP_FRESH_CAPTURE" == "1" && ! -f "$FRESH_DUCKDB" ]]; then
  echo "--skip-fresh-capture requires --fresh-duckdb to exist" >&2
  exit 64
fi

if [[ "$PRINT_PLAN" == "1" ]]; then
  python3 - "$RUN_ROOT" "$FRESH_DUCKDB" "$FRESH_REPORT_ROOT" "$FRESH_CAPTURE_SECONDS" "$OBSERVATION_SECONDS" "$LADDER_OUTPUT_DIR" "$SKIP_FRESH_CAPTURE" "$RUN_SELECTED_AB" <<'PY'
import json
import sys

(
    run_root,
    fresh_duckdb,
    fresh_report_root,
    fresh_capture_seconds,
    observation_seconds,
    ladder_output_dir,
    skip_fresh_capture,
    run_selected_ab,
) = sys.argv[1:]
print(json.dumps({
    "script": "scripts/run_runtime_touch_ab_auto_route.sh",
    "can_execute_trades": False,
    "execution_mode": "dry_run",
    "skip_fresh_capture": skip_fresh_capture == "1",
    "run_selected_ab": run_selected_ab == "1",
    "fresh_capture_seconds": int(fresh_capture_seconds),
    "observation_seconds": int(observation_seconds),
    "delegates_to": [
        "scripts/run_pre_live_dry_run.sh",
        "scripts/run_runtime_touch_ab_retry_ladder.sh",
        "scripts/run_runtime_touch_ab_cycle.sh",
    ],
    "outputs": {
        "run_root": run_root,
        "fresh_duckdb": fresh_duckdb,
        "fresh_report_root": fresh_report_root,
        "runtime_touch_ab_retry_ladder": f"{ladder_output_dir}/runtime_touch_ab_retry_ladder.json",
    },
}, indent=2, sort_keys=True))
PY
  exit 0
fi

mkdir -p "$RUN_ROOT"

if [[ "$SKIP_FRESH_CAPTURE" != "1" ]]; then
  REPORT_TIMESTAMP="$FRESH_TIMESTAMP" \
    DATA_LAKE_ROOT="$FRESH_DATA_LAKE_ROOT" \
    RESEARCH_REPORT_ROOT="$FRESH_REPORT_ROOT" \
    REAL_DRY_RUN_RESEARCH_MODE="data_lake_only" \
    REAL_DRY_RUN_PREFLIGHT_ALLOW_ZERO_SIGNALS="${REAL_DRY_RUN_PREFLIGHT_ALLOW_ZERO_SIGNALS:-1}" \
    REAL_DRY_RUN_ALLOW_EMPTY_SIGNALS="${REAL_DRY_RUN_ALLOW_EMPTY_SIGNALS:-1}" \
    "$ROOT_DIR/scripts/run_pre_live_dry_run.sh" --duration-seconds "$FRESH_CAPTURE_SECONDS"
fi

"$ROOT_DIR/scripts/run_runtime_touch_ab_retry_ladder.sh" \
  --fresh-duckdb "$FRESH_DUCKDB" \
  --fresh-report-root "$FRESH_REPORT_ROOT" \
  --output-dir "$LADDER_OUTPUT_DIR" \
  --duration-seconds "$OBSERVATION_SECONDS" \
  > "$RUN_ROOT/runtime_touch_ab_retry_ladder.stdout.json"

if [[ "$RUN_SELECTED_AB" != "1" ]]; then
  exit 0
fi

NEXT_COMMAND="$(python3 - "$LADDER_OUTPUT_DIR/runtime_touch_ab_retry_ladder.json" <<'PY'
import json
import sys
from pathlib import Path

report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
command = report.get("next_command")
if command:
    print(command)
PY
)"
if [[ -z "$NEXT_COMMAND" ]]; then
  echo "No selected A/B command; inspect $LADDER_OUTPUT_DIR/runtime_touch_ab_retry_ladder.json" >&2
  exit 20
fi

bash -lc "$NEXT_COMMAND"
