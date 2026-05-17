#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROBE_TIMESTAMP="${PROBE_TIMESTAMP:-runtime-touch-selection-probe-$(date -u +%Y%m%dT%H%M%SZ)}"
RUN_ROOT="${RUN_ROOT:-${ROOT_DIR}/.tmp/operational/${PROBE_TIMESTAMP}}"
FRESH_TIMESTAMP="${FRESH_TIMESTAMP:-${PROBE_TIMESTAMP}-fresh}"
FRESH_DATA_LAKE_ROOT="${FRESH_DATA_LAKE_ROOT:-${ROOT_DIR}/.tmp/real-dry-run-data-lake/${FRESH_TIMESTAMP}}"
FRESH_DUCKDB="${FRESH_DUCKDB:-${FRESH_DATA_LAKE_ROOT}/research.duckdb}"
FRESH_REPORT_ROOT="${FRESH_REPORT_ROOT:-${FRESH_DATA_LAKE_ROOT}/reports/${FRESH_TIMESTAMP}}"
FRESH_CAPTURE_SECONDS="${FRESH_CAPTURE_SECONDS:-1800}"
PRINT_PLAN=0
SKIP_FRESH_CAPTURE=0
MIN_ASSETS="${EXECUTION_PROBE_UNIVERSE_MIN_ASSETS:-2}"
DIAGNOSTIC_OUTPUT_DIR="${DIAGNOSTIC_OUTPUT_DIR:-${RUN_ROOT}/runtime_touch_signalability_diagnostic}"
EXPANSION_OUTPUT_DIR="${EXPANSION_OUTPUT_DIR:-${RUN_ROOT}/runtime_touch_candidate_expansion}"

usage() {
  cat <<'EOF'
Usage: scripts/run_runtime_touch_selection_probe.sh [--skip-fresh-capture --fresh-duckdb PATH --fresh-report-root PATH] [--fresh-capture-seconds N] [--print-plan]

Runs a research-only short selection probe:
fresh data-lake capture -> signalability diagnostic -> candidate expansion.
It does not launch A/B, does not execute live trades, and does not modify risk.
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
    --run-root)
      RUN_ROOT="$2"
      DIAGNOSTIC_OUTPUT_DIR="$2/runtime_touch_signalability_diagnostic"
      EXPANSION_OUTPUT_DIR="$2/runtime_touch_candidate_expansion"
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

if ! [[ "$FRESH_CAPTURE_SECONDS" =~ ^[0-9]+$ ]] || (( FRESH_CAPTURE_SECONDS < 1800 || FRESH_CAPTURE_SECONDS > 2700 )); then
  echo "fresh capture duration must be an integer between 1800 and 2700 seconds" >&2
  exit 64
fi
if ! [[ "$MIN_ASSETS" =~ ^[0-9]+$ ]] || (( MIN_ASSETS < 2 )); then
  echo "min assets must be an integer >= 2 for selection probes" >&2
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
  python3 - "$RUN_ROOT" "$FRESH_DUCKDB" "$FRESH_REPORT_ROOT" "$FRESH_CAPTURE_SECONDS" "$DIAGNOSTIC_OUTPUT_DIR" "$EXPANSION_OUTPUT_DIR" "$SKIP_FRESH_CAPTURE" "$MIN_ASSETS" <<'PY'
import json
import sys

(
    run_root,
    fresh_duckdb,
    fresh_report_root,
    fresh_capture_seconds,
    diagnostic_output_dir,
    expansion_output_dir,
    skip_fresh_capture,
    min_assets,
) = sys.argv[1:]
print(json.dumps({
    "script": "scripts/run_runtime_touch_selection_probe.sh",
    "can_execute_trades": False,
    "execution_mode": "dry_run",
    "skip_fresh_capture": skip_fresh_capture == "1",
    "fresh_capture_seconds": int(fresh_capture_seconds),
    "min_assets": int(min_assets),
    "delegates_to": [
        "scripts/run_pre_live_dry_run.sh",
        "src.research.runtime_touch_signalability_diagnostic",
        "src.research.runtime_touch_candidate_expansion",
    ],
    "outputs": {
        "run_root": run_root,
        "fresh_duckdb": fresh_duckdb,
        "fresh_report_root": fresh_report_root,
        "signalability_diagnostic": f"{diagnostic_output_dir}/runtime_touch_signalability_diagnostic.json",
        "candidate_expansion": f"{expansion_output_dir}/runtime_touch_candidate_expansion.json",
        "selection_probe_summary": f"{run_root}/runtime_touch_selection_probe_summary.json",
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

PYTHONPATH=python-service python3 -m src.research.runtime_touch_signalability_diagnostic \
  --duckdb "$FRESH_DUCKDB" \
  --output-dir "$DIAGNOSTIC_OUTPUT_DIR" \
  --min-assets "$MIN_ASSETS" \
  > "$RUN_ROOT/runtime_touch_signalability_diagnostic.stdout.json"

PYTHONPATH=python-service python3 -m src.research.runtime_touch_candidate_expansion \
  --duckdb "$FRESH_DUCKDB" \
  --output-dir "$EXPANSION_OUTPUT_DIR" \
  --min-assets "$MIN_ASSETS" \
  > "$RUN_ROOT/runtime_touch_candidate_expansion.stdout.json"

python3 - "$RUN_ROOT" "$FRESH_DUCKDB" "$FRESH_REPORT_ROOT" "$DIAGNOSTIC_OUTPUT_DIR" "$EXPANSION_OUTPUT_DIR" <<'PY'
import json
import sys
from pathlib import Path

run_root = Path(sys.argv[1])
diagnostic_path = Path(sys.argv[4]) / "runtime_touch_signalability_diagnostic.json"
expansion_path = Path(sys.argv[5]) / "runtime_touch_candidate_expansion.json"
diagnostic = json.loads(diagnostic_path.read_text(encoding="utf-8"))
expansion = json.loads(expansion_path.read_text(encoding="utf-8"))
payload = {
    "report_version": "runtime_touch_selection_probe_summary_v1",
    "can_execute_trades": False,
    "decision_policy": "offline_runtime_touch_selection_probe_only",
    "fresh_duckdb": sys.argv[2],
    "fresh_report_root": sys.argv[3],
    "signalability_status": diagnostic.get("status"),
    "signalable_assets_count": diagnostic.get("signalable_assets_count"),
    "candidate_expansion_status": expansion.get("status"),
    "candidate_expansion_assets": expansion.get("market_asset_ids_count"),
    "next_action": (
        "RUN_RUNTIME_TOUCH_AB_RETRY_LADDER_ON_THIS_DUCKDB"
        if expansion.get("status") == "ready"
        else "CHANGE_MARKET_TIMING_OR_DISCOVERY"
    ),
    "outputs": {
        "signalability_diagnostic": str(diagnostic_path),
        "candidate_expansion": str(expansion_path),
    },
}
(run_root / "runtime_touch_selection_probe_summary.json").write_text(
    json.dumps(payload, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
print(json.dumps(payload, indent=2, sort_keys=True))
PY
