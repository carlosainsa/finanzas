#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DURATION_SECONDS="${REAL_DRY_RUN_SECONDS:-3600}"
PRINT_PLAN=0
UNIVERSE_SELECTION_PATH="${EXECUTION_PROBE_UNIVERSE_SELECTION_PATH:-}"
PREDICTOR_PROFILE="${PREDICTOR_STRATEGY_PROFILE:-execution_probe_v11}"

usage() {
  cat <<'EOF'
Usage: scripts/run_runtime_touch_observation.sh --universe-selection PATH [--duration-seconds N] [--print-plan]

Runs a reproducible dry-run observation over a universe selected by
runtime_touch_ranking_v1. This is research-only and never enables live execution.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --universe-selection)
      UNIVERSE_SELECTION_PATH="$2"
      shift 2
      ;;
    --duration-seconds)
      DURATION_SECONDS="$2"
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

if [[ -z "$UNIVERSE_SELECTION_PATH" || ! -f "$UNIVERSE_SELECTION_PATH" ]]; then
  echo "--universe-selection must point to an existing file" >&2
  exit 64
fi
if ! [[ "$DURATION_SECONDS" =~ ^[0-9]+$ ]] || (( DURATION_SECONDS < 1800 || DURATION_SECONDS > 5400 )); then
  echo "duration must be an integer between 1800 and 5400 seconds" >&2
  exit 64
fi
if [[ "$PREDICTOR_PROFILE" != "execution_probe_v11" && "$PREDICTOR_PROFILE" != "execution_probe_v12" ]]; then
  echo "runtime-touch observation supports PREDICTOR_STRATEGY_PROFILE=execution_probe_v11 or execution_probe_v12 only" >&2
  exit 64
fi

python3 - "$UNIVERSE_SELECTION_PATH" <<'PY'
import json
import sys
from pathlib import Path

universe_path = Path(sys.argv[1])
universe = json.loads(universe_path.read_text(encoding="utf-8"))
if universe.get("can_execute_trades") is not False:
    raise SystemExit("universe selection must be research-only")
if universe.get("status") != "ready":
    raise SystemExit(f"universe selection is not ready: {universe.get('status')}")
if universe.get("source_report_version") != "runtime_touch_ranking_v1":
    raise SystemExit("runtime-touch observation requires runtime_touch_ranking_v1")
runtime_filter = universe.get("runtime_touch_filter")
if not isinstance(runtime_filter, dict) or not runtime_filter.get("enabled"):
    raise SystemExit("runtime-touch observation requires runtime_touch_filter")
ids = [str(item).strip() for item in universe.get("market_asset_ids", []) if str(item).strip()]
if len(ids) < 2:
    raise SystemExit("runtime-touch observation requires at least two asset ids")
PY

export EXECUTION_MODE="dry_run"
export DISABLE_MARKET_WS="false"
export PREDICTOR_STRATEGY_PROFILE="$PREDICTOR_PROFILE"
export PREDICTOR_QUOTE_PLACEMENT="${PREDICTOR_QUOTE_PLACEMENT:-near_touch}"
if [[ "$PREDICTOR_PROFILE" == "execution_probe_v12" ]]; then
  export PREDICTOR_EXECUTION_PROBE_V12_MIN_CONFIDENCE="${PREDICTOR_EXECUTION_PROBE_V12_MIN_CONFIDENCE:-0.57}"
  export PREDICTOR_EXECUTION_PROBE_V12_NEAR_TOUCH_MAX_SPREAD_FRACTION="${PREDICTOR_EXECUTION_PROBE_V12_NEAR_TOUCH_MAX_SPREAD_FRACTION:-1.0}"
  export PREDICTOR_EXECUTION_PROBE_V12_OFFSET_TICKS="${PREDICTOR_EXECUTION_PROBE_V12_OFFSET_TICKS:-0}"
else
  export PREDICTOR_EXECUTION_PROBE_V11_MIN_CONFIDENCE="${PREDICTOR_EXECUTION_PROBE_V11_MIN_CONFIDENCE:-0.55}"
  export PREDICTOR_EXECUTION_PROBE_V11_NEAR_TOUCH_MAX_SPREAD_FRACTION="${PREDICTOR_EXECUTION_PROBE_V11_NEAR_TOUCH_MAX_SPREAD_FRACTION:-0.90}"
  export PREDICTOR_EXECUTION_PROBE_V11_OFFSET_TICKS="${PREDICTOR_EXECUTION_PROBE_V11_OFFSET_TICKS:-0}"
fi
export SIGNAL_REJECTION_PROFILES="${SIGNAL_REJECTION_PROFILES:-$PREDICTOR_PROFILE}"
export SIGNAL_REJECTION_BASELINE_PROFILE="${SIGNAL_REJECTION_BASELINE_PROFILE:-$PREDICTOR_PROFILE}"
export SIGNAL_REJECTION_CANDIDATE_PROFILE="${SIGNAL_REJECTION_CANDIDATE_PROFILE:-$PREDICTOR_PROFILE}"
export EXECUTION_PROBE_UNIVERSE_SELECTION_PATH="$UNIVERSE_SELECTION_PATH"
export GO_NO_GO_PROFILE="pre_live"
export REAL_DRY_RUN_SECONDS="$DURATION_SECONDS"
if [[ -z "${PRE_LIVE_MIN_CAPTURE_DURATION_MS:-}" ]]; then
  effective_min_capture_seconds=$((DURATION_SECONDS - 60))
  if (( effective_min_capture_seconds < 60 )); then
    effective_min_capture_seconds=60
  fi
  export PRE_LIVE_MIN_CAPTURE_DURATION_MS="$((effective_min_capture_seconds * 1000))"
fi

if [[ "$PRINT_PLAN" == "1" ]]; then
  python3 - <<'PY'
import json
import os

plan = {
    "script": "scripts/run_runtime_touch_observation.sh",
    "delegates_to": "scripts/run_pre_live_dry_run.sh",
    "can_execute_trades": False,
    "execution_mode": os.environ["EXECUTION_MODE"],
    "predictor_strategy_profile": os.environ["PREDICTOR_STRATEGY_PROFILE"],
    "predictor_quote_placement": os.environ["PREDICTOR_QUOTE_PLACEMENT"],
    "signal_rejection_profiles": os.environ["SIGNAL_REJECTION_PROFILES"],
    "execution_probe_universe_selection_path": os.environ["EXECUTION_PROBE_UNIVERSE_SELECTION_PATH"],
    "real_dry_run_seconds": int(os.environ["REAL_DRY_RUN_SECONDS"]),
    "pre_live_min_capture_duration_ms": int(os.environ["PRE_LIVE_MIN_CAPTURE_DURATION_MS"]),
    "go_no_go_profile": os.environ["GO_NO_GO_PROFILE"],
}
profile = os.environ["PREDICTOR_STRATEGY_PROFILE"]
if profile == "execution_probe_v12":
    plan.update({
        "predictor_execution_probe_v12_min_confidence": float(os.environ["PREDICTOR_EXECUTION_PROBE_V12_MIN_CONFIDENCE"]),
        "predictor_execution_probe_v12_near_touch_max_spread_fraction": float(os.environ["PREDICTOR_EXECUTION_PROBE_V12_NEAR_TOUCH_MAX_SPREAD_FRACTION"]),
        "predictor_execution_probe_v12_offset_ticks": int(os.environ["PREDICTOR_EXECUTION_PROBE_V12_OFFSET_TICKS"]),
    })
else:
    plan.update({
        "predictor_execution_probe_v11_min_confidence": float(os.environ["PREDICTOR_EXECUTION_PROBE_V11_MIN_CONFIDENCE"]),
        "predictor_execution_probe_v11_near_touch_max_spread_fraction": float(os.environ["PREDICTOR_EXECUTION_PROBE_V11_NEAR_TOUCH_MAX_SPREAD_FRACTION"]),
        "predictor_execution_probe_v11_offset_ticks": int(os.environ["PREDICTOR_EXECUTION_PROBE_V11_OFFSET_TICKS"]),
    })
print(json.dumps(plan, indent=2, sort_keys=True))
PY
  exit 0
fi

"$ROOT_DIR/scripts/run_pre_live_dry_run.sh" --duration-seconds "$DURATION_SECONDS"
