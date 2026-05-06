#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DURATION_SECONDS="${REAL_DRY_RUN_SECONDS:-3600}"
PRINT_PLAN=0
UNIVERSE_SELECTION_PATH="${EXECUTION_PROBE_UNIVERSE_SELECTION_PATH:-}"
FRACTION_SELECTION_PATH="${PREDICTOR_EXECUTION_PROBE_V5_FRACTION_SELECTION_PATH:-}"

usage() {
  cat <<'EOF'
Usage: scripts/run_execution_probe_v5_observation.sh --universe-selection PATH --fraction-selection PATH [--duration-seconds N] [--print-plan]

Runs a reproducible multi-market execution_probe_v5 dry-run observation. The
universe and near-touch fraction inputs must be offline research artifacts with
can_execute_trades=false.

Options:
  --universe-selection PATH   execution_probe_universe_selection.json.
  --fraction-selection PATH   execution_probe_v5_fraction_selection.json.
  --duration-seconds N        Capture duration. Default: REAL_DRY_RUN_SECONDS or 3600.
  --print-plan                Print resolved plan and exit without starting services.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --universe-selection)
      UNIVERSE_SELECTION_PATH="$2"
      shift 2
      ;;
    --fraction-selection)
      FRACTION_SELECTION_PATH="$2"
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
if [[ -z "$FRACTION_SELECTION_PATH" || ! -f "$FRACTION_SELECTION_PATH" ]]; then
  echo "--fraction-selection must point to an existing file" >&2
  exit 64
fi
if ! [[ "$DURATION_SECONDS" =~ ^[0-9]+$ ]] || (( DURATION_SECONDS < 1800 || DURATION_SECONDS > 5400 )); then
  echo "duration must be an integer between 1800 and 5400 seconds" >&2
  exit 64
fi

python3 - "$UNIVERSE_SELECTION_PATH" "$FRACTION_SELECTION_PATH" <<'PY'
import json
import sys
from pathlib import Path

universe = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
fraction = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
if universe.get("can_execute_trades") is not False:
    raise SystemExit("universe selection must be research-only")
if universe.get("status") != "ready":
    raise SystemExit(f"universe selection is not ready: {universe.get('status')}")
if fraction.get("can_execute_trades") is not False:
    raise SystemExit("fraction selection must be research-only")
if fraction.get("profile") != "execution_probe_v5":
    raise SystemExit("fraction selection must target execution_probe_v5")
PY

export EXECUTION_MODE="dry_run"
export DISABLE_MARKET_WS="false"
export PREDICTOR_STRATEGY_PROFILE="execution_probe_v5"
export PREDICTOR_QUOTE_PLACEMENT="${PREDICTOR_QUOTE_PLACEMENT:-near_touch}"
export EXECUTION_PROBE_UNIVERSE_SELECTION_PATH="$UNIVERSE_SELECTION_PATH"
export PREDICTOR_EXECUTION_PROBE_V5_FRACTION_SELECTION_PATH="$FRACTION_SELECTION_PATH"
export GO_NO_GO_PROFILE="pre_live"
export REAL_DRY_RUN_SECONDS="$DURATION_SECONDS"

if [[ "$PRINT_PLAN" == "1" ]]; then
  python3 - <<'PY'
import json
import os

print(json.dumps({
    "script": "scripts/run_execution_probe_v5_observation.sh",
    "delegates_to": "scripts/run_pre_live_dry_run.sh",
    "execution_mode": os.environ["EXECUTION_MODE"],
    "predictor_strategy_profile": os.environ["PREDICTOR_STRATEGY_PROFILE"],
    "execution_probe_universe_selection_path": os.environ["EXECUTION_PROBE_UNIVERSE_SELECTION_PATH"],
    "predictor_execution_probe_v5_fraction_selection_path": os.environ["PREDICTOR_EXECUTION_PROBE_V5_FRACTION_SELECTION_PATH"],
    "real_dry_run_seconds": int(os.environ["REAL_DRY_RUN_SECONDS"]),
    "go_no_go_profile": os.environ["GO_NO_GO_PROFILE"],
}, indent=2, sort_keys=True))
PY
  exit 0
fi

"$ROOT_DIR/scripts/run_pre_live_dry_run.sh" --duration-seconds "$DURATION_SECONDS"
