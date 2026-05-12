#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DURATION_SECONDS="${REAL_DRY_RUN_SECONDS:-3600}"
PRINT_PLAN=0
UNIVERSE_SELECTION_PATH="${EXECUTION_PROBE_UNIVERSE_SELECTION_PATH:-}"
FRACTION_SELECTION_PATH="${PREDICTOR_EXECUTION_PROBE_V9_FRACTION_SELECTION_PATH:-}"

usage() {
  cat <<'EOF'
Usage: scripts/run_execution_probe_v9_observation.sh --universe-selection PATH [--fraction-selection PATH] [--duration-seconds N] [--print-plan]

Runs a reproducible multi-market execution_probe_v9 dry-run observation. V9 is
research-only and quotes slightly behind touch with stricter top-rotation and
cooldown filters to reduce toxic fills after v8 adverse-selection evidence.

Options:
  --universe-selection PATH   execution_probe_universe_selection.json.
  --fraction-selection PATH   optional execution_probe_v9_fraction_selection.json.
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
if ! [[ "$DURATION_SECONDS" =~ ^[0-9]+$ ]] || (( DURATION_SECONDS < 1800 || DURATION_SECONDS > 5400 )); then
  echo "duration must be an integer between 1800 and 5400 seconds" >&2
  exit 64
fi

python3 - "$UNIVERSE_SELECTION_PATH" "${FRACTION_SELECTION_PATH:-}" <<'PY'
import json
import sys
from pathlib import Path

universe_path = Path(sys.argv[1])
universe = json.loads(universe_path.read_text(encoding="utf-8"))
if universe.get("can_execute_trades") is not False:
    raise SystemExit("universe selection must be research-only")
if universe.get("status") != "ready":
    raise SystemExit(f"universe selection is not ready: {universe.get('status')}")
if universe.get("profile") != "execution_probe_v9":
    raise SystemExit("universe selection must target execution_probe_v9")
toxicity_filter = universe.get("toxicity_filter")
if isinstance(toxicity_filter, dict) and toxicity_filter.get("enabled"):
    blocklist_path = toxicity_filter.get("blocked_segments_path")
    if blocklist_path:
        blocklist = Path(str(blocklist_path))
        if not blocklist.is_absolute():
            blocklist = universe_path.parent / blocklist
        if not blocklist.exists():
            raise SystemExit(f"toxicity blocklist does not exist: {blocklist}")
fraction_path = sys.argv[2]
if fraction_path:
    fraction = json.loads(Path(fraction_path).read_text(encoding="utf-8"))
    if fraction.get("can_execute_trades") is not False:
        raise SystemExit("fraction selection must be research-only")
    if fraction.get("profile") != "execution_probe_v9":
        raise SystemExit("fraction selection must target execution_probe_v9")
PY

export EXECUTION_MODE="dry_run"
export DISABLE_MARKET_WS="false"
export PREDICTOR_STRATEGY_PROFILE="execution_probe_v9"
export PREDICTOR_QUOTE_PLACEMENT="${PREDICTOR_QUOTE_PLACEMENT:-near_touch}"
export PREDICTOR_EXECUTION_PROBE_V9_MIN_CONFIDENCE="${PREDICTOR_EXECUTION_PROBE_V9_MIN_CONFIDENCE:-0.55}"
export PREDICTOR_EXECUTION_PROBE_V9_NEAR_TOUCH_MAX_SPREAD_FRACTION="${PREDICTOR_EXECUTION_PROBE_V9_NEAR_TOUCH_MAX_SPREAD_FRACTION:-0.90}"
export PREDICTOR_EXECUTION_PROBE_V9_OFFSET_TICKS="${PREDICTOR_EXECUTION_PROBE_V9_OFFSET_TICKS:-0}"
export EXECUTION_PROBE_UNIVERSE_SELECTION_PATH="$UNIVERSE_SELECTION_PATH"
if [[ -n "$FRACTION_SELECTION_PATH" ]]; then
  export PREDICTOR_EXECUTION_PROBE_V9_FRACTION_SELECTION_PATH="$FRACTION_SELECTION_PATH"
fi
export GO_NO_GO_PROFILE="pre_live"
export REAL_DRY_RUN_SECONDS="$DURATION_SECONDS"
toxicity_blocklist_path="$(
  python3 - "$UNIVERSE_SELECTION_PATH" <<'PY'
import json
import sys
from pathlib import Path

universe_path = Path(sys.argv[1])
payload = json.loads(universe_path.read_text(encoding="utf-8"))
toxicity_filter = payload.get("toxicity_filter")
if isinstance(toxicity_filter, dict) and toxicity_filter.get("enabled"):
    blocklist_path = toxicity_filter.get("blocked_segments_path")
    if blocklist_path:
        blocklist = Path(str(blocklist_path))
        if not blocklist.is_absolute():
            blocklist = universe_path.parent / blocklist
        print(blocklist)
PY
)"
if [[ -n "$toxicity_blocklist_path" ]]; then
  export PREDICTOR_BLOCKED_SEGMENTS_PATH="$toxicity_blocklist_path"
fi
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

print(json.dumps({
    "script": "scripts/run_execution_probe_v9_observation.sh",
    "delegates_to": "scripts/run_pre_live_dry_run.sh",
    "execution_mode": os.environ["EXECUTION_MODE"],
    "predictor_strategy_profile": os.environ["PREDICTOR_STRATEGY_PROFILE"],
    "predictor_quote_placement": os.environ["PREDICTOR_QUOTE_PLACEMENT"],
    "predictor_execution_probe_v9_min_confidence": float(os.environ["PREDICTOR_EXECUTION_PROBE_V9_MIN_CONFIDENCE"]),
    "predictor_execution_probe_v9_near_touch_max_spread_fraction": float(os.environ["PREDICTOR_EXECUTION_PROBE_V9_NEAR_TOUCH_MAX_SPREAD_FRACTION"]),
    "predictor_execution_probe_v9_offset_ticks": int(os.environ["PREDICTOR_EXECUTION_PROBE_V9_OFFSET_TICKS"]),
    "execution_probe_universe_selection_path": os.environ["EXECUTION_PROBE_UNIVERSE_SELECTION_PATH"],
    "predictor_execution_probe_v9_fraction_selection_path": os.environ.get("PREDICTOR_EXECUTION_PROBE_V9_FRACTION_SELECTION_PATH"),
    "predictor_blocked_segments_path": os.environ.get("PREDICTOR_BLOCKED_SEGMENTS_PATH"),
    "real_dry_run_seconds": int(os.environ["REAL_DRY_RUN_SECONDS"]),
    "pre_live_min_capture_duration_ms": int(os.environ["PRE_LIVE_MIN_CAPTURE_DURATION_MS"]),
    "go_no_go_profile": os.environ["GO_NO_GO_PROFILE"],
}, indent=2, sort_keys=True))
PY
  exit 0
fi

"$ROOT_DIR/scripts/run_pre_live_dry_run.sh" --duration-seconds "$DURATION_SECONDS"
