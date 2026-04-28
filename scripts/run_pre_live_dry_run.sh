#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DURATION_SECONDS="${REAL_DRY_RUN_SECONDS:-3600}"
PRINT_PLAN=0

usage() {
  cat <<'EOF'
Usage: scripts/run_pre_live_dry_run.sh [--duration-seconds N] [--print-plan]

Runs the managed real-market dry-run with pre-live gates and writes an isolated
research bundle that includes pre_live_readiness.json.

Options:
  --duration-seconds N   Capture duration. Default: REAL_DRY_RUN_SECONDS or 3600.
  --print-plan          Print the resolved plan as JSON and exit without starting services.

Required for execution:
  Docker must be available for docker-compose.test.yml.
  EXECUTION_MODE must be dry_run or unset.
  DISABLE_MARKET_WS must be false or unset.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --duration-seconds)
      if [[ $# -lt 2 ]]; then
        echo "--duration-seconds requires a value" >&2
        exit 64
      fi
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

if ! [[ "$DURATION_SECONDS" =~ ^[0-9]+$ ]] || (( DURATION_SECONDS < 60 )); then
  echo "duration must be an integer >= 60 seconds" >&2
  exit 64
fi
if [[ "${EXECUTION_MODE:-dry_run}" != "dry_run" ]]; then
  echo "Refusing to run: EXECUTION_MODE must be dry_run or unset." >&2
  exit 64
fi
if [[ "${DISABLE_MARKET_WS:-false}" == "1" || "${DISABLE_MARKET_WS:-false}" == "true" ]]; then
  echo "Refusing to run: DISABLE_MARKET_WS must be false or unset." >&2
  exit 64
fi

export EXECUTION_MODE="dry_run"
export DISABLE_MARKET_WS="false"
export REQUIRE_POSTGRES_STATE="${REQUIRE_POSTGRES_STATE:-true}"
export GO_NO_GO_PROFILE="pre_live"
export REAL_DRY_RUN_SECONDS="$DURATION_SECONDS"
export PRE_LIVE_MIN_CAPTURE_DURATION_MS="${PRE_LIVE_MIN_CAPTURE_DURATION_MS:-$((DURATION_SECONDS * 1000))}"
export PRE_LIVE_MIN_SIGNALS="${PRE_LIVE_MIN_SIGNALS:-250}"
export REAL_DRY_RUN_ISOLATED="${REAL_DRY_RUN_ISOLATED:-1}"
export ALLOW_RESEARCH_GATE_FAILURE="${ALLOW_RESEARCH_GATE_FAILURE:-1}"

if [[ "$PRINT_PLAN" == "1" ]]; then
  python3 - <<'PY'
import json
import os

print(json.dumps({
    "script": "scripts/run_pre_live_dry_run.sh",
    "delegates_to": "scripts/run_real_dry_run_research.sh",
    "execution_mode": os.environ["EXECUTION_MODE"],
    "disable_market_ws": os.environ["DISABLE_MARKET_WS"],
    "go_no_go_profile": os.environ["GO_NO_GO_PROFILE"],
    "real_dry_run_seconds": int(os.environ["REAL_DRY_RUN_SECONDS"]),
    "pre_live_min_capture_duration_ms": int(os.environ["PRE_LIVE_MIN_CAPTURE_DURATION_MS"]),
    "pre_live_min_signals": int(os.environ["PRE_LIVE_MIN_SIGNALS"]),
    "isolated": os.environ["REAL_DRY_RUN_ISOLATED"],
    "allow_research_gate_failure": os.environ["ALLOW_RESEARCH_GATE_FAILURE"],
}, indent=2, sort_keys=True))
PY
  exit 0
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required to run the managed pre-live dry-run." >&2
  exit 69
fi

echo "pre_live_dry_run_start"
echo "duration_seconds=$REAL_DRY_RUN_SECONDS"
echo "go_no_go_profile=$GO_NO_GO_PROFILE"
echo "isolated=$REAL_DRY_RUN_ISOLATED"

"$ROOT_DIR/scripts/run_real_dry_run_research.sh"
