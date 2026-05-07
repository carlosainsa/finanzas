#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CYCLE_TIMESTAMP="${CYCLE_TIMESTAMP:-execution-probe-v7-cycle-$(date -u +%Y%m%dT%H%M%SZ)}"
RUN_ROOT="${RUN_ROOT:-${ROOT_DIR}/.tmp/operational/${CYCLE_TIMESTAMP}}"
REPORT_TIMESTAMP="${REPORT_TIMESTAMP:-$CYCLE_TIMESTAMP}"
DATA_LAKE_ROOT="${DATA_LAKE_ROOT:-${ROOT_DIR}/.tmp/real-dry-run-data-lake/${REPORT_TIMESTAMP}}"
REPORT_ROOT="${RESEARCH_REPORT_ROOT:-${DATA_LAKE_ROOT}/reports/${REPORT_TIMESTAMP}}"
MANIFEST_ROOT="${RESEARCH_MANIFEST_ROOT:-${DATA_LAKE_ROOT}/research_runs}"
DURATION_SECONDS="${REAL_DRY_RUN_SECONDS:-5400}"
UNIVERSE_DUCKDB=""
BASELINE_REPORT_ROOT="${BASELINE_REPORT_ROOT:-}"
PRINT_PLAN=0

usage() {
  cat <<'EOF'
Usage: scripts/run_execution_probe_v7_cycle.sh --universe-duckdb PATH [--baseline-report-root PATH] [--duration-seconds N] [--print-plan]

Runs the full execution_probe_v7 research cycle:
universe selection -> dry-run observation -> profile comparison -> next decision.
The cycle is research-only and never enables live execution.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --universe-duckdb)
      UNIVERSE_DUCKDB="$2"
      shift 2
      ;;
    --baseline-report-root)
      BASELINE_REPORT_ROOT="$2"
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

if [[ -z "$UNIVERSE_DUCKDB" ]]; then
  echo "--universe-duckdb is required" >&2
  exit 64
fi
if [[ "$PRINT_PLAN" != "1" && ! -f "$UNIVERSE_DUCKDB" ]]; then
  echo "--universe-duckdb must point to an existing DuckDB file" >&2
  exit 64
fi
if [[ -n "$BASELINE_REPORT_ROOT" && "$PRINT_PLAN" != "1" && ! -d "$BASELINE_REPORT_ROOT" ]]; then
  echo "--baseline-report-root must point to an existing directory" >&2
  exit 64
fi
if ! [[ "$DURATION_SECONDS" =~ ^[0-9]+$ ]] || (( DURATION_SECONDS < 1800 || DURATION_SECONDS > 5400 )); then
  echo "duration must be an integer between 1800 and 5400 seconds" >&2
  exit 64
fi

UNIVERSE_SELECTION_PATH="$RUN_ROOT/execution_probe_universe_selection/execution_probe_universe_selection.json"
OBSERVATION_COMMAND=(
  "$ROOT_DIR/scripts/run_execution_probe_v7_observation.sh"
  --universe-selection "$UNIVERSE_SELECTION_PATH"
  --duration-seconds "$DURATION_SECONDS"
)

if [[ "$PRINT_PLAN" == "1" ]]; then
  python3 - "$UNIVERSE_DUCKDB" "$BASELINE_REPORT_ROOT" "$RUN_ROOT" "$REPORT_TIMESTAMP" "$DATA_LAKE_ROOT" "$REPORT_ROOT" "$MANIFEST_ROOT" "$DURATION_SECONDS" "$UNIVERSE_SELECTION_PATH" <<'PY'
import json
import sys

(
    universe_duckdb,
    baseline_report_root,
    run_root,
    report_timestamp,
    data_lake_root,
    report_root,
    manifest_root,
    duration_seconds,
    universe_selection_path,
) = sys.argv[1:]

print(json.dumps({
    "script": "scripts/run_execution_probe_v7_cycle.sh",
    "can_execute_trades": False,
    "execution_mode": "dry_run",
    "profile": "execution_probe_v7",
    "universe_duckdb": universe_duckdb,
    "baseline_report_root": baseline_report_root or None,
    "run_root": run_root,
    "report_timestamp": report_timestamp,
    "data_lake_root": data_lake_root,
    "report_root": report_root,
    "manifest_root": manifest_root,
    "duration_seconds": int(duration_seconds),
    "universe_selection_path": universe_selection_path,
    "delegates_to": [
        "scripts/prepare_execution_probe_cycle.sh",
        "scripts/run_execution_probe_v7_observation.sh",
        "src.research.profile_observation_comparison",
        "src.research.execution_probe_next_decision",
    ],
    "outputs": {
        "profile_observation_comparison": f"{report_root}/profile_observation_comparison.json",
        "execution_probe_next_decision": f"{report_root}/execution_probe_next_decision.json",
        "cycle_summary": f"{run_root}/execution_probe_v7_cycle_summary.json",
    },
}, indent=2, sort_keys=True))
PY
  exit 0
fi

mkdir -p "$RUN_ROOT"

PREPARE_ARGS=(
  --universe-duckdb "$UNIVERSE_DUCKDB"
  --duration-seconds "$DURATION_SECONDS"
)
if [[ -n "$BASELINE_REPORT_ROOT" ]]; then
  PREPARE_ARGS+=(--baseline-report-root "$BASELINE_REPORT_ROOT")
fi

RUN_ROOT="$RUN_ROOT" PROFILE=execution_probe_v7 \
  "$ROOT_DIR/scripts/prepare_execution_probe_cycle.sh" \
    "${PREPARE_ARGS[@]}" \
    > "$RUN_ROOT/prepare_execution_probe_cycle.stdout.json"

export REPORT_TIMESTAMP
export DATA_LAKE_ROOT
export RESEARCH_REPORT_ROOT="$REPORT_ROOT"
export RESEARCH_MANIFEST_ROOT="$MANIFEST_ROOT"
export EXECUTION_MODE="dry_run"
export DISABLE_MARKET_WS="false"
export PROFILE_OBSERVATION_COMPARISON_REPORT_ROOTS="$BASELINE_REPORT_ROOT"

set +e
"${OBSERVATION_COMMAND[@]}"
observation_status=$?
set -e

if [[ "$observation_status" != "0" && "$observation_status" != "20" ]]; then
  echo "execution_probe_v7 observation failed with status $observation_status" >&2
  exit "$observation_status"
fi

PROFILE_ARGS=()
if [[ -n "$BASELINE_REPORT_ROOT" ]]; then
  PROFILE_ARGS+=(--report-root "$BASELINE_REPORT_ROOT")
fi
PROFILE_ARGS+=(--report-root "$REPORT_ROOT")

if [[ ! -f "$REPORT_ROOT/profile_observation_comparison.json" ]]; then
  PYTHONPATH=python-service python3 -m src.research.profile_observation_comparison \
    "${PROFILE_ARGS[@]}" \
    --output "$REPORT_ROOT/profile_observation_comparison.json" \
    > "$REPORT_ROOT/profile_observation_comparison.stdout.json"
fi

PYTHONPATH=python-service python3 -m src.research.execution_probe_next_decision \
  --comparison "$REPORT_ROOT/profile_observation_comparison.json" \
  --output "$REPORT_ROOT/execution_probe_next_decision.json" \
  --json \
  > "$REPORT_ROOT/execution_probe_next_decision.stdout.json"

python3 - "$RUN_ROOT" "$REPORT_ROOT" "$DATA_LAKE_ROOT" "$MANIFEST_ROOT" "$observation_status" <<'PY'
import json
import sys
from pathlib import Path

run_root = Path(sys.argv[1])
report_root = Path(sys.argv[2])
data_lake_root = Path(sys.argv[3])
manifest_root = Path(sys.argv[4])
observation_status = int(sys.argv[5])
decision_path = report_root / "execution_probe_next_decision.json"
decision = json.loads(decision_path.read_text(encoding="utf-8"))
summary = {
    "report_version": "execution_probe_v7_cycle_summary_v1",
    "can_execute_trades": False,
    "execution_mode": "dry_run",
    "observation_status": observation_status,
    "report_root": str(report_root),
    "data_lake_root": str(data_lake_root),
    "manifest_root": str(manifest_root),
    "profile_observation_comparison_path": str(report_root / "profile_observation_comparison.json"),
    "execution_probe_next_decision_path": str(decision_path),
    "recommendation": decision.get("recommendation"),
    "next_step": decision.get("next_step"),
}
run_root.mkdir(parents=True, exist_ok=True)
(run_root / "execution_probe_v7_cycle_summary.json").write_text(
    json.dumps(summary, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
print(json.dumps(summary, indent=2, sort_keys=True))
PY

if [[ "$observation_status" == "20" && "${ALLOW_CYCLE_GATE_FAILURE:-1}" != "1" && "${ALLOW_CYCLE_GATE_FAILURE:-1}" != "true" ]]; then
  exit 20
fi
