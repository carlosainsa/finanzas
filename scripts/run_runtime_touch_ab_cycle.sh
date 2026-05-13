#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CYCLE_TIMESTAMP="${CYCLE_TIMESTAMP:-runtime-touch-ab-cycle-$(date -u +%Y%m%dT%H%M%SZ)}"
RUN_ROOT="${RUN_ROOT:-${ROOT_DIR}/.tmp/operational/${CYCLE_TIMESTAMP}}"
FRESH_TIMESTAMP="${FRESH_TIMESTAMP:-${CYCLE_TIMESTAMP}-fresh}"
PROFILE_A="${PROFILE_A:-execution_probe_v11}"
PROFILE_B="${PROFILE_B:-execution_probe_v12}"
UNIVERSE_PROFILE="${UNIVERSE_PROFILE:-execution_probe_v11}"
FRESH_DATA_LAKE_ROOT="${FRESH_DATA_LAKE_ROOT:-${ROOT_DIR}/.tmp/real-dry-run-data-lake/${FRESH_TIMESTAMP}}"
FRESH_DUCKDB="${FRESH_DUCKDB:-${FRESH_DATA_LAKE_ROOT}/research.duckdb}"
FRESH_REPORT_ROOT="${FRESH_REPORT_ROOT:-${FRESH_DATA_LAKE_ROOT}/reports/${FRESH_TIMESTAMP}}"
FRESH_CAPTURE_SECONDS="${FRESH_CAPTURE_SECONDS:-1800}"
OBSERVATION_SECONDS="${REAL_DRY_RUN_SECONDS:-3600}"
COMPARISON_REPORT_ROOTS="${PROFILE_OBSERVATION_COMPARISON_REPORT_ROOTS:-}"
PRINT_PLAN=0
SKIP_FRESH_CAPTURE=0
UNIVERSE_LIMIT="${EXECUTION_PROBE_UNIVERSE_LIMIT:-10}"
UNIVERSE_MIN_ASSETS="${EXECUTION_PROBE_UNIVERSE_MIN_ASSETS:-2}"
RUNTIME_TOUCH_LOOKBACK_MS="${EXECUTION_PROBE_RUNTIME_TOUCH_LOOKBACK_MS:-900000}"
MIN_RUNTIME_TOUCH_CHANGE_RATE="${EXECUTION_PROBE_MIN_RUNTIME_TOUCH_CHANGE_RATE:-0.01}"
MIN_RUNTIME_TOUCH_SNAPSHOTS="${EXECUTION_PROBE_MIN_RUNTIME_TOUCH_SNAPSHOTS:-10}"
MIN_RUNTIME_ACTIVE_MINUTES="${EXECUTION_PROBE_MIN_RUNTIME_ACTIVE_MINUTES:-2}"
MIN_AVG_OPPORTUNITY_SPREAD="${EXECUTION_PROBE_MIN_AVG_OPPORTUNITY_SPREAD:-0.000625}"
MAX_AVG_OPPORTUNITY_SPREAD="${EXECUTION_PROBE_MAX_AVG_OPPORTUNITY_SPREAD:-}"

usage() {
  cat <<'EOF'
Usage: scripts/run_runtime_touch_ab_cycle.sh [--skip-fresh-capture --fresh-duckdb PATH --fresh-report-root PATH] [--duration-seconds N] [--print-plan]

Runs a research-only A/B cycle over one runtime-touch universe:
fresh dry-run capture -> runtime-touch universe -> profile A observation
-> profile B observation -> failure diagnostics -> profile/touch comparisons.

The cycle never enables live execution.
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
    --comparison-report-roots)
      COMPARISON_REPORT_ROOTS="$2"
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
    --universe-limit)
      UNIVERSE_LIMIT="$2"
      shift 2
      ;;
    --min-assets)
      UNIVERSE_MIN_ASSETS="$2"
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

if [[ "$PROFILE_A" != "execution_probe_v11" && "$PROFILE_A" != "execution_probe_v12" ]]; then
  echo "--profile-a must be execution_probe_v11 or execution_probe_v12" >&2
  exit 64
fi
if [[ "$PROFILE_B" != "execution_probe_v11" && "$PROFILE_B" != "execution_probe_v12" ]]; then
  echo "--profile-b must be execution_probe_v11 or execution_probe_v12" >&2
  exit 64
fi
if [[ "$PROFILE_A" == "$PROFILE_B" ]]; then
  echo "A/B profiles must be different" >&2
  exit 64
fi
if ! [[ "$FRESH_CAPTURE_SECONDS" =~ ^[0-9]+$ ]] || (( FRESH_CAPTURE_SECONDS < 1800 || FRESH_CAPTURE_SECONDS > 5400 )); then
  echo "fresh capture duration must be an integer between 1800 and 5400 seconds" >&2
  exit 64
fi
if ! [[ "$OBSERVATION_SECONDS" =~ ^[0-9]+$ ]] || (( OBSERVATION_SECONDS < 1800 || OBSERVATION_SECONDS > 5400 )); then
  echo "observation duration must be an integer between 1800 and 5400 seconds" >&2
  exit 64
fi
if ! [[ "$UNIVERSE_MIN_ASSETS" =~ ^[0-9]+$ ]] || (( UNIVERSE_MIN_ASSETS < 2 )); then
  echo "min assets must be an integer >= 2 for runtime-touch A/B" >&2
  exit 64
fi
if ! [[ "$UNIVERSE_LIMIT" =~ ^[0-9]+$ ]] || (( UNIVERSE_LIMIT < UNIVERSE_MIN_ASSETS )); then
  echo "universe limit must be an integer >= min assets" >&2
  exit 64
fi
if [[ "$PRINT_PLAN" != "1" && "$SKIP_FRESH_CAPTURE" == "1" && ! -f "$FRESH_DUCKDB" ]]; then
  echo "--skip-fresh-capture requires --fresh-duckdb to exist" >&2
  exit 64
fi

UNIVERSE_SELECTION_PATH="$RUN_ROOT/execution_probe_universe_selection/execution_probe_universe_selection.json"
RUNTIME_TOUCH_RANKING_DIR="$RUN_ROOT/runtime_touch_ranking"
PROFILE_A_TIMESTAMP="${CYCLE_TIMESTAMP}-${PROFILE_A}"
PROFILE_B_TIMESTAMP="${CYCLE_TIMESTAMP}-${PROFILE_B}"
PROFILE_A_DATA_LAKE_ROOT="${ROOT_DIR}/.tmp/real-dry-run-data-lake/${PROFILE_A_TIMESTAMP}"
PROFILE_B_DATA_LAKE_ROOT="${ROOT_DIR}/.tmp/real-dry-run-data-lake/${PROFILE_B_TIMESTAMP}"
PROFILE_A_REPORT_ROOT="${PROFILE_A_DATA_LAKE_ROOT}/reports/${PROFILE_A_TIMESTAMP}"
PROFILE_B_REPORT_ROOT="${PROFILE_B_DATA_LAKE_ROOT}/reports/${PROFILE_B_TIMESTAMP}"

if [[ "$PRINT_PLAN" == "1" ]]; then
  python3 - "$RUN_ROOT" "$FRESH_DUCKDB" "$FRESH_REPORT_ROOT" "$PROFILE_A" "$PROFILE_B" "$PROFILE_A_REPORT_ROOT" "$PROFILE_B_REPORT_ROOT" "$FRESH_CAPTURE_SECONDS" "$OBSERVATION_SECONDS" "$UNIVERSE_SELECTION_PATH" "$RUNTIME_TOUCH_RANKING_DIR" "$COMPARISON_REPORT_ROOTS" "$SKIP_FRESH_CAPTURE" <<'PY'
import json
import sys

(
    run_root,
    fresh_duckdb,
    fresh_report_root,
    profile_a,
    profile_b,
    profile_a_report_root,
    profile_b_report_root,
    fresh_capture_seconds,
    observation_seconds,
    universe_selection_path,
    runtime_touch_ranking_dir,
    comparison_roots_csv,
    skip_fresh_capture,
) = sys.argv[1:]
print(json.dumps({
    "script": "scripts/run_runtime_touch_ab_cycle.sh",
    "can_execute_trades": False,
    "execution_mode": "dry_run",
    "skip_fresh_capture": skip_fresh_capture == "1",
    "fresh_capture_seconds": int(fresh_capture_seconds),
    "observation_seconds_per_profile": int(observation_seconds),
    "profile_a": profile_a,
    "profile_b": profile_b,
    "fresh_duckdb": fresh_duckdb,
    "fresh_report_root": fresh_report_root,
    "comparison_report_roots": [item.strip() for item in comparison_roots_csv.split(",") if item.strip()],
    "delegates_to": [
        "scripts/run_pre_live_dry_run.sh",
        "src.research.runtime_touch_ranking",
        "src.research.execution_probe_universe_selection",
        "scripts/run_runtime_touch_observation.sh",
        "src.research.execution_failure_diagnostics",
        "src.research.profile_observation_comparison",
        "src.research.execution_probe_touch_comparison",
    ],
    "outputs": {
        "run_root": run_root,
        "runtime_touch_ranking": f"{runtime_touch_ranking_dir}/runtime_touch_ranking.json",
        "execution_probe_universe_selection": universe_selection_path,
        "profile_a_report_root": profile_a_report_root,
        "profile_b_report_root": profile_b_report_root,
        "profile_observation_comparison": f"{run_root}/profile_observation_comparison.json",
        "execution_probe_touch_comparison": f"{run_root}/execution_probe_touch_comparison.json",
        "cycle_summary": f"{run_root}/runtime_touch_ab_cycle_summary.json",
    },
}, indent=2, sort_keys=True))
PY
  exit 0
fi

mkdir -p "$RUN_ROOT"

if [[ "$SKIP_FRESH_CAPTURE" != "1" ]]; then
  set +e
  REPORT_TIMESTAMP="$FRESH_TIMESTAMP" \
    DATA_LAKE_ROOT="$FRESH_DATA_LAKE_ROOT" \
    RESEARCH_REPORT_ROOT="$FRESH_REPORT_ROOT" \
    REAL_DRY_RUN_SECONDS="$FRESH_CAPTURE_SECONDS" \
    REAL_DRY_RUN_RESEARCH_MODE="data_lake_only" \
    REAL_DRY_RUN_PREFLIGHT_ALLOW_ZERO_SIGNALS="${REAL_DRY_RUN_PREFLIGHT_ALLOW_ZERO_SIGNALS:-1}" \
    REAL_DRY_RUN_ALLOW_EMPTY_SIGNALS="${REAL_DRY_RUN_ALLOW_EMPTY_SIGNALS:-1}" \
    "$ROOT_DIR/scripts/run_pre_live_dry_run.sh" --duration-seconds "$FRESH_CAPTURE_SECONDS"
  fresh_status=$?
  set -e
  if [[ "$fresh_status" != "0" && "$fresh_status" != "20" ]]; then
    echo "fresh runtime capture failed with status $fresh_status" >&2
    exit "$fresh_status"
  fi
fi

if [[ ! -f "$FRESH_DUCKDB" ]]; then
  echo "fresh runtime DuckDB was not generated: $FRESH_DUCKDB" >&2
  exit 65
fi

"$ROOT_DIR/scripts/rank_runtime_touch_assets.sh" \
  --duckdb "$FRESH_DUCKDB" \
  --output-dir "$RUNTIME_TOUCH_RANKING_DIR" \
  --lookback-ms "$RUNTIME_TOUCH_LOOKBACK_MS" \
  --min-snapshots "$MIN_RUNTIME_TOUCH_SNAPSHOTS" \
  --min-active-minutes "$MIN_RUNTIME_ACTIVE_MINUTES" \
  --min-touch-change-rate "$MIN_RUNTIME_TOUCH_CHANGE_RATE" \
  --limit "$UNIVERSE_LIMIT" \
  > "$RUN_ROOT/runtime_touch_ranking.stdout.json"

UNIVERSE_ARGS=(
  -m src.research.execution_probe_universe_selection
  --duckdb "$FRESH_DUCKDB"
  --output-dir "$RUN_ROOT/execution_probe_universe_selection"
  --profile "$UNIVERSE_PROFILE"
  --limit "$UNIVERSE_LIMIT"
  --min-assets "$UNIVERSE_MIN_ASSETS"
  --selection-source runtime_touch
  --runtime-touch-lookback-ms "$RUNTIME_TOUCH_LOOKBACK_MS"
  --min-runtime-touch-change-rate "$MIN_RUNTIME_TOUCH_CHANGE_RATE"
  --min-runtime-touch-snapshots "$MIN_RUNTIME_TOUCH_SNAPSHOTS"
  --min-runtime-active-minutes "$MIN_RUNTIME_ACTIVE_MINUTES"
)
if [[ -n "$MIN_AVG_OPPORTUNITY_SPREAD" ]]; then
  UNIVERSE_ARGS+=(--min-avg-opportunity-spread "$MIN_AVG_OPPORTUNITY_SPREAD")
fi
if [[ -n "$MAX_AVG_OPPORTUNITY_SPREAD" ]]; then
  UNIVERSE_ARGS+=(--max-avg-opportunity-spread "$MAX_AVG_OPPORTUNITY_SPREAD")
fi
PYTHONPATH=python-service python3 "${UNIVERSE_ARGS[@]}" \
  > "$RUN_ROOT/execution_probe_universe_selection.stdout.json"

python3 - "$UNIVERSE_SELECTION_PATH" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
payload = json.loads(path.read_text(encoding="utf-8"))
if payload.get("status") != "ready":
    raise SystemExit(f"runtime-touch A/B universe is not ready: {payload.get('selection_reason')}")
PY

run_observation() {
  local profile="$1"
  local timestamp="$2"
  local data_lake_root="$3"
  local report_root="$4"

  set +e
  REPORT_TIMESTAMP="$timestamp" \
    DATA_LAKE_ROOT="$data_lake_root" \
    RESEARCH_REPORT_ROOT="$report_root" \
    PREDICTOR_STRATEGY_PROFILE="$profile" \
    EXECUTION_MODE="dry_run" \
    DISABLE_MARKET_WS="false" \
    "$ROOT_DIR/scripts/run_runtime_touch_observation.sh" \
      --universe-selection "$UNIVERSE_SELECTION_PATH" \
      --duration-seconds "$OBSERVATION_SECONDS"
  local status=$?
  set -e
  if [[ "$status" != "0" && "$status" != "20" ]]; then
    echo "$profile observation failed with status $status" >&2
    exit "$status"
  fi

  "$ROOT_DIR/scripts/analyze_at_touch_assets.sh" \
    --report-root "$report_root" \
    > "$report_root/at_touch_asset_diagnostic.stdout.json"
  PYTHONPATH=python-service python3 -m src.research.execution_failure_diagnostics \
    --report-root "$report_root" \
    --output "$report_root/execution_failure_diagnostics.json" \
    > "$report_root/execution_failure_diagnostics.stdout.json"
  mkdir -p "$report_root/runtime_touch_ranking"
  cp "$RUNTIME_TOUCH_RANKING_DIR"/runtime_touch_*.parquet "$report_root/runtime_touch_ranking/"
  cp "$RUNTIME_TOUCH_RANKING_DIR"/selected_runtime_touch_markets.parquet "$report_root/runtime_touch_ranking/"
  cp "$RUNTIME_TOUCH_RANKING_DIR"/runtime_touch_ranking.json "$report_root/runtime_touch_ranking/runtime_touch_ranking.json"
}

run_observation "$PROFILE_A" "$PROFILE_A_TIMESTAMP" "$PROFILE_A_DATA_LAKE_ROOT" "$PROFILE_A_REPORT_ROOT"
run_observation "$PROFILE_B" "$PROFILE_B_TIMESTAMP" "$PROFILE_B_DATA_LAKE_ROOT" "$PROFILE_B_REPORT_ROOT"

PROFILE_ROOTS=()
if [[ -n "$COMPARISON_REPORT_ROOTS" ]]; then
  IFS=',' read -r -a EXTRA_PROFILE_ROOTS <<< "$COMPARISON_REPORT_ROOTS"
  for profile_root in "${EXTRA_PROFILE_ROOTS[@]}"; do
    if [[ -n "$profile_root" ]]; then
      PROFILE_ROOTS+=("$profile_root")
    fi
  done
fi
PROFILE_ROOTS+=("$PROFILE_A_REPORT_ROOT" "$PROFILE_B_REPORT_ROOT")

TOUCH_COMPARISON_ARGS=()
for profile_root in "${PROFILE_ROOTS[@]}"; do
  TOUCH_COMPARISON_ARGS+=(--report-root "$profile_root")
done

PYTHONPATH=python-service python3 -m src.research.profile_observation_comparison \
  "${TOUCH_COMPARISON_ARGS[@]}" \
  --output "$RUN_ROOT/profile_observation_comparison.json" \
  > "$RUN_ROOT/profile_observation_comparison.stdout.json"

PYTHONPATH=python-service python3 -m src.research.execution_probe_touch_comparison \
  "${TOUCH_COMPARISON_ARGS[@]}" \
  --output "$RUN_ROOT/execution_probe_touch_comparison.json" \
  > "$RUN_ROOT/execution_probe_touch_comparison.stdout.json"

python3 - "$RUN_ROOT" "$FRESH_DUCKDB" "$FRESH_REPORT_ROOT" "$UNIVERSE_SELECTION_PATH" "$PROFILE_A_REPORT_ROOT" "$PROFILE_B_REPORT_ROOT" <<'PY'
import json
import sys
from pathlib import Path

run_root = Path(sys.argv[1])
fresh_duckdb = Path(sys.argv[2])
fresh_report_root = Path(sys.argv[3])
universe_selection_path = Path(sys.argv[4])
profile_a_report_root = Path(sys.argv[5])
profile_b_report_root = Path(sys.argv[6])

def read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}

summary = {
    "can_execute_trades": False,
    "decision_policy": "runtime_touch_ab_cycle_research_only",
    "fresh_duckdb": str(fresh_duckdb),
    "fresh_report_root": str(fresh_report_root),
    "universe_selection": read_json(universe_selection_path),
    "profile_a_report_root": str(profile_a_report_root),
    "profile_b_report_root": str(profile_b_report_root),
    "profile_a_execution_failure": read_json(profile_a_report_root / "execution_failure_diagnostics.json"),
    "profile_b_execution_failure": read_json(profile_b_report_root / "execution_failure_diagnostics.json"),
    "profile_observation_comparison": read_json(run_root / "profile_observation_comparison.json"),
    "execution_probe_touch_comparison": read_json(run_root / "execution_probe_touch_comparison.json"),
}
(run_root / "runtime_touch_ab_cycle_summary.json").write_text(
    json.dumps(summary, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
print(json.dumps(summary, indent=2, sort_keys=True))
PY
