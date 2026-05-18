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
SKIP_SIGNALABILITY_GATE="${EXECUTION_PROBE_SKIP_SIGNALABILITY_GATE:-0}"
UNIVERSE_LIMIT="${EXECUTION_PROBE_UNIVERSE_LIMIT:-10}"
UNIVERSE_MIN_ASSETS="${EXECUTION_PROBE_UNIVERSE_MIN_ASSETS:-2}"
RUNTIME_TOUCH_LOOKBACK_MS="${EXECUTION_PROBE_RUNTIME_TOUCH_LOOKBACK_MS:-900000}"
RUNTIME_TOUCH_FRESHNESS_ORDERING="${EXECUTION_PROBE_RUNTIME_TOUCH_FRESHNESS_ORDERING:-freshest_first}"
RECENT_SIGNALABLE_WINDOW_MS="${EXECUTION_PROBE_RECENT_SIGNALABLE_WINDOW_MS:-180000}"
MIN_RUNTIME_TOUCH_CHANGE_RATE="${EXECUTION_PROBE_MIN_RUNTIME_TOUCH_CHANGE_RATE:-0.01}"
MIN_RUNTIME_TOUCH_SNAPSHOTS="${EXECUTION_PROBE_MIN_RUNTIME_TOUCH_SNAPSHOTS:-10}"
MIN_RUNTIME_ACTIVE_MINUTES="${EXECUTION_PROBE_MIN_RUNTIME_ACTIVE_MINUTES:-2}"
MIN_RUNTIME_SIGNALABLE_SNAPSHOTS="${EXECUTION_PROBE_MIN_RUNTIME_SIGNALABLE_SNAPSHOTS:-3}"
MIN_RUNTIME_SIGNALABLE_DENSITY="${EXECUTION_PROBE_MIN_RUNTIME_SIGNALABLE_DENSITY:-0.05}"
RUNTIME_SIGNAL_MIN_SPREAD="${EXECUTION_PROBE_RUNTIME_SIGNAL_MIN_SPREAD:-0.01}"
RUNTIME_SIGNAL_MIN_DEPTH="${EXECUTION_PROBE_RUNTIME_SIGNAL_MIN_DEPTH:-1.5}"
MIN_AVG_OPPORTUNITY_SPREAD="${EXECUTION_PROBE_MIN_AVG_OPPORTUNITY_SPREAD:-0.000625}"
MAX_AVG_OPPORTUNITY_SPREAD="${EXECUTION_PROBE_MAX_AVG_OPPORTUNITY_SPREAD:-}"

usage() {
  cat <<'EOF'
Usage: scripts/run_runtime_touch_ab_cycle.sh [--skip-fresh-capture --fresh-duckdb PATH --fresh-report-root PATH] [--skip-signalability-gate] [--duration-seconds N] [--print-plan]

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
    --skip-signalability-gate)
      SKIP_SIGNALABILITY_GATE=1
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
    --freshness-ordering)
      RUNTIME_TOUCH_FRESHNESS_ORDERING="$2"
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
if [[ "$SKIP_SIGNALABILITY_GATE" != "0" && "$SKIP_SIGNALABILITY_GATE" != "1" ]]; then
  echo "EXECUTION_PROBE_SKIP_SIGNALABILITY_GATE must be 0 or 1" >&2
  exit 64
fi
if [[ "$RUNTIME_TOUCH_FRESHNESS_ORDERING" != "score_first" && "$RUNTIME_TOUCH_FRESHNESS_ORDERING" != "freshest_first" ]]; then
  echo "freshness ordering must be score_first or freshest_first" >&2
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
SIGNALABILITY_GATE_DIR="$RUN_ROOT/runtime_touch_signalability_gate"
SIGNALABILITY_GATE_PATH="$SIGNALABILITY_GATE_DIR/runtime_touch_signalability_diagnostic.json"
PROFILE_A_TIMESTAMP="${CYCLE_TIMESTAMP}-${PROFILE_A}"
PROFILE_B_TIMESTAMP="${CYCLE_TIMESTAMP}-${PROFILE_B}"
PROFILE_A_DATA_LAKE_ROOT="${ROOT_DIR}/.tmp/real-dry-run-data-lake/${PROFILE_A_TIMESTAMP}"
PROFILE_B_DATA_LAKE_ROOT="${ROOT_DIR}/.tmp/real-dry-run-data-lake/${PROFILE_B_TIMESTAMP}"
PROFILE_A_REPORT_ROOT="${PROFILE_A_DATA_LAKE_ROOT}/reports/${PROFILE_A_TIMESTAMP}"
PROFILE_B_REPORT_ROOT="${PROFILE_B_DATA_LAKE_ROOT}/reports/${PROFILE_B_TIMESTAMP}"

if [[ "$PRINT_PLAN" == "1" ]]; then
  python3 - "$RUN_ROOT" "$FRESH_DUCKDB" "$FRESH_REPORT_ROOT" "$PROFILE_A" "$PROFILE_B" "$PROFILE_A_REPORT_ROOT" "$PROFILE_B_REPORT_ROOT" "$FRESH_CAPTURE_SECONDS" "$OBSERVATION_SECONDS" "$UNIVERSE_SELECTION_PATH" "$RUNTIME_TOUCH_RANKING_DIR" "$SIGNALABILITY_GATE_PATH" "$COMPARISON_REPORT_ROOTS" "$SKIP_FRESH_CAPTURE" "$SKIP_SIGNALABILITY_GATE" "$MIN_RUNTIME_SIGNALABLE_SNAPSHOTS" "$MIN_RUNTIME_SIGNALABLE_DENSITY" "$RUNTIME_SIGNAL_MIN_SPREAD" "$RUNTIME_SIGNAL_MIN_DEPTH" "$RUNTIME_TOUCH_FRESHNESS_ORDERING" "$RECENT_SIGNALABLE_WINDOW_MS" <<'PY'
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
    signalability_gate_path,
    comparison_roots_csv,
    skip_fresh_capture,
    skip_signalability_gate,
    min_runtime_signalable_snapshots,
    min_runtime_signalable_density,
    runtime_signal_min_spread,
    runtime_signal_min_depth,
    runtime_touch_freshness_ordering,
    recent_signalable_window_ms,
) = sys.argv[1:]
print(json.dumps({
    "script": "scripts/run_runtime_touch_ab_cycle.sh",
    "can_execute_trades": False,
    "execution_mode": "dry_run",
    "skip_fresh_capture": skip_fresh_capture == "1",
    "signalability_gate_enabled": skip_signalability_gate != "1",
    "fresh_capture_seconds": int(fresh_capture_seconds),
    "observation_seconds_per_profile": int(observation_seconds),
    "profile_a": profile_a,
    "profile_b": profile_b,
    "min_runtime_signalable_snapshots": int(min_runtime_signalable_snapshots),
    "min_runtime_signalable_density": float(min_runtime_signalable_density),
    "runtime_signal_min_spread": float(runtime_signal_min_spread),
    "runtime_signal_min_depth": float(runtime_signal_min_depth),
    "runtime_touch_freshness_ordering": runtime_touch_freshness_ordering,
    "recent_signalable_window_ms": int(recent_signalable_window_ms),
    "fresh_duckdb": fresh_duckdb,
    "fresh_report_root": fresh_report_root,
    "comparison_report_roots": [item.strip() for item in comparison_roots_csv.split(",") if item.strip()],
    "delegates_to": [
        "scripts/run_pre_live_dry_run.sh",
        "src.research.runtime_touch_signalability_diagnostic",
        "src.research.runtime_touch_ranking",
        "src.research.execution_probe_universe_selection",
        "scripts/run_runtime_touch_observation.sh",
        "src.research.execution_failure_diagnostics",
        "src.research.profile_observation_comparison",
        "src.research.execution_probe_touch_comparison",
        "src.research.runtime_touch_ab_decision",
    ],
    "outputs": {
        "run_root": run_root,
        "signalability_gate": signalability_gate_path,
        "runtime_touch_ranking": f"{runtime_touch_ranking_dir}/runtime_touch_ranking.json",
        "execution_probe_universe_selection": universe_selection_path,
        "profile_a_report_root": profile_a_report_root,
        "profile_b_report_root": profile_b_report_root,
        "profile_observation_comparison": f"{run_root}/profile_observation_comparison.json",
        "execution_probe_touch_comparison": f"{run_root}/execution_probe_touch_comparison.json",
        "runtime_touch_ab_decision": f"{run_root}/runtime_touch_ab_decision.json",
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

if [[ "$SKIP_SIGNALABILITY_GATE" != "1" ]]; then
  PYTHONPATH=python-service python3 -m src.research.runtime_touch_signalability_diagnostic \
    --duckdb "$FRESH_DUCKDB" \
    --output-dir "$SIGNALABILITY_GATE_DIR" \
    --min-assets "$UNIVERSE_MIN_ASSETS" \
    --limit "$UNIVERSE_LIMIT" \
    --min-snapshots "$MIN_RUNTIME_TOUCH_SNAPSHOTS" \
    --min-active-minutes "$MIN_RUNTIME_ACTIVE_MINUTES" \
    --min-touch-change-rate "$MIN_RUNTIME_TOUCH_CHANGE_RATE" \
    --signal-min-spread "$RUNTIME_SIGNAL_MIN_SPREAD" \
    --signal-min-depth "$RUNTIME_SIGNAL_MIN_DEPTH" \
    --recent-signalable-window-ms "$RECENT_SIGNALABLE_WINDOW_MS" \
    --min-signalable-snapshots "$MIN_RUNTIME_SIGNALABLE_SNAPSHOTS" \
    --min-signalable-density "$MIN_RUNTIME_SIGNALABLE_DENSITY" \
    > "$RUN_ROOT/runtime_touch_signalability_gate.stdout.json"
  python3 - "$RUN_ROOT" "$FRESH_DUCKDB" "$FRESH_REPORT_ROOT" "$SIGNALABILITY_GATE_PATH" <<'PY'
import json
import sys
from pathlib import Path

run_root = Path(sys.argv[1])
fresh_duckdb = Path(sys.argv[2])
fresh_report_root = Path(sys.argv[3])
gate_path = Path(sys.argv[4])

gate = json.loads(gate_path.read_text(encoding="utf-8"))
if gate.get("status") == "ready":
    raise SystemExit(0)

summary = {
    "can_execute_trades": False,
    "decision_policy": "runtime_touch_ab_cycle_research_only",
    "status": "blocked",
    "blocker": "runtime_touch_signalability_gate",
    "fresh_duckdb": str(fresh_duckdb),
    "fresh_report_root": str(fresh_report_root),
    "signalability_gate": gate,
}
(run_root / "runtime_touch_ab_cycle_summary.json").write_text(
    json.dumps(summary, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
print(json.dumps(summary, indent=2, sort_keys=True))
raise SystemExit(20)
PY
fi

"$ROOT_DIR/scripts/rank_runtime_touch_assets.sh" \
  --duckdb "$FRESH_DUCKDB" \
  --output-dir "$RUNTIME_TOUCH_RANKING_DIR" \
  --lookback-ms "$RUNTIME_TOUCH_LOOKBACK_MS" \
  --min-snapshots "$MIN_RUNTIME_TOUCH_SNAPSHOTS" \
  --min-active-minutes "$MIN_RUNTIME_ACTIVE_MINUTES" \
  --min-touch-change-rate "$MIN_RUNTIME_TOUCH_CHANGE_RATE" \
  --min-signalable-snapshots "$MIN_RUNTIME_SIGNALABLE_SNAPSHOTS" \
  --min-signalable-density "$MIN_RUNTIME_SIGNALABLE_DENSITY" \
  --signal-min-spread "$RUNTIME_SIGNAL_MIN_SPREAD" \
  --signal-min-depth "$RUNTIME_SIGNAL_MIN_DEPTH" \
  --recent-signalable-window-ms "$RECENT_SIGNALABLE_WINDOW_MS" \
  --freshness-ordering "$RUNTIME_TOUCH_FRESHNESS_ORDERING" \
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
  --min-runtime-signalable-snapshots "$MIN_RUNTIME_SIGNALABLE_SNAPSHOTS"
  --min-runtime-signalable-density "$MIN_RUNTIME_SIGNALABLE_DENSITY"
  --runtime-signal-min-spread "$RUNTIME_SIGNAL_MIN_SPREAD"
  --runtime-signal-min-depth "$RUNTIME_SIGNAL_MIN_DEPTH"
  --runtime-recent-signalable-window-ms "$RECENT_SIGNALABLE_WINDOW_MS"
  --runtime-touch-freshness-ordering "$RUNTIME_TOUCH_FRESHNESS_ORDERING"
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
    local preflight_path="$report_root/real_dry_run_preflight.json"
    if [[ -f "$preflight_path" ]]; then
      PYTHONPATH=python-service python3 -m src.research.runtime_touch_ab_decision \
        --preflight-failure "$preflight_path" \
        --failed-profile "$profile" \
        --output "$RUN_ROOT/runtime_touch_ab_decision.json" \
        > "$RUN_ROOT/runtime_touch_ab_decision.stdout.json"
      write_failure_summary "$profile" "$status" "$report_root" "$preflight_path"
    fi
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

write_failure_summary() {
  local failed_profile="$1"
  local status="$2"
  local report_root="$3"
  local preflight_path="$4"
  python3 - "$RUN_ROOT" "$FRESH_DUCKDB" "$FRESH_REPORT_ROOT" "$UNIVERSE_SELECTION_PATH" "$failed_profile" "$status" "$report_root" "$preflight_path" <<'PY'
import json
import sys
from pathlib import Path

run_root = Path(sys.argv[1])
fresh_duckdb = Path(sys.argv[2])
fresh_report_root = Path(sys.argv[3])
universe_selection_path = Path(sys.argv[4])
failed_profile = sys.argv[5]
status = int(sys.argv[6])
report_root = Path(sys.argv[7])
preflight_path = Path(sys.argv[8])

def read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}

summary = {
    "can_execute_trades": False,
    "decision_policy": "runtime_touch_ab_cycle_research_only",
    "status": "failed",
    "failed_profile": failed_profile,
    "failed_status": status,
    "fresh_duckdb": str(fresh_duckdb),
    "fresh_report_root": str(fresh_report_root),
    "universe_selection": read_json(universe_selection_path),
    "failed_report_root": str(report_root),
    "failed_preflight": read_json(preflight_path),
    "runtime_touch_ab_decision": read_json(run_root / "runtime_touch_ab_decision.json"),
}
(run_root / "runtime_touch_ab_cycle_summary.json").write_text(
    json.dumps(summary, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
print(json.dumps(summary, indent=2, sort_keys=True))
PY
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

PYTHONPATH=python-service python3 -m src.research.runtime_touch_ab_decision \
  --profile-observation-comparison "$RUN_ROOT/profile_observation_comparison.json" \
  --touch-comparison "$RUN_ROOT/execution_probe_touch_comparison.json" \
  --output "$RUN_ROOT/runtime_touch_ab_decision.json" \
  > "$RUN_ROOT/runtime_touch_ab_decision.stdout.json"

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
    "runtime_touch_ab_decision": read_json(run_root / "runtime_touch_ab_decision.json"),
}
(run_root / "runtime_touch_ab_cycle_summary.json").write_text(
    json.dumps(summary, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
print(json.dumps(summary, indent=2, sort_keys=True))
PY
