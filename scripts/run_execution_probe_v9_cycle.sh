#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CYCLE_TIMESTAMP="${CYCLE_TIMESTAMP:-execution-probe-v9-cycle-$(date -u +%Y%m%dT%H%M%SZ)}"
RUN_ROOT="${RUN_ROOT:-${ROOT_DIR}/.tmp/operational/${CYCLE_TIMESTAMP}}"
REPORT_TIMESTAMP="${REPORT_TIMESTAMP:-$CYCLE_TIMESTAMP}"
DATA_LAKE_ROOT="${DATA_LAKE_ROOT:-${ROOT_DIR}/.tmp/real-dry-run-data-lake/${REPORT_TIMESTAMP}}"
REPORT_ROOT="${RESEARCH_REPORT_ROOT:-${DATA_LAKE_ROOT}/reports/${REPORT_TIMESTAMP}}"
MANIFEST_ROOT="${RESEARCH_MANIFEST_ROOT:-${DATA_LAKE_ROOT}/research_runs}"
DURATION_SECONDS="${REAL_DRY_RUN_SECONDS:-5400}"
UNIVERSE_DUCKDB=""
BASELINE_REPORT_ROOT="${BASELINE_REPORT_ROOT:-}"
COMPARISON_REPORT_ROOTS="${PROFILE_OBSERVATION_COMPARISON_REPORT_ROOTS:-}"
PRINT_PLAN=0
UNIVERSE_LIMIT="${EXECUTION_PROBE_UNIVERSE_LIMIT:-10}"
UNIVERSE_MIN_ASSETS="${EXECUTION_PROBE_UNIVERSE_MIN_ASSETS:-5}"
MARKET_TIMING_FILTER="${EXECUTION_PROBE_MARKET_TIMING_FILTER:-future_touch}"
MIN_FUTURE_TOUCH_RATE="${EXECUTION_PROBE_MIN_FUTURE_TOUCH_RATE:-0.00625}"
MIN_TIMING_SIGNALS="${EXECUTION_PROBE_MIN_TIMING_SIGNALS:-5}"
MIN_AVG_OPPORTUNITY_SPREAD="${EXECUTION_PROBE_MIN_AVG_OPPORTUNITY_SPREAD:-0.000625}"
MAX_AVG_OPPORTUNITY_SPREAD="${EXECUTION_PROBE_MAX_AVG_OPPORTUNITY_SPREAD:-}"
SELECTION_SOURCE="${EXECUTION_PROBE_UNIVERSE_SELECTION_SOURCE:-${EXECUTION_PROBE_SELECTION_SOURCE:-fillability}}"
ADVERSE_SELECTION_FILTER="${EXECUTION_PROBE_ADVERSE_SELECTION_FILTER:-none}"
MAX_ADVERSE_30S_RATE="${EXECUTION_PROBE_MAX_ADVERSE_30S_RATE:-0.50}"
MIN_ADVERSE_FILLED_EVENTS="${EXECUTION_PROBE_MIN_ADVERSE_FILLED_EVENTS:-10}"

usage() {
  cat <<'EOF'
Usage: scripts/run_execution_probe_v9_cycle.sh --universe-duckdb PATH [--baseline-report-root PATH] [--comparison-report-roots CSV] [--duration-seconds N] [--universe-selection-source candidate_market_ranking|fillability] [--market-timing-filter none|future_touch] [--print-plan]

Runs the full execution_probe_v9 research cycle:
universe selection -> toxic-fill-aware dry-run observation -> profile comparison -> next decision.
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
    --comparison-report-roots)
      COMPARISON_REPORT_ROOTS="$2"
      shift 2
      ;;
    --duration-seconds)
      DURATION_SECONDS="$2"
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
    --market-timing-filter)
      MARKET_TIMING_FILTER="$2"
      shift 2
      ;;
    --min-future-touch-rate)
      MIN_FUTURE_TOUCH_RATE="$2"
      shift 2
      ;;
    --min-timing-signals)
      MIN_TIMING_SIGNALS="$2"
      shift 2
      ;;
    --min-avg-opportunity-spread)
      MIN_AVG_OPPORTUNITY_SPREAD="$2"
      shift 2
      ;;
    --max-avg-opportunity-spread)
      MAX_AVG_OPPORTUNITY_SPREAD="$2"
      shift 2
      ;;
    --universe-selection-source|--selection-source)
      SELECTION_SOURCE="$2"
      shift 2
      ;;
    --adverse-selection-filter)
      ADVERSE_SELECTION_FILTER="$2"
      shift 2
      ;;
    --max-adverse-30s-rate)
      MAX_ADVERSE_30S_RATE="$2"
      shift 2
      ;;
    --min-adverse-filled-events)
      MIN_ADVERSE_FILLED_EVENTS="$2"
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
if [[ -n "$COMPARISON_REPORT_ROOTS" && "$PRINT_PLAN" != "1" ]]; then
  IFS=',' read -r -a COMPARISON_ROOTS_TO_VALIDATE <<< "$COMPARISON_REPORT_ROOTS"
  for comparison_root in "${COMPARISON_ROOTS_TO_VALIDATE[@]}"; do
    if [[ -n "$comparison_root" && ! -d "$comparison_root" ]]; then
      echo "--comparison-report-roots contains a missing directory: $comparison_root" >&2
      exit 64
    fi
  done
fi
if ! [[ "$DURATION_SECONDS" =~ ^[0-9]+$ ]] || (( DURATION_SECONDS < 1800 || DURATION_SECONDS > 5400 )); then
  echo "duration must be an integer between 1800 and 5400 seconds" >&2
  exit 64
fi
if [[ "$MARKET_TIMING_FILTER" != "none" && "$MARKET_TIMING_FILTER" != "future_touch" ]]; then
  echo "market timing filter must be none or future_touch" >&2
  exit 64
fi
if [[ "$SELECTION_SOURCE" != "candidate_market_ranking" && "$SELECTION_SOURCE" != "fillability" ]]; then
  echo "selection source must be candidate_market_ranking or fillability" >&2
  exit 64
fi
if [[ "$ADVERSE_SELECTION_FILTER" != "none" && "$ADVERSE_SELECTION_FILTER" != "market_side" ]]; then
  echo "adverse selection filter must be none or market_side" >&2
  exit 64
fi

UNIVERSE_SELECTION_PATH="$RUN_ROOT/execution_probe_universe_selection/execution_probe_universe_selection.json"
OBSERVATION_COMMAND=(
  "$ROOT_DIR/scripts/run_execution_probe_v9_observation.sh"
  --universe-selection "$UNIVERSE_SELECTION_PATH"
  --duration-seconds "$DURATION_SECONDS"
)

if [[ "$PRINT_PLAN" == "1" ]]; then
  python3 - "$UNIVERSE_DUCKDB" "$BASELINE_REPORT_ROOT" "$COMPARISON_REPORT_ROOTS" "$RUN_ROOT" "$REPORT_TIMESTAMP" "$DATA_LAKE_ROOT" "$REPORT_ROOT" "$MANIFEST_ROOT" "$DURATION_SECONDS" "$UNIVERSE_SELECTION_PATH" "$UNIVERSE_LIMIT" "$UNIVERSE_MIN_ASSETS" "$MARKET_TIMING_FILTER" "$MIN_FUTURE_TOUCH_RATE" "$MIN_TIMING_SIGNALS" "$MIN_AVG_OPPORTUNITY_SPREAD" "$MAX_AVG_OPPORTUNITY_SPREAD" "$SELECTION_SOURCE" "$ADVERSE_SELECTION_FILTER" "$MAX_ADVERSE_30S_RATE" "$MIN_ADVERSE_FILLED_EVENTS" <<'PY'
import json
import sys

(
    universe_duckdb,
    baseline_report_root,
    comparison_report_roots,
    run_root,
    report_timestamp,
    data_lake_root,
    report_root,
    manifest_root,
    duration_seconds,
    universe_selection_path,
    universe_limit,
    universe_min_assets,
    market_timing_filter,
    min_future_touch_rate,
    min_timing_signals,
    min_avg_opportunity_spread,
    max_avg_opportunity_spread,
    selection_source,
    adverse_selection_filter,
    max_adverse_30s_rate,
    min_adverse_filled_events,
) = sys.argv[1:]
comparison_roots = [
    item.strip()
    for item in comparison_report_roots.split(",")
    if item.strip()
]
if baseline_report_root and baseline_report_root not in comparison_roots:
    comparison_roots.insert(0, baseline_report_root)

print(json.dumps({
    "script": "scripts/run_execution_probe_v9_cycle.sh",
    "can_execute_trades": False,
    "execution_mode": "dry_run",
    "profile": "execution_probe_v9",
    "universe_duckdb": universe_duckdb,
    "baseline_report_root": baseline_report_root or None,
    "comparison_report_roots": comparison_roots,
    "run_root": run_root,
    "report_timestamp": report_timestamp,
    "data_lake_root": data_lake_root,
    "report_root": report_root,
    "manifest_root": manifest_root,
    "duration_seconds": int(duration_seconds),
    "universe_limit": int(universe_limit),
    "universe_min_assets": int(universe_min_assets),
    "market_timing_filter": market_timing_filter,
    "min_future_touch_rate": float(min_future_touch_rate),
    "min_timing_signals": int(min_timing_signals),
    "min_avg_opportunity_spread": float(min_avg_opportunity_spread) if min_avg_opportunity_spread else None,
    "max_avg_opportunity_spread": float(max_avg_opportunity_spread) if max_avg_opportunity_spread else None,
    "selection_source": selection_source,
    "adverse_selection_filter": adverse_selection_filter,
    "max_adverse_30s_rate": float(max_adverse_30s_rate),
    "min_adverse_filled_events": int(min_adverse_filled_events),
    "universe_selection_path": universe_selection_path,
    "delegates_to": [
        "scripts/prepare_execution_probe_cycle.sh",
        "scripts/run_execution_probe_v9_observation.sh",
        "src.research.fill_toxicity",
        "src.research.profile_observation_comparison",
        "src.research.execution_probe_next_decision",
        "src.research.asset_execution_decision",
    ],
    "outputs": {
        "fill_toxicity": f"{report_root}/fill_toxicity/fill_toxicity.json",
        "execution_probe_universe_adverse_exclusions": f"{run_root}/execution_probe_universe_selection/execution_probe_universe_adverse_exclusions.parquet",
        "profile_observation_comparison": f"{report_root}/profile_observation_comparison.json",
        "execution_probe_next_decision": f"{report_root}/execution_probe_next_decision.json",
        "asset_execution_decision": f"{report_root}/asset_execution_decision/asset_execution_decision.json",
        "cycle_summary": f"{run_root}/execution_probe_v9_cycle_summary.json",
    },
}, indent=2, sort_keys=True))
PY
  exit 0
fi

mkdir -p "$RUN_ROOT"

PREPARE_ARGS=(
  --universe-duckdb "$UNIVERSE_DUCKDB"
  --duration-seconds "$DURATION_SECONDS"
  --universe-limit "$UNIVERSE_LIMIT"
  --min-assets "$UNIVERSE_MIN_ASSETS"
  --market-timing-filter "$MARKET_TIMING_FILTER"
  --min-future-touch-rate "$MIN_FUTURE_TOUCH_RATE"
  --min-timing-signals "$MIN_TIMING_SIGNALS"
  --selection-source "$SELECTION_SOURCE"
  --adverse-selection-filter "$ADVERSE_SELECTION_FILTER"
  --max-adverse-30s-rate "$MAX_ADVERSE_30S_RATE"
  --min-adverse-filled-events "$MIN_ADVERSE_FILLED_EVENTS"
)
if [[ -n "$MIN_AVG_OPPORTUNITY_SPREAD" ]]; then
  PREPARE_ARGS+=(--min-avg-opportunity-spread "$MIN_AVG_OPPORTUNITY_SPREAD")
fi
if [[ -n "$MAX_AVG_OPPORTUNITY_SPREAD" ]]; then
  PREPARE_ARGS+=(--max-avg-opportunity-spread "$MAX_AVG_OPPORTUNITY_SPREAD")
fi
if [[ -n "$BASELINE_REPORT_ROOT" ]]; then
  PREPARE_ARGS+=(--baseline-report-root "$BASELINE_REPORT_ROOT")
fi

RUN_ROOT="$RUN_ROOT" PROFILE=execution_probe_v9 \
  "$ROOT_DIR/scripts/prepare_execution_probe_cycle.sh" \
    "${PREPARE_ARGS[@]}" \
    > "$RUN_ROOT/prepare_execution_probe_cycle.stdout.json"

export REPORT_TIMESTAMP
export DATA_LAKE_ROOT
export RESEARCH_REPORT_ROOT="$REPORT_ROOT"
export RESEARCH_MANIFEST_ROOT="$MANIFEST_ROOT"
export EXECUTION_MODE="dry_run"
export DISABLE_MARKET_WS="false"
PROFILE_COMPARISON_ROOTS=()
if [[ -n "$COMPARISON_REPORT_ROOTS" ]]; then
  IFS=',' read -r -a EXTRA_COMPARISON_ROOTS <<< "$COMPARISON_REPORT_ROOTS"
  for comparison_root in "${EXTRA_COMPARISON_ROOTS[@]}"; do
    if [[ -n "$comparison_root" ]]; then
      PROFILE_COMPARISON_ROOTS+=("$comparison_root")
    fi
  done
fi
if [[ -n "$BASELINE_REPORT_ROOT" ]]; then
  PROFILE_COMPARISON_ROOTS+=("$BASELINE_REPORT_ROOT")
fi
PROFILE_OBSERVATION_COMPARISON_REPORT_ROOTS="$(
  python3 - "${PROFILE_COMPARISON_ROOTS[@]}" <<'PY'
import sys

seen = set()
roots = []
for root in sys.argv[1:]:
    if root and root not in seen:
        roots.append(root)
        seen.add(root)
print(",".join(roots))
PY
)"
export PROFILE_OBSERVATION_COMPARISON_REPORT_ROOTS

set +e
"${OBSERVATION_COMMAND[@]}"
observation_status=$?
set -e

if [[ "$observation_status" != "0" && "$observation_status" != "20" ]]; then
  echo "execution_probe_v9 observation failed with status $observation_status" >&2
  exit "$observation_status"
fi

PROFILE_ARGS=()
if [[ -n "${PROFILE_OBSERVATION_COMPARISON_REPORT_ROOTS:-}" ]]; then
  IFS=',' read -r -a PROFILE_ROOTS <<< "$PROFILE_OBSERVATION_COMPARISON_REPORT_ROOTS"
  for profile_root in "${PROFILE_ROOTS[@]}"; do
    if [[ -n "$profile_root" ]]; then
      PROFILE_ARGS+=(--report-root "$profile_root")
    fi
  done
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

PYTHONPATH=python-service python3 -m src.research.asset_execution_decision \
  --report-root "$REPORT_ROOT" \
  --output-dir "$REPORT_ROOT/asset_execution_decision" \
  --json \
  > "$REPORT_ROOT/asset_execution_decision.stdout.json"

python3 - "$RUN_ROOT" "$REPORT_ROOT" "$DATA_LAKE_ROOT" "$MANIFEST_ROOT" "$observation_status" "${PROFILE_OBSERVATION_COMPARISON_REPORT_ROOTS:-}" <<'PY'
import json
import sys
from pathlib import Path

run_root = Path(sys.argv[1])
report_root = Path(sys.argv[2])
data_lake_root = Path(sys.argv[3])
manifest_root = Path(sys.argv[4])
observation_status = int(sys.argv[5])
comparison_report_roots = [item for item in sys.argv[6].split(",") if item]
decision_path = report_root / "execution_probe_next_decision.json"
asset_decision_path = report_root / "asset_execution_decision" / "asset_execution_decision.json"
decision = json.loads(decision_path.read_text(encoding="utf-8"))
asset_decision = json.loads(asset_decision_path.read_text(encoding="utf-8"))
summary = {
    "report_version": "execution_probe_v9_cycle_summary_v1",
    "can_execute_trades": False,
    "execution_mode": "dry_run",
    "observation_status": observation_status,
    "report_root": str(report_root),
    "data_lake_root": str(data_lake_root),
    "manifest_root": str(manifest_root),
    "comparison_report_roots": comparison_report_roots,
    "fill_toxicity_path": str(report_root / "fill_toxicity" / "fill_toxicity.json"),
    "profile_observation_comparison_path": str(report_root / "profile_observation_comparison.json"),
    "execution_probe_next_decision_path": str(decision_path),
    "asset_execution_decision_path": str(asset_decision_path),
    "asset_execution_summary": asset_decision.get("summary"),
    "recommendation": decision.get("recommendation"),
    "next_step": decision.get("next_step"),
}
run_root.mkdir(parents=True, exist_ok=True)
(run_root / "execution_probe_v9_cycle_summary.json").write_text(
    json.dumps(summary, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
print(json.dumps(summary, indent=2, sort_keys=True))
PY

if [[ "$observation_status" == "20" && "${ALLOW_CYCLE_GATE_FAILURE:-1}" != "1" && "${ALLOW_CYCLE_GATE_FAILURE:-1}" != "true" ]]; then
  exit 20
fi
