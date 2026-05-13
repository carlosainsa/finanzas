#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="${PROFILE:-execution_probe_v6}"
RUN_ROOT="${RUN_ROOT:-${ROOT_DIR}/.tmp/operational/${PROFILE}-cycle-$(date -u +%Y%m%dT%H%M%SZ)}"
UNIVERSE_DUCKDB=""
BASELINE_REPORT_ROOT="${BASELINE_REPORT_ROOT:-}"
DURATION_SECONDS="${REAL_DRY_RUN_SECONDS:-3600}"
UNIVERSE_LIMIT="${EXECUTION_PROBE_UNIVERSE_LIMIT:-10}"
UNIVERSE_MIN_ASSETS="${EXECUTION_PROBE_UNIVERSE_MIN_ASSETS:-5}"
MARKET_TIMING_FILTER="${EXECUTION_PROBE_MARKET_TIMING_FILTER:-none}"
MIN_FUTURE_TOUCH_RATE="${EXECUTION_PROBE_MIN_FUTURE_TOUCH_RATE:-0.10}"
MIN_TIMING_SIGNALS="${EXECUTION_PROBE_MIN_TIMING_SIGNALS:-5}"
MIN_AVG_OPPORTUNITY_SPREAD="${EXECUTION_PROBE_MIN_AVG_OPPORTUNITY_SPREAD:-}"
MAX_AVG_OPPORTUNITY_SPREAD="${EXECUTION_PROBE_MAX_AVG_OPPORTUNITY_SPREAD:-}"
SELECTION_SOURCE="${EXECUTION_PROBE_UNIVERSE_SELECTION_SOURCE:-${EXECUTION_PROBE_SELECTION_SOURCE:-candidate_market_ranking}}"
ADVERSE_SELECTION_FILTER="${EXECUTION_PROBE_ADVERSE_SELECTION_FILTER:-none}"
MAX_ADVERSE_30S_RATE="${EXECUTION_PROBE_MAX_ADVERSE_30S_RATE:-0.50}"
MIN_ADVERSE_FILLED_EVENTS="${EXECUTION_PROBE_MIN_ADVERSE_FILLED_EVENTS:-10}"
TOXICITY_FILTER="${EXECUTION_PROBE_TOXICITY_FILTER:-none}"
MIN_TOXICITY_FILLED_EVENTS="${EXECUTION_PROBE_MIN_TOXICITY_FILLED_EVENTS:-3}"
MIN_RUNTIME_ACTIVE_MINUTES="${EXECUTION_PROBE_MIN_RUNTIME_ACTIVE_MINUTES:-1}"
RUNTIME_ACTIVITY_BACKFILL="${EXECUTION_PROBE_RUNTIME_ACTIVITY_BACKFILL:-false}"
RUNTIME_BACKFILL_MIN_OPPORTUNITIES="${EXECUTION_PROBE_RUNTIME_BACKFILL_MIN_OPPORTUNITIES:-3}"
RUNTIME_BACKFILL_MIN_ACTIVE_MINUTES="${EXECUTION_PROBE_RUNTIME_BACKFILL_MIN_ACTIVE_MINUTES:-2}"
RUNTIME_TOUCH_LOOKBACK_MS="${EXECUTION_PROBE_RUNTIME_TOUCH_LOOKBACK_MS:-900000}"
MIN_RUNTIME_TOUCH_CHANGE_RATE="${EXECUTION_PROBE_MIN_RUNTIME_TOUCH_CHANGE_RATE:-0.01}"
MIN_RUNTIME_TOUCH_SNAPSHOTS="${EXECUTION_PROBE_MIN_RUNTIME_TOUCH_SNAPSHOTS:-10}"

usage() {
  cat <<'EOF'
Usage: scripts/prepare_execution_probe_cycle.sh --universe-duckdb PATH [--baseline-report-root PATH] [--duration-seconds N] [--universe-selection-source candidate_market_ranking|fillability|executable_segments|touch_probability|runtime_touch] [--market-timing-filter none|future_touch] [--toxicity-filter none|segment] [--runtime-activity-backfill]

Prepares a repeatable execution-probe cycle without starting services:
market universe selection -> observation command plan -> optional baseline compare
commands. The generated files are research-only and live under .tmp/operational.
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
    --toxicity-filter)
      TOXICITY_FILTER="$2"
      shift 2
      ;;
    --min-toxicity-filled-events)
      MIN_TOXICITY_FILLED_EVENTS="$2"
      shift 2
      ;;
    --min-runtime-active-minutes)
      MIN_RUNTIME_ACTIVE_MINUTES="$2"
      shift 2
      ;;
    --runtime-activity-backfill)
      RUNTIME_ACTIVITY_BACKFILL="true"
      shift
      ;;
    --no-runtime-activity-backfill)
      RUNTIME_ACTIVITY_BACKFILL="false"
      shift
      ;;
    --runtime-backfill-min-opportunities)
      RUNTIME_BACKFILL_MIN_OPPORTUNITIES="$2"
      shift 2
      ;;
    --runtime-backfill-min-active-minutes)
      RUNTIME_BACKFILL_MIN_ACTIVE_MINUTES="$2"
      shift 2
      ;;
    --runtime-touch-lookback-ms)
      RUNTIME_TOUCH_LOOKBACK_MS="$2"
      shift 2
      ;;
    --min-runtime-touch-change-rate)
      MIN_RUNTIME_TOUCH_CHANGE_RATE="$2"
      shift 2
      ;;
    --min-runtime-touch-snapshots)
      MIN_RUNTIME_TOUCH_SNAPSHOTS="$2"
      shift 2
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

if [[ "$PROFILE" != "execution_probe_v6" && "$PROFILE" != "execution_probe_v7" && "$PROFILE" != "execution_probe_v8" && "$PROFILE" != "execution_probe_v9" && "$PROFILE" != "execution_probe_v10" && "$PROFILE" != "execution_probe_v11" && "$PROFILE" != "execution_probe_v12" ]]; then
  echo "Only PROFILE=execution_probe_v6, PROFILE=execution_probe_v7, PROFILE=execution_probe_v8, PROFILE=execution_probe_v9, PROFILE=execution_probe_v10, PROFILE=execution_probe_v11, or PROFILE=execution_probe_v12 is supported by this cycle preparer." >&2
  exit 64
fi
if [[ -z "$UNIVERSE_DUCKDB" || ! -f "$UNIVERSE_DUCKDB" ]]; then
  echo "--universe-duckdb must point to an existing DuckDB file" >&2
  exit 64
fi
if ! [[ "$DURATION_SECONDS" =~ ^[0-9]+$ ]] || (( DURATION_SECONDS < 1800 || DURATION_SECONDS > 5400 )); then
  echo "duration must be an integer between 1800 and 5400 seconds" >&2
  exit 64
fi
if [[ "$MARKET_TIMING_FILTER" != "none" && "$MARKET_TIMING_FILTER" != "future_touch" ]]; then
  echo "market timing filter must be none or future_touch" >&2
  exit 64
fi
if [[ "$SELECTION_SOURCE" != "candidate_market_ranking" && "$SELECTION_SOURCE" != "fillability" && "$SELECTION_SOURCE" != "executable_segments" && "$SELECTION_SOURCE" != "touch_probability" && "$SELECTION_SOURCE" != "runtime_touch" ]]; then
  echo "selection source must be candidate_market_ranking, fillability, executable_segments, touch_probability, or runtime_touch" >&2
  exit 64
fi
if [[ "$ADVERSE_SELECTION_FILTER" != "none" && "$ADVERSE_SELECTION_FILTER" != "market_side" ]]; then
  echo "adverse selection filter must be none or market_side" >&2
  exit 64
fi
if [[ "$TOXICITY_FILTER" != "none" && "$TOXICITY_FILTER" != "segment" ]]; then
  echo "toxicity filter must be none or segment" >&2
  exit 64
fi
if ! [[ "$MIN_RUNTIME_ACTIVE_MINUTES" =~ ^[0-9]+$ ]] || (( MIN_RUNTIME_ACTIVE_MINUTES <= 0 )); then
  echo "min runtime active minutes must be a positive integer" >&2
  exit 64
fi
if [[ "$RUNTIME_ACTIVITY_BACKFILL" != "true" && "$RUNTIME_ACTIVITY_BACKFILL" != "false" && "$RUNTIME_ACTIVITY_BACKFILL" != "1" && "$RUNTIME_ACTIVITY_BACKFILL" != "0" ]]; then
  echo "runtime activity backfill must be true, false, 1, or 0" >&2
  exit 64
fi
if ! [[ "$RUNTIME_BACKFILL_MIN_OPPORTUNITIES" =~ ^[0-9]+$ ]] || (( RUNTIME_BACKFILL_MIN_OPPORTUNITIES <= 0 )); then
  echo "runtime backfill min opportunities must be a positive integer" >&2
  exit 64
fi
if ! [[ "$RUNTIME_BACKFILL_MIN_ACTIVE_MINUTES" =~ ^[0-9]+$ ]] || (( RUNTIME_BACKFILL_MIN_ACTIVE_MINUTES <= 0 )); then
  echo "runtime backfill min active minutes must be a positive integer" >&2
  exit 64
fi
if ! [[ "$RUNTIME_TOUCH_LOOKBACK_MS" =~ ^[0-9]+$ ]] || (( RUNTIME_TOUCH_LOOKBACK_MS <= 0 )); then
  echo "runtime touch lookback ms must be a positive integer" >&2
  exit 64
fi
if ! [[ "$MIN_RUNTIME_TOUCH_SNAPSHOTS" =~ ^[0-9]+$ ]] || (( MIN_RUNTIME_TOUCH_SNAPSHOTS <= 0 )); then
  echo "min runtime touch snapshots must be a positive integer" >&2
  exit 64
fi

mkdir -p "$RUN_ROOT"

UNIVERSE_SELECTION_ARGS=(
  --duckdb "$UNIVERSE_DUCKDB"
  --output-dir "$RUN_ROOT/execution_probe_universe_selection"
  --profile "$PROFILE"
  --limit "$UNIVERSE_LIMIT"
  --min-assets "$UNIVERSE_MIN_ASSETS"
  --market-timing-filter "$MARKET_TIMING_FILTER"
  --min-future-touch-rate "$MIN_FUTURE_TOUCH_RATE"
  --min-timing-signals "$MIN_TIMING_SIGNALS"
  --selection-source "$SELECTION_SOURCE"
  --adverse-selection-filter "$ADVERSE_SELECTION_FILTER"
  --max-adverse-30s-rate "$MAX_ADVERSE_30S_RATE"
  --min-adverse-filled-events "$MIN_ADVERSE_FILLED_EVENTS"
  --toxicity-filter "$TOXICITY_FILTER"
  --min-toxicity-filled-events "$MIN_TOXICITY_FILLED_EVENTS"
  --min-runtime-active-minutes "$MIN_RUNTIME_ACTIVE_MINUTES"
  --runtime-backfill-min-opportunities "$RUNTIME_BACKFILL_MIN_OPPORTUNITIES"
  --runtime-backfill-min-active-minutes "$RUNTIME_BACKFILL_MIN_ACTIVE_MINUTES"
  --runtime-touch-lookback-ms "$RUNTIME_TOUCH_LOOKBACK_MS"
  --min-runtime-touch-change-rate "$MIN_RUNTIME_TOUCH_CHANGE_RATE"
  --min-runtime-touch-snapshots "$MIN_RUNTIME_TOUCH_SNAPSHOTS"
)
if [[ "$RUNTIME_ACTIVITY_BACKFILL" == "true" || "$RUNTIME_ACTIVITY_BACKFILL" == "1" ]]; then
  UNIVERSE_SELECTION_ARGS+=(--runtime-activity-backfill)
fi
if [[ -n "$MIN_AVG_OPPORTUNITY_SPREAD" ]]; then
  UNIVERSE_SELECTION_ARGS+=(--min-avg-opportunity-spread "$MIN_AVG_OPPORTUNITY_SPREAD")
fi
if [[ -n "$MAX_AVG_OPPORTUNITY_SPREAD" ]]; then
  UNIVERSE_SELECTION_ARGS+=(--max-avg-opportunity-spread "$MAX_AVG_OPPORTUNITY_SPREAD")
fi

PYTHONPATH=python-service python3 -m src.research.execution_probe_universe_selection \
  "${UNIVERSE_SELECTION_ARGS[@]}" \
  > "$RUN_ROOT/execution_probe_universe_selection.stdout.json"

OBSERVATION_COMMAND=(
  "scripts/run_${PROFILE}_observation.sh"
  --universe-selection "$RUN_ROOT/execution_probe_universe_selection/execution_probe_universe_selection.json"
  --duration-seconds "$DURATION_SECONDS"
)

{
  printf 'prepared_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'profile=%s\n' "$PROFILE"
  printf 'run_root=%s\n' "$RUN_ROOT"
  printf 'universe_duckdb=%s\n' "$UNIVERSE_DUCKDB"
  printf 'duration_seconds=%s\n' "$DURATION_SECONDS"
  printf 'universe_limit=%s\n' "$UNIVERSE_LIMIT"
  printf 'universe_min_assets=%s\n' "$UNIVERSE_MIN_ASSETS"
  printf 'market_timing_filter=%s\n' "$MARKET_TIMING_FILTER"
  printf 'min_future_touch_rate=%s\n' "$MIN_FUTURE_TOUCH_RATE"
  printf 'min_timing_signals=%s\n' "$MIN_TIMING_SIGNALS"
  printf 'min_avg_opportunity_spread=%s\n' "$MIN_AVG_OPPORTUNITY_SPREAD"
  printf 'max_avg_opportunity_spread=%s\n' "$MAX_AVG_OPPORTUNITY_SPREAD"
  printf 'selection_source=%s\n' "$SELECTION_SOURCE"
  printf 'adverse_selection_filter=%s\n' "$ADVERSE_SELECTION_FILTER"
  printf 'max_adverse_30s_rate=%s\n' "$MAX_ADVERSE_30S_RATE"
  printf 'min_adverse_filled_events=%s\n' "$MIN_ADVERSE_FILLED_EVENTS"
  printf 'toxicity_filter=%s\n' "$TOXICITY_FILTER"
  printf 'min_toxicity_filled_events=%s\n' "$MIN_TOXICITY_FILLED_EVENTS"
  printf 'min_runtime_active_minutes=%s\n' "$MIN_RUNTIME_ACTIVE_MINUTES"
  printf 'runtime_activity_backfill=%s\n' "$RUNTIME_ACTIVITY_BACKFILL"
  printf 'runtime_backfill_min_opportunities=%s\n' "$RUNTIME_BACKFILL_MIN_OPPORTUNITIES"
  printf 'runtime_backfill_min_active_minutes=%s\n' "$RUNTIME_BACKFILL_MIN_ACTIVE_MINUTES"
  printf 'runtime_touch_lookback_ms=%s\n' "$RUNTIME_TOUCH_LOOKBACK_MS"
  printf 'min_runtime_touch_change_rate=%s\n' "$MIN_RUNTIME_TOUCH_CHANGE_RATE"
  printf 'min_runtime_touch_snapshots=%s\n' "$MIN_RUNTIME_TOUCH_SNAPSHOTS"
  printf 'toxicity_filter_input_report_path=%s\n' "$RUN_ROOT/execution_probe_universe_selection/fill_toxicity/fill_toxicity.json"
  printf 'toxicity_filter_input_blocklist_path=%s\n' "$RUN_ROOT/execution_probe_universe_selection/fill_toxicity/blocked_segments.json"
  printf 'universe_toxicity_quality_path=%s\n' "$RUN_ROOT/execution_probe_universe_selection/execution_probe_universe_toxicity_quality.parquet"
  printf 'segment_opportunity_ranking_path=%s\n' "$RUN_ROOT/execution_probe_universe_selection/segment_opportunity_ranking/segment_opportunity_ranking.json"
  printf 'runtime_touch_ranking_path=%s\n' "$RUN_ROOT/execution_probe_universe_selection/runtime_touch_ranking/runtime_touch_ranking.json"
  printf 'allowed_segments_path=%s\n' "$RUN_ROOT/execution_probe_universe_selection/segment_opportunity_ranking/allowed_segments.json"
  printf 'observation_command=%q ' "${OBSERVATION_COMMAND[@]}"
  printf '\n'
  if [[ -n "$BASELINE_REPORT_ROOT" ]]; then
    printf 'compare_after_run=PYTHONPATH=python-service python3 -m src.research.compare_runs --baseline-report-root %q --candidate-report-root <NEW_REPORT_ROOT> --json > <NEW_REPORT_ROOT>/comparison_vs_baseline.json\n' "$BASELINE_REPORT_ROOT"
    printf 'profile_compare_after_run=PYTHONPATH=python-service python3 -m src.research.profile_observation_comparison --report-root %q --report-root <NEW_REPORT_ROOT> --output <NEW_REPORT_ROOT>/profile_observation_comparison.json\n' "$BASELINE_REPORT_ROOT"
    printf 'decide_after_run=PYTHONPATH=python-service python3 -m src.research.execution_probe_next_decision --comparison <NEW_REPORT_ROOT>/profile_observation_comparison.json --output <NEW_REPORT_ROOT>/execution_probe_next_decision.json --json\n'
  fi
} > "$RUN_ROOT/execution_probe_cycle_plan.env"

"${OBSERVATION_COMMAND[@]}" --print-plan > "$RUN_ROOT/execution_probe_observation_plan.json"

cat "$RUN_ROOT/execution_probe_observation_plan.json"
