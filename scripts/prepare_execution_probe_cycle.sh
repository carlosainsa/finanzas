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

usage() {
  cat <<'EOF'
Usage: scripts/prepare_execution_probe_cycle.sh --universe-duckdb PATH [--baseline-report-root PATH] [--duration-seconds N] [--market-timing-filter none|future_touch]

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

if [[ "$PROFILE" != "execution_probe_v6" && "$PROFILE" != "execution_probe_v7" ]]; then
  echo "Only PROFILE=execution_probe_v6 or PROFILE=execution_probe_v7 is supported by this cycle preparer." >&2
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
)
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
