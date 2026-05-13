#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CYCLE_TIMESTAMP="${CYCLE_TIMESTAMP:-runtime-touch-cycle-$(date -u +%Y%m%dT%H%M%SZ)}"
RUN_ROOT="${RUN_ROOT:-${ROOT_DIR}/.tmp/operational/${CYCLE_TIMESTAMP}}"
FRESH_TIMESTAMP="${FRESH_TIMESTAMP:-${CYCLE_TIMESTAMP}-fresh}"
OBSERVATION_TIMESTAMP="${OBSERVATION_TIMESTAMP:-${CYCLE_TIMESTAMP}-observation}"
FRESH_DATA_LAKE_ROOT="${FRESH_DATA_LAKE_ROOT:-${ROOT_DIR}/.tmp/real-dry-run-data-lake/${FRESH_TIMESTAMP}}"
OBSERVATION_DATA_LAKE_ROOT="${OBSERVATION_DATA_LAKE_ROOT:-${ROOT_DIR}/.tmp/real-dry-run-data-lake/${OBSERVATION_TIMESTAMP}}"
FRESH_DUCKDB="${FRESH_DUCKDB:-${FRESH_DATA_LAKE_ROOT}/research.duckdb}"
FRESH_REPORT_ROOT="${FRESH_REPORT_ROOT:-${FRESH_DATA_LAKE_ROOT}/reports/${FRESH_TIMESTAMP}}"
OBSERVATION_REPORT_ROOT="${OBSERVATION_REPORT_ROOT:-${OBSERVATION_DATA_LAKE_ROOT}/reports/${OBSERVATION_TIMESTAMP}}"
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
Usage: scripts/run_runtime_touch_cycle.sh [--skip-fresh-capture --fresh-duckdb PATH --fresh-report-root PATH] [--fresh-capture-seconds N] [--duration-seconds N] [--comparison-report-roots CSV] [--print-plan]

Runs the research-only runtime-touch loop:
fresh dry-run capture -> runtime_touch ranking -> runtime_touch universe selection
-> dry-run observation -> at-touch diagnostic -> touch comparison.

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
    --universe-limit)
      UNIVERSE_LIMIT="$2"
      shift 2
      ;;
    --min-assets)
      UNIVERSE_MIN_ASSETS="$2"
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
    --min-runtime-active-minutes)
      MIN_RUNTIME_ACTIVE_MINUTES="$2"
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

if ! [[ "$FRESH_CAPTURE_SECONDS" =~ ^[0-9]+$ ]] || (( FRESH_CAPTURE_SECONDS < 1800 || FRESH_CAPTURE_SECONDS > 5400 )); then
  echo "fresh capture duration must be an integer between 1800 and 5400 seconds" >&2
  exit 64
fi
if ! [[ "$OBSERVATION_SECONDS" =~ ^[0-9]+$ ]] || (( OBSERVATION_SECONDS < 1800 || OBSERVATION_SECONDS > 5400 )); then
  echo "observation duration must be an integer between 1800 and 5400 seconds" >&2
  exit 64
fi
if ! [[ "$UNIVERSE_MIN_ASSETS" =~ ^[0-9]+$ ]] || (( UNIVERSE_MIN_ASSETS < 2 )); then
  echo "min assets must be an integer >= 2 for runtime-touch observation" >&2
  exit 64
fi
if ! [[ "$UNIVERSE_LIMIT" =~ ^[0-9]+$ ]] || (( UNIVERSE_LIMIT < UNIVERSE_MIN_ASSETS )); then
  echo "universe limit must be an integer >= min assets" >&2
  exit 64
fi
if ! [[ "$MIN_RUNTIME_ACTIVE_MINUTES" =~ ^[0-9]+$ ]] || (( MIN_RUNTIME_ACTIVE_MINUTES <= 0 )); then
  echo "min runtime active minutes must be a positive integer" >&2
  exit 64
fi
if [[ "$SKIP_FRESH_CAPTURE" == "1" && ! -f "$FRESH_DUCKDB" ]]; then
  echo "--skip-fresh-capture requires --fresh-duckdb to exist" >&2
  exit 64
fi

UNIVERSE_SELECTION_PATH="$RUN_ROOT/execution_probe_universe_selection/execution_probe_universe_selection.json"
RUNTIME_TOUCH_RANKING_DIR="$RUN_ROOT/runtime_touch_ranking"

if [[ "$PRINT_PLAN" == "1" ]]; then
  python3 - "$RUN_ROOT" "$FRESH_DUCKDB" "$FRESH_REPORT_ROOT" "$OBSERVATION_REPORT_ROOT" "$FRESH_CAPTURE_SECONDS" "$OBSERVATION_SECONDS" "$COMPARISON_REPORT_ROOTS" "$UNIVERSE_SELECTION_PATH" "$RUNTIME_TOUCH_RANKING_DIR" "$UNIVERSE_LIMIT" "$UNIVERSE_MIN_ASSETS" "$RUNTIME_TOUCH_LOOKBACK_MS" "$MIN_RUNTIME_TOUCH_CHANGE_RATE" "$MIN_RUNTIME_TOUCH_SNAPSHOTS" "$MIN_RUNTIME_ACTIVE_MINUTES" "$MIN_AVG_OPPORTUNITY_SPREAD" "$MAX_AVG_OPPORTUNITY_SPREAD" "$SKIP_FRESH_CAPTURE" <<'PY'
import json
import sys

(
    run_root,
    fresh_duckdb,
    fresh_report_root,
    observation_report_root,
    fresh_capture_seconds,
    observation_seconds,
    comparison_roots_csv,
    universe_selection_path,
    runtime_touch_ranking_dir,
    universe_limit,
    universe_min_assets,
    runtime_touch_lookback_ms,
    min_runtime_touch_change_rate,
    min_runtime_touch_snapshots,
    min_runtime_active_minutes,
    min_avg_opportunity_spread,
    max_avg_opportunity_spread,
    skip_fresh_capture,
) = sys.argv[1:]
comparison_roots = [item.strip() for item in comparison_roots_csv.split(",") if item.strip()]
print(json.dumps({
    "script": "scripts/run_runtime_touch_cycle.sh",
    "can_execute_trades": False,
    "execution_mode": "dry_run",
    "skip_fresh_capture": skip_fresh_capture == "1",
    "fresh_capture_seconds": int(fresh_capture_seconds),
    "observation_seconds": int(observation_seconds),
    "fresh_duckdb": fresh_duckdb,
    "fresh_report_root": fresh_report_root,
    "observation_report_root": observation_report_root,
    "comparison_report_roots": comparison_roots,
    "universe_limit": int(universe_limit),
    "universe_min_assets": int(universe_min_assets),
    "runtime_touch_lookback_ms": int(runtime_touch_lookback_ms),
    "min_runtime_touch_change_rate": float(min_runtime_touch_change_rate),
    "min_runtime_touch_snapshots": int(min_runtime_touch_snapshots),
    "min_runtime_active_minutes": int(min_runtime_active_minutes),
    "min_avg_opportunity_spread": float(min_avg_opportunity_spread) if min_avg_opportunity_spread else None,
    "max_avg_opportunity_spread": float(max_avg_opportunity_spread) if max_avg_opportunity_spread else None,
    "delegates_to": [
        "scripts/run_pre_live_dry_run.sh",
        "src.research.runtime_touch_ranking",
        "src.research.execution_probe_universe_selection",
        "scripts/run_runtime_touch_observation.sh",
        "scripts/analyze_at_touch_assets.sh",
        "src.research.execution_probe_touch_comparison",
    ],
    "outputs": {
        "run_root": run_root,
        "runtime_touch_ranking": f"{runtime_touch_ranking_dir}/runtime_touch_ranking.json",
        "execution_probe_universe_selection": universe_selection_path,
        "at_touch_asset_diagnostic": f"{observation_report_root}/at_touch_asset_diagnostic/at_touch_asset_diagnostic.json",
        "execution_probe_touch_comparison": f"{observation_report_root}/execution_probe_touch_comparison.json",
        "cycle_summary": f"{run_root}/runtime_touch_cycle_summary.json",
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
  --profile execution_probe_v11
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
    raise SystemExit(f"runtime-touch universe is not ready: {payload.get('selection_reason')}")
PY

PROFILE_ROOTS=()
if [[ -n "$COMPARISON_REPORT_ROOTS" ]]; then
  IFS=',' read -r -a EXTRA_PROFILE_ROOTS <<< "$COMPARISON_REPORT_ROOTS"
  for profile_root in "${EXTRA_PROFILE_ROOTS[@]}"; do
    if [[ -n "$profile_root" ]]; then
      PROFILE_ROOTS+=("$profile_root")
    fi
  done
fi
PROFILE_OBSERVATION_COMPARISON_REPORT_ROOTS="$(
  python3 - "${PROFILE_ROOTS[@]}" <<'PY'
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
REPORT_TIMESTAMP="$OBSERVATION_TIMESTAMP" \
  DATA_LAKE_ROOT="$OBSERVATION_DATA_LAKE_ROOT" \
  RESEARCH_REPORT_ROOT="$OBSERVATION_REPORT_ROOT" \
  EXECUTION_MODE="dry_run" \
  DISABLE_MARKET_WS="false" \
  "$ROOT_DIR/scripts/run_runtime_touch_observation.sh" \
    --universe-selection "$UNIVERSE_SELECTION_PATH" \
    --duration-seconds "$OBSERVATION_SECONDS"
observation_status=$?
set -e
if [[ "$observation_status" != "0" && "$observation_status" != "20" ]]; then
  echo "runtime-touch observation failed with status $observation_status" >&2
  exit "$observation_status"
fi

"$ROOT_DIR/scripts/analyze_at_touch_assets.sh" \
  --report-root "$OBSERVATION_REPORT_ROOT" \
  > "$OBSERVATION_REPORT_ROOT/at_touch_asset_diagnostic.stdout.json"

mkdir -p "$OBSERVATION_REPORT_ROOT/runtime_touch_ranking"
cp "$RUNTIME_TOUCH_RANKING_DIR"/runtime_touch_*.parquet \
  "$OBSERVATION_REPORT_ROOT/runtime_touch_ranking/"
cp "$RUNTIME_TOUCH_RANKING_DIR"/selected_runtime_touch_markets.parquet \
  "$OBSERVATION_REPORT_ROOT/runtime_touch_ranking/"
cp "$RUNTIME_TOUCH_RANKING_DIR"/runtime_touch_ranking.json \
  "$OBSERVATION_REPORT_ROOT/runtime_touch_ranking/runtime_touch_ranking.json"

TOUCH_COMPARISON_ARGS=()
for profile_root in "${PROFILE_ROOTS[@]}"; do
  TOUCH_COMPARISON_ARGS+=(--report-root "$profile_root")
done
TOUCH_COMPARISON_ARGS+=(--report-root "$OBSERVATION_REPORT_ROOT")
PYTHONPATH=python-service python3 -m src.research.execution_probe_touch_comparison \
  "${TOUCH_COMPARISON_ARGS[@]}" \
  --output "$OBSERVATION_REPORT_ROOT/execution_probe_touch_comparison.json" \
  > "$OBSERVATION_REPORT_ROOT/execution_probe_touch_comparison.stdout.json"

python3 - "$RUN_ROOT" "$FRESH_DUCKDB" "$FRESH_REPORT_ROOT" "$OBSERVATION_REPORT_ROOT" "$UNIVERSE_SELECTION_PATH" <<'PY'
import json
import sys
from pathlib import Path

run_root = Path(sys.argv[1])
fresh_duckdb = Path(sys.argv[2])
fresh_report_root = Path(sys.argv[3])
observation_report_root = Path(sys.argv[4])
universe_selection_path = Path(sys.argv[5])

def read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}

summary = {
    "can_execute_trades": False,
    "decision_policy": "runtime_touch_cycle_research_only",
    "fresh_duckdb": str(fresh_duckdb),
    "fresh_report_root": str(fresh_report_root),
    "observation_report_root": str(observation_report_root),
    "universe_selection": read_json(universe_selection_path),
    "at_touch_asset_diagnostic": read_json(
        observation_report_root / "at_touch_asset_diagnostic" / "at_touch_asset_diagnostic.json"
    ),
    "execution_probe_touch_comparison": read_json(
        observation_report_root / "execution_probe_touch_comparison.json"
    ),
}
(run_root / "runtime_touch_cycle_summary.json").write_text(
    json.dumps(summary, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
print(json.dumps(summary, indent=2, sort_keys=True))
PY
