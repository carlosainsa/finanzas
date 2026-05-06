#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_LAKE_ROOT="${DATA_LAKE_ROOT:-$ROOT_DIR/data_lake}"
DUCKDB_PATH="${DATA_LAKE_DUCKDB:-$DATA_LAKE_ROOT/research.duckdb}"
EXPORT_COUNT="${DATA_LAKE_EXPORT_COUNT:-1000}"
TRAIN_FRACTION="${RESEARCH_TRAIN_FRACTION:-0.70}"
BASELINE_QUOTE_PLACEMENT="${BASELINE_QUOTE_PLACEMENT:-passive_bid}"
BASELINE_NEAR_TOUCH_TICK_SIZE="${BASELINE_NEAR_TOUCH_TICK_SIZE:-0.01}"
BASELINE_NEAR_TOUCH_OFFSET_TICKS="${BASELINE_NEAR_TOUCH_OFFSET_TICKS:-0}"
BASELINE_NEAR_TOUCH_MAX_SPREAD_FRACTION="${BASELINE_NEAR_TOUCH_MAX_SPREAD_FRACTION:-1.0}"
GO_NO_GO_PROFILE="${GO_NO_GO_PROFILE:-dev}"
RESEARCH_RESOURCE_MODE="${RESEARCH_RESOURCE_MODE:-full}"
PRE_LIVE_PROMOTION_ARGS=()
GO_NO_GO_ARGS=(--profile "$GO_NO_GO_PROFILE")
MARKET_REGIME_ARGS=(--resource-mode "$RESEARCH_RESOURCE_MODE")
BASELINE_ARGS=(
  --quote-placement "$BASELINE_QUOTE_PLACEMENT"
  --near-touch-tick-size "$BASELINE_NEAR_TOUCH_TICK_SIZE"
  --near-touch-offset-ticks "$BASELINE_NEAR_TOUCH_OFFSET_TICKS"
  --near-touch-max-spread-fraction "$BASELINE_NEAR_TOUCH_MAX_SPREAD_FRACTION"
)
if [[ -n "${BASELINE_MAX_SNAPSHOTS_PER_ASSET:-}" ]]; then
  BASELINE_ARGS+=(--max-snapshots-per-asset "$BASELINE_MAX_SNAPSHOTS_PER_ASSET")
fi
if [[ -n "${MARKET_REGIME_MAX_SNAPSHOTS_PER_ASSET:-}" ]]; then
  MARKET_REGIME_ARGS+=(--max-snapshots-per-asset "$MARKET_REGIME_MAX_SNAPSHOTS_PER_ASSET")
fi
if [[ -n "${MARKET_REGIME_MAX_TRADE_CONTEXT_ROWS:-}" ]]; then
  MARKET_REGIME_ARGS+=(--max-trade-context-rows "$MARKET_REGIME_MAX_TRADE_CONTEXT_ROWS")
fi
if [[ -n "${PRE_LIVE_MIN_CAPTURE_DURATION_MS:-}" ]]; then
  PRE_LIVE_PROMOTION_ARGS+=(--min-capture-duration-ms "$PRE_LIVE_MIN_CAPTURE_DURATION_MS")
  GO_NO_GO_ARGS+=(--min-capture-duration-ms "$PRE_LIVE_MIN_CAPTURE_DURATION_MS")
fi
if [[ -n "${PRE_LIVE_MIN_SIGNALS:-}" ]]; then
  PRE_LIVE_PROMOTION_ARGS+=(--min-signals "$PRE_LIVE_MIN_SIGNALS")
  GO_NO_GO_ARGS+=(--min-signals "$PRE_LIVE_MIN_SIGNALS")
fi
if [[ -n "${PRE_LIVE_MIN_REALIZED_EDGE:-}" ]]; then
  PRE_LIVE_PROMOTION_ARGS+=(--min-realized-edge "$PRE_LIVE_MIN_REALIZED_EDGE")
  GO_NO_GO_ARGS+=(--min-realized-edge "$PRE_LIVE_MIN_REALIZED_EDGE")
fi
if [[ -n "${PRE_LIVE_MIN_FILL_RATE:-}" ]]; then
  PRE_LIVE_PROMOTION_ARGS+=(--min-fill-rate "$PRE_LIVE_MIN_FILL_RATE")
  GO_NO_GO_ARGS+=(--min-fill-rate "$PRE_LIVE_MIN_FILL_RATE")
fi
if [[ -n "${PRE_LIVE_MIN_DRY_RUN_OBSERVED_FILL_RATE:-}" ]]; then
  PRE_LIVE_PROMOTION_ARGS+=(--min-dry-run-observed-fill-rate "$PRE_LIVE_MIN_DRY_RUN_OBSERVED_FILL_RATE")
  GO_NO_GO_ARGS+=(--min-dry-run-observed-fill-rate "$PRE_LIVE_MIN_DRY_RUN_OBSERVED_FILL_RATE")
fi
if [[ -n "${PRE_LIVE_MAX_ABS_SIMULATOR_FILL_RATE_DELTA:-}" ]]; then
  PRE_LIVE_PROMOTION_ARGS+=(--max-abs-simulator-fill-rate-delta "$PRE_LIVE_MAX_ABS_SIMULATOR_FILL_RATE_DELTA")
  GO_NO_GO_ARGS+=(--max-abs-simulator-fill-rate-delta "$PRE_LIVE_MAX_ABS_SIMULATOR_FILL_RATE_DELTA")
fi
if [[ -n "${PRE_LIVE_MAX_ABS_SLIPPAGE:-}" ]]; then
  PRE_LIVE_PROMOTION_ARGS+=(--max-abs-slippage "$PRE_LIVE_MAX_ABS_SLIPPAGE")
  GO_NO_GO_ARGS+=(--max-abs-slippage "$PRE_LIVE_MAX_ABS_SLIPPAGE")
fi
if [[ -n "${PRE_LIVE_MAX_ADVERSE_SELECTION_RATE:-}" ]]; then
  PRE_LIVE_PROMOTION_ARGS+=(--max-adverse-selection-rate "$PRE_LIVE_MAX_ADVERSE_SELECTION_RATE")
  GO_NO_GO_ARGS+=(--max-adverse-selection-rate "$PRE_LIVE_MAX_ADVERSE_SELECTION_RATE")
fi
if [[ -n "${PRE_LIVE_MAX_DRAWDOWN:-}" ]]; then
  PRE_LIVE_PROMOTION_ARGS+=(--max-drawdown "$PRE_LIVE_MAX_DRAWDOWN")
  GO_NO_GO_ARGS+=(--max-drawdown "$PRE_LIVE_MAX_DRAWDOWN")
fi
if [[ -n "${PRE_LIVE_MAX_STALE_DATA_RATE:-}" ]]; then
  PRE_LIVE_PROMOTION_ARGS+=(--max-stale-data-rate "$PRE_LIVE_MAX_STALE_DATA_RATE")
  GO_NO_GO_ARGS+=(--max-stale-data-rate "$PRE_LIVE_MAX_STALE_DATA_RATE")
fi
if [[ -n "${PRE_LIVE_MAX_RECONCILIATION_DIVERGENCE_RATE:-}" ]]; then
  PRE_LIVE_PROMOTION_ARGS+=(--max-reconciliation-divergence-rate "$PRE_LIVE_MAX_RECONCILIATION_DIVERGENCE_RATE")
  GO_NO_GO_ARGS+=(--max-reconciliation-divergence-rate "$PRE_LIVE_MAX_RECONCILIATION_DIVERGENCE_RATE")
fi
if [[ -n "${PRE_LIVE_MAX_BRIER_SCORE:-}" ]]; then
  PRE_LIVE_PROMOTION_ARGS+=(--max-brier-score "$PRE_LIVE_MAX_BRIER_SCORE")
  GO_NO_GO_ARGS+=(--max-brier-score "$PRE_LIVE_MAX_BRIER_SCORE")
fi
if [[ -n "${PRE_LIVE_STALE_GAP_MS:-}" ]]; then
  PRE_LIVE_PROMOTION_ARGS+=(--stale-gap-ms "$PRE_LIVE_STALE_GAP_MS")
  GO_NO_GO_ARGS+=(--stale-gap-ms "$PRE_LIVE_STALE_GAP_MS")
fi
REPORT_TIMESTAMP="${REPORT_TIMESTAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"
REPORT_ROOT="${RESEARCH_REPORT_ROOT:-$DATA_LAKE_ROOT/reports/$REPORT_TIMESTAMP}"
MANIFEST_ROOT="${RESEARCH_MANIFEST_ROOT:-$DATA_LAKE_ROOT/research_runs}"
ALLOW_GATE_FAILURE="${ALLOW_RESEARCH_GATE_FAILURE:-0}"
INCLUDE_MARKET_METADATA="${INCLUDE_MARKET_METADATA:-1}"

mkdir -p "$REPORT_ROOT"

DATA_LAKE_ARGS=(
  -m src.research.data_lake
  --root "$DATA_LAKE_ROOT"
  --duckdb "$DUCKDB_PATH"
  --count "$EXPORT_COUNT"
)
if [[ "$INCLUDE_MARKET_METADATA" == "1" || "$INCLUDE_MARKET_METADATA" == "true" ]]; then
  DATA_LAKE_ARGS+=(--include-market-metadata)
fi

cd "$ROOT_DIR"
PYTHONPATH=python-service python3 "${DATA_LAKE_ARGS[@]}" > "$REPORT_ROOT/data_lake_export.json"
PYTHONPATH=python-service python3 -m src.research.deterministic_baseline \
  --duckdb "$DUCKDB_PATH" \
  --output-dir "$REPORT_ROOT/baseline" \
  "${BASELINE_ARGS[@]}" \
  > "$REPORT_ROOT/baseline.json"
PYTHONPATH=python-service python3 -m src.research.synthetic_fills \
  --duckdb "$DUCKDB_PATH" \
  --output-dir "$REPORT_ROOT/synthetic_fills" \
  --min-confidence "${MIN_CONFIDENCE:-0.55}" > "$REPORT_ROOT/synthetic_fills.json"
PYTHONPATH=python-service python3 -m src.research.backtest \
  --duckdb "$DUCKDB_PATH" \
  --output-dir "$REPORT_ROOT/backtest" \
  --pre-live-gate > "$REPORT_ROOT/backtest.json"
PYTHONPATH=python-service python3 -m src.research.game_theory \
  --duckdb "$DUCKDB_PATH" \
  --output-dir "$REPORT_ROOT/game_theory" > "$REPORT_ROOT/game_theory.json"
EXECUTION_QUALITY_ARGS=(
  -m src.research.execution_quality
  --duckdb "$DUCKDB_PATH"
  --output-dir "$REPORT_ROOT/execution_quality"
)
if [[ -n "${EXECUTION_QUALITY_MIN_SIGNALS:-}" ]]; then
  EXECUTION_QUALITY_ARGS+=(--min-signals "$EXECUTION_QUALITY_MIN_SIGNALS")
fi
if [[ -n "${EXECUTION_QUALITY_MAX_ERROR_RATE:-}" ]]; then
  EXECUTION_QUALITY_ARGS+=(--max-error-rate "$EXECUTION_QUALITY_MAX_ERROR_RATE")
fi
if [[ -n "${EXECUTION_QUALITY_MAX_UNFILLED_RATE:-}" ]]; then
  EXECUTION_QUALITY_ARGS+=(--max-unfilled-rate "$EXECUTION_QUALITY_MAX_UNFILLED_RATE")
fi
if [[ -n "${EXECUTION_QUALITY_MAX_ABS_SLIPPAGE:-}" ]]; then
  EXECUTION_QUALITY_ARGS+=(--max-abs-slippage "$EXECUTION_QUALITY_MAX_ABS_SLIPPAGE")
fi
if [[ -n "${EXECUTION_QUALITY_MAX_AVG_REPORT_LATENCY_MS:-}" ]]; then
  EXECUTION_QUALITY_ARGS+=(--max-avg-report-latency-ms "$EXECUTION_QUALITY_MAX_AVG_REPORT_LATENCY_MS")
fi
if [[ -n "${EXECUTION_QUALITY_LIMIT:-}" ]]; then
  EXECUTION_QUALITY_ARGS+=(--limit "$EXECUTION_QUALITY_LIMIT")
fi
PYTHONPATH=python-service python3 "${EXECUTION_QUALITY_ARGS[@]}" \
  > "$REPORT_ROOT/execution_quality.json"
PYTHONPATH=python-service python3 -m src.research.quote_execution_diagnostics \
  --duckdb "$DUCKDB_PATH" \
  --output-dir "$REPORT_ROOT/quote_execution_diagnostics" \
  > "$REPORT_ROOT/quote_execution_diagnostics.json"
MARKET_OPPORTUNITY_ARGS=(
  -m src.research.market_opportunity_selector
  --duckdb "$DUCKDB_PATH"
  --output-dir "$REPORT_ROOT/market_opportunity_selector"
)
if [[ -n "${MARKET_OPPORTUNITY_MIN_SPREAD:-}" ]]; then
  MARKET_OPPORTUNITY_ARGS+=(--min-spread "$MARKET_OPPORTUNITY_MIN_SPREAD")
fi
if [[ -n "${MARKET_OPPORTUNITY_MAX_SPREAD:-}" ]]; then
  MARKET_OPPORTUNITY_ARGS+=(--max-spread "$MARKET_OPPORTUNITY_MAX_SPREAD")
fi
if [[ -n "${MARKET_OPPORTUNITY_MIN_SNAPSHOTS:-}" ]]; then
  MARKET_OPPORTUNITY_ARGS+=(--min-snapshots "$MARKET_OPPORTUNITY_MIN_SNAPSHOTS")
fi
if [[ -n "${MARKET_OPPORTUNITY_MIN_DENSITY:-}" ]]; then
  MARKET_OPPORTUNITY_ARGS+=(--min-opportunity-density "$MARKET_OPPORTUNITY_MIN_DENSITY")
fi
if [[ -n "${MARKET_OPPORTUNITY_MIN_LIQUIDITY:-}" ]]; then
  MARKET_OPPORTUNITY_ARGS+=(--min-liquidity "$MARKET_OPPORTUNITY_MIN_LIQUIDITY")
fi
if [[ -n "${MARKET_OPPORTUNITY_MAX_STALE_RATE:-}" ]]; then
  MARKET_OPPORTUNITY_ARGS+=(--max-stale-rate "$MARKET_OPPORTUNITY_MAX_STALE_RATE")
fi
if [[ -n "${MARKET_OPPORTUNITY_LIMIT:-}" ]]; then
  MARKET_OPPORTUNITY_ARGS+=(--limit "$MARKET_OPPORTUNITY_LIMIT")
fi
PYTHONPATH=python-service python3 "${MARKET_OPPORTUNITY_ARGS[@]}" \
  > "$REPORT_ROOT/market_opportunity_selector.json"
CANDIDATE_MARKET_RANKING_ARGS=(
  -m src.research.candidate_market_ranking
  --duckdb "$DUCKDB_PATH"
  --output-dir "$REPORT_ROOT/candidate_market_ranking"
)
if [[ -n "${CANDIDATE_MARKET_OPPORTUNITY_WEIGHT:-}" ]]; then
  CANDIDATE_MARKET_RANKING_ARGS+=(--opportunity-weight "$CANDIDATE_MARKET_OPPORTUNITY_WEIGHT")
fi
if [[ -n "${CANDIDATE_MARKET_EXECUTION_WEIGHT:-}" ]]; then
  CANDIDATE_MARKET_RANKING_ARGS+=(--execution-weight "$CANDIDATE_MARKET_EXECUTION_WEIGHT")
fi
if [[ -n "${CANDIDATE_MARKET_MIN_COMBINED_SCORE:-}" ]]; then
  CANDIDATE_MARKET_RANKING_ARGS+=(--min-combined-score "$CANDIDATE_MARKET_MIN_COMBINED_SCORE")
fi
if [[ -n "${CANDIDATE_MARKET_MIN_EXECUTION_FILL_RATE:-}" ]]; then
  CANDIDATE_MARKET_RANKING_ARGS+=(--min-execution-fill-rate "$CANDIDATE_MARKET_MIN_EXECUTION_FILL_RATE")
fi
if [[ -n "${CANDIDATE_MARKET_MAX_UNFILLED_RATE:-}" ]]; then
  CANDIDATE_MARKET_RANKING_ARGS+=(--max-unfilled-rate "$CANDIDATE_MARKET_MAX_UNFILLED_RATE")
fi
if [[ -n "${CANDIDATE_MARKET_MAX_STALE_RATE:-}" ]]; then
  CANDIDATE_MARKET_RANKING_ARGS+=(--max-stale-rate "$CANDIDATE_MARKET_MAX_STALE_RATE")
fi
if [[ -n "${CANDIDATE_MARKET_LIMIT:-}" ]]; then
  CANDIDATE_MARKET_RANKING_ARGS+=(--limit "$CANDIDATE_MARKET_LIMIT")
fi
PYTHONPATH=python-service python3 "${CANDIDATE_MARKET_RANKING_ARGS[@]}" \
  > "$REPORT_ROOT/candidate_market_ranking.json"
PYTHONPATH=python-service python3 -m src.research.market_regime \
  --duckdb "$DUCKDB_PATH" \
  --output-dir "$REPORT_ROOT/market_regime" \
  "${MARKET_REGIME_ARGS[@]}" > "$REPORT_ROOT/market_regime.json"
PYTHONPATH=python-service python3 -m src.research.sentiment_features \
  --duckdb "$DUCKDB_PATH" \
  --output-dir "$REPORT_ROOT/sentiment_features" > "$REPORT_ROOT/sentiment_features.json"
PYTHONPATH=python-service python3 -m src.research.sentiment_lift \
  --duckdb "$DUCKDB_PATH" \
  --output-dir "$REPORT_ROOT/sentiment_lift" > "$REPORT_ROOT/sentiment_lift.json"
PYTHONPATH=python-service python3 -m src.research.feature_blocklist_candidates \
  --duckdb "$DUCKDB_PATH" \
  --output-dir "$REPORT_ROOT/feature_blocklist_candidates" > "$REPORT_ROOT/feature_blocklist_candidates.json"
PYTHONPATH=python-service python3 -m src.research.calibration \
  --duckdb "$DUCKDB_PATH" \
  --output-dir "$REPORT_ROOT/calibration" \
  --train-fraction "$TRAIN_FRACTION" > "$REPORT_ROOT/calibration.json"
PYTHONPATH=python-service python3 -m src.research.near_touch_calibration \
  --duckdb "$DUCKDB_PATH" \
  --output-dir "$REPORT_ROOT/near_touch_calibration" \
  --fractions "${NEAR_TOUCH_CALIBRATION_FRACTIONS:-0.60,0.65,0.70,0.75,0.80,0.85}" \
  --min-signals "${NEAR_TOUCH_CALIBRATION_MIN_SIGNALS:-50}" \
  --min-adjusted-synthetic-fill-rate "${NEAR_TOUCH_CALIBRATION_MIN_ADJUSTED_SYNTHETIC_FILL_RATE:-0.02}" \
  --max-adjusted-synthetic-fill-rate "${NEAR_TOUCH_CALIBRATION_MAX_ADJUSTED_SYNTHETIC_FILL_RATE:-0.15}" \
  --max-raw-synthetic-fill-rate "${NEAR_TOUCH_CALIBRATION_MAX_RAW_SYNTHETIC_FILL_RATE:-0.50}" \
  > "$REPORT_ROOT/near_touch_calibration.json"
PYTHONPATH=python-service python3 -m src.research.pre_live_promotion \
  --duckdb "$DUCKDB_PATH" \
  --output-dir "$REPORT_ROOT/pre_live_promotion" \
  "${PRE_LIVE_PROMOTION_ARGS[@]}" > "$REPORT_ROOT/pre_live_promotion.json"
PYTHONPATH=python-service python3 -m src.research.go_no_go \
  --duckdb "$DUCKDB_PATH" \
  --output-dir "$REPORT_ROOT/go_no_go" \
  "${GO_NO_GO_ARGS[@]}" > "$REPORT_ROOT/go_no_go.json"
SIGNAL_REJECTION_ARGS=(
  -m src.research.signal_rejection_diagnostics
  --duckdb "$DUCKDB_PATH"
  --output-dir "$REPORT_ROOT/signal_rejection_diagnostics"
  --quote-placement "${SIGNAL_REJECTION_QUOTE_PLACEMENT:-${PREDICTOR_QUOTE_PLACEMENT:-near_touch}}"
)
if [[ -n "${SIGNAL_REJECTION_PROFILES:-}" ]]; then
  SIGNAL_REJECTION_ARGS+=(--profiles "$SIGNAL_REJECTION_PROFILES")
fi
if [[ -n "${SIGNAL_REJECTION_BASELINE_PROFILE:-}" ]]; then
  SIGNAL_REJECTION_ARGS+=(--baseline-profile "$SIGNAL_REJECTION_BASELINE_PROFILE")
fi
if [[ -n "${SIGNAL_REJECTION_CANDIDATE_PROFILE:-}" ]]; then
  SIGNAL_REJECTION_ARGS+=(--candidate-profile "$SIGNAL_REJECTION_CANDIDATE_PROFILE")
fi
if [[ -n "${SIGNAL_REJECTION_MAX_SNAPSHOTS:-}" ]]; then
  SIGNAL_REJECTION_ARGS+=(--max-snapshots "$SIGNAL_REJECTION_MAX_SNAPSHOTS")
fi
PYTHONPATH=python-service python3 "${SIGNAL_REJECTION_ARGS[@]}" \
  > "$REPORT_ROOT/signal_rejection_diagnostics.json"
PYTHONPATH=python-service python3 -m src.research.agent_advisory \
  --duckdb "$DUCKDB_PATH" \
  --output-dir "$REPORT_ROOT/agent_advisory" > "$REPORT_ROOT/agent_advisory.json"
NIM_ADVISORY_ARGS=(
  -m src.research.nim_advisory
  --duckdb "$DUCKDB_PATH"
  --output-dir "$REPORT_ROOT/nim_advisory"
)
if [[ -n "${NIM_ADVISORY_LIMIT:-}" ]]; then
  NIM_ADVISORY_ARGS+=(--limit "$NIM_ADVISORY_LIMIT")
fi
if [[ "${ENABLE_NIM_ADVISORY:-0}" == "1" || "${ENABLE_NIM_ADVISORY:-0}" == "true" ]]; then
  NIM_ADVISORY_ARGS+=(--enabled)
fi
set +e
PYTHONPATH=python-service python3 "${NIM_ADVISORY_ARGS[@]}" > "$REPORT_ROOT/nim_advisory.json"
NIM_ADVISORY_EXIT_CODE="$?"
set -e

python3 - "$REPORT_ROOT" "$MANIFEST_ROOT" <<'PY' > "$REPORT_ROOT/feature_research_decision.json"
import json
import os
import subprocess
import sys
from pathlib import Path

report_root = Path(sys.argv[1])
manifest_root = Path(sys.argv[2])
root_dir = Path.cwd()
sys.path.insert(0, str(root_dir / "python-service"))
index_path = manifest_root / "research_runs.jsonl"

previous_report_root = None
if index_path.exists():
    for line in index_path.read_text(encoding="utf-8").splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        value = item.get("report_root")
        if isinstance(value, str) and value and Path(value) != report_root:
            previous_report_root = Path(value)

if previous_report_root and previous_report_root.exists():
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "src.research.feature_research_decision",
            "--baseline-report-root",
            str(previous_report_root),
            "--candidate-report-root",
            str(report_root),
            "--json",
        ],
        check=True,
        capture_output=True,
        env={**os.environ, "PYTHONPATH": str(root_dir / "python-service")},
        text=True,
    )
    print(completed.stdout.strip())
else:
    from src.research.feature_research_decision import create_missing_baseline_report

    print(json.dumps(create_missing_baseline_report(report_root), indent=2, sort_keys=True))
PY

NIM_ADVISORY_EXIT_CODE="$NIM_ADVISORY_EXIT_CODE" python3 - "$REPORT_ROOT" <<'PY'
import json
import os
import sys
from pathlib import Path

root = Path(sys.argv[1])

def read_json(name: str) -> dict[str, object]:
    path = root / name
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))

backtest = read_json("backtest.json")
market_opportunity_selector = read_json("market_opportunity_selector.json")
execution_quality = read_json("execution_quality.json")
quote_execution_diagnostics = read_json("quote_execution_diagnostics.json")
candidate_market_ranking = read_json("candidate_market_ranking.json")
calibration = read_json("calibration.json")
near_touch_calibration = read_json("near_touch_calibration.json")
promotion = read_json("pre_live_promotion.json")
go_no_go = read_json("go_no_go.json")
advisory = read_json("agent_advisory.json")
signal_rejection_diagnostics = read_json("signal_rejection_diagnostics.json")
nim_advisory = read_json("nim_advisory.json")
nim_advisory_exit_code = int(os.environ.get("NIM_ADVISORY_EXIT_CODE", "0"))
synthetic_fills = read_json("synthetic_fills.json")
feature_research_decision = read_json("feature_research_decision.json")
profile_observation_comparison = read_json("profile_observation_comparison.json")
pre_live = backtest.get("pre_live_gate") if isinstance(backtest.get("pre_live_gate"), dict) else {}
advisory_summary = advisory.get("summary") if isinstance(advisory.get("summary"), dict) else {}
summary = {
    "report_root": str(root),
    "data_lake": read_json("data_lake_export.json"),
    "baseline": read_json("baseline.json"),
    "synthetic_fills": synthetic_fills,
    "backtest_exports": backtest.get("exports", {}),
    "game_theory_exports": read_json("game_theory.json"),
    "market_opportunity_selector": market_opportunity_selector,
    "execution_quality": execution_quality,
    "quote_execution_diagnostics": quote_execution_diagnostics,
    "candidate_market_ranking": candidate_market_ranking,
    "market_regime": read_json("market_regime.json"),
    "sentiment_features": read_json("sentiment_features.json"),
    "sentiment_lift": read_json("sentiment_lift.json"),
    "signal_rejection_diagnostics": signal_rejection_diagnostics,
    "feature_blocklist_candidates": read_json("feature_blocklist_candidates.json"),
    "near_touch_calibration": near_touch_calibration,
    "pre_live_gate_passed": pre_live.get("passed") if isinstance(pre_live, dict) else False,
    "calibration_passed": calibration.get("passed", False),
    "pre_live_promotion_passed": promotion.get("passed", False),
    "go_no_go_passed": go_no_go.get("passed", False),
    "agent_advisory_acceptable": advisory_summary.get("advisory_acceptable", False),
    "pre_live_promotion": promotion,
    "go_no_go": go_no_go,
    "agent_advisory": advisory,
    "nim_advisory": nim_advisory,
    "nim_advisory_exit_code": nim_advisory_exit_code,
    "feature_research_decision": feature_research_decision,
    "profile_observation_comparison": profile_observation_comparison,
}
summary["passed"] = bool(
    summary["pre_live_gate_passed"]
    and summary["calibration_passed"]
    and summary["pre_live_promotion_passed"]
    and summary["go_no_go_passed"]
    and summary["agent_advisory_acceptable"]
    and nim_advisory_exit_code == 0
)
(root / "research_summary.json").write_text(
    json.dumps(summary, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
(root / "research_exit_code.txt").write_text(
    "0\n" if summary["passed"] else f"{nim_advisory_exit_code or 2}\n",
    encoding="utf-8",
)
print(json.dumps(summary, indent=2, sort_keys=True))
PY

set +e
PYTHONPATH=python-service python3 -m src.research.pre_live_candidate_report \
  --report-root "$REPORT_ROOT" \
  --output "$REPORT_ROOT/pre_live_candidate_report.json" \
  > "$REPORT_ROOT/pre_live_candidate_report.stdout.json"
candidate_report_status=$?
set -e
if [[ "$candidate_report_status" != "0" && "$candidate_report_status" != "2" ]]; then
  exit "$candidate_report_status"
fi

STRATEGY_FAMILY_REPORT_ROOTS=("$REPORT_ROOT")
if [[ -n "${STRATEGY_FAMILY_COMPARISON_REPORT_ROOTS:-}" ]]; then
  IFS=',' read -r -a EXTRA_STRATEGY_FAMILY_REPORT_ROOTS <<< "$STRATEGY_FAMILY_COMPARISON_REPORT_ROOTS"
  STRATEGY_FAMILY_REPORT_ROOTS=("${EXTRA_STRATEGY_FAMILY_REPORT_ROOTS[@]}" "$REPORT_ROOT")
fi
STRATEGY_FAMILY_ARGS=()
for strategy_family_report_root in "${STRATEGY_FAMILY_REPORT_ROOTS[@]}"; do
  if [[ -n "$strategy_family_report_root" ]]; then
    STRATEGY_FAMILY_ARGS+=(--report-root "$strategy_family_report_root")
  fi
done
PYTHONPATH=python-service python3 -m src.research.strategy_family_comparison \
  "${STRATEGY_FAMILY_ARGS[@]}" \
  --output "$REPORT_ROOT/strategy_family_comparison.json"
PROFILE_OBSERVATION_REPORT_ROOTS=("$REPORT_ROOT")
if [[ -n "${PROFILE_OBSERVATION_COMPARISON_REPORT_ROOTS:-}" ]]; then
  IFS=',' read -r -a EXTRA_PROFILE_OBSERVATION_REPORT_ROOTS <<< "$PROFILE_OBSERVATION_COMPARISON_REPORT_ROOTS"
  PROFILE_OBSERVATION_REPORT_ROOTS=("${EXTRA_PROFILE_OBSERVATION_REPORT_ROOTS[@]}" "$REPORT_ROOT")
fi
PROFILE_OBSERVATION_ARGS=()
for profile_observation_report_root in "${PROFILE_OBSERVATION_REPORT_ROOTS[@]}"; do
  if [[ -n "$profile_observation_report_root" ]]; then
    PROFILE_OBSERVATION_ARGS+=(--report-root "$profile_observation_report_root")
  fi
done
PYTHONPATH=python-service python3 -m src.research.profile_observation_comparison \
  "${PROFILE_OBSERVATION_ARGS[@]}" \
  --output "$REPORT_ROOT/profile_observation_comparison.json" \
  > "$REPORT_ROOT/profile_observation_comparison.stdout.json"
PYTHONPATH=python-service python3 -m src.research.execution_probe_next_decision \
  --comparison "$REPORT_ROOT/profile_observation_comparison.json" \
  --output "$REPORT_ROOT/execution_probe_next_decision.json" \
  --json \
  > "$REPORT_ROOT/execution_probe_next_decision.stdout.json"
python3 - "$REPORT_ROOT" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
summary_path = root / "research_summary.json"
if summary_path.exists():
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["execution_probe_next_decision"] = json.loads(
        (root / "execution_probe_next_decision.json").read_text(encoding="utf-8")
    )
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
PY
if [[ -n "${SIGNAL_ACTIVITY_BASELINE_REPORT_ROOT:-}" ]]; then
  PYTHONPATH=python-service python3 -m src.research.signal_activity_audit \
    --baseline-report-root "$SIGNAL_ACTIVITY_BASELINE_REPORT_ROOT" \
    --candidate-report-root "$REPORT_ROOT" \
    --output "$REPORT_ROOT/signal_activity_audit.json"
fi

PYTHONPATH=python-service python3 -m src.research.run_manifest \
  --report-root "$REPORT_ROOT" \
  --manifest-root "$MANIFEST_ROOT" \
  --run-id "$REPORT_TIMESTAMP" \
  --source "${RESEARCH_RUN_SOURCE:-research_loop}" \
  > "$REPORT_ROOT/research_manifest.json"

RESEARCH_EXIT_CODE="$(tr -d '[:space:]' < "$REPORT_ROOT/research_exit_code.txt")"
if [[ "$RESEARCH_EXIT_CODE" != "0" && "$ALLOW_GATE_FAILURE" != "1" && "$ALLOW_GATE_FAILURE" != "true" ]]; then
  exit "$RESEARCH_EXIT_CODE"
fi
