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
PRE_LIVE_PROMOTION_ARGS=()
if [[ -n "${PRE_LIVE_MIN_CAPTURE_DURATION_MS:-}" ]]; then
  PRE_LIVE_PROMOTION_ARGS+=(--min-capture-duration-ms "$PRE_LIVE_MIN_CAPTURE_DURATION_MS")
fi
if [[ -n "${PRE_LIVE_MIN_SIGNALS:-}" ]]; then
  PRE_LIVE_PROMOTION_ARGS+=(--min-signals "$PRE_LIVE_MIN_SIGNALS")
fi
if [[ -n "${PRE_LIVE_MIN_REALIZED_EDGE:-}" ]]; then
  PRE_LIVE_PROMOTION_ARGS+=(--min-realized-edge "$PRE_LIVE_MIN_REALIZED_EDGE")
fi
if [[ -n "${PRE_LIVE_MIN_FILL_RATE:-}" ]]; then
  PRE_LIVE_PROMOTION_ARGS+=(--min-fill-rate "$PRE_LIVE_MIN_FILL_RATE")
fi
if [[ -n "${PRE_LIVE_MIN_DRY_RUN_OBSERVED_FILL_RATE:-}" ]]; then
  PRE_LIVE_PROMOTION_ARGS+=(--min-dry-run-observed-fill-rate "$PRE_LIVE_MIN_DRY_RUN_OBSERVED_FILL_RATE")
fi
if [[ -n "${PRE_LIVE_MAX_ABS_SIMULATOR_FILL_RATE_DELTA:-}" ]]; then
  PRE_LIVE_PROMOTION_ARGS+=(--max-abs-simulator-fill-rate-delta "$PRE_LIVE_MAX_ABS_SIMULATOR_FILL_RATE_DELTA")
fi
if [[ -n "${PRE_LIVE_MAX_ABS_SLIPPAGE:-}" ]]; then
  PRE_LIVE_PROMOTION_ARGS+=(--max-abs-slippage "$PRE_LIVE_MAX_ABS_SLIPPAGE")
fi
if [[ -n "${PRE_LIVE_MAX_RECONCILIATION_DIVERGENCE_RATE:-}" ]]; then
  PRE_LIVE_PROMOTION_ARGS+=(--max-reconciliation-divergence-rate "$PRE_LIVE_MAX_RECONCILIATION_DIVERGENCE_RATE")
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
  --quote-placement "$BASELINE_QUOTE_PLACEMENT" \
  --near-touch-tick-size "$BASELINE_NEAR_TOUCH_TICK_SIZE" \
  --near-touch-offset-ticks "$BASELINE_NEAR_TOUCH_OFFSET_TICKS" \
  --near-touch-max-spread-fraction "$BASELINE_NEAR_TOUCH_MAX_SPREAD_FRACTION" \
  > "$REPORT_ROOT/baseline.json"
PYTHONPATH=python-service python3 -m src.research.synthetic_fills \
  --duckdb "$DUCKDB_PATH" \
  --output-dir "$REPORT_ROOT/synthetic_fills" > "$REPORT_ROOT/synthetic_fills.json"
PYTHONPATH=python-service python3 -m src.research.backtest \
  --duckdb "$DUCKDB_PATH" \
  --output-dir "$REPORT_ROOT/backtest" \
  --pre-live-gate > "$REPORT_ROOT/backtest.json"
PYTHONPATH=python-service python3 -m src.research.game_theory \
  --duckdb "$DUCKDB_PATH" \
  --output-dir "$REPORT_ROOT/game_theory" > "$REPORT_ROOT/game_theory.json"
PYTHONPATH=python-service python3 -m src.research.market_regime \
  --duckdb "$DUCKDB_PATH" \
  --output-dir "$REPORT_ROOT/market_regime" > "$REPORT_ROOT/market_regime.json"
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
PYTHONPATH=python-service python3 -m src.research.pre_live_promotion \
  --duckdb "$DUCKDB_PATH" \
  --output-dir "$REPORT_ROOT/pre_live_promotion" \
  "${PRE_LIVE_PROMOTION_ARGS[@]}" > "$REPORT_ROOT/pre_live_promotion.json"
PYTHONPATH=python-service python3 -m src.research.go_no_go \
  --duckdb "$DUCKDB_PATH" \
  --output-dir "$REPORT_ROOT/go_no_go" > "$REPORT_ROOT/go_no_go.json"
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
calibration = read_json("calibration.json")
promotion = read_json("pre_live_promotion.json")
go_no_go = read_json("go_no_go.json")
advisory = read_json("agent_advisory.json")
nim_advisory = read_json("nim_advisory.json")
nim_advisory_exit_code = int(os.environ.get("NIM_ADVISORY_EXIT_CODE", "0"))
synthetic_fills = read_json("synthetic_fills.json")
feature_research_decision = read_json("feature_research_decision.json")
pre_live = backtest.get("pre_live_gate") if isinstance(backtest.get("pre_live_gate"), dict) else {}
advisory_summary = advisory.get("summary") if isinstance(advisory.get("summary"), dict) else {}
summary = {
    "report_root": str(root),
    "data_lake": read_json("data_lake_export.json"),
    "baseline": read_json("baseline.json"),
    "synthetic_fills": synthetic_fills,
    "backtest_exports": backtest.get("exports", {}),
    "game_theory_exports": read_json("game_theory.json"),
    "market_regime": read_json("market_regime.json"),
    "sentiment_features": read_json("sentiment_features.json"),
    "sentiment_lift": read_json("sentiment_lift.json"),
    "feature_blocklist_candidates": read_json("feature_blocklist_candidates.json"),
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
