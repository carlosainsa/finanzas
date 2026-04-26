#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_LAKE_ROOT="${DATA_LAKE_ROOT:-$ROOT_DIR/data_lake}"
DUCKDB_PATH="${DATA_LAKE_DUCKDB:-$DATA_LAKE_ROOT/research.duckdb}"
EXPORT_COUNT="${DATA_LAKE_EXPORT_COUNT:-1000}"
TRAIN_FRACTION="${RESEARCH_TRAIN_FRACTION:-0.70}"
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
  --output-dir "$REPORT_ROOT/baseline" > "$REPORT_ROOT/baseline.json"
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
PYTHONPATH=python-service python3 -m src.research.calibration \
  --duckdb "$DUCKDB_PATH" \
  --output-dir "$REPORT_ROOT/calibration" \
  --train-fraction "$TRAIN_FRACTION" > "$REPORT_ROOT/calibration.json"
PYTHONPATH=python-service python3 -m src.research.pre_live_promotion \
  --duckdb "$DUCKDB_PATH" \
  --output-dir "$REPORT_ROOT/pre_live_promotion" > "$REPORT_ROOT/pre_live_promotion.json"
PYTHONPATH=python-service python3 -m src.research.agent_advisory \
  --duckdb "$DUCKDB_PATH" \
  --output-dir "$REPORT_ROOT/agent_advisory" > "$REPORT_ROOT/agent_advisory.json"

python3 - "$REPORT_ROOT" <<'PY'
import json
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
advisory = read_json("agent_advisory.json")
synthetic_fills = read_json("synthetic_fills.json")
pre_live = backtest.get("pre_live_gate") if isinstance(backtest.get("pre_live_gate"), dict) else {}
advisory_summary = advisory.get("summary") if isinstance(advisory.get("summary"), dict) else {}
summary = {
    "report_root": str(root),
    "data_lake": read_json("data_lake_export.json"),
    "baseline": read_json("baseline.json"),
    "synthetic_fills": synthetic_fills,
    "backtest_exports": backtest.get("exports", {}),
    "game_theory_exports": read_json("game_theory.json"),
    "pre_live_gate_passed": pre_live.get("passed") if isinstance(pre_live, dict) else False,
    "calibration_passed": calibration.get("passed", False),
    "pre_live_promotion_passed": promotion.get("passed", False),
    "agent_advisory_acceptable": advisory_summary.get("advisory_acceptable", False),
    "pre_live_promotion": promotion,
    "agent_advisory": advisory,
}
summary["passed"] = bool(
    summary["pre_live_gate_passed"]
    and summary["calibration_passed"]
    and summary["pre_live_promotion_passed"]
    and summary["agent_advisory_acceptable"]
)
(root / "research_summary.json").write_text(
    json.dumps(summary, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
(root / "research_exit_code.txt").write_text(
    "0\n" if summary["passed"] else "2\n",
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
