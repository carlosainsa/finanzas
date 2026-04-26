#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_LAKE_ROOT="${DATA_LAKE_ROOT:-$ROOT_DIR/data_lake}"
DUCKDB_PATH="${DATA_LAKE_DUCKDB:-$DATA_LAKE_ROOT/research.duckdb}"
EXPORT_COUNT="${DATA_LAKE_EXPORT_COUNT:-1000}"
TRAIN_FRACTION="${RESEARCH_TRAIN_FRACTION:-0.70}"
REPORT_TIMESTAMP="${REPORT_TIMESTAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"
REPORT_ROOT="${RESEARCH_REPORT_ROOT:-$DATA_LAKE_ROOT/reports/$REPORT_TIMESTAMP}"
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
pre_live = backtest.get("pre_live_gate") if isinstance(backtest.get("pre_live_gate"), dict) else {}
summary = {
    "report_root": str(root),
    "data_lake": read_json("data_lake_export.json"),
    "baseline": read_json("baseline.json"),
    "backtest_exports": backtest.get("exports", {}),
    "game_theory_exports": read_json("game_theory.json"),
    "pre_live_gate_passed": pre_live.get("passed") if isinstance(pre_live, dict) else False,
    "calibration_passed": calibration.get("passed", False),
}
summary["passed"] = bool(summary["pre_live_gate_passed"] and summary["calibration_passed"])
(root / "research_summary.json").write_text(
    json.dumps(summary, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
print(json.dumps(summary, indent=2, sort_keys=True))
raise SystemExit(0 if summary["passed"] else 2)
PY
