#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOOP_TIMESTAMP="${LOOP_TIMESTAMP:-runtime-touch-discovery-loop-$(date -u +%Y%m%dT%H%M%SZ)}"
RUN_ROOT="${RUN_ROOT:-${ROOT_DIR}/.tmp/operational/${LOOP_TIMESTAMP}}"
DISCOVERY_OUTPUT_DIR="${DISCOVERY_OUTPUT_DIR:-${RUN_ROOT}/runtime_touch_discovery_batches}"
BATCH_RESULTS_ROOT="${BATCH_RESULTS_ROOT:-${RUN_ROOT}/batches}"
COMPARISON_OUTPUT_DIR="${COMPARISON_OUTPUT_DIR:-${RUN_ROOT}/runtime_touch_discovery_batch_comparison}"
DIAGNOSTICS_OUTPUT_DIR="${DIAGNOSTICS_OUTPUT_DIR:-${RUN_ROOT}/runtime_touch_discovery_batch_diagnostics}"
FAMILY_MEMORY_UPDATE_OUTPUT_DIR="${FAMILY_MEMORY_UPDATE_OUTPUT_DIR:-${RUN_ROOT}/market_family_memory_update}"
DISCOVERY_LIMIT="${DISCOVERY_LIMIT:-50}"
BATCH_SIZE="${DISCOVERY_BATCH_SIZE:-20}"
BATCH_CAPTURE_SECONDS="${BATCH_CAPTURE_SECONDS:-1800}"
MIN_ASSETS="${EXECUTION_PROBE_UNIVERSE_MIN_ASSETS:-2}"
MAX_BATCHES="${DISCOVERY_MAX_BATCHES:-0}"
MARKET_FILLABILITY_SCORE_PATH="${MARKET_FILLABILITY_SCORE_PATH:-}"
MARKET_FAMILY_MEMORY_PATH="${MARKET_FAMILY_MEMORY_PATH:-}"
DISCOVERY_EXPLORATION_RATE="${DISCOVERY_EXPLORATION_RATE:-0.20}"
EARLY_STOP_ON_READY="${DISCOVERY_EARLY_STOP_ON_READY:-1}"
PRINT_PLAN=0

usage() {
  cat <<'EOF'
Usage: scripts/run_runtime_touch_discovery_loop.sh [--print-plan] [--discovery-limit N] [--batch-size N] [--batch-capture-seconds N] [--max-batches N] [--no-early-stop]

Runs the research-only discovery -> scout -> selection loop:
Gamma discovery batches -> per-batch runtime-touch selection probe -> batch comparison.
It keeps EXECUTION_MODE=dry_run, does not execute live trades, and does not modify risk.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --print-plan)
      PRINT_PLAN=1
      shift
      ;;
    --run-root)
      RUN_ROOT="$2"
      DISCOVERY_OUTPUT_DIR="$2/runtime_touch_discovery_batches"
      BATCH_RESULTS_ROOT="$2/batches"
      COMPARISON_OUTPUT_DIR="$2/runtime_touch_discovery_batch_comparison"
      DIAGNOSTICS_OUTPUT_DIR="$2/runtime_touch_discovery_batch_diagnostics"
      FAMILY_MEMORY_UPDATE_OUTPUT_DIR="$2/market_family_memory_update"
      shift 2
      ;;
    --discovery-limit)
      DISCOVERY_LIMIT="$2"
      shift 2
      ;;
    --batch-size)
      BATCH_SIZE="$2"
      shift 2
      ;;
    --batch-capture-seconds)
      BATCH_CAPTURE_SECONDS="$2"
      shift 2
      ;;
    --min-assets)
      MIN_ASSETS="$2"
      shift 2
      ;;
    --max-batches)
      MAX_BATCHES="$2"
      shift 2
      ;;
    --market-fillability-score)
      MARKET_FILLABILITY_SCORE_PATH="$2"
      shift 2
      ;;
    --market-family-memory)
      MARKET_FAMILY_MEMORY_PATH="$2"
      shift 2
      ;;
    --exploration-rate)
      DISCOVERY_EXPLORATION_RATE="$2"
      shift 2
      ;;
    --early-stop-on-ready)
      EARLY_STOP_ON_READY=1
      shift
      ;;
    --no-early-stop)
      EARLY_STOP_ON_READY=0
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

if ! [[ "$DISCOVERY_LIMIT" =~ ^[0-9]+$ ]] || (( DISCOVERY_LIMIT <= 0 )); then
  echo "discovery limit must be a positive integer" >&2
  exit 64
fi
if ! [[ "$BATCH_SIZE" =~ ^[0-9]+$ ]] || (( BATCH_SIZE < 2 )); then
  echo "batch size must be an integer >= 2" >&2
  exit 64
fi
if ! [[ "$BATCH_CAPTURE_SECONDS" =~ ^[0-9]+$ ]] || (( BATCH_CAPTURE_SECONDS < 1800 || BATCH_CAPTURE_SECONDS > 2700 )); then
  echo "batch capture duration must be an integer between 1800 and 2700 seconds" >&2
  exit 64
fi
if ! [[ "$MIN_ASSETS" =~ ^[0-9]+$ ]] || (( MIN_ASSETS < 2 )); then
  echo "min assets must be an integer >= 2" >&2
  exit 64
fi
if ! [[ "$MAX_BATCHES" =~ ^[0-9]+$ ]]; then
  echo "max batches must be a non-negative integer" >&2
  exit 64
fi
if [[ "${EXECUTION_MODE:-dry_run}" != "dry_run" ]]; then
  echo "Refusing to run: EXECUTION_MODE must be dry_run or unset." >&2
  exit 64
fi
python3 - "$DISCOVERY_EXPLORATION_RATE" <<'PY'
import sys
value = float(sys.argv[1])
if value < 0 or value > 1:
    raise SystemExit("exploration rate must be between 0 and 1")
PY
if [[ "$EARLY_STOP_ON_READY" != "0" && "$EARLY_STOP_ON_READY" != "1" ]]; then
  echo "early stop must be 0 or 1" >&2
  exit 64
fi

DISCOVERY_BATCHES_JSON="${DISCOVERY_OUTPUT_DIR}/runtime_touch_discovery_batches.json"
COMPARISON_JSON="${COMPARISON_OUTPUT_DIR}/runtime_touch_discovery_batch_comparison.json"
DIAGNOSTICS_JSON="${DIAGNOSTICS_OUTPUT_DIR}/runtime_touch_discovery_batch_diagnostics.json"
FAMILY_MEMORY_UPDATE_JSON="${FAMILY_MEMORY_UPDATE_OUTPUT_DIR}/market_family_memory.json"
EARLY_STOP_DECISION_JSON="${RUN_ROOT}/runtime_touch_early_stop_decision.json"
PROCESSED_BATCHES_FILE="${RUN_ROOT}/processed_discovery_batches.txt"

if [[ "$PRINT_PLAN" == "1" ]]; then
  python3 - "$RUN_ROOT" "$DISCOVERY_OUTPUT_DIR" "$BATCH_RESULTS_ROOT" "$COMPARISON_OUTPUT_DIR" "$DIAGNOSTICS_OUTPUT_DIR" "$FAMILY_MEMORY_UPDATE_OUTPUT_DIR" "$DISCOVERY_LIMIT" "$BATCH_SIZE" "$BATCH_CAPTURE_SECONDS" "$MIN_ASSETS" "$MAX_BATCHES" "$MARKET_FILLABILITY_SCORE_PATH" "$MARKET_FAMILY_MEMORY_PATH" "$DISCOVERY_EXPLORATION_RATE" "$EARLY_STOP_ON_READY" <<'PY'
import json
import sys

(
    run_root,
    discovery_output_dir,
    batch_results_root,
    comparison_output_dir,
    diagnostics_output_dir,
    family_memory_update_output_dir,
    discovery_limit,
    batch_size,
    batch_capture_seconds,
    min_assets,
    max_batches,
    market_fillability_score,
    market_family_memory,
    exploration_rate,
    early_stop_on_ready,
) = sys.argv[1:]
print(json.dumps({
    "script": "scripts/run_runtime_touch_discovery_loop.sh",
    "can_execute_trades": False,
    "execution_mode": "dry_run",
    "discovery_limit": int(discovery_limit),
    "batch_size": int(batch_size),
    "batch_capture_seconds": int(batch_capture_seconds),
    "min_assets": int(min_assets),
    "max_batches": int(max_batches),
    "market_fillability_score": market_fillability_score or None,
    "market_family_memory": market_family_memory or None,
    "exploration_rate": float(exploration_rate),
    "early_stop_on_ready": early_stop_on_ready == "1",
    "delegates_to": [
        "src.research.runtime_touch_discovery_batches",
        "scripts/run_runtime_touch_selection_probe.sh",
        "src.research.runtime_touch_discovery_loop_control",
        "src.research.runtime_touch_discovery_batch_comparison",
        "src.research.runtime_touch_discovery_batch_diagnostics",
        "src.research.market_family_memory",
    ],
    "outputs": {
        "run_root": run_root,
        "discovery_batches": f"{discovery_output_dir}/runtime_touch_discovery_batches.json",
        "batch_results_root": batch_results_root,
        "early_stop_decision": f"{run_root}/runtime_touch_early_stop_decision.json",
        "batch_comparison": f"{comparison_output_dir}/runtime_touch_discovery_batch_comparison.json",
        "batch_diagnostics": f"{diagnostics_output_dir}/runtime_touch_discovery_batch_diagnostics.json",
        "family_memory_update": f"{family_memory_update_output_dir}/market_family_memory.json",
        "loop_summary": f"{run_root}/runtime_touch_discovery_loop_summary.json",
    },
}, indent=2, sort_keys=True))
PY
  exit 0
fi

mkdir -p "$DISCOVERY_OUTPUT_DIR" "$BATCH_RESULTS_ROOT" "$COMPARISON_OUTPUT_DIR" "$DIAGNOSTICS_OUTPUT_DIR" "$FAMILY_MEMORY_UPDATE_OUTPUT_DIR"
: > "$PROCESSED_BATCHES_FILE"
cat > "$EARLY_STOP_DECISION_JSON" <<'JSON'
{
  "can_execute_trades": false,
  "decision_policy": "runtime_touch_discovery_loop_early_stop_control",
  "early_stop_triggered": false,
  "reason": "not_evaluated_yet",
  "report_version": "runtime_touch_discovery_loop_control_v1"
}
JSON

DISCOVERY_ARGS=(
  --output-dir "$DISCOVERY_OUTPUT_DIR"
  --discovery-limit "$DISCOVERY_LIMIT"
  --batch-size "$BATCH_SIZE"
  --exploration-rate "$DISCOVERY_EXPLORATION_RATE"
)
if [[ -n "$MARKET_FILLABILITY_SCORE_PATH" ]]; then
  DISCOVERY_ARGS+=(--market-fillability-score "$MARKET_FILLABILITY_SCORE_PATH")
fi
if [[ -n "$MARKET_FAMILY_MEMORY_PATH" ]]; then
  DISCOVERY_ARGS+=(--market-family-memory "$MARKET_FAMILY_MEMORY_PATH")
fi

PYTHONPATH=python-service python3 -m src.research.runtime_touch_discovery_batches \
  "${DISCOVERY_ARGS[@]}" \
  > "$RUN_ROOT/runtime_touch_discovery_batches.stdout.json"

processed_batches=0
while IFS=$'\t' read -r batch_id asset_ids_csv; do
  if [[ -z "$batch_id" || -z "$asset_ids_csv" ]]; then
    continue
  fi
  if (( MAX_BATCHES > 0 && processed_batches >= MAX_BATCHES )); then
    break
  fi
  MARKET_ASSET_IDS="$asset_ids_csv" \
    PROBE_TIMESTAMP="${LOOP_TIMESTAMP}-${batch_id}" \
    "$ROOT_DIR/scripts/run_runtime_touch_selection_probe.sh" \
      --run-root "$BATCH_RESULTS_ROOT/$batch_id" \
      --fresh-capture-seconds "$BATCH_CAPTURE_SECONDS" \
      --min-assets "$MIN_ASSETS"
  printf '%s\n' "$batch_id" >> "$PROCESSED_BATCHES_FILE"
  processed_batches=$((processed_batches + 1))
  PYTHONPATH=python-service python3 -m src.research.runtime_touch_discovery_loop_control \
    --batch-root "$BATCH_RESULTS_ROOT/$batch_id" \
    --output "$EARLY_STOP_DECISION_JSON" \
    --min-assets "$MIN_ASSETS" \
    > "$RUN_ROOT/runtime_touch_early_stop_decision.stdout.json"
  if [[ "$EARLY_STOP_ON_READY" == "1" ]]; then
    should_stop="$(python3 - "$EARLY_STOP_DECISION_JSON" <<'PY'
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
print("1" if payload.get("should_stop") is True else "0")
PY
)"
    if [[ "$should_stop" == "1" ]]; then
      break
    fi
  fi
done < <(
  python3 - "$DISCOVERY_BATCHES_JSON" <<'PY'
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
for batch in payload.get("batches", []):
    batch_id = str(batch.get("batch_id") or "").strip()
    csv = str(batch.get("market_asset_ids_csv") or "").strip()
    if batch_id and csv:
        print(f"{batch_id}\t{csv}")
PY
)

PYTHONPATH=python-service python3 -m src.research.runtime_touch_discovery_batch_comparison \
  --discovery-batches "$DISCOVERY_BATCHES_JSON" \
  --batch-results-root "$BATCH_RESULTS_ROOT" \
  --output-dir "$COMPARISON_OUTPUT_DIR" \
  --min-assets "$MIN_ASSETS" \
  > "$RUN_ROOT/runtime_touch_discovery_batch_comparison.stdout.json"

PYTHONPATH=python-service python3 -m src.research.runtime_touch_discovery_batch_diagnostics \
  --discovery-batches "$DISCOVERY_BATCHES_JSON" \
  --batch-results-root "$BATCH_RESULTS_ROOT" \
  --comparison "$COMPARISON_JSON" \
  --output-dir "$DIAGNOSTICS_OUTPUT_DIR" \
  --min-assets "$MIN_ASSETS" \
  > "$RUN_ROOT/runtime_touch_discovery_batch_diagnostics.stdout.json"

FAMILY_MEMORY_ARGS=(
  --output-dir "$FAMILY_MEMORY_UPDATE_OUTPUT_DIR"
  --discovery-batches "$DISCOVERY_BATCHES_JSON"
  --batch-comparison "$COMPARISON_JSON"
  --batch-diagnostics "$DIAGNOSTICS_JSON"
)
if [[ -n "$MARKET_FILLABILITY_SCORE_PATH" ]]; then
  FAMILY_MEMORY_ARGS+=(--fillability-score "$MARKET_FILLABILITY_SCORE_PATH")
fi
PYTHONPATH=python-service python3 -m src.research.market_family_memory \
  "${FAMILY_MEMORY_ARGS[@]}" \
  > "$RUN_ROOT/market_family_memory_update.stdout.json"

python3 - "$RUN_ROOT" "$DISCOVERY_BATCHES_JSON" "$COMPARISON_JSON" "$DIAGNOSTICS_JSON" "$FAMILY_MEMORY_UPDATE_JSON" "$EARLY_STOP_DECISION_JSON" "$PROCESSED_BATCHES_FILE" "$EARLY_STOP_ON_READY" "$MAX_BATCHES" <<'PY'
import json
import sys
from pathlib import Path

run_root = Path(sys.argv[1])
discovery_batches_path = Path(sys.argv[2])
comparison_path = Path(sys.argv[3])
diagnostics_path = Path(sys.argv[4])
family_memory_update_path = Path(sys.argv[5])
early_stop_path = Path(sys.argv[6])
processed_path = Path(sys.argv[7])
early_stop_enabled = sys.argv[8] == "1"
max_batches = int(sys.argv[9])

def read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}

discovery = read_json(discovery_batches_path)
comparison = read_json(comparison_path)
diagnostics = read_json(diagnostics_path)
family_memory_update = read_json(family_memory_update_path)
early_stop = read_json(early_stop_path)
processed = [
    line.strip()
    for line in processed_path.read_text(encoding="utf-8").splitlines()
    if line.strip()
] if processed_path.exists() else []
all_batch_ids = [
    str(batch.get("batch_id"))
    for batch in discovery.get("batches", [])
    if isinstance(batch, dict) and batch.get("batch_id")
]
summary = {
    "report_version": "runtime_touch_discovery_loop_summary_v1",
    "can_execute_trades": False,
    "decision_policy": "runtime_touch_discovery_loop_research_only",
    "early_stop_enabled": early_stop_enabled,
    "early_stop_triggered": early_stop_enabled and early_stop.get("should_stop") is True,
    "max_batches": max_batches,
    "max_batches_reached": max_batches > 0 and len(processed) >= max_batches,
    "early_stop_decision": early_stop,
    "processed_batch_ids": processed,
    "skipped_batch_ids": [batch_id for batch_id in all_batch_ids if batch_id not in set(processed)],
    "comparison_status": comparison.get("status"),
    "comparison_next_action": comparison.get("next_action"),
    "recommended_selector_adjustment": diagnostics.get("recommended_selector_adjustment"),
    "family_memory_update_counts": family_memory_update.get("counts"),
    "selected_batch": comparison.get("selected_batch"),
    "recommended_next_run": comparison.get("recommended_next_run"),
    "outputs": {
        "discovery_batches": str(discovery_batches_path),
        "early_stop_decision": str(early_stop_path),
        "batch_comparison": str(comparison_path),
        "batch_diagnostics": str(diagnostics_path),
        "family_memory_update": str(family_memory_update_path),
    },
}
(run_root / "runtime_touch_discovery_loop_summary.json").write_text(
    json.dumps(summary, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
print(json.dumps(summary, indent=2, sort_keys=True))
PY
