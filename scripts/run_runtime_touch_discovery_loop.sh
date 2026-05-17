#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOOP_TIMESTAMP="${LOOP_TIMESTAMP:-runtime-touch-discovery-loop-$(date -u +%Y%m%dT%H%M%SZ)}"
RUN_ROOT="${RUN_ROOT:-${ROOT_DIR}/.tmp/operational/${LOOP_TIMESTAMP}}"
DISCOVERY_OUTPUT_DIR="${DISCOVERY_OUTPUT_DIR:-${RUN_ROOT}/runtime_touch_discovery_batches}"
BATCH_RESULTS_ROOT="${BATCH_RESULTS_ROOT:-${RUN_ROOT}/batches}"
COMPARISON_OUTPUT_DIR="${COMPARISON_OUTPUT_DIR:-${RUN_ROOT}/runtime_touch_discovery_batch_comparison}"
DISCOVERY_LIMIT="${DISCOVERY_LIMIT:-50}"
BATCH_SIZE="${DISCOVERY_BATCH_SIZE:-20}"
BATCH_CAPTURE_SECONDS="${BATCH_CAPTURE_SECONDS:-1800}"
MIN_ASSETS="${EXECUTION_PROBE_UNIVERSE_MIN_ASSETS:-2}"
PRINT_PLAN=0

usage() {
  cat <<'EOF'
Usage: scripts/run_runtime_touch_discovery_loop.sh [--print-plan] [--discovery-limit N] [--batch-size N] [--batch-capture-seconds N]

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
if [[ "${EXECUTION_MODE:-dry_run}" != "dry_run" ]]; then
  echo "Refusing to run: EXECUTION_MODE must be dry_run or unset." >&2
  exit 64
fi

DISCOVERY_BATCHES_JSON="${DISCOVERY_OUTPUT_DIR}/runtime_touch_discovery_batches.json"
COMPARISON_JSON="${COMPARISON_OUTPUT_DIR}/runtime_touch_discovery_batch_comparison.json"

if [[ "$PRINT_PLAN" == "1" ]]; then
  python3 - "$RUN_ROOT" "$DISCOVERY_OUTPUT_DIR" "$BATCH_RESULTS_ROOT" "$COMPARISON_OUTPUT_DIR" "$DISCOVERY_LIMIT" "$BATCH_SIZE" "$BATCH_CAPTURE_SECONDS" "$MIN_ASSETS" <<'PY'
import json
import sys

(
    run_root,
    discovery_output_dir,
    batch_results_root,
    comparison_output_dir,
    discovery_limit,
    batch_size,
    batch_capture_seconds,
    min_assets,
) = sys.argv[1:]
print(json.dumps({
    "script": "scripts/run_runtime_touch_discovery_loop.sh",
    "can_execute_trades": False,
    "execution_mode": "dry_run",
    "discovery_limit": int(discovery_limit),
    "batch_size": int(batch_size),
    "batch_capture_seconds": int(batch_capture_seconds),
    "min_assets": int(min_assets),
    "delegates_to": [
        "src.research.runtime_touch_discovery_batches",
        "scripts/run_runtime_touch_selection_probe.sh",
        "src.research.runtime_touch_discovery_batch_comparison",
    ],
    "outputs": {
        "run_root": run_root,
        "discovery_batches": f"{discovery_output_dir}/runtime_touch_discovery_batches.json",
        "batch_results_root": batch_results_root,
        "batch_comparison": f"{comparison_output_dir}/runtime_touch_discovery_batch_comparison.json",
    },
}, indent=2, sort_keys=True))
PY
  exit 0
fi

mkdir -p "$DISCOVERY_OUTPUT_DIR" "$BATCH_RESULTS_ROOT" "$COMPARISON_OUTPUT_DIR"

PYTHONPATH=python-service python3 -m src.research.runtime_touch_discovery_batches \
  --output-dir "$DISCOVERY_OUTPUT_DIR" \
  --discovery-limit "$DISCOVERY_LIMIT" \
  --batch-size "$BATCH_SIZE" \
  > "$RUN_ROOT/runtime_touch_discovery_batches.stdout.json"

while IFS=$'\t' read -r batch_id asset_ids_csv; do
  if [[ -z "$batch_id" || -z "$asset_ids_csv" ]]; then
    continue
  fi
  MARKET_ASSET_IDS="$asset_ids_csv" \
    PROBE_TIMESTAMP="${LOOP_TIMESTAMP}-${batch_id}" \
    "$ROOT_DIR/scripts/run_runtime_touch_selection_probe.sh" \
      --run-root "$BATCH_RESULTS_ROOT/$batch_id" \
      --fresh-capture-seconds "$BATCH_CAPTURE_SECONDS" \
      --min-assets "$MIN_ASSETS"
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
