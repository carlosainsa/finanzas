#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPORT_ROOT=""
OUTPUT_DIR=""
ASSET_ARGS=()

usage() {
  cat <<'EOF'
Usage: scripts/analyze_at_touch_assets.sh --report-root PATH [--output-dir PATH] [--asset-id ASSET_ID ...]

Exports a research-only at-touch diagnostic for assets in an existing dry-run
report root. The output is advisory and cannot enable live trading.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --report-root)
      REPORT_ROOT="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --asset-id)
      ASSET_ARGS+=(--asset-id "$2")
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

if [[ -z "$REPORT_ROOT" || ! -d "$REPORT_ROOT" ]]; then
  echo "--report-root must point to an existing report directory" >&2
  exit 64
fi

ARGS=(
  -m src.research.at_touch_asset_diagnostic
  --report-root "$REPORT_ROOT"
)
if [[ -n "$OUTPUT_DIR" ]]; then
  ARGS+=(--output-dir "$OUTPUT_DIR")
fi
ARGS+=("${ASSET_ARGS[@]}")

cd "$ROOT_DIR"
PYTHONPATH=python-service python3 "${ARGS[@]}"
