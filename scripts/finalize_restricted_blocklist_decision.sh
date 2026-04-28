#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ $# -lt 2 || $# -gt 4 ]]; then
  echo "Usage: scripts/finalize_restricted_blocklist_decision.sh --observation-root PATH [--output PATH]" >&2
  exit 64
fi

OBSERVATION_ROOT=""
OUTPUT=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --observation-root)
      OBSERVATION_ROOT="${2:-}"
      shift 2
      ;;
    --output)
      OUTPUT="${2:-}"
      shift 2
      ;;
    -h|--help)
      echo "Usage: scripts/finalize_restricted_blocklist_decision.sh --observation-root PATH [--output PATH]"
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 64
      ;;
  esac
done

if [[ -z "$OBSERVATION_ROOT" || ! -d "$OBSERVATION_ROOT" ]]; then
  echo "observation root does not exist: $OBSERVATION_ROOT" >&2
  exit 64
fi

ARGS=(
  -m src.research.restricted_blocklist_decision
  --observation-root "$OBSERVATION_ROOT"
  --json
)
if [[ -n "$OUTPUT" ]]; then
  ARGS+=(--output "$OUTPUT")
fi

PYTHONPATH="$ROOT_DIR/python-service" python3 "${ARGS[@]}"
