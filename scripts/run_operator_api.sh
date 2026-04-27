#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPERATOR_API_HOST="${OPERATOR_API_HOST:-127.0.0.1}"
OPERATOR_API_PORT="${OPERATOR_API_PORT:-18000}"

export PYTHONPATH="${ROOT_DIR}/python-service${PYTHONPATH:+:${PYTHONPATH}}"
export OPERATOR_API_URL="${OPERATOR_API_URL:-http://127.0.0.1:${OPERATOR_API_PORT}}"

cd "$ROOT_DIR"
exec python3 -m uvicorn src.api.app:app \
  --app-dir python-service \
  --host "$OPERATOR_API_HOST" \
  --port "$OPERATOR_API_PORT"
