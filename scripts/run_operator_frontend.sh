#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPERATOR_FRONTEND_HOST="${OPERATOR_FRONTEND_HOST:-127.0.0.1}"
OPERATOR_FRONTEND_PORT="${OPERATOR_FRONTEND_PORT:-5174}"

export OPERATOR_API_URL="${OPERATOR_API_URL:-http://127.0.0.1:18000}"

cd "${ROOT_DIR}/frontend"
exec npm run dev -- \
  --host "$OPERATOR_FRONTEND_HOST" \
  --port "$OPERATOR_FRONTEND_PORT" \
  --strictPort
