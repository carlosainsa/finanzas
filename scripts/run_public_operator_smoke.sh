#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "${ROOT_DIR}/.env.production" ]]; then
  set -a
  # shellcheck disable=SC1091
  . "${ROOT_DIR}/.env.production"
  set +a
fi

exec python3 "${ROOT_DIR}/scripts/public_operator_smoke.py" "$@"
