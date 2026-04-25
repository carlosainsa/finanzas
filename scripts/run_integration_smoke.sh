#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/.tmp/integration-smoke"
mkdir -p "$LOG_DIR"

export TEST_REDIS_PORT="${TEST_REDIS_PORT:-6381}"
export TEST_POSTGRES_PORT="${TEST_POSTGRES_PORT:-5433}"
export TEST_OPERATOR_API_PORT="${TEST_OPERATOR_API_PORT:-18000}"
export REDIS_URL="${REDIS_URL:-redis://127.0.0.1:${TEST_REDIS_PORT}}"
export DATABASE_URL="${DATABASE_URL:-postgres://finanzas:finanzas@127.0.0.1:${TEST_POSTGRES_PORT}/finanzas}"
export EXECUTION_MODE="${EXECUTION_MODE:-dry_run}"
export APP_ENV="${APP_ENV:-development}"
export REQUIRE_POSTGRES_STATE="${REQUIRE_POSTGRES_STATE:-true}"
export OPERATOR_READ_TOKEN="${OPERATOR_READ_TOKEN:-smoke-read}"
export OPERATOR_CONTROL_TOKEN="${OPERATOR_CONTROL_TOKEN:-smoke-control}"
export OPERATOR_API_URL="${OPERATOR_API_URL:-http://127.0.0.1:${TEST_OPERATOR_API_PORT}}"
export DISABLE_MARKET_WS=true
export ORDER_RECONCILIATION_TIMEOUT_MS="${ORDER_RECONCILIATION_TIMEOUT_MS:-1000}"
export RUST_LOG="${RUST_LOG:-info}"

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait "${pids[@]:-}" 2>/dev/null || true
  if [[ "${KEEP_INTEGRATION_SERVICES:-0}" != "1" ]]; then
    docker compose -f "$ROOT_DIR/docker-compose.test.yml" down --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

cd "$ROOT_DIR"
docker compose -f docker-compose.test.yml up -d --wait

(
  cd "$ROOT_DIR/rust-engine"
  cargo run
) >"$LOG_DIR/rust-engine.log" 2>&1 &
pids+=("$!")

PYTHONPATH=python-service python3 -m src.data.consumer \
  >"$LOG_DIR/consumer.log" 2>&1 &
pids+=("$!")

PYTHONPATH=python-service uvicorn src.api.app:app \
  --app-dir python-service \
  --host 127.0.0.1 \
  --port "$TEST_OPERATOR_API_PORT" \
  >"$LOG_DIR/api.log" 2>&1 &
pids+=("$!")

PYTHONPATH=python-service python3 - <<'PY'
import os
import time

import httpx

deadline = time.monotonic() + 30
headers = {"Authorization": f"Bearer {os.environ['OPERATOR_READ_TOKEN']}"}
while time.monotonic() < deadline:
    try:
        response = httpx.get(f"{os.environ['OPERATOR_API_URL']}/status", headers=headers, timeout=1.0)
        if response.status_code == 200:
            raise SystemExit(0)
    except Exception:
        pass
    time.sleep(0.5)
raise SystemExit("operator API did not become ready")
PY

PYTHONPATH=python-service python3 - <<'PY'
import asyncio
import os
import time

import redis.asyncio as redis

async def main() -> None:
    client = redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    targets = {
        os.getenv("ORDERBOOK_STREAM", "orderbook:stream"): os.getenv("ORDERBOOK_CONSUMER_GROUP", "python-predictor"),
        os.getenv("SIGNALS_STREAM", "signals:stream"): os.getenv("EXECUTOR_CONSUMER_GROUP", "rust-executor"),
    }
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        ready = True
        for stream, group in targets.items():
            try:
                groups = await client.xinfo_groups(stream)
            except Exception:
                ready = False
                break
            if not any(item.get("name") == group for item in groups):
                ready = False
                break
        if ready:
            return
        await asyncio.sleep(0.5)
    raise SystemExit("Redis stream consumer groups did not become ready")

asyncio.run(main())
PY

PYTHONPATH=python-service python3 scripts/integration_smoke.py
