#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/.tmp/real-dry-run-research"
mkdir -p "$LOG_DIR"

export TEST_REDIS_PORT="${TEST_REDIS_PORT:-6382}"
export TEST_POSTGRES_PORT="${TEST_POSTGRES_PORT:-5434}"
export TEST_OPERATOR_API_PORT="${TEST_OPERATOR_API_PORT:-18001}"
export REDIS_URL="${REDIS_URL:-redis://127.0.0.1:${TEST_REDIS_PORT}}"
export DATABASE_URL="${DATABASE_URL:-postgres://finanzas:finanzas@127.0.0.1:${TEST_POSTGRES_PORT}/finanzas}"
export EXECUTION_MODE="${EXECUTION_MODE:-dry_run}"
export APP_ENV="${APP_ENV:-development}"
export REQUIRE_POSTGRES_STATE="${REQUIRE_POSTGRES_STATE:-true}"
export OPERATOR_READ_TOKEN="${OPERATOR_READ_TOKEN:-real-dry-run-read}"
export OPERATOR_CONTROL_TOKEN="${OPERATOR_CONTROL_TOKEN:-real-dry-run-control}"
export OPERATOR_API_URL="${OPERATOR_API_URL:-http://127.0.0.1:${TEST_OPERATOR_API_PORT}}"
export DISABLE_MARKET_WS="${DISABLE_MARKET_WS:-false}"
export ORDER_RECONCILIATION_TIMEOUT_MS="${ORDER_RECONCILIATION_TIMEOUT_MS:-1000}"
export REAL_DRY_RUN_SECONDS="${REAL_DRY_RUN_SECONDS:-900}"
export DISCOVERY_LIMIT="${DISCOVERY_LIMIT:-25}"
export DISCOVERY_MIN_LIQUIDITY="${DISCOVERY_MIN_LIQUIDITY:-100}"
export DISCOVERY_MIN_VOLUME="${DISCOVERY_MIN_VOLUME:-100}"
export PREDICTOR_MIN_SPREAD="${PREDICTOR_MIN_SPREAD:-0.001}"
export PREDICTOR_MIN_CONFIDENCE="${PREDICTOR_MIN_CONFIDENCE:-0.50}"
export DATA_LAKE_EXPORT_COUNT="${DATA_LAKE_EXPORT_COUNT:-50000}"
export ALLOW_RESEARCH_GATE_FAILURE="${ALLOW_RESEARCH_GATE_FAILURE:-1}"
export RESEARCH_RUN_SOURCE="${RESEARCH_RUN_SOURCE:-real_market_dry_run}"
export REPORT_TIMESTAMP="${REPORT_TIMESTAMP:-real-dry-run-$(date -u +%Y%m%dT%H%M%SZ)}"
REAL_DRY_RUN_ISOLATED="${REAL_DRY_RUN_ISOLATED:-${ISOLATED_REAL_DRY_RUN:-1}}"
if [[ "$REAL_DRY_RUN_ISOLATED" == "1" || "$REAL_DRY_RUN_ISOLATED" == "true" ]]; then
  export DATA_LAKE_ROOT="${DATA_LAKE_ROOT:-${ROOT_DIR}/.tmp/real-dry-run-data-lake/${REPORT_TIMESTAMP}}"
fi
export DATA_LAKE_ROOT="${DATA_LAKE_ROOT:-${ROOT_DIR}/data_lake}"
export DATA_LAKE_DUCKDB="${DATA_LAKE_DUCKDB:-${DATA_LAKE_ROOT}/research.duckdb}"
if [[ "$REAL_DRY_RUN_ISOLATED" == "1" || "$REAL_DRY_RUN_ISOLATED" == "true" ]]; then
  if [[ -e "$DATA_LAKE_ROOT" && "${REAL_DRY_RUN_ALLOW_EXISTING_ROOT:-0}" != "1" ]]; then
    echo "Refusing to reuse isolated DATA_LAKE_ROOT=$DATA_LAKE_ROOT. Set REAL_DRY_RUN_ALLOW_EXISTING_ROOT=1 to override." >&2
    exit 64
  fi
  export RESEARCH_REPORT_ROOT="${RESEARCH_REPORT_ROOT:-${DATA_LAKE_ROOT}/reports/${REPORT_TIMESTAMP}}"
  export RESEARCH_MANIFEST_ROOT="${RESEARCH_MANIFEST_ROOT:-${DATA_LAKE_ROOT}/research_runs}"
else
  export RESEARCH_REPORT_ROOT="${RESEARCH_REPORT_ROOT:-${DATA_LAKE_ROOT}/reports/${REPORT_TIMESTAMP}}"
fi
export RUST_LOG="${RUST_LOG:-info}"

if [[ "$EXECUTION_MODE" != "dry_run" ]]; then
  echo "Refusing to run: EXECUTION_MODE must be dry_run for real market dry-run research." >&2
  exit 64
fi
if [[ "$DISABLE_MARKET_WS" == "1" || "$DISABLE_MARKET_WS" == "true" ]]; then
  echo "Refusing to run: DISABLE_MARKET_WS must be false to collect real market data." >&2
  exit 64
fi

cat <<EOF
real_dry_run_start
run_id=$REPORT_TIMESTAMP
isolated=$REAL_DRY_RUN_ISOLATED
data_lake_root=$DATA_LAKE_ROOT
duckdb=$DATA_LAKE_DUCKDB
report_root=$RESEARCH_REPORT_ROOT
manifest_root=${RESEARCH_MANIFEST_ROOT:-$DATA_LAKE_ROOT/research_runs}
redis_url=$REDIS_URL
capture_seconds=$REAL_DRY_RUN_SECONDS
EOF

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait "${pids[@]:-}" 2>/dev/null || true
  if [[ "${KEEP_REAL_DRY_RUN_SERVICES:-0}" != "1" ]]; then
    docker compose -f "$ROOT_DIR/docker-compose.test.yml" down --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

cd "$ROOT_DIR"
docker compose -f docker-compose.test.yml up -d --wait

if [[ -z "${MARKET_ASSET_IDS:-}" ]]; then
  MARKET_ASSET_IDS="$(
    PYTHONPATH=python-service python3 - <<'PY'
import asyncio
import os

from src.discovery.markets import discover_markets

async def main() -> None:
    markets = await discover_markets(
        limit=int(os.getenv("DISCOVERY_LIMIT", "25")),
        min_liquidity=float(os.getenv("DISCOVERY_MIN_LIQUIDITY", "100")),
        min_volume=float(os.getenv("DISCOVERY_MIN_VOLUME", "100")),
    )
    token_ids: list[str] = []
    for scored in markets:
        for token_id in scored.market.clob_token_ids:
            if token_id not in token_ids:
                token_ids.append(token_id)
            if len(token_ids) >= 20:
                print(",".join(token_ids))
                return
    print(",".join(token_ids))

asyncio.run(main())
PY
  )"
  export MARKET_ASSET_IDS
fi

PYTHONPATH=python-service python3 - <<'PY'
import os

ids = [item for item in os.getenv("MARKET_ASSET_IDS", "").split(",") if item.strip()]
if len(ids) < 2:
    raise SystemExit("MARKET_ASSET_IDS must contain at least two token IDs")
PY

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

sleep "$REAL_DRY_RUN_SECONDS"

PYTHONPATH=python-service python3 - <<'PY'
import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import redis.asyncio as redis

async def main() -> None:
    client = redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    streams = {
        "orderbook": os.getenv("ORDERBOOK_STREAM", "orderbook:stream"),
        "signals": os.getenv("SIGNALS_STREAM", "signals:stream"),
        "reports": os.getenv("EXECUTION_REPORTS_STREAM", "execution:reports:stream"),
    }
    lengths = {name: await client.xlen(stream) for name, stream in streams.items()}
    if any(length <= 0 for length in lengths.values()):
        raise SystemExit(f"missing real dry-run stream data: {lengths}")
    reports = await client.xrevrange(streams["reports"], count=100)
    parsed = []
    for _, fields in reports:
        payload = fields.get("payload")
        if payload:
            value = json.loads(payload)
            if isinstance(value, dict):
                parsed.append(value)
    if not any(str(item.get("order_id", "")).startswith("dry-run-") for item in parsed):
        raise SystemExit("no dry-run execution report found")
    if not any(item.get("status") in {"DELAYED", "UNMATCHED", "MATCHED", "PARTIAL"} for item in parsed):
        raise SystemExit("no valid dry-run report status found")
    status_counts: dict[str, int] = {}
    for item in parsed:
        status = str(item.get("status", "UNKNOWN"))
        status_counts[status] = status_counts.get(status, 0) + 1
    evidence = {
        "status": "ok",
        "run_id": os.environ["REPORT_TIMESTAMP"],
        "created_at": datetime.now(timezone.utc).isoformat(),
        "execution_mode": os.environ["EXECUTION_MODE"],
        "disable_market_ws": os.environ["DISABLE_MARKET_WS"],
        "capture_seconds": int(os.environ["REAL_DRY_RUN_SECONDS"]),
        "market_asset_ids_count": len([item for item in os.environ.get("MARKET_ASSET_IDS", "").split(",") if item.strip()]),
        "stream_lengths": lengths,
        "recent_report_status_counts": status_counts,
        "data_lake_root": os.environ["DATA_LAKE_ROOT"],
        "research_report_root": os.environ["RESEARCH_REPORT_ROOT"],
        "research_manifest_root": os.environ.get("RESEARCH_MANIFEST_ROOT"),
    }
    report_root = Path(os.environ["RESEARCH_REPORT_ROOT"])
    report_root.mkdir(parents=True, exist_ok=True)
    (report_root / "real_dry_run_evidence.json").write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(evidence, sort_keys=True))

asyncio.run(main())
PY

set +e
scripts/run_research_loop.sh
research_status=$?
set -e

research_exit_code="0"
if [[ -f "$RESEARCH_REPORT_ROOT/research_exit_code.txt" ]]; then
  research_exit_code="$(tr -d '[:space:]' < "$RESEARCH_REPORT_ROOT/research_exit_code.txt")"
fi

if [[ "$research_status" == "0" ]]; then
  if [[ "$research_exit_code" == "2" ]]; then
    echo "Research infra succeeded but promotion gates failed; inspect $RESEARCH_REPORT_ROOT/research_summary.json." >&2
    exit 20
  fi
  exit 0
fi
if [[ "$research_status" == "2" ]]; then
  echo "Research infra succeeded but promotion gates failed; inspect the generated research_summary.json." >&2
  exit 20
fi
exit "$research_status"
