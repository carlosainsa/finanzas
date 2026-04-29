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
export REAL_DRY_RUN_SECONDS="${REAL_DRY_RUN_SECONDS:-3600}"
export DISCOVERY_LIMIT="${DISCOVERY_LIMIT:-25}"
export DISCOVERY_MIN_LIQUIDITY="${DISCOVERY_MIN_LIQUIDITY:-100}"
export DISCOVERY_MIN_VOLUME="${DISCOVERY_MIN_VOLUME:-100}"
export PREDICTOR_MIN_SPREAD="${PREDICTOR_MIN_SPREAD:-0.001}"
export PREDICTOR_MIN_CONFIDENCE="${PREDICTOR_MIN_CONFIDENCE:-0.50}"
export MIN_CONFIDENCE="${MIN_CONFIDENCE:-$PREDICTOR_MIN_CONFIDENCE}"
export PREDICTOR_QUOTE_PLACEMENT="${PREDICTOR_QUOTE_PLACEMENT:-near_touch}"
export PREDICTOR_NEAR_TOUCH_RESEARCH_ONLY="${PREDICTOR_NEAR_TOUCH_RESEARCH_ONLY:-true}"
export PREDICTOR_NEAR_TOUCH_TICK_SIZE="${PREDICTOR_NEAR_TOUCH_TICK_SIZE:-0.01}"
export PREDICTOR_NEAR_TOUCH_OFFSET_TICKS="${PREDICTOR_NEAR_TOUCH_OFFSET_TICKS:-0}"
export PREDICTOR_NEAR_TOUCH_MAX_SPREAD_FRACTION="${PREDICTOR_NEAR_TOUCH_MAX_SPREAD_FRACTION:-1.0}"
export DATA_LAKE_EXPORT_COUNT="${DATA_LAKE_EXPORT_COUNT:-50000}"
export RESEARCH_RESOURCE_MODE="${RESEARCH_RESOURCE_MODE:-resource_limited}"
export MARKET_REGIME_MAX_SNAPSHOTS_PER_ASSET="${MARKET_REGIME_MAX_SNAPSHOTS_PER_ASSET:-250}"
export MARKET_REGIME_MAX_TRADE_CONTEXT_ROWS="${MARKET_REGIME_MAX_TRADE_CONTEXT_ROWS:-2000}"
export MARKET_REGIME_DUCKDB_THREADS="${MARKET_REGIME_DUCKDB_THREADS:-2}"
export GO_NO_GO_PROFILE="${GO_NO_GO_PROFILE:-pre_live}"
export PRE_LIVE_MIN_CAPTURE_DURATION_MS="${PRE_LIVE_MIN_CAPTURE_DURATION_MS:-$((REAL_DRY_RUN_SECONDS * 1000))}"
export PRE_LIVE_MIN_SIGNALS="${PRE_LIVE_MIN_SIGNALS:-250}"
export PRE_LIVE_MIN_DRY_RUN_OBSERVED_FILL_RATE="${PRE_LIVE_MIN_DRY_RUN_OBSERVED_FILL_RATE:-0.01}"
export PRE_LIVE_MAX_ABS_SIMULATOR_FILL_RATE_DELTA="${PRE_LIVE_MAX_ABS_SIMULATOR_FILL_RATE_DELTA:-0.75}"
export ALLOW_RESEARCH_GATE_FAILURE="${ALLOW_RESEARCH_GATE_FAILURE:-1}"
export RESEARCH_RUN_SOURCE="${RESEARCH_RUN_SOURCE:-real_market_dry_run}"
export REPORT_TIMESTAMP="${REPORT_TIMESTAMP:-real-dry-run-$(date -u +%Y%m%dT%H%M%SZ)}"
export REAL_DRY_RUN_PREFLIGHT_ENABLED="${REAL_DRY_RUN_PREFLIGHT_ENABLED:-1}"
export REAL_DRY_RUN_PREFLIGHT_SECONDS="${REAL_DRY_RUN_PREFLIGHT_SECONDS:-120}"
export REAL_DRY_RUN_PREFLIGHT_POLL_SECONDS="${REAL_DRY_RUN_PREFLIGHT_POLL_SECONDS:-5}"
export REAL_DRY_RUN_PREFLIGHT_REQUIRE_REPORTS="${REAL_DRY_RUN_PREFLIGHT_REQUIRE_REPORTS:-false}"
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
if [[ -n "${PREDICTOR_BLOCKED_SEGMENTS_PATH:-}" && ! -f "$PREDICTOR_BLOCKED_SEGMENTS_PATH" ]]; then
  echo "Refusing to run: PREDICTOR_BLOCKED_SEGMENTS_PATH does not exist: $PREDICTOR_BLOCKED_SEGMENTS_PATH" >&2
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
go_no_go_profile=$GO_NO_GO_PROFILE
redis_url=$REDIS_URL
capture_seconds=$REAL_DRY_RUN_SECONDS
preflight_enabled=$REAL_DRY_RUN_PREFLIGHT_ENABLED
preflight_seconds=$REAL_DRY_RUN_PREFLIGHT_SECONDS
preflight_require_reports=$REAL_DRY_RUN_PREFLIGHT_REQUIRE_REPORTS
blocked_segments_path=${PREDICTOR_BLOCKED_SEGMENTS_PATH:-}
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

tail_service_logs() {
  for name in rust-engine consumer api; do
    log_path="$LOG_DIR/${name}.log"
    if [[ -f "$log_path" ]]; then
      echo "--- ${name}.log tail ---" >&2
      tail -n 80 "$log_path" >&2 || true
    fi
  done
}

assert_services_running() {
  local pid
  for pid in "${pids[@]:-}"; do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "Service process exited before dry-run capture completed: pid=$pid" >&2
      tail_service_logs
      exit 70
    fi
  done
}

capture_with_service_monitoring() {
  local elapsed="${REAL_DRY_RUN_PREFLIGHT_ELAPSED_SECONDS:-0}"
  local remaining=$((REAL_DRY_RUN_SECONDS - elapsed))
  if (( remaining < 0 )); then
    remaining=0
  fi
  local interval
  while (( remaining > 0 )); do
    assert_services_running
    interval=5
    if (( remaining < interval )); then
      interval="$remaining"
    fi
    sleep "$interval"
    remaining=$((remaining - interval))
  done
  assert_services_running
}

run_preflight_with_service_monitoring() {
  if [[ "$REAL_DRY_RUN_PREFLIGHT_ENABLED" != "1" && "$REAL_DRY_RUN_PREFLIGHT_ENABLED" != "true" ]]; then
    REAL_DRY_RUN_PREFLIGHT_ELAPSED_SECONDS=0
    return 0
  fi
  local seconds="$REAL_DRY_RUN_PREFLIGHT_SECONDS"
  if (( seconds <= 0 )); then
    REAL_DRY_RUN_PREFLIGHT_ELAPSED_SECONDS=0
    return 0
  fi
  if (( seconds > REAL_DRY_RUN_SECONDS )); then
    seconds="$REAL_DRY_RUN_SECONDS"
  fi
  local require_reports_flag=()
  if [[ "$REAL_DRY_RUN_PREFLIGHT_REQUIRE_REPORTS" == "1" || "$REAL_DRY_RUN_PREFLIGHT_REQUIRE_REPORTS" == "true" ]]; then
    require_reports_flag=(--require-reports)
  fi
  set +e
  PYTHONPATH=python-service python3 -m src.research.real_dry_run_preflight \
    --redis-url "$REDIS_URL" \
    --output "$RESEARCH_REPORT_ROOT/real_dry_run_preflight.json" \
    --check-seconds "$seconds" \
    --poll-seconds "$REAL_DRY_RUN_PREFLIGHT_POLL_SECONDS" \
    --capture-seconds "$REAL_DRY_RUN_SECONDS" \
    "${require_reports_flag[@]}" \
    --json
  preflight_status=$?
  set -e
  if [[ "$preflight_status" != "0" ]]; then
    echo "Real dry-run preflight failed; inspect $RESEARCH_REPORT_ROOT/real_dry_run_preflight.json." >&2
    tail_service_logs
    exit "$preflight_status"
  fi
  REAL_DRY_RUN_PREFLIGHT_ELAPSED_SECONDS="$(
    python3 - "$RESEARCH_REPORT_ROOT/real_dry_run_preflight.json" <<'PY'
import json
import math
import sys

try:
    payload = json.loads(open(sys.argv[1], encoding="utf-8").read())
    print(max(0, math.ceil(float(payload.get("elapsed_seconds", 0)))))
except Exception:
    print(0)
PY
  )"
}

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

run_preflight_with_service_monitoring
capture_with_service_monitoring

PYTHONPATH=python-service python3 - <<'PY'
import asyncio
import hashlib
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
    reports = []
    next_max = "+"
    while True:
        batch = await client.xrevrange(
            streams["reports"],
            max=next_max,
            min="-",
            count=1000,
        )
        if not batch:
            break
        reports.extend(batch)
        last_id = batch[-1][0]
        next_max = f"({last_id}"
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
    market_asset_ids = [
        item.strip()
        for item in os.environ.get("MARKET_ASSET_IDS", "").split(",")
        if item.strip()
    ]
    market_asset_ids_csv = ",".join(market_asset_ids)
    evidence = {
        "status": "ok",
        "run_id": os.environ["REPORT_TIMESTAMP"],
        "created_at": datetime.now(timezone.utc).isoformat(),
        "execution_mode": os.environ["EXECUTION_MODE"],
        "go_no_go_profile": os.environ["GO_NO_GO_PROFILE"],
        "disable_market_ws": os.environ["DISABLE_MARKET_WS"],
        "capture_seconds": int(os.environ["REAL_DRY_RUN_SECONDS"]),
        "market_asset_ids_count": len(market_asset_ids),
        "market_asset_ids": market_asset_ids,
        "market_asset_ids_sha256": hashlib.sha256(
            market_asset_ids_csv.encode("utf-8")
        ).hexdigest(),
        "stream_lengths": lengths,
        "recent_report_status_counts": status_counts,
        "data_lake_root": os.environ["DATA_LAKE_ROOT"],
        "research_report_root": os.environ["RESEARCH_REPORT_ROOT"],
        "research_manifest_root": os.environ.get("RESEARCH_MANIFEST_ROOT"),
        "blocked_segments_path": os.environ.get("PREDICTOR_BLOCKED_SEGMENTS_PATH"),
        "blocked_segments_enabled": bool(os.environ.get("PREDICTOR_BLOCKED_SEGMENTS_PATH")),
        "predictor_strategy_profile": os.environ.get("PREDICTOR_STRATEGY_PROFILE", "baseline"),
        "predictor_quote_placement": os.environ.get("PREDICTOR_QUOTE_PLACEMENT"),
        "predictor_conservative_min_confidence": os.environ.get(
            "PREDICTOR_CONSERVATIVE_MIN_CONFIDENCE"
        ),
        "predictor_conservative_min_depth": os.environ.get(
            "PREDICTOR_CONSERVATIVE_MIN_DEPTH"
        ),
        "predictor_conservative_max_top_changes": os.environ.get(
            "PREDICTOR_CONSERVATIVE_MAX_TOP_CHANGES"
        ),
        "predictor_balanced_min_confidence": os.environ.get(
            "PREDICTOR_BALANCED_MIN_CONFIDENCE"
        ),
        "predictor_balanced_min_depth": os.environ.get(
            "PREDICTOR_BALANCED_MIN_DEPTH"
        ),
        "predictor_balanced_max_top_changes": os.environ.get(
            "PREDICTOR_BALANCED_MAX_TOP_CHANGES"
        ),
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

set +e
PYTHONPATH=python-service python3 -m src.research.pre_live_readiness \
  --manifest-root "${RESEARCH_MANIFEST_ROOT:-${DATA_LAKE_ROOT}/research_runs}" \
  --database-url "$DATABASE_URL" \
  --output "$RESEARCH_REPORT_ROOT/pre_live_readiness.json"
readiness_status=$?
set -e
if [[ "$readiness_status" != "0" ]]; then
  echo "Pre-live readiness is not ready; inspect $RESEARCH_REPORT_ROOT/pre_live_readiness.json." >&2
fi
if [[ -f "$RESEARCH_REPORT_ROOT/pre_live_readiness.json" ]]; then
  scripts/summarize_pre_live_readiness.sh "$RESEARCH_REPORT_ROOT/pre_live_readiness.json" || true
fi

if [[ "$research_status" == "0" ]]; then
  if [[ "$readiness_status" != "0" && "$ALLOW_RESEARCH_GATE_FAILURE" != "1" && "$ALLOW_RESEARCH_GATE_FAILURE" != "true" ]]; then
    echo "Pre-live readiness failed and ALLOW_RESEARCH_GATE_FAILURE is disabled." >&2
    exit 20
  fi
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
