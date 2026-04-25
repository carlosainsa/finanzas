#!/usr/bin/env python3
"""Smoke test for a local dry-run operator stack.

Expected services:
  docker compose -f docker-compose.test.yml up -d
  python-service API running on OPERATOR_API_URL
  python-service consumer running against Redis
  rust-engine running with EXECUTION_MODE=dry_run
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any

import httpx
import redis.asyncio as redis


REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379")
API_URL = os.getenv("OPERATOR_API_URL", "http://127.0.0.1:8000")
ORDERBOOK_STREAM = os.getenv("ORDERBOOK_STREAM", "orderbook:stream")
SIGNALS_STREAM = os.getenv("SIGNALS_STREAM", "signals:stream")
REPORTS_STREAM = os.getenv("EXECUTION_REPORTS_STREAM", "execution:reports:stream")
READ_TOKEN = os.getenv("OPERATOR_READ_TOKEN")
TIMEOUT_SECONDS = float(os.getenv("INTEGRATION_TIMEOUT_SECONDS", "15"))


async def main() -> int:
    client = redis.from_url(REDIS_URL, decode_responses=True)
    now_ms = int(time.time() * 1000)
    orderbook = {
        "market_id": "integration-market",
        "asset_id": "integration-asset",
        "bids": [{"price": 0.48, "size": 25.0}],
        "asks": [{"price": 0.52, "size": 25.0}],
        "timestamp_ms": now_ms,
        "source_timestamp_ms": now_ms,
    }
    await client.xadd(ORDERBOOK_STREAM, {"payload": json.dumps(orderbook)})

    signal = await wait_for_stream_payload(client, SIGNALS_STREAM)
    if signal.get("market_id") != orderbook["market_id"]:
        raise RuntimeError(f"unexpected signal payload: {signal}")

    report = await wait_for_stream_payload(client, REPORTS_STREAM)
    if not report.get("order_id") or report.get("status") not in {
        "DELAYED",
        "UNMATCHED",
        "MATCHED",
        "PARTIAL",
    }:
        raise RuntimeError(f"unexpected execution report payload: {report}")

    headers = {"Authorization": f"Bearer {READ_TOKEN}"} if READ_TOKEN else {}
    with httpx.Client(base_url=API_URL, timeout=5.0, headers=headers) as api:
        response = api.get("/status")
        response.raise_for_status()
        if response.json().get("status") != "ok":
            raise RuntimeError(f"operator API unhealthy: {response.text}")

    print(
        json.dumps(
            {
                "status": "ok",
                "signal_id": signal.get("signal_id"),
                "order_id": report.get("order_id"),
                "report_status": report.get("status"),
            },
            sort_keys=True,
        )
    )
    return 0


async def wait_for_stream_payload(
    client: redis.Redis, stream: str
) -> dict[str, Any]:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    last_id = "$"
    while time.monotonic() < deadline:
        messages = await client.xread({stream: last_id}, count=1, block=1000)
        for _, entries in messages:
            for stream_id, fields in entries:
                last_id = stream_id
                payload = fields.get("payload")
                if payload:
                    parsed = json.loads(payload)
                    if isinstance(parsed, dict):
                        return parsed
    raise TimeoutError(f"timed out waiting for {stream}")


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
