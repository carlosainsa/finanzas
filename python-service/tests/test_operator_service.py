import json
import asyncio
from typing import Any

import pytest
from fastapi.testclient import TestClient

from src.api import app as api_app
from src.api.operator_service import (
    control_results,
    kill_switch_enabled,
    open_orders,
    positions,
    request_cancel_all,
    request_cancel_bot_open,
    set_kill_switch,
    strategy_metrics,
    stream_summary,
    prometheus_metrics,
)
from src.config import settings


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.streams: dict[str, list[tuple[str, dict[str, str]]]] = {}

    async def get(self, name: str) -> str | None:
        return self.values.get(name)

    async def set(self, name: str, value: str) -> bool:
        self.values[name] = value
        return True

    async def xadd(self, name: str, fields: dict[str, str]) -> str:
        entries = self.streams.setdefault(name, [])
        stream_id = f"{len(entries) + 1}-0"
        entries.append((stream_id, fields))
        return stream_id

    async def xlen(self, name: str) -> int:
        return len(self.streams.get(name, []))

    async def xpending(self, name: str, groupname: str) -> dict[str, Any]:
        return {"pending": 0, "min": None, "max": None, "consumers": []}

    async def xrevrange(
        self, name: str, max: str = "+", min: str = "-", count: int | None = None
    ) -> list[tuple[str, dict[str, str]]]:
        entries = list(reversed(self.streams.get(name, [])))
        return entries if count is None else entries[:count]


def test_kill_switch_state_is_written_and_read() -> None:
    redis = FakeRedis()

    async def run() -> dict[str, object]:
        return await set_kill_switch(redis, True, "maintenance", "operator-1")

    result = asyncio.run(run())

    assert result["kill_switch"] is True
    assert asyncio.run(kill_switch_enabled(redis)) is True
    command = json.loads(redis.streams[settings.operator_commands_stream][0][1]["payload"])
    assert command["reason"] == "maintenance"


def test_cancel_all_command_is_published() -> None:
    redis = FakeRedis()

    result = asyncio.run(
        request_cancel_all(
            redis,
            "risk off",
            "operator-1",
            confirm=True,
            confirmation_phrase="CANCEL ALL OPEN ORDERS",
        )
    )

    assert result["accepted"] is True
    command = json.loads(redis.streams[settings.operator_commands_stream][0][1]["payload"])
    assert command["type"] == "cancel_all"
    assert command["reason"] == "risk off"
    assert command["command_id"]
    assert command["confirmation_phrase"] == "CANCEL ALL OPEN ORDERS"


def test_cancel_bot_open_command_is_published() -> None:
    redis = FakeRedis()

    result = asyncio.run(request_cancel_bot_open(redis, "rebalance", "operator-1"))

    assert result["accepted"] is True
    command = json.loads(redis.streams[settings.operator_commands_stream][0][1]["payload"])
    assert command["type"] == "cancel_bot_open"


def test_stream_summary_handles_missing_streams() -> None:
    redis = FakeRedis()

    streams = asyncio.run(stream_summary(redis))

    assert streams
    assert all(stream["length"] == 0 for stream in streams)


def test_open_orders_uses_latest_execution_report_status() -> None:
    redis = FakeRedis()
    asyncio.run(redis.xadd(
        settings.execution_reports_stream,
        {
            "payload": json.dumps(
                {
                    "signal_id": "signal-1",
                    "order_id": "order-1",
                    "status": "UNMATCHED",
                    "timestamp_ms": 1,
                }
            )
        },
    ))

    orders = asyncio.run(open_orders(redis))

    assert len(orders) == 1
    assert orders[0]["order_id"] == "order-1"


def test_positions_derive_matched_buy_exposure() -> None:
    redis = FakeRedis()
    asyncio.run(redis.xadd(
        settings.signals_stream,
        {
            "payload": json.dumps(
                {
                    "signal_id": "signal-1",
                    "market_id": "market-1",
                    "asset_id": "asset-1",
                    "side": "BUY",
                }
            )
        },
    ))
    asyncio.run(redis.xadd(
        settings.execution_reports_stream,
        {
            "payload": json.dumps(
                {
                    "signal_id": "signal-1",
                    "order_id": "order-1",
                    "status": "MATCHED",
                    "filled_size": 3,
                    "timestamp_ms": 1,
                }
            )
        },
    ))

    derived_positions = asyncio.run(positions(redis))

    assert derived_positions == [
        {"market_id": "market-1", "asset_id": "asset-1", "position": 3.0}
    ]


def test_strategy_metrics_summarize_recent_reports() -> None:
    redis = FakeRedis()
    asyncio.run(
        redis.xadd(
            settings.execution_reports_stream,
            {
                "payload": json.dumps(
                    {
                        "signal_id": "signal-1",
                        "order_id": "order-1",
                        "status": "MATCHED",
                        "filled_size": 3,
                        "timestamp_ms": 1,
                    }
                )
            },
        )
    )
    asyncio.run(
        redis.xadd(
            settings.execution_reports_stream,
            {
                "payload": json.dumps(
                    {
                        "signal_id": "signal-2",
                        "order_id": "order-2",
                        "status": "ERROR",
                        "timestamp_ms": 2,
                    }
                )
            },
        )
    )

    metrics = asyncio.run(strategy_metrics(redis))

    assert metrics["sample_size"] == 2
    assert metrics["matched"] == 1
    assert metrics["errors"] == 1
    assert metrics["filled_size"] == 3.0


def test_prometheus_metrics_formats_numeric_values() -> None:
    output = prometheus_metrics(
        {
            "signals_received": 2,
            "signals_rejected": 1,
            "ws_to_report_latency_ms": 12.5,
        }
    )

    assert "polymarket_signals_received_total 2" in output
    assert "# TYPE polymarket_signals_received_total counter" in output
    assert "# TYPE polymarket_ws_to_report_latency_ms gauge" in output
    assert "polymarket_ws_to_report_latency_ms 12.5" in output


def test_resume_requires_confirmation(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = FakeRedis()

    async def fake_get_redis() -> FakeRedis:
        return redis

    monkeypatch.setattr(api_app, "get_redis", fake_get_redis)
    client = TestClient(api_app.app)

    response = client.post(
        "/control/resume", json={"confirm": False, "reason": "resume test"}
    )

    assert response.status_code == 400


def test_cancel_all_requires_strong_confirmation(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = FakeRedis()

    async def fake_get_redis() -> FakeRedis:
        return redis

    monkeypatch.setattr(api_app, "get_redis", fake_get_redis)
    client = TestClient(api_app.app)

    response = client.post("/orders/cancel-all", json={"reason": "risk off"})

    assert response.status_code == 400


def test_cancel_all_returns_accepted_command(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = FakeRedis()

    async def fake_get_redis() -> FakeRedis:
        return redis

    monkeypatch.setattr(api_app, "get_redis", fake_get_redis)
    client = TestClient(api_app.app)

    response = client.post(
        "/orders/cancel-all",
        json={
            "reason": "risk off",
            "confirm": True,
            "confirmation_phrase": "CANCEL ALL OPEN ORDERS",
        },
    )

    assert response.status_code == 202
    assert response.json()["accepted"] is True


def test_control_results_reads_operator_results_stream() -> None:
    redis = FakeRedis()
    asyncio.run(
        redis.xadd(
            settings.operator_results_stream,
            {
                "payload": json.dumps(
                    {
                        "type": "cancel_bot_open_result",
                        "command_id": "command-1",
                        "status": "CONFIRMED",
                        "timestamp_ms": 1,
                    }
                )
            },
        )
    )

    results = asyncio.run(control_results(redis))

    assert results[0]["command_id"] == "command-1"


def test_api_prefix_aliases_operator_routes(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = FakeRedis()

    async def fake_get_redis() -> FakeRedis:
        return redis

    monkeypatch.setattr(api_app, "get_redis", fake_get_redis)
    client = TestClient(api_app.app)

    response = client.get("/api/status")

    assert response.status_code == 200


def test_operator_auth_rejects_missing_read_token(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = FakeRedis()

    async def fake_get_redis() -> FakeRedis:
        return redis

    monkeypatch.setattr(api_app, "get_redis", fake_get_redis)
    monkeypatch.setattr(settings, "operator_read_token", "read-secret")
    client = TestClient(api_app.app)

    response = client.get("/status")

    assert response.status_code == 401


def test_operator_auth_accepts_read_bearer_token(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = FakeRedis()

    async def fake_get_redis() -> FakeRedis:
        return redis

    monkeypatch.setattr(api_app, "get_redis", fake_get_redis)
    monkeypatch.setattr(settings, "operator_read_token", "read-secret")
    client = TestClient(api_app.app)

    response = client.get("/status", headers={"Authorization": "Bearer read-secret"})

    assert response.status_code == 200


def test_read_token_cannot_control(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = FakeRedis()

    async def fake_get_redis() -> FakeRedis:
        return redis

    monkeypatch.setattr(api_app, "get_redis", fake_get_redis)
    monkeypatch.setattr(settings, "operator_read_token", "read-secret")
    monkeypatch.setattr(settings, "operator_control_token", "control-secret")
    client = TestClient(api_app.app)

    response = client.post(
        "/control/kill-switch",
        json={"reason": "test"},
        headers={"Authorization": "Bearer read-secret"},
    )

    assert response.status_code == 401


def test_control_token_can_read_and_control(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = FakeRedis()

    async def fake_get_redis() -> FakeRedis:
        return redis

    monkeypatch.setattr(api_app, "get_redis", fake_get_redis)
    monkeypatch.setattr(settings, "operator_read_token", "read-secret")
    monkeypatch.setattr(settings, "operator_control_token", "control-secret")
    client = TestClient(api_app.app)

    read_response = client.get(
        "/status", headers={"Authorization": "Bearer control-secret"}
    )
    control_response = client.post(
        "/control/kill-switch",
        json={"reason": "test"},
        headers={"Authorization": "Bearer control-secret"},
    )

    assert read_response.status_code == 200
    assert control_response.status_code == 200


def test_frontend_dist_path_points_to_repo_frontend() -> None:
    path = api_app.frontend_dist_path()

    assert path.name == "dist"
    assert path.parent.name == "frontend"
    assert path.parent.parent.name == "finanzas"
