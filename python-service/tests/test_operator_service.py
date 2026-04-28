import json
import asyncio
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient

from src.api import app as api_app
from src.api.operator_service import (
    control_results,
    kill_switch_enabled,
    open_orders,
    positions,
    preview_cancel_all,
    preview_cancel_bot_open,
    request_cancel_all,
    request_cancel_bot_open,
    set_kill_switch,
    strategy_metrics_from_records,
    runtime_metrics_from_records,
    signal_index_from_records,
    strategy_metrics,
    stream_summary,
    prometheus_metrics,
)
from src.api.state_store import jsonb_payload_to_dict
from src.config import settings
from src.config import validate_production_settings


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


class FakePostgresPool:
    def __init__(self, redis: FakeRedis | None = None) -> None:
        self.redis = redis
        self.commands: list[dict[str, object]] = []

    async def execute(self, query: str, *args: object) -> str:
        if self.redis is not None:
            assert settings.operator_commands_stream not in self.redis.streams
        self.commands.append(
            {
                "command_id": args[0],
                "command_type": args[1],
                "status": args[2],
                "operator": args[3],
                "reason": args[4],
                "payload": json.loads(cast(str, args[5])),
                "created_at_ms": args[6],
            }
        )
        return "INSERT 0 1"


def test_kill_switch_state_is_written_and_read() -> None:
    redis = FakeRedis()

    async def run() -> dict[str, object]:
        return await set_kill_switch(redis, True, "maintenance", "operator-1")

    result = asyncio.run(run())

    assert result["kill_switch"] is True
    assert asyncio.run(kill_switch_enabled(redis)) is True
    command = json.loads(redis.streams[settings.operator_commands_stream][0][1]["payload"])
    assert command["reason"] == "maintenance"


def test_kill_switch_command_is_audited_before_stream_publish() -> None:
    redis = FakeRedis()
    postgres = FakePostgresPool(redis)

    result = asyncio.run(
        set_kill_switch(
            redis,
            True,
            "maintenance",
            "operator-1",
            postgres_pool=cast(Any, postgres),
        )
    )

    command = cast(dict[str, object], result["command"])
    assert command["command_id"]
    assert postgres.commands[0]["command_id"] == command["command_id"]
    assert postgres.commands[0]["command_type"] == "kill_switch"
    assert postgres.commands[0]["status"] == "PUBLISHED"
    assert settings.operator_commands_stream in redis.streams


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


def test_cancel_command_is_audited_before_stream_publish() -> None:
    redis = FakeRedis()
    postgres = FakePostgresPool(redis)

    result = asyncio.run(
        request_cancel_bot_open(
            redis,
            "rebalance",
            "operator-1",
            postgres_pool=cast(Any, postgres),
        )
    )

    command = cast(dict[str, object], result["command"])
    assert postgres.commands[0]["command_id"] == command["command_id"]
    assert postgres.commands[0]["command_type"] == "cancel_bot_open"
    assert settings.operator_commands_stream in redis.streams


def test_cancel_bot_open_command_is_published() -> None:
    redis = FakeRedis()

    result = asyncio.run(request_cancel_bot_open(redis, "rebalance", "operator-1"))

    assert result["accepted"] is True
    command = json.loads(redis.streams[settings.operator_commands_stream][0][1]["payload"])
    assert command["type"] == "cancel_bot_open"


def test_cancel_bot_open_preview_has_no_side_effects() -> None:
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

    preview = asyncio.run(preview_cancel_bot_open(redis))

    assert preview["command_type"] == "cancel_bot_open"
    assert preview["affected_count"] == 1
    assert preview["would_publish"] is False
    assert settings.operator_commands_stream not in redis.streams


def test_cancel_all_preview_warns_about_account_scope() -> None:
    redis = FakeRedis()

    preview = asyncio.run(preview_cancel_all(redis))

    assert preview["command_type"] == "cancel_all"
    assert preview["scope"] == "account"
    assert preview["requires_confirmation"] is True
    assert preview["confirmation_phrase"] == "CANCEL ALL OPEN ORDERS"
    warnings = preview["warnings"]
    assert isinstance(warnings, list)
    assert any("may affect orders not tracked" in str(warning) for warning in warnings)
    assert settings.operator_commands_stream not in redis.streams


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


def test_open_orders_include_partial_reports() -> None:
    redis = FakeRedis()
    asyncio.run(redis.xadd(
        settings.execution_reports_stream,
        {
            "payload": json.dumps(
                {
                    "signal_id": "signal-1",
                    "order_id": "order-1",
                    "status": "PARTIAL",
                    "remaining_size": 4,
                    "timestamp_ms": 1,
                }
            )
        },
    ))

    orders = asyncio.run(open_orders(redis))

    assert len(orders) == 1
    assert orders[0]["status"] == "PARTIAL"


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
                    "status": "PARTIAL",
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


def test_strategy_metrics_from_postgres_records_uses_canonical_source() -> None:
    signals = signal_index_from_records(
        [
            {
                "signal_id": "signal-1",
                "timestamp_ms": 10,
            }
        ]
    )
    reports: list[dict[str, object]] = [
        {
            "signal_id": "signal-1",
            "order_id": "order-1",
            "status": "MATCHED",
            "filled_size": 2,
            "timestamp_ms": 20,
        }
    ]

    metrics = strategy_metrics_from_records(reports, signals, source="postgres")

    assert metrics["source"] == "postgres"
    assert metrics["sample_size"] == 1
    assert metrics["latency_ms"] == 10


def test_runtime_metrics_from_postgres_records_uses_canonical_source() -> None:
    signals = signal_index_from_records(
        [
            {
                "signal_id": "signal-1",
                "source_timestamp_ms": 1,
                "timestamp_ms": 10,
            }
        ]
    )
    reports = [
        {
            "signal_id": "signal-1",
            "order_id": "order-1",
            "status": "MATCHED",
            "timestamp_ms": 20,
        }
    ]
    results: list[dict[str, object]] = [
        {
            "command_id": "command-1",
            "command_type": "cancel_bot_open",
            "status": "CONFIRMED",
        }
    ]

    metrics = runtime_metrics_from_records(reports, signals, results, source=["postgres"])

    assert metrics["source"] == ["postgres"]
    assert metrics["signals_received"] == 1
    assert metrics["control_results"] == 1
    assert metrics["ws_to_signal_latency_ms"] == 9
    assert metrics["signal_to_order_latency_ms"] == 10


def test_prometheus_metrics_formats_numeric_values() -> None:
    output = prometheus_metrics(
        {
            "signals_received": 2,
            "signals_rejected": 1,
            "ws_to_report_latency_ms": 12.5,
            "signal_to_order_latency_ms": 2.0,
            "execution_reports_by_status": {"PARTIAL": 1, "MATCHED": 2},
            "control_results_by_type": {"cancel_bot_open": 1},
            "clob_errors_by_type": {"timeout": 1},
        }
    )

    assert "polymarket_signals_received_total 2" in output
    assert "# TYPE polymarket_signals_received_total counter" in output
    assert "# TYPE polymarket_ws_to_report_latency_ms gauge" in output
    assert "polymarket_ws_to_report_latency_ms 12.5" in output
    assert "polymarket_signal_to_order_latency_ms 2.0" in output
    assert 'polymarket_execution_reports_by_status_total{status="PARTIAL"} 1' in output
    assert (
        'polymarket_control_results_by_type_total{command_type="cancel_bot_open"} 1'
        in output
    )
    assert 'polymarket_clob_errors_by_type_total{error_type="timeout"} 1' in output


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


def test_jsonb_payload_to_dict_accepts_asyncpg_string_payload() -> None:
    payload = jsonb_payload_to_dict('{"order_id": "order-1", "status": "DELAYED"}')

    assert payload == {"order_id": "order-1", "status": "DELAYED"}


def test_execution_reports_use_postgres_when_pool_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = FakeRedis()

    async def fake_get_redis() -> FakeRedis:
        return redis

    async def fake_require_pool() -> object:
        return object()

    async def fake_execution_reports_from_postgres(
        pool: object, count: int
    ) -> list[dict[str, object]]:
        assert count == 25
        return [
            {
                "signal_id": "signal-1",
                "order_id": "order-1",
                "status": "PARTIAL",
                "timestamp_ms": 1,
            }
        ]

    monkeypatch.setattr(api_app, "get_redis", fake_get_redis)
    monkeypatch.setattr(api_app, "require_pool", fake_require_pool)
    monkeypatch.setattr(
        api_app, "execution_reports_from_postgres", fake_execution_reports_from_postgres
    )
    client = TestClient(api_app.app)

    response = client.get("/execution-reports?limit=25")

    assert response.status_code == 200
    assert response.json()["source"] == "postgres"
    assert response.json()["reports"][0]["status"] == "PARTIAL"


def test_control_results_use_postgres_when_pool_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = FakeRedis()

    async def fake_get_redis() -> FakeRedis:
        return redis

    async def fake_require_pool() -> object:
        return object()

    async def fake_control_results_from_postgres(
        pool: object, count: int
    ) -> list[dict[str, object]]:
        assert count == 25
        return [
            {
                "type": "cancel_bot_open_result",
                "command_id": "command-1",
                "command_type": "cancel_bot_open",
                "status": "CONFIRMED",
                "timestamp_ms": 1,
                "operator": "operator-1",
                "reason": "risk off",
                "command_created_at_ms": 1,
                "completed_at_ms": 2,
            }
        ]

    monkeypatch.setattr(api_app, "get_redis", fake_get_redis)
    monkeypatch.setattr(api_app, "require_pool", fake_require_pool)
    monkeypatch.setattr(
        api_app, "control_results_from_postgres", fake_control_results_from_postgres
    )
    client = TestClient(api_app.app)

    response = client.get("/control/results?limit=25")

    assert response.status_code == 200
    assert response.json()["source"] == "postgres"
    assert response.json()["results"][0]["command_id"] == "command-1"
    assert response.json()["results"][0]["operator"] == "operator-1"


def test_preview_endpoints_return_affected_orders_without_publishing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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

    async def fake_get_redis() -> FakeRedis:
        return redis

    async def fake_require_pool() -> None:
        return None

    monkeypatch.setattr(api_app, "get_redis", fake_get_redis)
    monkeypatch.setattr(api_app, "require_pool", fake_require_pool)
    client = TestClient(api_app.app)

    response = client.post("/control/preview/cancel-bot-open")

    assert response.status_code == 200
    assert response.json()["affected_count"] == 1
    assert response.json()["would_publish"] is False
    assert settings.operator_commands_stream not in redis.streams


def test_required_postgres_state_returns_503_instead_of_redis_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = FakeRedis()

    async def fake_get_redis() -> FakeRedis:
        return redis

    async def fake_require_pool() -> object:
        raise RuntimeError(
            "DATABASE_URL is required when REQUIRE_POSTGRES_STATE=true or APP_ENV=production"
        )

    monkeypatch.setattr(api_app, "get_redis", fake_get_redis)
    monkeypatch.setattr(api_app, "require_pool", fake_require_pool)
    client = TestClient(api_app.app)

    for path in (
        "/orders/open",
        "/positions",
        "/execution-reports",
        "/control/results",
        "/reconciliation/status",
        "/risk",
        "/strategy/metrics",
        "/metrics",
        "/metrics/prometheus",
    ):
        response = client.get(path)
        assert response.status_code == 503
        assert "DATABASE_URL is required" in response.json()["detail"]

    post_cases = (
        ("/control/kill-switch", {"reason": "pause"}),
        ("/control/resume", {"confirm": True, "reason": "resume"}),
        (
            "/orders/cancel-all",
            {
                "reason": "risk off",
                "confirm": True,
                "confirmation_phrase": "CANCEL ALL OPEN ORDERS",
            },
        ),
        ("/orders/cancel-bot-open", {"reason": "rebalance"}),
    )
    for path, payload in post_cases:
        response = client.post(path, json=payload)
        assert response.status_code == 503
        assert "DATABASE_URL is required" in response.json()["detail"]


def test_reconciliation_status_uses_postgres_when_pool_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = FakeRedis()

    async def fake_get_redis() -> FakeRedis:
        return redis

    async def fake_require_pool() -> object:
        return object()

    async def fake_reconciliation_status_from_postgres(
        pool: object, limit: int
    ) -> dict[str, object]:
        assert limit == 25
        return {
            "status": "warning",
            "source": "postgres",
            "open_local_orders": 1,
            "pending_cancel_requests": 1,
            "diverged_cancel_requests": 0,
            "stale_orders": 0,
            "recent_event_count": 1,
            "events_by_severity": {"warning": 1},
            "events_by_type": {"duplicate_trade": 1},
            "recent_events": [
                {
                    "event_id": "event-1",
                    "order_id": "order-1",
                    "signal_id": "signal-1",
                    "event_type": "duplicate_trade",
                    "severity": "warning",
                    "details": {"trade_id": "trade-1"},
                    "created_at": "2026-01-01T00:00:00+00:00",
                }
            ],
            "last_reconciled_at_ms": 1,
        }

    monkeypatch.setattr(api_app, "get_redis", fake_get_redis)
    monkeypatch.setattr(api_app, "require_pool", fake_require_pool)
    monkeypatch.setattr(
        api_app,
        "reconciliation_status_from_postgres",
        fake_reconciliation_status_from_postgres,
    )
    client = TestClient(api_app.app)

    response = client.get("/reconciliation/status?limit=25")

    assert response.status_code == 200
    assert response.json()["source"] == "postgres"
    assert response.json()["status"] == "warning"


def test_reconciliation_status_fallback_reports_control_divergence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = FakeRedis()
    asyncio.run(
        redis.xadd(
            settings.operator_results_stream,
            {
                "payload": json.dumps(
                    {
                        "type": "cancel_bot_open_result",
                        "command_id": "command-1",
                        "command_type": "cancel_bot_open",
                        "status": "DIVERGED",
                        "timestamp_ms": 1,
                    }
                )
            },
        )
    )

    async def fake_get_redis() -> FakeRedis:
        return redis

    async def fake_require_pool() -> None:
        return None

    monkeypatch.setattr(api_app, "get_redis", fake_get_redis)
    monkeypatch.setattr(api_app, "require_pool", fake_require_pool)
    client = TestClient(api_app.app)

    response = client.get("/reconciliation/status")

    assert response.status_code == 200
    assert response.json()["status"] == "diverged"


def test_require_postgres_state_fails_startup_without_database_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "app_env", "development")
    monkeypatch.setattr(settings, "require_postgres_state", True)
    monkeypatch.setattr(settings, "database_url", None)

    with pytest.raises(RuntimeError, match="DATABASE_URL"):
        validate_production_settings()


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
