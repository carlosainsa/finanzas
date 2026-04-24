import json
import time
from typing import Any, Protocol, cast

from fastapi import HTTPException, status

from src.config import settings


class RedisLike(Protocol):
    async def get(self, name: str) -> Any: ...
    async def set(self, name: str, value: str) -> Any: ...
    async def xadd(self, name: str, fields: dict[str, str]) -> Any: ...
    async def xlen(self, name: str) -> int: ...
    async def xpending(self, name: str, groupname: str) -> Any: ...
    async def xrevrange(
        self, name: str, max: str = "+", min: str = "-", count: int | None = None
    ) -> list[tuple[str, dict[str, str]]]: ...


STREAM_GROUPS: dict[str, str] = {
    settings.orderbook_stream: settings.orderbook_consumer_group,
    settings.signals_stream: settings.executor_consumer_group,
}


def managed_streams() -> list[str]:
    return [
        settings.orderbook_stream,
        settings.signals_stream,
        settings.execution_reports_stream,
        settings.orderbook_deadletter_stream,
        settings.signals_deadletter_stream,
        settings.operator_commands_stream,
    ]


async def kill_switch_enabled(redis: RedisLike) -> bool:
    value = await redis.get(settings.operator_kill_switch_key)
    if isinstance(value, bytes):
        value = value.decode()
    return str(value).lower() in {"1", "true", "yes", "on"}


async def set_kill_switch(
    redis: RedisLike, enabled: bool, reason: str, operator: str | None
) -> dict[str, object]:
    await redis.set(settings.operator_kill_switch_key, "1" if enabled else "0")
    command = {
        "type": "kill_switch",
        "enabled": enabled,
        "reason": reason,
        "operator": operator,
        "timestamp_ms": now_ms(),
    }
    await redis.xadd(settings.operator_commands_stream, {"payload": json.dumps(command)})
    return {"kill_switch": enabled, "command": command}


async def stream_summary(redis: RedisLike) -> list[dict[str, object]]:
    summaries: list[dict[str, object]] = []
    for stream in managed_streams():
        length = await safe_xlen(redis, stream)
        group = STREAM_GROUPS.get(stream)
        pending = await safe_xpending(redis, stream, group) if group else None
        summaries.append(
            {
                "stream": stream,
                "length": length,
                "consumer_group": group,
                "pending": pending,
            }
        )
    return summaries


async def status_summary(redis: RedisLike) -> dict[str, object]:
    return {
        "status": "ok",
        "kill_switch": await kill_switch_enabled(redis),
        "streams": await stream_summary(redis),
        "predictor": {
            "min_spread": settings.predictor_min_spread,
            "order_size": settings.predictor_order_size,
            "min_confidence": settings.predictor_min_confidence,
        },
    }


async def risk_summary(redis: RedisLike) -> dict[str, object]:
    return {
        "kill_switch": await kill_switch_enabled(redis),
        "source": settings.operator_kill_switch_key,
        "execution_mode": settings.execution_mode,
        "limits": {
            "max_order_size": settings.max_order_size,
            "min_confidence": settings.min_confidence,
            "signal_max_age_ms": settings.signal_max_age_ms,
            "max_market_exposure": settings.max_market_exposure,
            "max_daily_loss": settings.max_daily_loss,
            "predictor_min_confidence": settings.predictor_min_confidence,
            "predictor_order_size": settings.predictor_order_size,
        },
        "enforcement": "rust-engine",
    }


async def open_orders(redis: RedisLike, count: int = 200) -> list[dict[str, object]]:
    reports = await recent_execution_reports(redis, count=count)
    latest_by_order: dict[str, dict[str, object]] = {}
    for report in reports:
        order_id = str(report.get("order_id", ""))
        if order_id and order_id not in latest_by_order:
            latest_by_order[order_id] = report

    return [
        report
        for report in latest_by_order.values()
        if report.get("status") in {"DELAYED", "UNMATCHED"}
    ]


async def positions(redis: RedisLike, count: int = 500) -> list[dict[str, object]]:
    signals = await signal_index(redis, count=count)
    exposure: dict[tuple[str, str], float] = {}
    for report in await recent_execution_reports(redis, count=count):
        if report.get("status") != "MATCHED":
            continue
        signal = signals.get(str(report.get("signal_id", "")))
        if signal is None:
            continue
        filled_size = as_float(report.get("filled_size"))
        if filled_size <= 0:
            continue
        multiplier = 1.0 if signal.get("side") == "BUY" else -1.0
        key = (str(signal["market_id"]), str(signal["asset_id"]))
        exposure[key] = exposure.get(key, 0.0) + multiplier * filled_size

    return [
        {"market_id": market_id, "asset_id": asset_id, "position": position}
        for (market_id, asset_id), position in sorted(exposure.items())
    ]


async def strategy_metrics(redis: RedisLike, count: int = 500) -> dict[str, object]:
    reports = await recent_execution_reports(redis, count=count)
    total = len(reports)
    matched = sum(1 for report in reports if report.get("status") == "MATCHED")
    open_count = sum(1 for report in reports if report.get("status") in {"DELAYED", "UNMATCHED"})
    errors = sum(1 for report in reports if report.get("status") == "ERROR")
    filled_size = sum(as_float(report.get("filled_size")) for report in reports)
    return {
        "sample_size": total,
        "matched": matched,
        "open": open_count,
        "errors": errors,
        "match_rate": matched / total if total else 0.0,
        "error_rate": errors / total if total else 0.0,
        "filled_size": filled_size,
        "source": settings.execution_reports_stream,
    }


def cancel_all_unavailable() -> None:
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="cancel-all requires Rust CLOB cancel support before it can be enabled",
    )


async def recent_execution_reports(
    redis: RedisLike, count: int = 200
) -> list[dict[str, object]]:
    entries = await redis.xrevrange(settings.execution_reports_stream, count=count)
    return [payload for _, payload in parse_stream_payloads(entries)]


async def signal_index(redis: RedisLike, count: int = 500) -> dict[str, dict[str, object]]:
    entries = await redis.xrevrange(settings.signals_stream, count=count)
    signals: dict[str, dict[str, object]] = {}
    for _, payload in parse_stream_payloads(entries):
        signal_id = payload.get("signal_id")
        if isinstance(signal_id, str):
            signals.setdefault(signal_id, payload)
    return signals


def parse_stream_payloads(
    entries: list[tuple[str, dict[str, str]]],
) -> list[tuple[str, dict[str, object]]]:
    parsed: list[tuple[str, dict[str, object]]] = []
    for stream_id, fields in entries:
        payload = fields.get("payload")
        if payload is None:
            continue
        try:
            value = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            parsed.append((stream_id, cast(dict[str, object], value)))
    return parsed


async def safe_xlen(redis: RedisLike, stream: str) -> int:
    try:
        return int(await redis.xlen(stream))
    except Exception:
        return 0


async def safe_xpending(
    redis: RedisLike, stream: str, group: str | None
) -> dict[str, object] | None:
    if group is None:
        return None
    try:
        pending = await redis.xpending(stream, group)
    except Exception:
        return None
    if isinstance(pending, dict):
        return {
            "pending": pending.get("pending", 0),
            "min": pending.get("min"),
            "max": pending.get("max"),
            "consumers": pending.get("consumers", []),
        }
    return {"raw": str(pending)}


def as_float(value: object) -> float:
    if not isinstance(value, (str, bytes, int, float)):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def now_ms() -> int:
    return int(time.time() * 1000)
