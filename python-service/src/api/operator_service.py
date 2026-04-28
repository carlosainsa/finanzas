import json
import time
from collections.abc import Iterable
from typing import Any, Protocol, cast
from uuid import uuid4

import asyncpg  # type: ignore[import-untyped]

from src.config import settings
from src.api.state_store import (
    open_orders_from_postgres,
    positions_from_postgres,
    record_control_command_in_postgres,
)


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
        settings.operator_results_stream,
    ]


async def kill_switch_enabled(redis: RedisLike) -> bool:
    value = await redis.get(settings.operator_kill_switch_key)
    if isinstance(value, bytes):
        value = value.decode()
    return str(value).lower() in {"1", "true", "yes", "on"}


async def set_kill_switch(
    redis: RedisLike,
    enabled: bool,
    reason: str,
    operator: str | None,
    postgres_pool: asyncpg.Pool | None = None,
) -> dict[str, object]:
    command: dict[str, object] = {
        "type": "kill_switch",
        "command_id": str(uuid4()),
        "enabled": enabled,
        "reason": reason,
        "operator": operator,
        "timestamp_ms": now_ms(),
    }
    await persist_control_command(command, postgres_pool)
    await redis.set(settings.operator_kill_switch_key, "1" if enabled else "0")
    await redis.xadd(settings.operator_commands_stream, {"payload": json.dumps(command)})
    return {"kill_switch": enabled, "command": command}


async def request_cancel_all(
    redis: RedisLike,
    reason: str,
    operator: str | None,
    confirm: bool,
    confirmation_phrase: str | None,
    postgres_pool: asyncpg.Pool | None = None,
) -> dict[str, object]:
    return await publish_control_command(
        redis,
        "cancel_all",
        reason,
        operator,
        {
            "confirm": confirm,
            "confirmation_phrase": confirmation_phrase,
            "scope": "account",
        },
        postgres_pool=postgres_pool,
    )


async def request_cancel_bot_open(
    redis: RedisLike,
    reason: str,
    operator: str | None,
    postgres_pool: asyncpg.Pool | None = None,
) -> dict[str, object]:
    return await publish_control_command(
        redis, "cancel_bot_open", reason, operator, postgres_pool=postgres_pool
    )


async def preview_cancel_bot_open(
    redis: RedisLike, postgres_pool: asyncpg.Pool | None = None
) -> dict[str, object]:
    orders = await open_orders(redis, postgres_pool=postgres_pool)
    source = "postgres" if postgres_pool is not None else settings.execution_reports_stream
    warnings = []
    if postgres_pool is None:
        warnings.append("preview is derived from Redis fallback, not canonical Postgres state")
    return {
        "command_type": "cancel_bot_open",
        "scope": "bot_known_open_orders",
        "affected_count": len(orders),
        "affected_orders": orders,
        "source": source,
        "warnings": warnings,
        "requires_confirmation": False,
        "confirmation_phrase": None,
        "would_publish": False,
    }


async def preview_cancel_all(
    redis: RedisLike, postgres_pool: asyncpg.Pool | None = None
) -> dict[str, object]:
    orders = await open_orders(redis, postgres_pool=postgres_pool)
    source = "postgres" if postgres_pool is not None else settings.execution_reports_stream
    warnings = [
        "cancel-all targets the authenticated CLOB account and may affect orders not tracked by this bot",
        "use cancel-bot-open unless this is an emergency",
    ]
    if postgres_pool is None:
        warnings.append("known bot order count is derived from Redis fallback")
    return {
        "command_type": "cancel_all",
        "scope": "account",
        "affected_count": len(orders),
        "affected_orders": orders,
        "source": source,
        "warnings": warnings,
        "requires_confirmation": True,
        "confirmation_phrase": "CANCEL ALL OPEN ORDERS",
        "would_publish": False,
    }


async def publish_control_command(
    redis: RedisLike,
    command_type: str,
    reason: str,
    operator: str | None,
    extra_fields: dict[str, object] | None = None,
    postgres_pool: asyncpg.Pool | None = None,
) -> dict[str, object]:
    command_id = str(uuid4())
    command: dict[str, object] = {
        "type": command_type,
        "command_id": command_id,
        "reason": reason,
        "operator": operator,
        "timestamp_ms": now_ms(),
    }
    if extra_fields:
        command.update(extra_fields)
    await persist_control_command(command, postgres_pool)
    await redis.xadd(settings.operator_commands_stream, {"payload": json.dumps(command)})
    return {"accepted": True, "command": command}


async def persist_control_command(
    command: dict[str, object], postgres_pool: asyncpg.Pool | None
) -> None:
    if postgres_pool is None:
        return
    await record_control_command_in_postgres(postgres_pool, command)


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


async def open_orders(
    redis: RedisLike, count: int = 200, postgres_pool: asyncpg.Pool | None = None
) -> list[dict[str, object]]:
    if postgres_pool is not None:
        orders = await open_orders_from_postgres(postgres_pool)
        return cast(list[dict[str, object]], orders)

    reports = await recent_execution_reports(redis, count=count)
    latest_by_order: dict[str, dict[str, object]] = {}
    for report in reports:
        order_id = str(report.get("order_id", ""))
        if order_id and order_id not in latest_by_order:
            latest_by_order[order_id] = report

    return [
        report
        for report in latest_by_order.values()
        if report.get("status") in {"DELAYED", "UNMATCHED", "PARTIAL"}
    ]


async def positions(
    redis: RedisLike, count: int = 500, postgres_pool: asyncpg.Pool | None = None
) -> list[dict[str, object]]:
    if postgres_pool is not None:
        stored_positions = await positions_from_postgres(postgres_pool)
        return cast(list[dict[str, object]], stored_positions)

    signals = await signal_index(redis, count=count)
    exposure: dict[tuple[str, str], float] = {}
    for report in await recent_execution_reports(redis, count=count):
        if report.get("status") not in {"MATCHED", "PARTIAL"}:
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
    signals = await signal_index(redis, count=count)
    return strategy_metrics_from_records(
        reports, signals, source=settings.execution_reports_stream
    )


def strategy_metrics_from_records(
    reports: list[dict[str, object]],
    signals: dict[str, dict[str, object]],
    source: str,
) -> dict[str, object]:
    total = len(reports)
    matched = sum(1 for report in reports if report.get("status") == "MATCHED")
    open_count = sum(1 for report in reports if report.get("status") in {"DELAYED", "UNMATCHED"})
    errors = sum(1 for report in reports if report.get("status") == "ERROR")
    filled_size = sum(as_float(report.get("filled_size")) for report in reports)
    latency_values = report_latencies(reports, signals)
    return {
        "sample_size": total,
        "matched": matched,
        "open": open_count,
        "errors": errors,
        "match_rate": matched / total if total else 0.0,
        "error_rate": errors / total if total else 0.0,
        "filled_size": filled_size,
        "latency_ms": sum(latency_values) / len(latency_values)
        if latency_values
        else None,
        "source": source,
    }


async def control_results(
    redis: RedisLike, count: int = 100
) -> list[dict[str, object]]:
    entries = await redis.xrevrange(settings.operator_results_stream, count=count)
    return [payload for _, payload in parse_stream_payloads(entries)]


async def runtime_metrics(redis: RedisLike, count: int = 500) -> dict[str, object]:
    reports = await recent_execution_reports(redis, count=count)
    results = await control_results(redis, count=count)
    signals = await signal_index(redis, count=count)
    return runtime_metrics_from_records(
        reports,
        signals,
        results,
        source=[
            settings.signals_stream,
            settings.execution_reports_stream,
            settings.operator_results_stream,
        ],
    )


def runtime_metrics_from_records(
    reports: list[dict[str, object]],
    signals: dict[str, dict[str, object]],
    results: list[dict[str, object]],
    source: list[str],
) -> dict[str, object]:
    latency = stage_latencies(reports, signals)
    return {
        "signals_received": len(signals),
        "signals_rejected": sum(1 for report in reports if report.get("status") == "ERROR"),
        "orders_submitted": sum(
            1
            for report in reports
            if report.get("status") in {"DELAYED", "UNMATCHED", "PARTIAL", "MATCHED"}
        ),
        "clob_errors": sum(1 for report in reports if report.get("error")),
        "clob_errors_by_type": count_by_key(error_type(report.get("error")) for report in reports),
        "execution_reports": len(reports),
        "execution_reports_by_status": count_by_key(
            str(report.get("status") or "unknown") for report in reports
        ),
        "control_results": len(results),
        "control_results_by_type": count_by_key(
            str(result.get("command_type") or result.get("type") or "unknown")
            for result in results
        ),
        "ws_to_report_latency_ms": average(latency["ws_to_report"]),
        "ws_to_signal_latency_ms": average(latency["ws_to_signal"]),
        "signal_to_order_latency_ms": average(latency["signal_to_order"]),
        "order_to_report_latency_ms": average(latency["order_to_report"]),
        "source": source,
    }


async def reconciliation_status_fallback(redis: RedisLike, count: int = 200) -> dict[str, object]:
    reports = await recent_execution_reports(redis, count=count)
    results = await control_results(redis, count=count)
    open_local_orders = sum(
        1 for report in reports if report.get("status") in {"DELAYED", "UNMATCHED", "PARTIAL"}
    )
    diverged_results = [
        result for result in results if str(result.get("status") or "").upper() == "DIVERGED"
    ]
    failed_results = [
        result for result in results if str(result.get("status") or "").upper() == "FAILED"
    ]
    status = "diverged" if diverged_results else "warning" if failed_results else "watching" if open_local_orders else "healthy"
    return {
        "status": status,
        "source": settings.operator_results_stream,
        "open_local_orders": open_local_orders,
        "pending_cancel_requests": 0,
        "diverged_cancel_requests": len(diverged_results),
        "stale_orders": 0,
        "recent_event_count": len(diverged_results) + len(failed_results),
        "events_by_severity": {
            "error": len(diverged_results),
            "warning": len(failed_results),
        },
        "events_by_type": {
            "control_result_diverged": len(diverged_results),
            "control_result_failed": len(failed_results),
        },
        "recent_events": [],
        "last_reconciled_at_ms": None,
    }


def report_latencies(
    reports: list[dict[str, object]], signals: dict[str, dict[str, object]]
) -> list[float]:
    values: list[float] = []
    for report in reports:
        signal = signals.get(str(report.get("signal_id", "")))
        if signal is None:
            continue
        report_ts = as_float(report.get("timestamp_ms"))
        signal_ts = as_float(signal.get("timestamp_ms"))
        if report_ts >= signal_ts > 0:
            values.append(report_ts - signal_ts)
    return values


def stage_latencies(
    reports: list[dict[str, object]], signals: dict[str, dict[str, object]]
) -> dict[str, list[float]]:
    values: dict[str, list[float]] = {
        "ws_to_signal": [],
        "signal_to_order": [],
        "order_to_report": [],
        "ws_to_report": [],
    }
    reports_by_signal: dict[str, list[dict[str, object]]] = {}
    for signal_id, signal in signals.items():
        signal_ts = as_float(signal.get("timestamp_ms"))
        source_ts = as_float(signal.get("source_timestamp_ms"))
        if signal_ts >= source_ts > 0:
            values["ws_to_signal"].append(signal_ts - source_ts)
        reports_by_signal[signal_id] = []

    for report in reports:
        signal_id = str(report.get("signal_id", ""))
        if signal_id in reports_by_signal:
            reports_by_signal[signal_id].append(report)

    terminal_statuses = {"MATCHED", "CANCELLED", "ERROR"}
    for signal_id, signal_reports in reports_by_signal.items():
        signal = signals[signal_id]
        signal_ts = as_float(signal.get("timestamp_ms"))
        source_ts = as_float(signal.get("source_timestamp_ms"))
        ordered = sorted(signal_reports, key=lambda report: as_float(report.get("timestamp_ms")))
        if not ordered:
            continue
        first_report_ts = as_float(ordered[0].get("timestamp_ms"))
        if first_report_ts >= signal_ts > 0:
            values["signal_to_order"].append(first_report_ts - signal_ts)
        terminal_reports = [
            report for report in ordered if report.get("status") in terminal_statuses
        ]
        if terminal_reports and first_report_ts > 0:
            terminal_ts = as_float(terminal_reports[-1].get("timestamp_ms"))
            if terminal_ts >= first_report_ts:
                values["order_to_report"].append(terminal_ts - first_report_ts)
            if terminal_ts >= source_ts > 0:
                values["ws_to_report"].append(terminal_ts - source_ts)
        elif first_report_ts >= source_ts > 0:
            values["ws_to_report"].append(first_report_ts - source_ts)
    return values


def average(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def count_by_key(values: Iterable[object]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        if not value:
            continue
        key = str(value)
        counts[key] = counts.get(key, 0) + 1
    return counts


def error_type(value: object) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text.split(":", 1)[0].lower().replace(" ", "_")


def prometheus_metrics(metrics: dict[str, object]) -> str:
    names = {
        "signals_received": "polymarket_signals_received_total",
        "signals_rejected": "polymarket_signals_rejected_total",
        "orders_submitted": "polymarket_orders_submitted_total",
        "clob_errors": "polymarket_clob_errors_total",
        "execution_reports": "polymarket_execution_reports_total",
        "control_results": "polymarket_control_results_total",
        "ws_to_report_latency_ms": "polymarket_ws_to_report_latency_ms",
        "ws_to_signal_latency_ms": "polymarket_ws_to_signal_latency_ms",
        "signal_to_order_latency_ms": "polymarket_signal_to_order_latency_ms",
        "order_to_report_latency_ms": "polymarket_order_to_report_latency_ms",
    }
    lines = []
    for key, metric_name in names.items():
        value = metrics.get(key)
        if not isinstance(value, (int, float)):
            continue
        metric_type = "counter" if metric_name.endswith("_total") else "gauge"
        lines.append(f"# TYPE {metric_name} {metric_type}")
        lines.append(f"{metric_name} {value}")
    labeled_metrics = {
        "clob_errors_by_type": (
            "polymarket_clob_errors_by_type_total",
            "error_type",
        ),
        "control_results_by_type": (
            "polymarket_control_results_by_type_total",
            "command_type",
        ),
        "execution_reports_by_status": (
            "polymarket_execution_reports_by_status_total",
            "status",
        ),
    }
    for key, (metric_name, label_name) in labeled_metrics.items():
        values = metrics.get(key)
        if not isinstance(values, dict):
            continue
        lines.append(f"# TYPE {metric_name} counter")
        for label_value, value in sorted(values.items()):
            if not isinstance(value, (int, float)):
                continue
            lines.append(
                f'{metric_name}{{{label_name}="{escape_label_value(str(label_value))}"}} {value}'
            )
    return "\n".join(lines) + "\n"


def escape_label_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


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


def signal_index_from_records(
    records: Iterable[dict[str, object]],
) -> dict[str, dict[str, object]]:
    signals: dict[str, dict[str, object]] = {}
    for payload in records:
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
