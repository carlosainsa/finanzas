import json
from collections.abc import Iterable
from typing import Any
from pathlib import Path

import asyncpg  # type: ignore[import-untyped]

from src.config import settings


_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool | None:
    global _pool
    if settings.database_url is None:
        return None
    if _pool is None:
        _pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=5)
    return _pool


async def require_pool() -> asyncpg.Pool | None:
    pool = await get_pool()
    require_canonical_state = (
        settings.require_postgres_state or settings.app_env.lower() == "production"
    )
    if pool is None and require_canonical_state:
        raise RuntimeError(
            "DATABASE_URL is required when REQUIRE_POSTGRES_STATE=true or APP_ENV=production"
        )
    if pool is not None and require_canonical_state:
        await validate_schema_version(pool)
    return pool


async def validate_schema_version(pool: asyncpg.Pool) -> None:
    exists = await pool.fetchval(
        "select to_regclass('public.schema_migrations') is not null"
    )
    if not exists:
        raise RuntimeError("schema_migrations table is missing")
    required_version = required_schema_version()
    applied = await pool.fetchval(
        "select exists(select 1 from schema_migrations where version = $1)",
        required_version,
    )
    if not applied:
        raise RuntimeError(f"required migration {required_version} is missing")


def required_schema_version() -> str:
    migrations_dir = Path(__file__).resolve().parents[3] / "shared" / "migrations"
    versions = sorted(path.stem for path in migrations_dir.glob("*.sql"))
    if not versions:
        raise RuntimeError("shared migrations directory is empty")
    return versions[-1]


async def open_orders_from_postgres(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    rows = await pool.fetch(
        """
        select er.payload
        from execution_reports er
        join (
            select order_id, max(created_at) as created_at
            from execution_reports
            where order_id <> ''
            group by order_id
        ) latest
          on er.order_id = latest.order_id
         and er.created_at = latest.created_at
        where er.status in ('Delayed', 'Unmatched', 'Partial', 'DELAYED', 'UNMATCHED', 'PARTIAL')
        order by er.created_at desc
        """
    )
    return [payload for row in rows if (payload := jsonb_payload_to_dict(row["payload"]))]


async def execution_reports_from_postgres(
    pool: asyncpg.Pool, count: int
) -> list[dict[str, Any]]:
    rows = await pool.fetch(
        """
        select payload
        from execution_reports
        order by created_at desc
        limit $1
        """,
        count,
    )
    return [payload for row in rows if (payload := jsonb_payload_to_dict(row["payload"]))]


async def control_results_from_postgres(
    pool: asyncpg.Pool, count: int
) -> list[dict[str, Any]]:
    rows = await pool.fetch(
        """
        select
            payload,
            operator,
            reason,
            error,
            command_created_at_ms,
            completed_at_ms
        from control_results
        order by updated_at desc
        limit $1
        """,
        count,
    )
    results: list[dict[str, Any]] = []
    for row in rows:
        payload = jsonb_payload_to_dict(row["payload"])
        if payload is None:
            continue
        merge_optional(payload, "operator", row["operator"])
        merge_optional(payload, "reason", row["reason"])
        merge_optional(payload, "error", row["error"])
        merge_optional(payload, "command_created_at_ms", row["command_created_at_ms"])
        merge_optional(payload, "completed_at_ms", row["completed_at_ms"])
        results.append(payload)
    return results


async def reconciliation_status_from_postgres(
    pool: asyncpg.Pool, limit: int
) -> dict[str, Any]:
    open_local_orders = await pool.fetchval(
        """
        select count(*)
        from orders
        where status in ('SUBMITTED', 'DELAYED', 'UNMATCHED', 'PARTIAL', 'Delayed', 'Unmatched', 'Partial')
           or coalesce(remaining_size, 0) > 0
        """
    )
    pending_cancel_requests = await pool.fetchval(
        "select count(*) from cancel_requests where status = 'SENT'"
    )
    diverged_cancel_requests = await pool.fetchval(
        "select count(*) from cancel_requests where status = 'DIVERGED'"
    )
    stale_orders = await pool.fetchval(
        """
        select count(*)
        from orders
        where (
            status in ('SUBMITTED', 'DELAYED', 'UNMATCHED', 'PARTIAL', 'Delayed', 'Unmatched', 'Partial')
            or coalesce(remaining_size, 0) > 0
        )
          and updated_at < now() - interval '5 minutes'
        """
    )
    event_rows = await pool.fetch(
        """
        select event_id, order_id, signal_id, event_type, severity, details, created_at
        from reconciliation_events
        order by created_at desc
        limit $1
        """,
        limit,
    )
    recent_events = [
        {
            "event_id": row["event_id"],
            "order_id": row["order_id"],
            "signal_id": row["signal_id"],
            "event_type": row["event_type"],
            "severity": row["severity"],
            "details": jsonb_payload_to_dict(row["details"]) or {},
            "created_at": row["created_at"].isoformat(),
        }
        for row in event_rows
    ]
    events_by_severity = count_by_key(event["severity"] for event in recent_events)
    events_by_type = count_by_key(event["event_type"] for event in recent_events)
    last_reconciled_at_ms = await pool.fetchval(
        """
        select floor(extract(epoch from max(created_at)) * 1000)::bigint
        from reconciliation_events
        """
    )
    return {
        "status": reconciliation_health(
            int(open_local_orders or 0),
            int(pending_cancel_requests or 0),
            int(diverged_cancel_requests or 0),
            int(stale_orders or 0),
            events_by_severity,
        ),
        "source": "postgres",
        "open_local_orders": int(open_local_orders or 0),
        "pending_cancel_requests": int(pending_cancel_requests or 0),
        "diverged_cancel_requests": int(diverged_cancel_requests or 0),
        "stale_orders": int(stale_orders or 0),
        "recent_event_count": len(recent_events),
        "events_by_severity": events_by_severity,
        "events_by_type": events_by_type,
        "recent_events": recent_events,
        "last_reconciled_at_ms": int(last_reconciled_at_ms) if last_reconciled_at_ms else None,
    }


def reconciliation_health(
    open_local_orders: int,
    pending_cancel_requests: int,
    diverged_cancel_requests: int,
    stale_orders: int,
    events_by_severity: dict[str, int],
) -> str:
    if diverged_cancel_requests > 0 or events_by_severity.get("error", 0) > 0:
        return "diverged"
    if pending_cancel_requests > 0 or stale_orders > 0 or events_by_severity.get("warning", 0) > 0:
        return "warning"
    if open_local_orders > 0:
        return "watching"
    return "healthy"


def count_by_key(values: Iterable[object]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        key = str(value)
        counts[key] = counts.get(key, 0) + 1
    return counts


def merge_optional(payload: dict[str, Any], key: str, value: Any) -> None:
    if value is not None and payload.get(key) is None:
        payload[key] = value


def jsonb_payload_to_dict(value: object) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return None
        if isinstance(parsed, dict):
            return parsed
    return None


async def positions_from_postgres(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    rows = await pool.fetch(
        """
        select market_id, asset_id, position
        from positions
        order by market_id, asset_id
        """
    )
    return [
        {
            "market_id": row["market_id"],
            "asset_id": row["asset_id"],
            "position": float(row["position"] or 0.0),
        }
        for row in rows
    ]
