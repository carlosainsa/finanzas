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
    return [dict(row["payload"]) for row in rows if isinstance(row["payload"], dict)]


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
