from typing import Any

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
        where er.status in ('Delayed', 'Unmatched', 'DELAYED', 'UNMATCHED')
        order by er.created_at desc
        """
    )
    return [dict(row["payload"]) for row in rows if isinstance(row["payload"], dict)]


async def positions_from_postgres(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    rows = await pool.fetch(
        """
        select
            ts.market_id,
            ts.asset_id,
            sum(
                case
                    when ts.payload->>'side' = 'BUY' then
                        coalesce((er.payload->>'filled_size')::double precision, 0)
                    else
                        -coalesce((er.payload->>'filled_size')::double precision, 0)
                end
            ) as position
        from execution_reports er
        join trade_signals ts on ts.signal_id = er.signal_id
        where er.status in ('Matched', 'MATCHED')
        group by ts.market_id, ts.asset_id
        order by ts.market_id, ts.asset_id
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
