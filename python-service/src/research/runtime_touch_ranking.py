import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd  # type: ignore[import-untyped]

from src.research.backtest import duckdb_literal
from src.research.game_theory import relation_exists
from src.research.market_opportunity_selector import ensure_market_metadata


REPORT_VERSION = "runtime_touch_ranking_v1"


@dataclass(frozen=True)
class RuntimeTouchRankingConfig:
    lookback_ms: int = 900_000
    min_snapshots: int = 10
    min_active_minutes: int = 2
    min_touch_change_rate: float = 0.01
    min_spread: float = 0.000625
    max_spread: float | None = None
    max_stale_rate: float = 0.10
    stale_gap_ms: int = 60_000
    limit: int = 20

    def __post_init__(self) -> None:
        if self.lookback_ms <= 0:
            raise ValueError("lookback_ms must be positive")
        if self.min_snapshots <= 0:
            raise ValueError("min_snapshots must be positive")
        if self.min_active_minutes <= 0:
            raise ValueError("min_active_minutes must be positive")
        if not 0 <= self.min_touch_change_rate <= 1:
            raise ValueError("min_touch_change_rate must be between 0 and 1")
        if self.min_spread < 0:
            raise ValueError("min_spread must be non-negative")
        if self.max_spread is not None and self.max_spread < 0:
            raise ValueError("max_spread must be non-negative")
        if self.max_spread is not None and self.min_spread > self.max_spread:
            raise ValueError("min_spread must be less than or equal to max_spread")
        if not 0 <= self.max_stale_rate <= 1:
            raise ValueError("max_stale_rate must be between 0 and 1")
        if self.stale_gap_ms <= 0:
            raise ValueError("stale_gap_ms must be positive")
        if self.limit <= 0:
            raise ValueError("limit must be positive")


def create_runtime_touch_ranking_report(
    db_path: Path,
    output_dir: Path,
    config: RuntimeTouchRankingConfig = RuntimeTouchRankingConfig(),
) -> dict[str, object]:
    create_runtime_touch_ranking_views(db_path, config)
    output_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
        counts = copy_views(
            conn,
            output_dir,
            (
                "runtime_touch_features",
                "runtime_touch_ranking",
                "selected_runtime_touch_markets",
            ),
        )
        selected = normalize_records(
            conn.execute(
                """
                select *
                from selected_runtime_touch_markets
                order by rank
                """
            )
            .fetch_df()
            .to_dict(orient="records")
        )
    report: dict[str, object] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_runtime_touch_ranking_only",
        "config": asdict(config),
        "counts": counts,
        "selected_market_asset_ids": [str(row["asset_id"]) for row in selected],
        "selected": selected,
        "outputs": [
            "runtime_touch_features.parquet",
            "runtime_touch_ranking.parquet",
            "selected_runtime_touch_markets.parquet",
            "runtime_touch_ranking.json",
        ],
    }
    (output_dir / "runtime_touch_ranking.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return report


def create_runtime_touch_ranking_views(
    db_path: Path,
    config: RuntimeTouchRankingConfig = RuntimeTouchRankingConfig(),
) -> None:
    with duckdb.connect(str(db_path)) as conn:
        ensure_orderbook_snapshots(conn)
        ensure_market_metadata(conn)
        max_ts = conn.execute(
            "select max(event_timestamp_ms) from orderbook_snapshots"
        ).fetchone()
        max_timestamp_ms = int(max_ts[0]) if max_ts and max_ts[0] is not None else 0
        min_timestamp_ms = max(0, max_timestamp_ms - config.lookback_ms)
        max_spread_enabled_sql = "true" if config.max_spread is None else "false"
        max_spread_value_sql = config.max_spread if config.max_spread is not None else 1.0
        max_spread_filter = (
            f"and avg_spread <= {config.max_spread}"
            if config.max_spread is not None
            else ""
        )
        conn.execute(
            f"""
            create or replace view runtime_touch_recent_books as
            select
                market_id,
                asset_id,
                event_timestamp_ms,
                best_bid,
                best_ask,
                spread,
                bid_depth,
                ask_depth,
                event_timestamp_ms - lag(event_timestamp_ms) over (
                    partition by market_id, asset_id
                    order by event_timestamp_ms
                ) as snapshot_gap_ms,
                case
                    when lag(best_bid) over (
                        partition by market_id, asset_id
                        order by event_timestamp_ms
                    ) is not null
                    and abs(
                        best_bid - lag(best_bid) over (
                            partition by market_id, asset_id
                            order by event_timestamp_ms
                        )
                    ) > 0.000000001
                    then 1 else 0
                end as best_bid_changed,
                case
                    when lag(best_ask) over (
                        partition by market_id, asset_id
                        order by event_timestamp_ms
                    ) is not null
                    and abs(
                        best_ask - lag(best_ask) over (
                            partition by market_id, asset_id
                            order by event_timestamp_ms
                        )
                    ) > 0.000000001
                    then 1 else 0
                end as best_ask_changed
            from orderbook_snapshots
            where event_timestamp_ms >= {min_timestamp_ms}
              and event_timestamp_ms <= {max_timestamp_ms}
              and best_bid is not null
              and best_ask is not null
              and best_ask > best_bid
              and bid_depth > 0
              and ask_depth > 0
            """
        )
        conn.execute(
            f"""
            create or replace view runtime_touch_features as
            with grouped as (
                select
                    market_id,
                    asset_id,
                    count(*) as snapshots,
                    count(distinct floor(event_timestamp_ms / 60000)) as active_minutes,
                    min(event_timestamp_ms) as first_timestamp_ms,
                    max(event_timestamp_ms) as last_timestamp_ms,
                    avg(spread) as avg_spread,
                    min(spread) as min_spread,
                    max(spread) as max_spread,
                    avg(bid_depth + ask_depth) as avg_total_depth,
                    avg(bid_depth) as avg_bid_depth,
                    avg(ask_depth) as avg_ask_depth,
                    sum(best_bid_changed) as best_bid_changes,
                    sum(best_ask_changed) as best_ask_changes,
                    case
                        when count(*) > 0
                        then (sum(best_bid_changed) + sum(best_ask_changed))::double
                           / count(*)
                        else 0
                    end as touch_change_rate,
                    avg(
                        case
                            when spread >= {config.min_spread}
                             and ({max_spread_enabled_sql} or spread <= {max_spread_value_sql})
                            then 1.0 else 0.0
                        end
                    ) as spread_opportunity_density,
                    coalesce(
                        avg(
                            case
                                when coalesce(snapshot_gap_ms, 0) > {config.stale_gap_ms}
                                then 1.0 else 0.0
                            end
                        ),
                        0
                    ) as stale_rate
                from runtime_touch_recent_books
                group by market_id, asset_id
            ),
            latest_metadata as (
                select *
                from (
                    select
                        *,
                        row_number() over (
                            partition by market_id, asset_id
                            order by ingested_at_ms desc nulls last
                        ) as rn
                    from market_metadata
                )
                where rn = 1
            )
            select
                grouped.*,
                metadata.outcome,
                metadata.question,
                metadata.slug,
                coalesce(metadata.liquidity, 0) as liquidity,
                coalesce(metadata.volume, 0) as volume,
                metadata.active,
                metadata.closed,
                metadata.archived,
                metadata.enable_order_book,
                (
                    coalesce(touch_change_rate, 0) * 100
                    + coalesce(spread_opportunity_density, 0) * 30
                    + least(coalesce(avg_total_depth, 0), 1000000) / 100000
                    + least(coalesce(metadata.liquidity, 0), 100000) / 25000
                    + coalesce(active_minutes, 0) * 2
                    - coalesce(stale_rate, 0) * 30
                    - coalesce(avg_spread, 0) * 10
                ) as runtime_touch_score,
                case
                    when snapshots >= {config.min_snapshots}
                     and active_minutes >= {config.min_active_minutes}
                     and touch_change_rate >= {config.min_touch_change_rate}
                     and avg_spread >= {config.min_spread}
                     {max_spread_filter}
                     and stale_rate <= {config.max_stale_rate}
                     and coalesce(metadata.active, true)
                     and not coalesce(metadata.closed, false)
                     and not coalesce(metadata.archived, false)
                     and coalesce(metadata.enable_order_book, true)
                    then 'PROMOTE_TO_OBSERVATION'
                    when snapshots < {config.min_snapshots} then 'NEEDS_RUNTIME_SAMPLE'
                    when touch_change_rate < {config.min_touch_change_rate} then 'KEEP_DIAGNOSTIC'
                    else 'KEEP_DIAGNOSTIC'
                end as recommendation
            from grouped
            left join latest_metadata metadata
              on metadata.market_id = grouped.market_id
             and metadata.asset_id = grouped.asset_id
            """
        )
        conn.execute(
            """
            create or replace view runtime_touch_ranking as
            select
                row_number() over (
                    order by
                        runtime_touch_score desc,
                        touch_change_rate desc,
                        active_minutes desc,
                        snapshots desc,
                        asset_id
                ) as rank,
                *
            from runtime_touch_features
            """
        )
        conn.execute(
            f"""
            create or replace view selected_runtime_touch_markets as
            select *
            from runtime_touch_ranking
            where recommendation = 'PROMOTE_TO_OBSERVATION'
            order by rank
            limit {config.limit}
            """
        )


def ensure_orderbook_snapshots(conn: duckdb.DuckDBPyConnection) -> None:
    if relation_exists(conn, "orderbook_snapshots"):
        return
    conn.execute(
        """
        create or replace view orderbook_snapshots as
        select
            cast(null as varchar) as market_id,
            cast(null as varchar) as asset_id,
            cast(null as bigint) as event_timestamp_ms,
            cast(null as double) as best_bid,
            cast(null as double) as best_ask,
            cast(null as double) as spread,
            cast(null as double) as bid_depth,
            cast(null as double) as ask_depth
        where false
        """
    )


def copy_views(
    conn: duckdb.DuckDBPyConnection,
    output_dir: Path,
    view_names: tuple[str, ...],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for view_name in view_names:
        target = output_dir / f"{view_name}.parquet"
        conn.execute(
            f"copy (select * from {view_name}) to '{duckdb_literal(target.as_posix())}' (format parquet)"
        )
        row = conn.execute(f"select count(*) from {view_name}").fetchone()
        counts[view_name] = int(row[0]) if row else 0
    return counts


def normalize_records(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return [{key: normalize_value(value) for key, value in row.items()} for row in rows]


def normalize_value(value: object) -> object:
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        return value.item()  # type: ignore[no-any-return]
    return value


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Rank markets by fresh runtime touch activity"
    )
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--lookback-ms", type=int, default=RuntimeTouchRankingConfig.lookback_ms)
    parser.add_argument("--min-snapshots", type=int, default=RuntimeTouchRankingConfig.min_snapshots)
    parser.add_argument(
        "--min-active-minutes",
        type=int,
        default=RuntimeTouchRankingConfig.min_active_minutes,
    )
    parser.add_argument(
        "--min-touch-change-rate",
        type=float,
        default=RuntimeTouchRankingConfig.min_touch_change_rate,
    )
    parser.add_argument("--min-spread", type=float, default=RuntimeTouchRankingConfig.min_spread)
    parser.add_argument("--max-spread", type=float, default=None)
    parser.add_argument(
        "--max-stale-rate",
        type=float,
        default=RuntimeTouchRankingConfig.max_stale_rate,
    )
    parser.add_argument("--limit", type=int, default=RuntimeTouchRankingConfig.limit)
    args = parser.parse_args()
    report = create_runtime_touch_ranking_report(
        Path(args.duckdb),
        Path(args.output_dir),
        RuntimeTouchRankingConfig(
            lookback_ms=args.lookback_ms,
            min_snapshots=args.min_snapshots,
            min_active_minutes=args.min_active_minutes,
            min_touch_change_rate=args.min_touch_change_rate,
            min_spread=args.min_spread,
            max_spread=args.max_spread,
            max_stale_rate=args.max_stale_rate,
            limit=args.limit,
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
