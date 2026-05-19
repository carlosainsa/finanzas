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
    min_signalable_snapshots: int = 0
    min_signalable_density: float = 0.0
    signal_min_spread: float = 0.01
    signal_min_depth: float = 1.5
    recent_signalable_window_ms: int = 180_000
    freshness_ordering: str = "score_first"
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
        if self.min_signalable_snapshots < 0:
            raise ValueError("min_signalable_snapshots must be non-negative")
        if not 0 <= self.min_signalable_density <= 1:
            raise ValueError("min_signalable_density must be between 0 and 1")
        if self.signal_min_spread < 0:
            raise ValueError("signal_min_spread must be non-negative")
        if self.signal_min_depth < 0:
            raise ValueError("signal_min_depth must be non-negative")
        if self.recent_signalable_window_ms <= 0:
            raise ValueError("recent_signalable_window_ms must be positive")
        if self.freshness_ordering not in {"score_first", "freshest_first"}:
            raise ValueError("freshness_ordering must be score_first or freshest_first")
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
        ensure_orderbook_levels(conn)
        ensure_predictor_decisions(conn)
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
        recent_min_timestamp_ms = max(
            0,
            max_timestamp_ms - config.recent_signalable_window_ms,
        )
        ranking_order_sql = (
            """
                        current_is_signalable desc,
                        predictor_accept_density desc nulls last,
                        last_signalable_timestamp_ms desc nulls last,
                        recent_signalable_density desc,
                        book_age_ms asc,
                        stale_rate asc,
                        runtime_touch_score desc,
                        asset_id
            """
            if config.freshness_ordering == "freshest_first"
            else """
                        runtime_touch_score desc,
                        predictor_accept_density desc nulls last,
                        signalable_density desc,
                        signalable_snapshots desc,
                        touch_change_rate desc,
                        active_minutes desc,
                        snapshots desc,
                        asset_id
            """
        )
        conn.execute(
            f"""
            create or replace view runtime_touch_recent_books as
            with top_levels as (
                select
                    market_id,
                    asset_id,
                    timestamp_ms as event_timestamp_ms,
                    max(case when side = 'bid' and level_index = 0 then size else null end) as top_bid_size,
                    max(case when side = 'ask' and level_index = 0 then size else null end) as top_ask_size
                from orderbook_levels
                group by market_id, asset_id, timestamp_ms
            )
            select
                snapshots.market_id,
                snapshots.asset_id,
                snapshots.event_timestamp_ms,
                snapshots.best_bid,
                snapshots.best_ask,
                snapshots.spread,
                snapshots.bid_depth,
                snapshots.ask_depth,
                coalesce(top_levels.top_bid_size, snapshots.bid_depth) as signal_bid_depth,
                coalesce(top_levels.top_ask_size, snapshots.ask_depth) as signal_ask_depth,
                snapshots.event_timestamp_ms - lag(snapshots.event_timestamp_ms) over (
                    partition by snapshots.market_id, snapshots.asset_id
                    order by snapshots.event_timestamp_ms
                ) as snapshot_gap_ms,
                case
                    when lag(snapshots.best_bid) over (
                        partition by snapshots.market_id, snapshots.asset_id
                        order by snapshots.event_timestamp_ms
                    ) is not null
                    and abs(
                        snapshots.best_bid - lag(snapshots.best_bid) over (
                            partition by snapshots.market_id, snapshots.asset_id
                            order by snapshots.event_timestamp_ms
                        )
                    ) > 0.000000001
                    then 1 else 0
                end as best_bid_changed,
                case
                    when lag(snapshots.best_ask) over (
                        partition by snapshots.market_id, snapshots.asset_id
                        order by snapshots.event_timestamp_ms
                    ) is not null
                    and abs(
                        snapshots.best_ask - lag(snapshots.best_ask) over (
                            partition by snapshots.market_id, snapshots.asset_id
                            order by snapshots.event_timestamp_ms
                        )
                    ) > 0.000000001
                    then 1 else 0
                end as best_ask_changed
            from orderbook_snapshots snapshots
            left join top_levels
              on top_levels.market_id = snapshots.market_id
             and top_levels.asset_id = snapshots.asset_id
             and top_levels.event_timestamp_ms = snapshots.event_timestamp_ms
            where snapshots.event_timestamp_ms >= {min_timestamp_ms}
              and snapshots.event_timestamp_ms <= {max_timestamp_ms}
              and snapshots.best_bid is not null
              and snapshots.best_ask is not null
              and snapshots.best_ask > snapshots.best_bid
              and snapshots.bid_depth > 0
              and snapshots.ask_depth > 0
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
                    {max_timestamp_ms} - max(event_timestamp_ms) as book_age_ms,
                    avg(spread) as avg_spread,
                    min(spread) as min_spread,
                    max(spread) as max_spread,
                    arg_max(spread, event_timestamp_ms) as current_spread,
                    arg_max(signal_bid_depth, event_timestamp_ms) as current_bid_depth,
                    arg_max(signal_ask_depth, event_timestamp_ms) as current_ask_depth,
                    arg_max(bid_depth + ask_depth, event_timestamp_ms) as current_total_depth,
                    case
                        when arg_max(spread, event_timestamp_ms) >= {config.signal_min_spread}
                         and least(
                             arg_max(signal_bid_depth, event_timestamp_ms),
                             arg_max(signal_ask_depth, event_timestamp_ms)
                         ) >= {config.signal_min_depth}
                        then true else false
                    end as current_is_signalable,
                    avg(bid_depth + ask_depth) as avg_total_depth,
                    avg(bid_depth) as avg_bid_depth,
                    avg(ask_depth) as avg_ask_depth,
                    avg(signal_bid_depth) as avg_signal_bid_depth,
                    avg(signal_ask_depth) as avg_signal_ask_depth,
                    sum(best_bid_changed) as best_bid_changes,
                    sum(best_ask_changed) as best_ask_changes,
                    sum(
                        case
                            when spread >= {config.signal_min_spread}
                             and least(signal_bid_depth, signal_ask_depth) >= {config.signal_min_depth}
                            then 1 else 0
                        end
                    ) as signalable_snapshots,
                    max(
                        case
                            when spread >= {config.signal_min_spread}
                             and least(signal_bid_depth, signal_ask_depth) >= {config.signal_min_depth}
                            then event_timestamp_ms else null
                        end
                    ) as last_signalable_timestamp_ms,
                    case
                        when max(
                            case
                                when spread >= {config.signal_min_spread}
                                 and least(signal_bid_depth, signal_ask_depth) >= {config.signal_min_depth}
                                then event_timestamp_ms else null
                            end
                        ) is not null
                        then {max_timestamp_ms} - max(
                            case
                                when spread >= {config.signal_min_spread}
                                 and least(signal_bid_depth, signal_ask_depth) >= {config.signal_min_depth}
                                then event_timestamp_ms else null
                            end
                        )
                        else null
                    end as last_signalable_age_ms,
                    sum(
                        case
                            when event_timestamp_ms >= {recent_min_timestamp_ms}
                            then 1 else 0
                        end
                    ) as recent_snapshots,
                    sum(
                        case
                            when event_timestamp_ms >= {recent_min_timestamp_ms}
                             and spread >= {config.signal_min_spread}
                             and least(signal_bid_depth, signal_ask_depth) >= {config.signal_min_depth}
                            then 1 else 0
                        end
                    ) as recent_signalable_snapshots,
                    avg(
                        case
                            when event_timestamp_ms >= {recent_min_timestamp_ms}
                            then case
                                when spread >= {config.signal_min_spread}
                                 and least(signal_bid_depth, signal_ask_depth) >= {config.signal_min_depth}
                                then 1.0 else 0.0
                            end
                            else null
                        end
                    ) as recent_signalable_density,
                    avg(
                        case
                            when spread >= {config.signal_min_spread}
                             and least(bid_depth, ask_depth) >= {config.signal_min_depth}
                            then 1.0 else 0.0
                        end
                    ) as signalable_density,
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
            ),
            predictor_decision_features as (
                select
                    market_id,
                    asset_id,
                    count(*) as predictor_decisions,
                    sum(case when accepted then 1 else 0 end) as predictor_accepted_count,
                    sum(case when accepted then 0 else 1 end) as predictor_rejected_count,
                    avg(case when accepted then 1.0 else 0.0 end) as predictor_accept_density,
                    max(rejection_reason) filter (
                        where not accepted
                    ) as predictor_primary_rejection_reason
                from predictor_decisions_for_ranking
                where coalesce(source_timestamp_ms, timestamp_ms) >= {min_timestamp_ms}
                  and coalesce(source_timestamp_ms, timestamp_ms) <= {max_timestamp_ms}
                group by market_id, asset_id
            )
            select
                grouped.*,
                predictor.predictor_decisions,
                predictor.predictor_accepted_count,
                predictor.predictor_rejected_count,
                predictor.predictor_accept_density,
                predictor.predictor_primary_rejection_reason,
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
                    case when coalesce(current_is_signalable, false) then 100 else 0 end
                    + coalesce(recent_signalable_density, 0) * 50
                    + coalesce(predictor.predictor_accept_density, 0) * 60
                    + coalesce(signalable_density, 0) * 20
                    - least(
                        coalesce(last_signalable_age_ms, {config.lookback_ms})::double
                        / {config.recent_signalable_window_ms},
                        4
                    ) * 15
                    - least(
                        coalesce(book_age_ms, {config.lookback_ms})::double
                        / {config.recent_signalable_window_ms},
                        4
                    ) * 10
                    - coalesce(stale_rate, 0) * 25
                ) as freshness_score,
                (
                    coalesce(touch_change_rate, 0) * 100
                    + coalesce(spread_opportunity_density, 0) * 30
                    + coalesce(signalable_density, 0) * 60
                    + coalesce(predictor.predictor_accept_density, 0) * 80
                    + least(coalesce(avg_signal_bid_depth, 0), 10000) / 1000
                    + least(coalesce(avg_signal_ask_depth, 0), 10000) / 1000
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
                     and coalesce(current_is_signalable, false)
                     and avg_spread >= {config.min_spread}
                     {max_spread_filter}
                     and signalable_snapshots >= {config.min_signalable_snapshots}
                     and signalable_density >= {config.min_signalable_density}
                     and (
                         predictor.predictor_decisions is null
                         or predictor.predictor_accepted_count > 0
                     )
                     and stale_rate <= {config.max_stale_rate}
                     and coalesce(metadata.active, true)
                     and not coalesce(metadata.closed, false)
                     and not coalesce(metadata.archived, false)
                     and coalesce(metadata.enable_order_book, true)
                    then 'PROMOTE_TO_OBSERVATION'
                    when snapshots < {config.min_snapshots} then 'NEEDS_RUNTIME_SAMPLE'
                    when touch_change_rate < {config.min_touch_change_rate} then 'KEEP_DIAGNOSTIC'
                    when not coalesce(current_is_signalable, false) then 'KEEP_DIAGNOSTIC'
                    when signalable_snapshots < {config.min_signalable_snapshots} then 'KEEP_DIAGNOSTIC'
                    when signalable_density < {config.min_signalable_density} then 'KEEP_DIAGNOSTIC'
                    when predictor.predictor_decisions is not null
                     and predictor.predictor_accepted_count = 0
                    then 'KEEP_DIAGNOSTIC'
                    else 'KEEP_DIAGNOSTIC'
                end as recommendation
            from grouped
            left join predictor_decision_features predictor
              on predictor.market_id = grouped.market_id
             and predictor.asset_id = grouped.asset_id
            left join latest_metadata metadata
              on metadata.market_id = grouped.market_id
             and metadata.asset_id = grouped.asset_id
            """
        )
        conn.execute(
            f"""
            create or replace view runtime_touch_ranking as
            select
                row_number() over (
                    order by
                        {ranking_order_sql}
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


def ensure_orderbook_levels(conn: duckdb.DuckDBPyConnection) -> None:
    if relation_exists(conn, "orderbook_levels"):
        return
    conn.execute(
        """
        create or replace view orderbook_levels as
        select
            cast(null as varchar) as market_id,
            cast(null as varchar) as asset_id,
            cast(null as bigint) as timestamp_ms,
            cast(null as varchar) as side,
            cast(null as integer) as level_index,
            cast(null as double) as size
        where false
        """
    )


def ensure_predictor_decisions(conn: duckdb.DuckDBPyConnection) -> None:
    if relation_exists(conn, "predictor_decisions"):
        columns = relation_column_names(conn, "predictor_decisions")
    else:
        conn.execute(
            """
            create or replace view predictor_decisions as
            select
                cast(null as varchar) as market_id,
                cast(null as varchar) as asset_id,
                cast(null as boolean) as accepted,
                cast(null as varchar) as rejection_reason,
                cast(null as bigint) as source_timestamp_ms,
                cast(null as bigint) as timestamp_ms
            where false
            """
        )
        columns = relation_column_names(conn, "predictor_decisions")
    source_timestamp_expr = (
        "source_timestamp_ms"
        if "source_timestamp_ms" in columns
        else "cast(null as bigint)"
    )
    timestamp_expr = (
        "timestamp_ms" if "timestamp_ms" in columns else "cast(null as bigint)"
    )
    conn.execute(
        f"""
        create or replace view predictor_decisions_for_ranking as
        select
            market_id,
            asset_id,
            accepted,
            rejection_reason,
            {source_timestamp_expr} as source_timestamp_ms,
            {timestamp_expr} as timestamp_ms
        from predictor_decisions
        """
    )


def relation_column_names(conn: duckdb.DuckDBPyConnection, relation: str) -> set[str]:
    return {str(row[0]) for row in conn.execute(f"describe {relation}").fetchall()}


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
    if isinstance(value, (dict, list, tuple)):
        return value
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
        "--min-signalable-snapshots",
        type=int,
        default=RuntimeTouchRankingConfig.min_signalable_snapshots,
    )
    parser.add_argument(
        "--min-signalable-density",
        type=float,
        default=RuntimeTouchRankingConfig.min_signalable_density,
    )
    parser.add_argument(
        "--signal-min-spread",
        type=float,
        default=RuntimeTouchRankingConfig.signal_min_spread,
    )
    parser.add_argument(
        "--signal-min-depth",
        type=float,
        default=RuntimeTouchRankingConfig.signal_min_depth,
    )
    parser.add_argument(
        "--recent-signalable-window-ms",
        type=int,
        default=RuntimeTouchRankingConfig.recent_signalable_window_ms,
    )
    parser.add_argument(
        "--freshness-ordering",
        choices=("score_first", "freshest_first"),
        default=RuntimeTouchRankingConfig.freshness_ordering,
    )
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
            min_signalable_snapshots=args.min_signalable_snapshots,
            min_signalable_density=args.min_signalable_density,
            signal_min_spread=args.signal_min_spread,
            signal_min_depth=args.signal_min_depth,
            recent_signalable_window_ms=args.recent_signalable_window_ms,
            freshness_ordering=args.freshness_ordering,
            max_stale_rate=args.max_stale_rate,
            limit=args.limit,
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
