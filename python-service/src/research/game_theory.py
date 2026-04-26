import json
from pathlib import Path

import duckdb


HORIZONS_MS = (5_000, 30_000, 300_000)


def create_game_theory_views(db_path: Path) -> None:
    with duckdb.connect(str(db_path)) as conn:
        ensure_base_views(conn)
        create_canonical_execution_reports_view(conn)
        conn.execute(
            """
            create or replace view market_marks as
            select
                market_id,
                asset_id,
                event_timestamp_ms,
                best_bid,
                best_ask,
                spread,
                bid_depth,
                ask_depth,
                case
                    when best_bid is not null and best_ask is not null then (best_bid + best_ask) / 2
                    else null
                end as mid_price
            from orderbook_snapshots
            where event_timestamp_ms is not null
            """
        )
        conn.execute(
            """
            create or replace view signal_market_context as
            select
                s.signal_id,
                s.market_id,
                s.asset_id,
                s.side,
                s.price,
                s.size,
                s.confidence,
                coalesce(s.strategy, 'unknown') as strategy,
                s.model_version::varchar as model_version,
                s.data_version::varchar as data_version,
                s.feature_version::varchar as feature_version,
                s.event_timestamp_ms as signal_timestamp_ms,
                (
                    select m.mid_price
                    from market_marks m
                    where m.market_id = s.market_id
                      and m.asset_id = s.asset_id
                      and m.event_timestamp_ms <= s.event_timestamp_ms
                    order by m.event_timestamp_ms desc
                    limit 1
                ) as signal_mid_price,
                (
                    select m.spread
                    from market_marks m
                    where m.market_id = s.market_id
                      and m.asset_id = s.asset_id
                      and m.event_timestamp_ms <= s.event_timestamp_ms
                    order by m.event_timestamp_ms desc
                    limit 1
                ) as signal_spread
            from signals s
            """
        )
        conn.execute(
            """
            create or replace view fill_events as
            select
                s.signal_id,
                s.market_id,
                s.asset_id,
                s.side,
                s.price as signal_price,
                s.size as signal_size,
                s.confidence,
                s.strategy,
                s.model_version::varchar as model_version,
                s.data_version::varchar as data_version,
                s.feature_version::varchar as feature_version,
                s.signal_timestamp_ms,
                s.signal_mid_price,
                abs(s.price - s.signal_mid_price) as distance_to_mid,
                er.order_id,
                er.status,
                er.event_timestamp_ms as fill_timestamp_ms,
                er.filled_price,
                coalesce(er.cumulative_filled_size, er.filled_size, 0) as filled_size,
                er.error
            from signal_market_context s
            join canonical_execution_reports er on er.signal_id = s.signal_id
            where er.filled_price is not null
              and coalesce(er.cumulative_filled_size, er.filled_size, 0) > 0
              and er.event_timestamp_ms is not null
            """
        )
        conn.execute(
            """
            create or replace view post_fill_pnl_horizons as
            select
                f.*,
                case when f.side = 'BUY' then marks.mark_5s - f.filled_price
                     when f.side = 'SELL' then f.filled_price - marks.mark_5s
                     else null
                end as pnl_5s,
                case when f.side = 'BUY' then marks.mark_30s - f.filled_price
                     when f.side = 'SELL' then f.filled_price - marks.mark_30s
                     else null
                end as pnl_30s,
                case when f.side = 'BUY' then marks.mark_300s - f.filled_price
                     when f.side = 'SELL' then f.filled_price - marks.mark_300s
                     else null
                end as pnl_300s,
                marks.mark_5s,
                marks.mark_30s,
                marks.mark_300s
            from fill_events f
            left join lateral (
                select
                    (
                        select m.mid_price
                        from market_marks m
                        where m.market_id = f.market_id
                          and m.asset_id = f.asset_id
                          and m.event_timestamp_ms >= f.fill_timestamp_ms + 5000
                        order by m.event_timestamp_ms asc
                        limit 1
                    ) as mark_5s,
                    (
                        select m.mid_price
                        from market_marks m
                        where m.market_id = f.market_id
                          and m.asset_id = f.asset_id
                          and m.event_timestamp_ms >= f.fill_timestamp_ms + 30000
                        order by m.event_timestamp_ms asc
                        limit 1
                    ) as mark_30s,
                    (
                        select m.mid_price
                        from market_marks m
                        where m.market_id = f.market_id
                          and m.asset_id = f.asset_id
                          and m.event_timestamp_ms >= f.fill_timestamp_ms + 300000
                        order by m.event_timestamp_ms asc
                        limit 1
                    ) as mark_300s
            ) marks on true
            """
        )
        conn.execute(
            """
            create or replace view fill_rate_by_distance_to_mid as
            select
                strategy,
                market_id,
                side,
                case
                    when distance_to_mid is null then 'unknown'
                    when distance_to_mid <= 0.005 then '000_050bps'
                    when distance_to_mid <= 0.010 then '050_100bps'
                    when distance_to_mid <= 0.025 then '100_250bps'
                    when distance_to_mid <= 0.050 then '250_500bps'
                    else '500bps_plus'
                end as distance_bucket,
                count(*) as signals,
                sum(case when filled_size > 0 then 1 else 0 end) as filled_signals,
                avg(case when signal_size > 0 then filled_size / signal_size else 0 end) as fill_rate,
                avg(distance_to_mid) as avg_distance_to_mid
            from (
                select
                    smc.strategy,
                    smc.market_id,
                    smc.side,
                    smc.size as signal_size,
                    abs(smc.price - smc.signal_mid_price) as distance_to_mid,
                    coalesce(er.cumulative_filled_size, er.filled_size, 0) as filled_size
                from signal_market_context smc
                left join canonical_execution_reports er on er.signal_id = smc.signal_id
            )
            group by strategy, market_id, side, distance_bucket
            """
        )
        conn.execute(
            """
            create or replace view adverse_selection_by_strategy as
            select
                strategy,
                market_id,
                side,
                count(*) as filled_events,
                avg(pnl_5s) as avg_pnl_5s,
                avg(pnl_30s) as avg_pnl_30s,
                avg(pnl_300s) as avg_pnl_300s,
                sum(case when pnl_30s < 0 then 1 else 0 end) as adverse_30s_count,
                avg(case when pnl_30s < 0 then 1.0 else 0.0 end) as adverse_30s_rate
            from post_fill_pnl_horizons
            group by strategy, market_id, side
            """
        )
        conn.execute(
            """
            create or replace view quote_competition as
            select
                market_id,
                asset_id,
                count(*) as snapshots,
                sum(case when quote_changed then 1 else 0 end) as quote_changes,
                avg(case when quote_changed then 1.0 else 0.0 end) as quote_change_rate,
                avg(spread) as avg_spread,
                avg(bid_depth + ask_depth) as avg_depth
            from (
                select
                    market_id,
                    asset_id,
                    spread,
                    bid_depth,
                    ask_depth,
                    best_bid is distinct from lag(best_bid) over (
                        partition by market_id, asset_id order by event_timestamp_ms
                    )
                    or best_ask is distinct from lag(best_ask) over (
                        partition by market_id, asset_id order by event_timestamp_ms
                    ) as quote_changed
                from market_marks
            )
            group by market_id, asset_id
            """
        )
        conn.execute(
            """
            create or replace view latest_market_metadata as
            select *
            from (
                select
                    *,
                    row_number() over (
                        partition by market_id, asset_id
                        order by ingested_at_ms desc nulls last
                    ) as metadata_rank
                from market_metadata
            )
            where metadata_rank = 1
            """
        )
        conn.execute(
            """
            create or replace view binary_no_arbitrage as
            select
                yes_book.market_id,
                yes_book.event_timestamp_ms,
                yes_book.asset_id as yes_asset_id,
                no_book.asset_id as no_asset_id,
                yes_meta.outcome as yes_outcome,
                no_meta.outcome as no_outcome,
                yes_book.mid_price as yes_mid,
                no_book.mid_price as no_mid,
                yes_book.asset_id as asset_a_id,
                no_book.asset_id as asset_b_id,
                yes_book.mid_price as asset_a_mid,
                no_book.mid_price as asset_b_mid,
                yes_book.mid_price + no_book.mid_price as probability_sum,
                yes_book.mid_price + no_book.mid_price - 1 as no_arbitrage_gap
            from market_marks yes_book
            join latest_market_metadata yes_meta
              on yes_meta.market_id = yes_book.market_id
             and yes_meta.asset_id = yes_book.asset_id
             and lower(yes_meta.outcome) = 'yes'
            join latest_market_metadata no_meta
              on no_meta.market_id = yes_meta.market_id
             and lower(no_meta.outcome) = 'no'
            join market_marks no_book
              on no_book.market_id = yes_book.market_id
             and no_book.asset_id = no_meta.asset_id
             and no_book.event_timestamp_ms = yes_book.event_timestamp_ms
            where yes_book.mid_price is not null
              and no_book.mid_price is not null
            """
        )


def export_game_theory_report(db_path: Path, output_dir: Path) -> dict[str, int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    create_game_theory_views(db_path)
    outputs = (
        "post_fill_pnl_horizons",
        "fill_rate_by_distance_to_mid",
        "adverse_selection_by_strategy",
        "quote_competition",
        "binary_no_arbitrage",
    )
    counts: dict[str, int] = {}
    with duckdb.connect(str(db_path)) as conn:
        for view_name in outputs:
            target = output_dir / f"{view_name}.parquet"
            conn.execute(
                f"copy (select * from {view_name}) to '{duckdb_literal(target.as_posix())}' (format parquet)"
            )
            row = conn.execute(f"select count(*) from {view_name}").fetchone()
            counts[view_name] = int(row[0]) if row else 0
    return counts


def ensure_base_views(conn: duckdb.DuckDBPyConnection) -> None:
    if not relation_exists(conn, "signals"):
        conn.execute(
            """
            create or replace view signals as
            select
                cast(null as varchar) as signal_id,
                cast(null as varchar) as market_id,
                cast(null as varchar) as asset_id,
                cast(null as varchar) as side,
                cast(null as double) as price,
                cast(null as double) as size,
                cast(null as double) as confidence,
                cast(null as varchar) as strategy,
                cast(null as varchar) as model_version,
                cast(null as varchar) as data_version,
                cast(null as varchar) as feature_version,
                cast(null as bigint) as event_timestamp_ms
            where false
            """
        )
    if not relation_exists(conn, "execution_reports"):
        conn.execute(
            """
            create or replace view execution_reports as
            select
                cast(null as varchar) as signal_id,
                cast(null as varchar) as order_id,
                cast(null as varchar) as status,
                cast(null as double) as filled_price,
                cast(null as double) as filled_size,
                cast(null as double) as cumulative_filled_size,
                cast(null as double) as remaining_size,
                cast(null as varchar) as error,
                cast(null as bigint) as event_timestamp_ms
            where false
            """
        )
    if not relation_exists(conn, "market_metadata"):
        conn.execute(
            """
            create or replace view market_metadata as
            select
                cast(null as varchar) as market_id,
                cast(null as varchar) as asset_id,
                cast(null as varchar) as outcome,
                cast(null as integer) as outcome_index,
                cast(null as varchar) as question,
                cast(null as varchar) as slug,
                cast(null as boolean) as active,
                cast(null as boolean) as closed,
                cast(null as boolean) as archived,
                cast(null as boolean) as enable_order_book,
                cast(null as double) as liquidity,
                cast(null as double) as volume,
                cast(null as double) as outcome_price,
                cast(null as varchar) as end_date,
                cast(null as varchar) as tags_json,
                cast(null as bigint) as ingested_at_ms
            where false
            """
        )
    if not relation_exists(conn, "orderbook_snapshots"):
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


def create_canonical_execution_reports_view(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        create or replace view canonical_execution_reports as
        select
            signal_id,
            order_id,
            status,
            filled_price,
            filled_size,
            cumulative_filled_size,
            remaining_size,
            cast(error as varchar) as error,
            event_timestamp_ms
        from (
            select
                *,
                row_number() over (
                    partition by signal_id, coalesce(order_id, signal_id)
                    order by
                        case status
                            when 'MATCHED' then 6
                            when 'CANCELLED' then 5
                            when 'ERROR' then 4
                            when 'PARTIAL' then 3
                            when 'UNMATCHED' then 2
                            when 'DELAYED' then 1
                            else 0
                        end desc,
                        event_timestamp_ms desc nulls last,
                        coalesce(cumulative_filled_size, filled_size, 0) desc
                ) as report_rank
            from execution_reports
        )
        where report_rank = 1
        """
    )


def relation_exists(conn: duckdb.DuckDBPyConnection, name: str) -> bool:
    row = conn.execute(
        """
        select count(*)
        from information_schema.tables
        where table_name = ?
        """,
        [name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def duckdb_literal(value: str) -> str:
    return value.replace("'", "''")


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="research-game-theory")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    counts = export_game_theory_report(Path(args.duckdb), Path(args.output_dir))
    print(json.dumps(counts, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
