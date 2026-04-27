import json
from pathlib import Path

import duckdb

from src.research.game_theory import duckdb_literal, ensure_base_views, relation_exists


MARKET_REGIME_OUTPUTS = (
    "market_regime_summary",
    "market_tail_risk",
    "whale_pressure",
)


def create_market_regime_views(db_path: Path) -> None:
    with duckdb.connect(str(db_path)) as conn:
        ensure_base_views(conn)
        ensure_orderbook_levels_view(conn)
        conn.execute(
            """
            create or replace view market_regime_marks as
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
                    when best_bid is not null and best_ask is not null
                    then (best_bid + best_ask) / 2
                    else null
                end as mid_price,
                case
                    when bid_depth + ask_depth > 0
                    then (bid_depth - ask_depth) / (bid_depth + ask_depth)
                    else null
                end as orderbook_imbalance
            from orderbook_snapshots
            where event_timestamp_ms is not null
            """
        )
        conn.execute(
            """
            create or replace view market_regime_returns as
            select
                *,
                case
                    when previous_mid_price > 0 and mid_price > 0
                    then ln(mid_price / previous_mid_price)
                    else null
                end as log_return,
                abs(
                    case
                        when previous_mid_price > 0 and mid_price > 0
                        then ln(mid_price / previous_mid_price)
                        else null
                    end
                ) as abs_log_return,
                mid_price - previous_mid_price as mid_price_delta,
                bid_depth + ask_depth as total_depth,
                (bid_depth + ask_depth) - previous_total_depth as total_depth_delta
            from (
                select
                    *,
                    lag(mid_price) over (
                        partition by market_id, asset_id order by event_timestamp_ms
                    ) as previous_mid_price,
                    lag(bid_depth + ask_depth) over (
                        partition by market_id, asset_id order by event_timestamp_ms
                    ) as previous_total_depth
                from market_regime_marks
                where mid_price is not null
            )
            """
        )
        conn.execute(
            """
            create or replace view market_regime_features as
            select
                *,
                lag(log_return) over (
                    partition by market_id, asset_id order by event_timestamp_ms
                ) as lag_log_return,
                lag(abs_log_return) over (
                    partition by market_id, asset_id order by event_timestamp_ms
                ) as lag_abs_log_return,
                min(mid_price / nullif(running_mid_high, 0) - 1) over (
                    partition by market_id, asset_id
                ) as max_mid_drawdown
            from (
                select
                    *,
                    max(mid_price) over (
                        partition by market_id, asset_id
                        order by event_timestamp_ms
                        rows between unbounded preceding and current row
                    ) as running_mid_high
                from market_regime_returns
            )
            """
        )
        conn.execute(
            """
            create or replace view market_tail_risk as
            with thresholds as (
                select
                    market_id,
                    asset_id,
                    quantile_cont(abs_log_return, 0.90) as tail_threshold
                from market_regime_features
                where abs_log_return is not null and abs_log_return > 0
                group by market_id, asset_id
            ),
            tail_events as (
                select
                    f.market_id,
                    f.asset_id,
                    f.abs_log_return,
                    t.tail_threshold
                from market_regime_features f
                join thresholds t using (market_id, asset_id)
                where f.abs_log_return >= t.tail_threshold
                  and f.abs_log_return > 0
                  and t.tail_threshold > 0
            )
            select
                market_id,
                asset_id,
                count(*) as tail_events,
                max(tail_threshold) as tail_threshold,
                max(abs_log_return) as max_abs_log_return,
                case
                    when sum(ln(abs_log_return / tail_threshold)) > 0
                    then count(*) / sum(ln(abs_log_return / tail_threshold))
                    else null
                end as hill_tail_index,
                avg(abs_log_return) as avg_tail_abs_return
            from tail_events
            group by market_id, asset_id
            """
        )
        conn.execute(
            """
            create or replace view market_regime_summary as
            select
                f.market_id,
                f.asset_id,
                count(*) as snapshots,
                count(log_return) as return_observations,
                avg(spread) as avg_spread,
                avg(total_depth) as avg_total_depth,
                avg(abs_log_return) as avg_abs_return,
                stddev_samp(log_return) as realized_volatility,
                max(abs_log_return) as max_abs_return,
                min(max_mid_drawdown) as max_mid_drawdown,
                corr(log_return, lag_log_return) as return_autocorrelation,
                corr(abs_log_return, lag_abs_log_return) as volatility_cluster_score,
                least(
                    0.95,
                    greatest(0.05, 0.5 + 0.25 * coalesce(corr(log_return, lag_log_return), 0))
                ) as hurst_proxy,
                2 - least(
                    0.95,
                    greatest(0.05, 0.5 + 0.25 * coalesce(corr(log_return, lag_log_return), 0))
                ) as fractal_dimension_proxy,
                coalesce(t.hill_tail_index, null) as hill_tail_index,
                coalesce(t.tail_events, 0) as tail_events
            from market_regime_features f
            left join market_tail_risk t using (market_id, asset_id)
            group by f.market_id, f.asset_id, t.hill_tail_index, t.tail_events
            """
        )
        conn.execute(
            """
            create or replace view orderbook_top_levels as
            select
                market_id,
                asset_id,
                timestamp_ms as event_timestamp_ms,
                max(case when side = 'bid' and level_index = 0 then size else null end) as top_bid_size,
                max(case when side = 'ask' and level_index = 0 then size else null end) as top_ask_size,
                max(size) as max_level_size
            from orderbook_levels
            group by market_id, asset_id, timestamp_ms
            """
        )
        conn.execute(
            """
            create or replace view whale_pressure as
            with level_thresholds as (
                select
                    market_id,
                    asset_id,
                    quantile_cont(size, 0.95) as large_level_threshold
                from orderbook_levels
                where size is not null and size > 0
                group by market_id, asset_id
            ),
            pressure_inputs as (
                select
                    m.market_id,
                    m.asset_id,
                    m.event_timestamp_ms,
                    m.bid_depth,
                    m.ask_depth,
                    m.orderbook_imbalance,
                    m.total_depth_delta,
                    coalesce(l.top_bid_size, 0) as top_bid_size,
                    coalesce(l.top_ask_size, 0) as top_ask_size,
                    coalesce(l.max_level_size, 0) as max_level_size,
                    coalesce(t.large_level_threshold, 0) as large_level_threshold
                from market_regime_returns m
                left join orderbook_top_levels l using (market_id, asset_id, event_timestamp_ms)
                left join level_thresholds t using (market_id, asset_id)
            )
            select
                market_id,
                asset_id,
                count(*) as snapshots,
                avg(bid_depth) as avg_bid_depth,
                avg(ask_depth) as avg_ask_depth,
                avg(orderbook_imbalance) as avg_orderbook_imbalance,
                max(max_level_size) as max_level_size,
                max(large_level_threshold) as large_level_threshold,
                sum(case when max_level_size >= large_level_threshold and large_level_threshold > 0 then 1 else 0 end) as large_level_updates,
                avg(case when max_level_size >= large_level_threshold and large_level_threshold > 0 then 1.0 else 0.0 end) as large_order_ratio,
                sum(case when total_depth_delta < -1 * greatest(large_level_threshold * 0.5, 1.0) then 1 else 0 end) as depth_withdrawal_events,
                avg(case when total_depth_delta < -1 * greatest(large_level_threshold * 0.5, 1.0) then 1.0 else 0.0 end) as depth_withdrawal_rate,
                least(
                    1.0,
                    greatest(
                        0.0,
                        0.45 * avg(case when max_level_size >= large_level_threshold and large_level_threshold > 0 then 1.0 else 0.0 end)
                        + 0.35 * avg(case when total_depth_delta < -1 * greatest(large_level_threshold * 0.5, 1.0) then 1.0 else 0.0 end)
                        + 0.20 * avg(abs(coalesce(orderbook_imbalance, 0)))
                    )
                ) as whale_pressure_score
            from pressure_inputs
            group by market_id, asset_id
            """
        )


def ensure_orderbook_levels_view(conn: duckdb.DuckDBPyConnection) -> None:
    if relation_exists(conn, "orderbook_levels"):
        return
    conn.execute(
        """
        create or replace view orderbook_levels as
        select
            cast(null as varchar) as stream_id,
            cast(null as varchar) as market_id,
            cast(null as varchar) as asset_id,
            cast(null as bigint) as timestamp_ms,
            cast(null as varchar) as side,
            cast(null as integer) as level_index,
            cast(null as double) as price,
            cast(null as double) as size
        where false
        """
    )


def export_market_regime_report(db_path: Path, output_dir: Path) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    create_market_regime_views(db_path)
    counts: dict[str, int] = {}
    with duckdb.connect(str(db_path)) as conn:
        for view_name in MARKET_REGIME_OUTPUTS:
            target = output_dir / f"{view_name}.parquet"
            conn.execute(
                f"copy (select * from {view_name}) to '{duckdb_literal(target.as_posix())}' (format parquet)"
            )
            row = conn.execute(f"select count(*) from {view_name}").fetchone()
            counts[view_name] = int(row[0]) if row else 0
    report: dict[str, object] = {
        "report_version": "market_regime_diagnostics_v1",
        "decision_policy": "offline_diagnostics_only",
        "can_execute_trades": False,
        "counts": counts,
        "outputs": list(MARKET_REGIME_OUTPUTS),
    }
    (output_dir / "market_regime.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return report


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="research-market-regime")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    report = export_market_regime_report(Path(args.duckdb), Path(args.output_dir))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
