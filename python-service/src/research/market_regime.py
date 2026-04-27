import json
from pathlib import Path

import duckdb

from src.research.backtest import create_backtest_views
from src.research.game_theory import duckdb_literal, ensure_base_views, relation_exists


MARKET_REGIME_OUTPUTS = (
    "market_regime_summary",
    "market_tail_risk",
    "whale_pressure",
    "market_regime_trade_context",
    "market_regime_trade_buckets",
    "market_regime_bucket_drawdown",
    "market_regime_bucket_performance",
)


def create_market_regime_views(db_path: Path) -> None:
    create_backtest_views(db_path)
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
        conn.execute(
            """
            create or replace view market_regime_trade_context as
            select
                t.signal_id,
                t.market_id,
                t.asset_id,
                t.side,
                t.strategy,
                t.model_version::varchar as model_version,
                t.signal_timestamp_ms,
                t.signal_price,
                t.signal_size,
                t.confidence,
                t.status,
                t.filled_price,
                t.filled_size,
                t.fill_rate,
                t.slippage,
                t.model_edge,
                t.realized_edge_after_slippage,
                t.filled_notional,
                r.realized_volatility,
                r.volatility_cluster_score,
                r.hurst_proxy,
                r.fractal_dimension_proxy,
                r.max_mid_drawdown,
                r.hill_tail_index,
                r.tail_events,
                w.whale_pressure_score,
                w.large_order_ratio,
                w.depth_withdrawal_rate,
                case
                    when r.hill_tail_index is null then 'unknown'
                    when r.hill_tail_index < 2 then 'heavy_tail'
                    when r.hill_tail_index < 4 then 'moderate_tail'
                    else 'thin_tail'
                end as tail_risk_bucket,
                case
                    when r.volatility_cluster_score is null then 'unknown'
                    when r.volatility_cluster_score >= 0.5 then 'high_cluster'
                    when r.volatility_cluster_score >= 0.1 then 'medium_cluster'
                    else 'low_cluster'
                end as volatility_cluster_bucket,
                case
                    when r.hurst_proxy is null then 'unknown'
                    when r.hurst_proxy >= 0.55 then 'persistent'
                    when r.hurst_proxy <= 0.45 then 'mean_reverting'
                    else 'diffusive'
                end as hurst_bucket,
                case
                    when w.whale_pressure_score is null then 'unknown'
                    when w.whale_pressure_score >= 0.5 then 'high_whale_pressure'
                    when w.whale_pressure_score >= 0.2 then 'medium_whale_pressure'
                    else 'low_whale_pressure'
                end as whale_pressure_bucket,
                case
                    when coalesce(t.realized_edge_after_slippage, 0) < 0 then 1
                    else 0
                end as adverse_edge_event
            from backtest_trades t
            left join market_regime_summary r using (market_id, asset_id)
            left join whale_pressure w using (market_id, asset_id)
            """
        )
        conn.execute(
            """
            create or replace view market_regime_trade_buckets as
                select 'tail_risk' as bucket_type, tail_risk_bucket as bucket, * from market_regime_trade_context
                union all
                select 'volatility_cluster' as bucket_type, volatility_cluster_bucket as bucket, * from market_regime_trade_context
                union all
                select 'hurst' as bucket_type, hurst_bucket as bucket, * from market_regime_trade_context
                union all
                select 'whale_pressure' as bucket_type, whale_pressure_bucket as bucket, * from market_regime_trade_context
            """
        )
        conn.execute(
            """
            create or replace view market_regime_bucket_drawdown as
            with bucket_pnl as (
                select
                    bucket_type,
                    bucket,
                    coalesce(strategy, 'unknown') as strategy,
                    side,
                    signal_timestamp_ms,
                    signal_id,
                    coalesce(realized_edge_after_slippage, 0) * coalesce(filled_size, 0) as pnl
                from market_regime_trade_buckets
            ),
            equity as (
                select
                    *,
                    sum(pnl) over (
                        partition by bucket_type, bucket, strategy, side
                        order by signal_timestamp_ms nulls last, signal_id
                        rows between unbounded preceding and current row
                    ) as cumulative_pnl
                from bucket_pnl
            ),
            drawdowns as (
                select
                    *,
                    max(cumulative_pnl) over (
                        partition by bucket_type, bucket, strategy, side
                        order by signal_timestamp_ms nulls last, signal_id
                        rows between unbounded preceding and current row
                    ) - cumulative_pnl as drawdown
                from equity
            )
            select
                bucket_type,
                bucket,
                strategy,
                side,
                max(drawdown) as max_drawdown
            from drawdowns
            group by bucket_type, bucket, strategy, side
            """
        )
        conn.execute(
            """
            create or replace view market_regime_bucket_performance as
            with aggregates as (
                select
                    bucket_type,
                    bucket,
                    coalesce(strategy, 'unknown') as strategy,
                    side,
                    count(distinct signal_id) as signals,
                    count(distinct case when filled_size > 0 then signal_id else null end) as filled_signals,
                    avg(fill_rate) as avg_fill_rate,
                    avg(realized_edge_after_slippage) as avg_realized_edge_after_slippage,
                    sum(coalesce(realized_edge_after_slippage, 0) * coalesce(filled_size, 0)) as realized_edge_pnl,
                    avg(adverse_edge_event) as adverse_edge_rate,
                    avg(slippage) as avg_slippage,
                    avg(whale_pressure_score) as avg_whale_pressure_score,
                    avg(realized_volatility) as avg_realized_volatility,
                    avg(max_mid_drawdown) as avg_mid_drawdown
                from market_regime_trade_buckets
                group by bucket_type, bucket, strategy, side
            )
            select
                a.*,
                coalesce(d.max_drawdown, 0) as max_drawdown,
                case
                    when a.signals > 0 then a.realized_edge_pnl / a.signals
                    else null
                end as pnl_per_signal
            from aggregates a
            left join market_regime_bucket_drawdown d
              on d.bucket_type = a.bucket_type
             and d.bucket = a.bucket
             and d.strategy = a.strategy
             and d.side = a.side
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
