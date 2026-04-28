import json
import os
from pathlib import Path

import duckdb

from src.research.backtest import create_backtest_views
from src.research.game_theory import duckdb_literal, ensure_base_views, relation_exists


MARKET_REGIME_OUTPUTS = (
    "market_regime_summary",
    "market_tail_risk",
    "whale_pressure",
    "market_regime_point_in_time",
    "market_regime_trade_context",
    "market_regime_trade_buckets",
    "market_regime_bucket_drawdown",
    "market_regime_bucket_performance",
)

MARKET_REGIME_EXPORT_SOURCES = {
    "market_regime_trade_context": "market_regime_trade_context_export",
    "market_regime_trade_buckets": "market_regime_trade_buckets_export",
    "market_regime_bucket_drawdown": "market_regime_bucket_drawdown_export",
    "market_regime_bucket_performance": "market_regime_bucket_performance_export",
}


def _positive_int(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    parsed = int(value)
    if parsed <= 0:
        raise ValueError("resource limits must be positive integers")
    return parsed


def create_market_regime_views(
    db_path: Path,
    *,
    max_snapshots_per_asset: int | None = None,
    max_trade_context_rows: int | None = None,
) -> None:
    create_backtest_views(db_path)
    with duckdb.connect(str(db_path)) as conn:
        ensure_base_views(conn)
        ensure_orderbook_levels_view(conn)
        snapshot_limit_filter = ""
        if max_snapshots_per_asset is not None:
            snapshot_limit_filter = (
                f"qualify row_number() over (partition by market_id, asset_id "
                f"order by event_timestamp_ms desc) <= {max_snapshots_per_asset}"
            )
        conn.execute(
            f"""
            create or replace view market_regime_source_snapshots as
            select *
            from orderbook_snapshots
            {snapshot_limit_filter}
            """
        )
        conn.execute(
            """
            create or replace view market_regime_source_levels as
            select levels.*
            from orderbook_levels levels
            join market_regime_source_snapshots snapshots
              on snapshots.market_id = levels.market_id
             and snapshots.asset_id = levels.asset_id
             and snapshots.event_timestamp_ms = levels.timestamp_ms
            """
        )
        trade_context_limit_filter = ""
        if max_trade_context_rows is not None:
            trade_context_limit_filter = (
                f"qualify row_number() over (order by signal_timestamp_ms desc nulls last, signal_id) "
                f"<= {max_trade_context_rows}"
            )
        conn.execute(
            f"""
            create or replace view market_regime_source_trades as
            select *
            from backtest_trades
            {trade_context_limit_filter}
            """
        )
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
            from market_regime_source_snapshots
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
                    order by event_timestamp_ms
                    rows between unbounded preceding and current row
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
            create or replace view market_regime_point_in_time as
            with point_features as (
                select
                    market_id,
                    asset_id,
                    event_timestamp_ms,
                    abs_log_return,
                    count(*) over point_window as snapshots_so_far,
                    avg(spread) over point_window as avg_spread,
                    avg(total_depth) over point_window as avg_total_depth,
                    avg(abs_log_return) over point_window as avg_abs_return,
                    stddev_samp(log_return) over point_window as realized_volatility,
                    max(abs_log_return) over point_window as max_abs_return,
                    min(max_mid_drawdown) over point_window as max_mid_drawdown,
                    corr(log_return, lag_log_return) over point_window as return_autocorrelation,
                    corr(abs_log_return, lag_abs_log_return) over point_window as volatility_cluster_score,
                    least(
                        0.95,
                        greatest(
                            0.05,
                            0.5 + 0.25 * coalesce(corr(log_return, lag_log_return) over point_window, 0)
                        )
                    ) as hurst_proxy,
                    2 - least(
                        0.95,
                        greatest(
                            0.05,
                            0.5 + 0.25 * coalesce(corr(log_return, lag_log_return) over point_window, 0)
                        )
                    ) as fractal_dimension_proxy,
                    quantile_cont(abs_log_return, 0.90) over point_window as point_in_time_tail_threshold
                from market_regime_features
                window point_window as (
                    partition by market_id, asset_id
                    order by event_timestamp_ms
                    rows between unbounded preceding and current row
                )
            )
            select
                market_id,
                asset_id,
                event_timestamp_ms,
                snapshots_so_far,
                avg_spread,
                avg_total_depth,
                avg_abs_return,
                realized_volatility,
                max_abs_return,
                max_mid_drawdown,
                return_autocorrelation,
                volatility_cluster_score,
                hurst_proxy,
                fractal_dimension_proxy,
                point_in_time_tail_threshold,
                sum(
                    case
                        when abs_log_return >= point_in_time_tail_threshold
                         and point_in_time_tail_threshold > 0
                        then 1
                        else 0
                    end
                ) over (
                    partition by market_id, asset_id
                    order by event_timestamp_ms
                    rows between unbounded preceding and current row
                ) as tail_events_so_far
            from point_features
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
            from market_regime_source_levels
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
                from market_regime_source_levels
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
            create or replace view whale_pressure_point_in_time as
            with level_enriched as (
                select
                    top_levels.*,
                    avg(max_level_size) over (
                        partition by market_id, asset_id
                        order by event_timestamp_ms
                        rows between unbounded preceding and current row
                    ) as avg_level_size_so_far
                from orderbook_top_levels top_levels
            ),
            pressure_inputs as (
                select
                    m.market_id,
                    m.asset_id,
                    m.event_timestamp_ms,
                    m.orderbook_imbalance,
                    m.total_depth_delta,
                    coalesce(l.max_level_size, 0) as max_level_size,
                    coalesce(l.avg_level_size_so_far, 0) as avg_level_size_so_far
                from market_regime_returns m
                left join level_enriched l using (market_id, asset_id, event_timestamp_ms)
            ),
            pressure_windows as (
                select
                    *,
                    avg(
                        case
                            when max_level_size >= avg_level_size_so_far
                             and avg_level_size_so_far > 0
                            then 1.0
                            else 0.0
                        end
                    ) over pressure_window as large_order_ratio,
                    avg(
                        case
                            when total_depth_delta < -1 * greatest(avg_level_size_so_far, 1.0)
                            then 1.0
                            else 0.0
                        end
                    ) over pressure_window as depth_withdrawal_rate
                from pressure_inputs
                window pressure_window as (
                    partition by market_id, asset_id
                    order by event_timestamp_ms
                    rows between unbounded preceding and current row
                )
            )
            select
                market_id,
                asset_id,
                event_timestamp_ms,
                large_order_ratio,
                depth_withdrawal_rate,
                least(
                    1.0,
                    greatest(
                        0.0,
                        0.45 * large_order_ratio
                        + 0.35 * depth_withdrawal_rate
                        + 0.20 * abs(coalesce(orderbook_imbalance, 0))
                    )
                ) as whale_pressure_score
            from pressure_windows
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
                p.event_timestamp_ms as regime_timestamp_ms,
                p.realized_volatility,
                p.volatility_cluster_score,
                p.hurst_proxy,
                p.fractal_dimension_proxy,
                p.max_mid_drawdown,
                p.point_in_time_tail_threshold,
                p.tail_events_so_far as tail_events,
                wpt.whale_pressure_score,
                wpt.large_order_ratio,
                wpt.depth_withdrawal_rate,
                case
                    when p.point_in_time_tail_threshold is null then 'unknown'
                    when p.max_abs_return >= p.point_in_time_tail_threshold and p.point_in_time_tail_threshold > 0 then 'heavy_tail'
                    when p.tail_events_so_far >= 2 then 'moderate_tail'
                    else 'thin_tail'
                end as tail_risk_bucket,
                case
                    when p.volatility_cluster_score is null then 'unknown'
                    when p.volatility_cluster_score >= 0.5 then 'high_cluster'
                    when p.volatility_cluster_score >= 0.1 then 'medium_cluster'
                    else 'low_cluster'
                end as volatility_cluster_bucket,
                case
                    when p.hurst_proxy is null then 'unknown'
                    when p.hurst_proxy >= 0.55 then 'persistent'
                    when p.hurst_proxy <= 0.45 then 'mean_reverting'
                    else 'diffusive'
                end as hurst_bucket,
                case
                    when wpt.whale_pressure_score is null then 'unknown'
                    when wpt.whale_pressure_score >= 0.5 then 'high_whale_pressure'
                    when wpt.whale_pressure_score >= 0.2 then 'medium_whale_pressure'
                    else 'low_whale_pressure'
                end as whale_pressure_bucket,
                case
                    when coalesce(t.realized_edge_after_slippage, 0) < 0 then 1
                    else 0
                end as adverse_edge_event
            from market_regime_source_trades t
            asof left join market_regime_point_in_time p
              on t.market_id = p.market_id
             and t.asset_id = p.asset_id
             and t.signal_timestamp_ms >= p.event_timestamp_ms
            asof left join whale_pressure_point_in_time wpt
              on t.market_id = wpt.market_id
             and t.asset_id = wpt.asset_id
             and t.signal_timestamp_ms >= wpt.event_timestamp_ms
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


def export_market_regime_report(
    db_path: Path,
    output_dir: Path,
    *,
    resource_mode: str = "full",
    max_snapshots_per_asset: int | None = None,
    max_trade_context_rows: int | None = None,
) -> dict[str, object]:
    if resource_mode not in {"full", "resource_limited"}:
        raise ValueError("resource_mode must be full or resource_limited")
    output_dir.mkdir(parents=True, exist_ok=True)
    create_market_regime_views(
        db_path,
        max_snapshots_per_asset=max_snapshots_per_asset
        if resource_mode == "resource_limited"
        else None,
        max_trade_context_rows=max_trade_context_rows
        if resource_mode == "resource_limited"
        else None,
    )
    counts: dict[str, int] = {}
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("set preserve_insertion_order = false")
        if memory_limit := os.environ.get("MARKET_REGIME_DUCKDB_MEMORY_LIMIT"):
            conn.execute(f"set memory_limit = '{memory_limit}'")
        if threads := _positive_int(os.environ.get("MARKET_REGIME_DUCKDB_THREADS")):
            conn.execute(f"set threads = {threads}")
        if temp_dir := os.environ.get("MARKET_REGIME_DUCKDB_TEMP_DIR"):
            Path(temp_dir).mkdir(parents=True, exist_ok=True)
            conn.execute(f"set temp_directory = '{duckdb_literal(temp_dir)}'")
        conn.execute(
            """
            create or replace temp table market_regime_trade_context_materialized as
            select * from market_regime_trade_context
            """
        )
        conn.execute(
            """
            create or replace temp table market_regime_trade_buckets_materialized as
                select 'tail_risk' as bucket_type, tail_risk_bucket as bucket, * from market_regime_trade_context_materialized
                union all
                select 'volatility_cluster' as bucket_type, volatility_cluster_bucket as bucket, * from market_regime_trade_context_materialized
                union all
                select 'hurst' as bucket_type, hurst_bucket as bucket, * from market_regime_trade_context_materialized
                union all
                select 'whale_pressure' as bucket_type, whale_pressure_bucket as bucket, * from market_regime_trade_context_materialized
            """
        )
        conn.execute(
            """
            create or replace temp view market_regime_trade_context_export as
            select * from market_regime_trade_context_materialized
            """
        )
        conn.execute(
            """
            create or replace temp view market_regime_trade_buckets_export as
            select * from market_regime_trade_buckets_materialized
            """
        )
        conn.execute(
            """
            create or replace temp view market_regime_bucket_drawdown_export as
            with bucket_pnl as (
                select
                    bucket_type,
                    bucket,
                    coalesce(strategy, 'unknown') as strategy,
                    side,
                    signal_timestamp_ms,
                    signal_id,
                    coalesce(realized_edge_after_slippage, 0) * coalesce(filled_size, 0) as pnl
                from market_regime_trade_buckets_materialized
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
            create or replace temp view market_regime_bucket_performance_export as
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
                from market_regime_trade_buckets_materialized
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
            left join market_regime_bucket_drawdown_export d
              on d.bucket_type = a.bucket_type
             and d.bucket = a.bucket
             and d.strategy = a.strategy
             and d.side = a.side
            """
        )
        for view_name in MARKET_REGIME_OUTPUTS:
            source_name = MARKET_REGIME_EXPORT_SOURCES.get(view_name, view_name)
            target = output_dir / f"{view_name}.parquet"
            conn.execute(
                f"copy (select * from {source_name}) to '{duckdb_literal(target.as_posix())}' (format parquet)"
            )
            row = conn.execute(f"select count(*) from {source_name}").fetchone()
            counts[view_name] = int(row[0]) if row else 0
    report: dict[str, object] = {
        "report_version": "market_regime_diagnostics_v1",
        "resource_mode": resource_mode,
        "limits": {
            "max_snapshots_per_asset": max_snapshots_per_asset,
            "max_trade_context_rows": max_trade_context_rows,
        },
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
    parser.add_argument(
        "--resource-mode",
        choices=("full", "resource_limited"),
        default=os.environ.get("RESEARCH_RESOURCE_MODE", "full"),
    )
    parser.add_argument(
        "--max-snapshots-per-asset",
        type=int,
        default=_positive_int(os.environ.get("MARKET_REGIME_MAX_SNAPSHOTS_PER_ASSET")),
    )
    parser.add_argument(
        "--max-trade-context-rows",
        type=int,
        default=_positive_int(os.environ.get("MARKET_REGIME_MAX_TRADE_CONTEXT_ROWS")),
    )
    args = parser.parse_args()

    report = export_market_regime_report(
        Path(args.duckdb),
        Path(args.output_dir),
        resource_mode=args.resource_mode,
        max_snapshots_per_asset=args.max_snapshots_per_asset,
        max_trade_context_rows=args.max_trade_context_rows,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
