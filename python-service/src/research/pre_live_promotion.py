import json
import math
from decimal import Decimal
from dataclasses import asdict, dataclass
from pathlib import Path

import duckdb

from src.research.backtest import create_backtest_views, duckdb_literal
from src.research.calibration import create_calibration_views
from src.research.game_theory import create_game_theory_views, relation_exists


PROMOTION_REPORT_VERSION = "pre_live_promotion_v1"


@dataclass(frozen=True)
class PromotionConfig:
    min_realized_edge: float = 0.0
    min_fill_rate: float = 0.10
    max_abs_slippage: float = 0.05
    max_adverse_selection_rate: float = 0.50
    max_drawdown: float = 0.10
    max_stale_data_rate: float = 0.05
    max_reconciliation_divergence_rate: float = 0.01
    max_brier_score: float = 0.25
    stale_gap_ms: int = 60_000


def create_promotion_views(
    db_path: Path, config: PromotionConfig = PromotionConfig()
) -> None:
    ensure_minimal_input_views(db_path)
    drop_promotion_views(db_path)
    if not db_relation_exists(db_path, "backtest_trades"):
        try:
            create_backtest_views(db_path)
        except duckdb.Error:
            ensure_empty_backtest_views(db_path)
    if not db_relation_exists(db_path, "walk_forward_metrics"):
        try:
            create_calibration_views(db_path)
        except (ValueError, duckdb.Error):
            ensure_empty_calibration_views(db_path)
    if not db_relation_exists(db_path, "adverse_selection_by_strategy"):
        try:
            create_game_theory_views(db_path)
        except duckdb.Error:
            ensure_empty_game_theory_views(db_path)
    with duckdb.connect(str(db_path)) as conn:
        ensure_optional_views(conn)
        conn.execute(
            f"""
            create or replace table pre_live_equity_curve as
            select
                signal_timestamp_ms,
                cast(signal_id as varchar) as signal_id,
                coalesce(cast(order_id as varchar), '') as order_id,
                coalesce(realized_edge_after_slippage, 0) * coalesce(filled_size, 0) as pnl,
                sum(coalesce(realized_edge_after_slippage, 0) * coalesce(filled_size, 0)) over (
                    order by signal_timestamp_ms, cast(signal_id as varchar), coalesce(cast(order_id as varchar), '')
                    rows between unbounded preceding and current row
                ) as cumulative_pnl
            from backtest_trades
            where signal_timestamp_ms is not null
            """
        )
        conn.execute(
            """
            create or replace table pre_live_drawdown as
            select
                signal_timestamp_ms,
                signal_id,
                order_id,
                pnl,
                cumulative_pnl,
                max(cumulative_pnl) over (
                    order by signal_timestamp_ms, signal_id, order_id
                    rows between unbounded preceding and current row
                ) as running_peak,
                greatest(
                    0,
                    max(cumulative_pnl) over (
                        order by signal_timestamp_ms, signal_id, order_id
                        rows between unbounded preceding and current row
                    ) - cumulative_pnl
                ) as drawdown
            from pre_live_equity_curve
            """
        )
        if relation_exists(conn, "baseline_filter_decisions"):
            conn.execute(
                """
                create or replace table pre_live_stale_data as
                select
                    cast(null as varchar) as market_id,
                    cast(null as varchar) as asset_id,
                    cast(null as bigint) as event_timestamp_ms,
                    cast(null as bigint) as gap_ms,
                    not passes_stale as is_stale_gap
                from baseline_filter_decisions
                """
            )
        else:
            conn.execute(
                f"""
                create or replace table pre_live_stale_data as
                select
                    market_id,
                    asset_id,
                    event_timestamp_ms,
                    event_timestamp_ms - lag(event_timestamp_ms) over (
                        partition by market_id, asset_id
                        order by event_timestamp_ms
                    ) as gap_ms,
                    coalesce(
                        event_timestamp_ms - lag(event_timestamp_ms) over (
                            partition by market_id, asset_id
                            order by event_timestamp_ms
                        ),
                        0
                    ) > {config.stale_gap_ms} as is_stale_gap
                from orderbook_snapshots
                where event_timestamp_ms is not null
                """
            )
        if relation_exists(conn, "reconciliation_events"):
            conn.execute(
                """
                create or replace table pre_live_reconciliation_divergence as
                select
                    cast(signal_id as varchar) as signal_id,
                    cast(order_id as varchar) as order_id,
                    cast(severity as varchar) as status,
                    cast(event_type as varchar) as error,
                    case
                        when lower(coalesce(cast(severity as varchar), '')) = 'error' then true
                        when lower(coalesce(cast(event_type as varchar), '')) like '%diverg%' then true
                        when lower(coalesce(cast(event_type as varchar), '')) like '%reconcil%' then true
                        else false
                    end as is_divergent
                from reconciliation_events
                """
            )
        else:
            conn.execute(
                """
                create or replace table pre_live_reconciliation_divergence as
                select
                    cast(signal_id as varchar) as signal_id,
                    cast(order_id as varchar) as order_id,
                    cast(status as varchar) as status,
                    cast(error as varchar) as error,
                    case
                        when upper(coalesce(cast(status as varchar), '')) = 'DIVERGED' then true
                        when lower(coalesce(cast(error as varchar), '')) like '%diverg%' then true
                        when lower(coalesce(cast(error as varchar), '')) like '%reconcil%' then true
                        else false
                    end as is_divergent
                from execution_reports
                """
            )
        conn.execute(
            f"""
            create or replace table pre_live_promotion_metrics as
            with trade_metrics as (
                select
                    count(distinct signal_id) as signals,
                    count(distinct case when filled_size > 0 then signal_id else null end) as filled_signals,
                    coalesce(avg(fill_rate), 0) as fill_rate,
                    avg(slippage) as avg_slippage,
                    avg(realized_edge_after_slippage) as realized_edge
                from backtest_trades
            ),
            adverse_metrics as (
                select avg(adverse_30s_rate) as adverse_selection_rate
                from adverse_selection_by_strategy
            ),
            drawdown_metrics as (
                select coalesce(max(drawdown), 0) as max_drawdown
                from pre_live_drawdown
            ),
            stale_metrics as (
                select
                    count(*) as orderbook_snapshots,
                    coalesce(avg(case when is_stale_gap then 1.0 else 0.0 end), 0) as stale_data_rate
                from pre_live_stale_data
            ),
            reconciliation_metrics as (
                select
                    count(*) as reconciliation_events,
                    coalesce(avg(case when is_divergent then 1.0 else 0.0 end), 0) as reconciliation_divergence_rate
                from pre_live_reconciliation_divergence
            ),
            calibration_metrics as (
                select
                    avg(case when split = 'test' then brier_score else null end) as test_brier_score,
                    avg(case when split = 'test' then log_loss else null end) as test_log_loss
                from walk_forward_metrics
            )
            select
                '{PROMOTION_REPORT_VERSION}' as report_version,
                trade_metrics.signals,
                trade_metrics.filled_signals,
                trade_metrics.fill_rate,
                trade_metrics.avg_slippage,
                trade_metrics.realized_edge,
                adverse_metrics.adverse_selection_rate,
                drawdown_metrics.max_drawdown,
                stale_metrics.orderbook_snapshots,
                stale_metrics.stale_data_rate,
                reconciliation_metrics.reconciliation_events,
                reconciliation_metrics.reconciliation_divergence_rate,
                calibration_metrics.test_brier_score,
                calibration_metrics.test_log_loss
            from trade_metrics
            cross join adverse_metrics
            cross join drawdown_metrics
            cross join stale_metrics
            cross join reconciliation_metrics
            cross join calibration_metrics
            """
        )
        conn.execute(
            f"""
            create or replace table pre_live_promotion_checks as
            with metrics as (
                select *
                from pre_live_promotion_metrics
            )
            select *
            from (
                select
                    'has_signals' as check_name,
                    signals::double as metric_value,
                    1.0 as threshold,
                    signals > 0 as passed
                from metrics
                union all
                select
                    'has_fills',
                    filled_signals::double,
                    1.0,
                    filled_signals > 0
                from metrics
                union all
                select
                    'positive_realized_edge',
                    realized_edge,
                    {config.min_realized_edge},
                    realized_edge is not null and realized_edge > {config.min_realized_edge}
                from metrics
                union all
                select
                    'acceptable_fill_rate',
                    fill_rate,
                    {config.min_fill_rate},
                    fill_rate >= {config.min_fill_rate}
                from metrics
                union all
                select
                    'bounded_slippage',
                    abs(avg_slippage),
                    {config.max_abs_slippage},
                    avg_slippage is null or abs(avg_slippage) <= {config.max_abs_slippage}
                from metrics
                union all
                select
                    'no_persistent_adverse_selection',
                    adverse_selection_rate,
                    {config.max_adverse_selection_rate},
                    adverse_selection_rate is null or adverse_selection_rate <= {config.max_adverse_selection_rate}
                from metrics
                union all
                select
                    'bounded_drawdown',
                    max_drawdown,
                    {config.max_drawdown},
                    max_drawdown <= {config.max_drawdown}
                from metrics
                union all
                select
                    'fresh_market_data',
                    stale_data_rate,
                    {config.max_stale_data_rate},
                    stale_data_rate <= {config.max_stale_data_rate}
                from metrics
                union all
                select
                    'clean_reconciliation',
                    reconciliation_divergence_rate,
                    {config.max_reconciliation_divergence_rate},
                    reconciliation_divergence_rate <= {config.max_reconciliation_divergence_rate}
                from metrics
                union all
                select
                    'calibration_available',
                    test_brier_score,
                    {config.max_brier_score},
                    test_brier_score is not null and test_brier_score <= {config.max_brier_score}
                from metrics
            )
            """
        )


def create_promotion_report(
    db_path: Path, config: PromotionConfig = PromotionConfig()
) -> dict[str, object]:
    create_promotion_views(db_path, config)
    with duckdb.connect(str(db_path)) as conn:
        metrics_row = conn.execute("select * from pre_live_promotion_metrics").fetchone()
        columns = [column[0] for column in conn.description or []]
        checks = [
            {
                "check_name": str(row[0]),
                "metric_value": finite_float(row[1]),
                "threshold": finite_float(row[2]),
                "passed": bool(row[3]),
            }
            for row in conn.execute(
                """
                select check_name, metric_value, threshold, passed
                from pre_live_promotion_checks
                order by check_name
                """
            ).fetchall()
        ]
    metrics = dict(zip(columns, metrics_row, strict=False)) if metrics_row else {}
    normalized_metrics = {
        key: normalize_metric(value) for key, value in metrics.items()
    }
    add_metric_aliases(normalized_metrics)
    checks_passed = all(bool(check["passed"]) for check in checks)
    return {
        "report_version": PROMOTION_REPORT_VERSION,
        "config": asdict(config),
        "metrics": normalized_metrics,
        "availability": collect_availability(db_path),
        "checks": checks,
        "passed": checks_passed,
    }


def export_promotion_report(
    db_path: Path, output_dir: Path, config: PromotionConfig = PromotionConfig()
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    report = create_promotion_report(db_path, config)
    with duckdb.connect(str(db_path)) as conn:
        for view_name in (
            "pre_live_promotion_metrics",
            "pre_live_promotion_checks",
            "pre_live_drawdown",
            "pre_live_stale_data",
            "pre_live_reconciliation_divergence",
        ):
            target = output_dir / f"{view_name}.parquet"
            conn.execute(
                f"copy (select * from {view_name}) to '{duckdb_literal(target.as_posix())}' (format parquet)"
            )
    (output_dir / "pre_live_promotion.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return report


def ensure_optional_views(conn: duckdb.DuckDBPyConnection) -> None:
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


def ensure_minimal_input_views(db_path: Path) -> None:
    with duckdb.connect(str(db_path)) as conn:
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
                    cast(null as bigint) as event_timestamp_ms
                where false
                """
            )
        ensure_optional_views(conn)


def ensure_empty_backtest_views(db_path: Path) -> None:
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            create or replace view backtest_trades as
            select
                cast(null as varchar) as signal_id,
                cast(null as varchar) as market_id,
                cast(null as varchar) as asset_id,
                cast(null as varchar) as side,
                cast(null as varchar) as strategy,
                cast(null as varchar) as model_version,
                cast(null as varchar) as data_version,
                cast(null as varchar) as feature_version,
                cast(null as bigint) as signal_timestamp_ms,
                cast(null as double) as signal_price,
                cast(null as double) as signal_size,
                cast(null as double) as confidence,
                cast(null as varchar) as order_id,
                cast(null as varchar) as status,
                cast(null as double) as filled_price,
                cast(null as double) as filled_size,
                cast(null as double) as fill_rate,
                cast(null as double) as slippage,
                cast(null as double) as filled_notional,
                cast(null as double) as model_edge,
                cast(null as double) as realized_edge_after_slippage,
                cast(null as varchar) as error
            where false
            """
        )


def drop_promotion_views(db_path: Path) -> None:
    with duckdb.connect(str(db_path)) as conn:
        for relation_name in (
            "pre_live_promotion_checks",
            "pre_live_promotion_metrics",
            "pre_live_metrics",
            "pre_live_reconciliation_divergence",
            "pre_live_reconciliation_quality",
            "pre_live_stale_data",
            "pre_live_drawdown",
            "pre_live_equity_curve",
        ):
            drop_relation_if_exists(conn, relation_name)


def drop_relation_if_exists(conn: duckdb.DuckDBPyConnection, name: str) -> None:
    row = conn.execute(
        """
        select table_type
        from information_schema.tables
        where table_name = ?
        """,
        [name],
    ).fetchone()
    if row is None:
        return
    relation_type = str(row[0]).upper()
    if relation_type == "VIEW":
        conn.execute(f"drop view {name}")
    else:
        conn.execute(f"drop table {name}")


def db_relation_exists(db_path: Path, name: str) -> bool:
    with duckdb.connect(str(db_path)) as conn:
        return relation_exists(conn, name)


def ensure_empty_game_theory_views(db_path: Path) -> None:
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            create or replace view adverse_selection_by_strategy as
            select
                cast(null as varchar) as strategy,
                cast(null as varchar) as market_id,
                cast(null as varchar) as side,
                cast(0 as bigint) as filled_events,
                cast(null as double) as avg_pnl_5s,
                cast(null as double) as avg_pnl_30s,
                cast(null as double) as avg_pnl_300s,
                cast(0 as bigint) as adverse_30s_count,
                cast(null as double) as adverse_30s_rate
            where false
            """
        )


def ensure_empty_calibration_views(db_path: Path) -> None:
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            create or replace view walk_forward_metrics as
            select
                cast(null as varchar) as split,
                cast(0 as bigint) as samples,
                cast(null as double) as positive_rate,
                cast(null as double) as brier_score,
                cast(null as double) as log_loss,
                cast(null as double) as avg_realized_edge_after_slippage,
                cast(null as double) as avg_fill_rate
            where false
            """
        )


def normalize_metric(value: object) -> object:
    if isinstance(value, (int, float)):
        return finite_float(value)
    return value


def add_metric_aliases(metrics: dict[str, object]) -> None:
    if "avg_slippage" in metrics:
        metrics["slippage"] = metrics["avg_slippage"]
    if "adverse_selection_rate" in metrics:
        metrics["adverse_selection"] = metrics["adverse_selection_rate"]
    if "max_drawdown" in metrics:
        metrics["drawdown"] = metrics["max_drawdown"]


def collect_availability(db_path: Path) -> dict[str, bool]:
    with duckdb.connect(str(db_path)) as conn:
        return {
            "baseline": relation_exists(conn, "baseline_summary")
            or relation_exists(conn, "baseline_signals"),
            "backtest": relation_exists(conn, "backtest_trades"),
            "game_theory": relation_exists(conn, "adverse_selection_by_strategy"),
            "calibration": relation_exists(conn, "walk_forward_metrics"),
            "orderbook_snapshots": relation_exists(conn, "orderbook_snapshots"),
            "execution_reports": relation_exists(conn, "execution_reports"),
        }


def has_relation(db_path: Path, name: str) -> bool:
    with duckdb.connect(str(db_path)) as conn:
        return relation_exists(conn, name)


def finite_float(value: object) -> float | None:
    if isinstance(value, (Decimal, int, float)):
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    return None


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="research-pre-live-promotion")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--min-fill-rate", type=float, default=PromotionConfig.min_fill_rate)
    parser.add_argument("--max-stale-data-rate", type=float, default=PromotionConfig.max_stale_data_rate)
    parser.add_argument(
        "--max-reconciliation-divergence-rate",
        type=float,
        default=PromotionConfig.max_reconciliation_divergence_rate,
    )
    parser.add_argument("--stale-gap-ms", type=int, default=PromotionConfig.stale_gap_ms)
    args = parser.parse_args()

    report = export_promotion_report(
        Path(args.duckdb),
        Path(args.output_dir),
        PromotionConfig(
            min_fill_rate=args.min_fill_rate,
            max_stale_data_rate=args.max_stale_data_rate,
            max_reconciliation_divergence_rate=args.max_reconciliation_divergence_rate,
            stale_gap_ms=args.stale_gap_ms,
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
