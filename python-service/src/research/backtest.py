import json
from pathlib import Path

import duckdb


def create_backtest_views(db_path: Path) -> None:
    with duckdb.connect(str(db_path)) as conn:
        ensure_optional_execution_reports_view(conn)
        ensure_optional_synthetic_execution_reports_view(conn)
        create_observed_execution_reports_view(conn)
        create_canonical_execution_reports_view(conn)
        conn.execute(
            """
            create or replace view signal_fills as
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
                s.event_timestamp_ms as signal_timestamp_ms,
                er.order_id,
                er.status,
                er.filled_price,
                er.filled_size,
                er.cumulative_filled_size,
                er.remaining_size,
                er.error
            from signals s
            left join canonical_execution_reports er on er.signal_id = s.signal_id
            """
        )
        conn.execute(
            """
            create or replace view backtest_trades as
            select
                signal_id,
                market_id,
                asset_id,
                side,
                coalesce(strategy, 'unknown') as strategy,
                model_version::varchar as model_version,
                data_version::varchar as data_version,
                feature_version::varchar as feature_version,
                signal_timestamp_ms,
                signal_price,
                signal_size,
                confidence,
                order_id,
                status,
                filled_price,
                coalesce(cumulative_filled_size, filled_size, 0) as filled_size,
                case
                    when signal_size > 0 then coalesce(cumulative_filled_size, filled_size, 0) / signal_size
                    else 0
                end as fill_rate,
                case
                    when filled_price is null then null
                    when side = 'BUY' then filled_price - signal_price
                    when side = 'SELL' then signal_price - filled_price
                    else null
                end as slippage,
                coalesce(filled_price, 0) * coalesce(cumulative_filled_size, filled_size, 0) as filled_notional,
                case
                    when side = 'BUY' then confidence - signal_price
                    when side = 'SELL' then signal_price - (1 - confidence)
                    else null
                end as model_edge,
                case
                    when filled_price is null then null
                    when side = 'BUY' then confidence - signal_price - (filled_price - signal_price)
                    when side = 'SELL' then signal_price - (1 - confidence) - (signal_price - filled_price)
                    else null
                end as realized_edge_after_slippage,
                cast(error as varchar) as error
            from signal_fills
            """
        )
        conn.execute(
            """
            create or replace view backtest_summary as
            select
                strategy,
                coalesce(model_version, 'unknown') as model_version,
                coalesce(data_version, 'unknown') as data_version,
                coalesce(feature_version, 'unknown') as feature_version,
                market_id,
                side,
                count(distinct signal_id) as signals,
                count(*) as orders,
                count(distinct case when filled_size > 0 then signal_id else null end) as filled_signals,
                sum(case when filled_size > 0 then 1 else 0 end) as filled_orders,
                avg(fill_rate) as fill_rate,
                avg(slippage) as avg_slippage,
                avg(model_edge) as avg_edge,
                avg(realized_edge_after_slippage) as avg_realized_edge_after_slippage,
                sum(filled_size) as total_filled_size,
                sum(case when error is not null then 1 else 0 end) as error_count
            from backtest_trades
            group by strategy, model_version, data_version, feature_version, market_id, side
            """
        )
        create_observed_vs_synthetic_fill_views(conn)


def export_backtest_report(db_path: Path, output_dir: Path) -> dict[str, int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    create_backtest_views(db_path)
    outputs = {
        "backtest_trades": output_dir / "backtest_trades.parquet",
        "backtest_summary": output_dir / "backtest_summary.parquet",
        "observed_vs_synthetic_fills": output_dir
        / "observed_vs_synthetic_fills.parquet",
        "observed_vs_synthetic_fill_summary": output_dir
        / "observed_vs_synthetic_fill_summary.parquet",
    }
    counts: dict[str, int] = {}
    with duckdb.connect(str(db_path)) as conn:
        for view_name, target in outputs.items():
            conn.execute(
                f"copy (select * from {view_name}) to '{duckdb_literal(target.as_posix())}' (format parquet)"
            )
            row = conn.execute(f"select count(*) from {view_name}").fetchone()
            counts[view_name] = int(row[0]) if row else 0
    return counts


def create_pre_live_gate_report(db_path: Path) -> dict[str, object]:
    create_backtest_views(db_path)
    with duckdb.connect(str(db_path)) as conn:
        ensure_optional_game_theory_view(conn, "adverse_selection_by_strategy")
        summary = conn.execute(
            """
            select
                count(*) as signals,
                coalesce(sum(case when filled_size > 0 then 1 else 0 end), 0) as filled_signals,
                coalesce(avg(fill_rate), 0) as fill_rate,
                avg(slippage) as avg_slippage,
                avg(realized_edge_after_slippage) as avg_realized_edge_after_slippage,
                coalesce(sum(case when error is not null then 1 else 0 end), 0) as error_count
            from backtest_trades
            """
        ).fetchone()
        adverse = conn.execute(
            """
            select avg(adverse_30s_rate)
            from adverse_selection_by_strategy
            """
        ).fetchone()

    signals = int(summary[0]) if summary else 0
    filled_signals = int(summary[1]) if summary else 0
    fill_rate = float(summary[2]) if summary and summary[2] is not None else 0.0
    avg_slippage = float(summary[3]) if summary and summary[3] is not None else None
    realized_edge = float(summary[4]) if summary and summary[4] is not None else None
    error_count = int(summary[5]) if summary else 0
    adverse_30s_rate = float(adverse[0]) if adverse and adverse[0] is not None else None
    checks = {
        "has_signals": signals > 0,
        "has_fills": filled_signals > 0,
        "positive_realized_edge_after_slippage": realized_edge is not None
        and realized_edge > 0,
        "acceptable_error_rate": signals > 0 and error_count / signals <= 0.01,
        "no_persistent_adverse_selection": adverse_30s_rate is None or adverse_30s_rate < 0.50,
    }
    return {
        "signals": signals,
        "filled_signals": filled_signals,
        "fill_rate": fill_rate,
        "avg_slippage": avg_slippage,
        "avg_realized_edge_after_slippage": realized_edge,
        "adverse_30s_rate": adverse_30s_rate,
        "error_count": error_count,
        "checks": checks,
        "passed": all(checks.values()),
    }


def export_pre_live_gate_report(db_path: Path, output_dir: Path) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    report = create_pre_live_gate_report(db_path)
    (output_dir / "pre_live_gate.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return report


def duckdb_literal(value: str) -> str:
    return value.replace("'", "''")


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
            error,
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
                        report_source_priority asc,
                        event_timestamp_ms desc nulls last,
                        coalesce(cumulative_filled_size, filled_size, 0) desc
                ) as report_rank
            from (
                select
                    0 as report_source_priority,
                    signal_id,
                    order_id,
                    status,
                    cast(filled_price as double) as filled_price,
                    cast(filled_size as double) as filled_size,
                    cast(cumulative_filled_size as double) as cumulative_filled_size,
                    cast(remaining_size as double) as remaining_size,
                    cast(error as varchar) as error,
                    event_timestamp_ms
                from execution_reports
                union all
                select
                    1 as report_source_priority,
                    signal_id,
                    order_id,
                    status,
                    cast(filled_price as double) as filled_price,
                    cast(filled_size as double) as filled_size,
                    cast(cumulative_filled_size as double) as cumulative_filled_size,
                    cast(remaining_size as double) as remaining_size,
                    cast(error as varchar) as error,
                    event_timestamp_ms
                from synthetic_execution_reports
            )
        )
        where report_rank = 1
        """
    )


def create_observed_execution_reports_view(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        create or replace view observed_execution_reports as
        select
            signal_id,
            order_id,
            status,
            cast(filled_price as double) as filled_price,
            cast(filled_size as double) as filled_size,
            cast(cumulative_filled_size as double) as cumulative_filled_size,
            cast(remaining_size as double) as remaining_size,
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


def create_observed_vs_synthetic_fill_views(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        create or replace view observed_vs_synthetic_fills as
        select
            s.signal_id,
            s.market_id,
            s.asset_id,
            s.side,
            coalesce(s.strategy, 'unknown') as strategy,
            s.model_version::varchar as model_version,
            s.data_version::varchar as data_version,
            s.feature_version::varchar as feature_version,
            s.price as signal_price,
            s.size as signal_size,
            s.confidence,
            s.event_timestamp_ms as signal_timestamp_ms,
            observed.order_id as observed_order_id,
            observed.status as observed_status,
            observed.filled_price as observed_filled_price,
            observed.filled_size as observed_filled_size,
            observed.cumulative_filled_size as observed_cumulative_filled_size,
            observed.remaining_size as observed_remaining_size,
            observed.event_timestamp_ms as observed_report_timestamp_ms,
            synthetic.order_id as synthetic_order_id,
            synthetic.status as synthetic_status,
            synthetic.filled_price as synthetic_filled_price,
            synthetic.filled_size as synthetic_filled_size,
            synthetic.cumulative_filled_size as synthetic_cumulative_filled_size,
            synthetic.remaining_size as synthetic_remaining_size,
            synthetic.event_timestamp_ms as synthetic_report_timestamp_ms,
            coalesce(observed.cumulative_filled_size, observed.filled_size, 0) as observed_fill_size,
            coalesce(synthetic.cumulative_filled_size, synthetic.filled_size, 0) as synthetic_fill_size,
            case
                when s.size > 0 then coalesce(observed.cumulative_filled_size, observed.filled_size, 0) / s.size
                else 0
            end as observed_fill_rate,
            case
                when s.size > 0 then coalesce(synthetic.cumulative_filled_size, synthetic.filled_size, 0) / s.size
                else 0
            end as synthetic_fill_rate,
            case
                when observed.filled_price is null then null
                when s.side = 'BUY' then observed.filled_price - s.price
                when s.side = 'SELL' then s.price - observed.filled_price
                else null
            end as observed_slippage,
            case
                when synthetic.filled_price is null then null
                when s.side = 'BUY' then synthetic.filled_price - s.price
                when s.side = 'SELL' then s.price - synthetic.filled_price
                else null
            end as synthetic_slippage,
            case
                when observed.filled_price is null then null
                when s.side = 'BUY' then s.confidence - s.price - (observed.filled_price - s.price)
                when s.side = 'SELL' then s.price - (1 - s.confidence) - (s.price - observed.filled_price)
                else null
            end as observed_realized_edge_after_slippage,
            case
                when synthetic.filled_price is null then null
                when s.side = 'BUY' then s.confidence - s.price - (synthetic.filled_price - s.price)
                when s.side = 'SELL' then s.price - (1 - s.confidence) - (s.price - synthetic.filled_price)
                else null
            end as synthetic_realized_edge_after_slippage,
            case
                when coalesce(observed.cumulative_filled_size, observed.filled_size, 0) > 0
                 and coalesce(synthetic.cumulative_filled_size, synthetic.filled_size, 0) > 0
                    then 'both'
                when coalesce(observed.cumulative_filled_size, observed.filled_size, 0) > 0
                    then 'observed_only'
                when coalesce(synthetic.cumulative_filled_size, synthetic.filled_size, 0) > 0
                    then 'synthetic_only'
                else 'neither'
            end as fill_evidence
        from signals s
        left join observed_execution_reports observed on observed.signal_id = s.signal_id
        left join synthetic_execution_reports synthetic on synthetic.signal_id = s.signal_id
        where s.signal_id is not null
        """
    )
    conn.execute(
        """
        create or replace view observed_vs_synthetic_fill_summary as
        select
            strategy,
            coalesce(model_version, 'unknown') as model_version,
            coalesce(data_version, 'unknown') as data_version,
            coalesce(feature_version, 'unknown') as feature_version,
            market_id,
            side,
            count(*) as signals,
            sum(case when observed_fill_size > 0 then 1 else 0 end) as observed_filled_signals,
            sum(case when synthetic_fill_size > 0 then 1 else 0 end) as synthetic_filled_signals,
            avg(observed_fill_rate) as observed_fill_rate,
            avg(synthetic_fill_rate) as synthetic_fill_rate,
            avg(synthetic_fill_rate - observed_fill_rate) as fill_rate_delta,
            avg(observed_slippage) as observed_avg_slippage,
            avg(synthetic_slippage) as synthetic_avg_slippage,
            avg(synthetic_slippage - observed_slippage) as slippage_delta,
            avg(observed_realized_edge_after_slippage) as observed_avg_realized_edge_after_slippage,
            avg(synthetic_realized_edge_after_slippage) as synthetic_avg_realized_edge_after_slippage,
            avg(synthetic_realized_edge_after_slippage - observed_realized_edge_after_slippage) as realized_edge_delta,
            sum(case when fill_evidence = 'both' then 1 else 0 end) as both_filled,
            sum(case when fill_evidence = 'observed_only' then 1 else 0 end) as observed_only,
            sum(case when fill_evidence = 'synthetic_only' then 1 else 0 end) as synthetic_only,
            sum(case when fill_evidence = 'neither' then 1 else 0 end) as neither_filled
        from observed_vs_synthetic_fills
        group by strategy, model_version, data_version, feature_version, market_id, side
        """
    )


def ensure_optional_execution_reports_view(conn: duckdb.DuckDBPyConnection) -> None:
    exists = conn.execute(
        """
        select count(*)
        from information_schema.tables
        where table_name = 'execution_reports'
        """
    ).fetchone()
    if exists and int(exists[0]) > 0:
        return
    conn.execute(
        """
        create or replace view execution_reports as
        select
            cast(null as varchar) as signal_id,
            cast(null as varchar) as order_id,
            cast(null as varchar) as status,
            cast(null as varchar) as model_version,
            cast(null as varchar) as data_version,
            cast(null as varchar) as feature_version,
            cast(null as double) as filled_price,
            cast(null as double) as filled_size,
            cast(null as double) as cumulative_filled_size,
            cast(null as double) as remaining_size,
            cast(null as varchar) as error,
            cast(null as bigint) as event_timestamp_ms
        where false
        """
    )


def ensure_optional_synthetic_execution_reports_view(conn: duckdb.DuckDBPyConnection) -> None:
    exists = conn.execute(
        """
        select count(*)
        from information_schema.tables
        where table_name = 'synthetic_execution_reports'
        """
    ).fetchone()
    if exists and int(exists[0]) > 0:
        return
    conn.execute(
        """
        create or replace view synthetic_execution_reports as
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


def ensure_optional_game_theory_view(conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
    exists = conn.execute(
        """
        select count(*)
        from information_schema.tables
        where table_name = ?
        """,
        [table_name],
    ).fetchone()
    if exists and int(exists[0]) > 0:
        return
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


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="research-backtest")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--pre-live-gate", action="store_true")
    args = parser.parse_args()

    counts = export_backtest_report(Path(args.duckdb), Path(args.output_dir))
    output: dict[str, object] = {"exports": counts}
    if args.pre_live_gate:
        output["pre_live_gate"] = export_pre_live_gate_report(
            Path(args.duckdb), Path(args.output_dir)
        )
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
