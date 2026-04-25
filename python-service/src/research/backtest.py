import json
from pathlib import Path

import duckdb


def create_backtest_views(db_path: Path) -> None:
    with duckdb.connect(str(db_path)) as conn:
        ensure_optional_execution_reports_view(conn)
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
                er.order_id,
                er.status,
                er.filled_price,
                er.filled_size,
                er.error
            from signals s
            left join execution_reports er on er.signal_id = s.signal_id
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
                signal_price,
                signal_size,
                confidence,
                order_id,
                status,
                filled_price,
                coalesce(filled_size, 0) as filled_size,
                case
                    when signal_size > 0 then coalesce(filled_size, 0) / signal_size
                    else 0
                end as fill_rate,
                case
                    when filled_price is null then null
                    when side = 'BUY' then filled_price - signal_price
                    when side = 'SELL' then signal_price - filled_price
                    else null
                end as slippage,
                coalesce(filled_price, 0) * coalesce(filled_size, 0) as filled_notional,
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
                error
            from signal_fills
            """
        )
        conn.execute(
            """
            create or replace view backtest_summary as
            select
                strategy,
                market_id,
                side,
                count(*) as signals,
                sum(case when filled_size > 0 then 1 else 0 end) as filled_signals,
                avg(fill_rate) as fill_rate,
                avg(slippage) as avg_slippage,
                avg(model_edge) as avg_edge,
                avg(realized_edge_after_slippage) as avg_realized_edge_after_slippage,
                sum(filled_size) as total_filled_size,
                sum(case when error is not null then 1 else 0 end) as error_count
            from backtest_trades
            group by strategy, market_id, side
            """
        )


def export_backtest_report(db_path: Path, output_dir: Path) -> dict[str, int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    create_backtest_views(db_path)
    outputs = {
        "backtest_trades": output_dir / "backtest_trades.parquet",
        "backtest_summary": output_dir / "backtest_summary.parquet",
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


def duckdb_literal(value: str) -> str:
    return value.replace("'", "''")


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
            cast(null as double) as filled_price,
            cast(null as double) as filled_size,
            cast(null as varchar) as error
        where false
        """
    )


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="research-backtest")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    counts = export_backtest_report(Path(args.duckdb), Path(args.output_dir))
    print(json.dumps(counts, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
