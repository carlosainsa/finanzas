import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd  # type: ignore[import-untyped]

from src.research.backtest import create_observed_execution_reports_view, duckdb_literal
from src.research.game_theory import ensure_base_views


REPORT_VERSION = "signal_to_order_conversion_v1"
OUTPUT_VIEWS = (
    "signal_to_order_signal_context",
    "signal_to_order_lifecycle",
    "signal_to_order_outcomes",
    "signal_to_order_root_causes",
    "signal_to_order_by_asset_strategy",
    "signal_to_order_status_summary",
    "signal_to_order_examples",
)


@dataclass(frozen=True)
class SignalToOrderConversionConfig:
    missing_report_after_ms: int = 300_000
    stale_book_ms: int = 60_000
    examples_limit: int = 100
    min_signals: int = 1

    def __post_init__(self) -> None:
        if self.missing_report_after_ms <= 0:
            raise ValueError("missing_report_after_ms must be positive")
        if self.stale_book_ms <= 0:
            raise ValueError("stale_book_ms must be positive")
        if self.examples_limit <= 0:
            raise ValueError("examples_limit must be positive")
        if self.min_signals <= 0:
            raise ValueError("min_signals must be positive")


def create_signal_to_order_conversion_report(
    db_path: Path,
    output_dir: Path,
    config: SignalToOrderConversionConfig = SignalToOrderConversionConfig(),
) -> dict[str, object]:
    create_signal_to_order_conversion_views(db_path, config)
    output_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
        counts = copy_views(conn, output_dir, OUTPUT_VIEWS)
        summary_rows = normalize_records(
            conn.execute("select * from signal_to_order_summary")
            .fetch_df()
            .to_dict(orient="records")
        )
        root_causes = normalize_records(
            conn.execute(
                """
                select *
                from signal_to_order_root_causes
                order by signals desc, root_cause
                limit 25
                """
            )
            .fetch_df()
            .to_dict(orient="records")
        )
        asset_gaps = normalize_records(
            conn.execute(
                """
                select *
                from signal_to_order_by_asset_strategy
                order by missing_reports desc, signals desc, asset_id
                limit 25
                """
            )
            .fetch_df()
            .to_dict(orient="records")
        )
    report: dict[str, object] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_signal_to_order_conversion_only",
        "config": asdict(config),
        "counts": counts,
        "summary": summary_rows[0] if summary_rows else empty_summary(),
        "top_root_causes": root_causes,
        "top_asset_strategy_gaps": asset_gaps,
        "outputs": [f"{view}.parquet" for view in OUTPUT_VIEWS]
        + ["signal_to_order_conversion.json"],
    }
    (output_dir / "signal_to_order_conversion.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return report


def create_signal_to_order_conversion_views(
    db_path: Path,
    config: SignalToOrderConversionConfig = SignalToOrderConversionConfig(),
) -> None:
    with duckdb.connect(str(db_path)) as conn:
        ensure_base_views(conn)
        create_observed_execution_reports_view(conn)
        conn.execute(
            """
            create or replace view signal_to_order_signal_context as
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
                book.event_timestamp_ms as book_timestamp_ms,
                case
                    when book.event_timestamp_ms is not null and s.event_timestamp_ms is not null
                    then s.event_timestamp_ms - book.event_timestamp_ms
                    else null
                end as book_age_ms,
                book.best_bid,
                book.best_ask,
                book.spread,
                book.bid_depth,
                book.ask_depth,
                case
                    when book.best_bid is null or book.best_ask is null then 'no_book'
                    when s.side = 'BUY' and s.price > book.best_ask then 'crossing_touch'
                    when s.side = 'BUY' and abs(s.price - book.best_ask) <= 0.000000001 then 'at_touch'
                    when s.side = 'BUY' and s.price > book.best_bid then 'inside_spread'
                    when s.side = 'BUY' then 'behind_touch'
                    when s.side = 'SELL' and s.price < book.best_bid then 'crossing_touch'
                    when s.side = 'SELL' and abs(s.price - book.best_bid) <= 0.000000001 then 'at_touch'
                    when s.side = 'SELL' and s.price < book.best_ask then 'inside_spread'
                    when s.side = 'SELL' then 'behind_touch'
                    else 'unsupported_side'
                end as quote_relation
            from signals s
            left join lateral (
                select *
                from orderbook_snapshots book
                where book.market_id = s.market_id
                  and book.asset_id = s.asset_id
                  and book.event_timestamp_ms <= s.event_timestamp_ms
                order by book.event_timestamp_ms desc
                limit 1
            ) book on true
            where s.signal_id is not null
            """
        )
        conn.execute(
            """
            create or replace view signal_to_order_lifecycle as
            select
                context.*,
                coalesce(raw_reports.raw_report_rows, 0) as raw_report_rows,
                raw_reports.first_report_timestamp_ms,
                raw_reports.terminal_report_timestamp_ms,
                observed.order_id,
                observed.status as terminal_status,
                observed.error,
                observed.filled_price,
                coalesce(observed.cumulative_filled_size, observed.filled_size, 0) as filled_size,
                observed.remaining_size,
                observed.event_timestamp_ms as terminal_report_event_timestamp_ms,
                case
                    when observed.event_timestamp_ms is not null and context.signal_timestamp_ms is not null
                    then observed.event_timestamp_ms - context.signal_timestamp_ms
                    else null
                end as report_latency_ms,
                observed.order_id is not null and observed.order_id <> '' as order_created
            from signal_to_order_signal_context context
            left join observed_execution_reports observed
              on observed.signal_id = context.signal_id
            left join (
                select
                    signal_id,
                    count(*) as raw_report_rows,
                    min(event_timestamp_ms) as first_report_timestamp_ms,
                    max(event_timestamp_ms) as terminal_report_timestamp_ms
                from execution_reports
                group by signal_id
            ) raw_reports on raw_reports.signal_id = context.signal_id
            """
        )
        conn.execute(
            f"""
            create or replace view signal_to_order_outcomes as
            select
                *,
                raw_report_rows > 0 as has_report,
                order_created as has_order_id,
                coalesce(order_id, '') like 'dry-run-%' as is_dry_run_order,
                coalesce(filled_size, 0) > 0 as is_filled,
                terminal_status = 'PARTIAL' as is_partial,
                terminal_status = 'ERROR' or error is not null as is_error,
                terminal_status in ('CANCELLED', 'UNMATCHED') as is_cancelled_or_unmatched,
                raw_report_rows = 0 as missing_report,
                case
                    when raw_report_rows = 0 then 'signal_only'
                    when terminal_status = 'ERROR' or error is not null then 'error_terminal'
                    when terminal_status = 'PARTIAL' then 'partial'
                    when coalesce(filled_size, 0) > 0 then 'filled'
                    when order_created then 'order_created'
                    else 'report_observed'
                end as conversion_stage,
                case
                    when signal_timestamp_ms is null or signal_price is null or signal_size is null
                    then 'missing_signal_fields'
                    when book_timestamp_ms is null then 'missing_book_context'
                    when book_age_ms > {config.stale_book_ms} then 'stale_book_context'
                    when raw_report_rows = 0 then 'missing_execution_report'
                    when terminal_status = 'ERROR' or error is not null then 'executor_reported_error'
                    when terminal_status = 'PARTIAL' then 'order_created_partial'
                    when coalesce(filled_size, 0) > 0 then 'order_created_filled'
                    when order_created and terminal_status in ('UNMATCHED', 'CANCELLED', 'DELAYED')
                    then 'order_created_unfilled'
                    when terminal_status is not null then 'unsupported_or_unknown_status'
                    else 'unknown_no_report'
                end as root_cause
            from signal_to_order_lifecycle
            """
        )
        conn.execute(
            """
            create or replace view signal_to_order_root_causes as
            select
                root_cause,
                strategy,
                asset_id,
                terminal_status,
                count(*) as signals,
                sum(case when has_report then 1 else 0 end) as reports,
                sum(case when has_order_id then 1 else 0 end) as orders_created,
                sum(case when is_filled then 1 else 0 end) as filled_signals,
                sum(case when is_error then 1 else 0 end) as error_reports,
                avg(report_latency_ms) as avg_report_latency_ms
            from signal_to_order_outcomes
            group by root_cause, strategy, asset_id, terminal_status
            """
        )
        conn.execute(
            """
            create or replace view signal_to_order_by_asset_strategy as
            select
                market_id,
                asset_id,
                strategy,
                side,
                model_version,
                count(*) as signals,
                coalesce(sum(case when has_report then 1 else 0 end), 0) as reports,
                coalesce(sum(case when missing_report then 1 else 0 end), 0) as missing_reports,
                coalesce(sum(case when has_order_id then 1 else 0 end), 0) as orders_created,
                coalesce(sum(case when is_filled then 1 else 0 end), 0) as filled_signals,
                coalesce(sum(case when is_partial then 1 else 0 end), 0) as partial_signals,
                coalesce(sum(case when is_error then 1 else 0 end), 0) as error_reports,
                sum(case when has_report then 1 else 0 end)::double / count(*) as report_rate,
                sum(case when has_order_id then 1 else 0 end)::double / count(*) as order_creation_rate,
                sum(case when is_filled then 1 else 0 end)::double / count(*) as fill_rate,
                sum(case when missing_report then 1 else 0 end)::double / count(*) as missing_report_rate,
                sum(case when is_error then 1 else 0 end)::double / count(*) as error_rate,
                avg(report_latency_ms) as avg_report_latency_ms
            from signal_to_order_outcomes
            group by market_id, asset_id, strategy, side, model_version
            """
        )
        conn.execute(
            """
            create or replace view signal_to_order_status_summary as
            select
                coalesce(terminal_status, 'NO_REPORT') as terminal_status,
                conversion_stage,
                count(*) as signals,
                sum(case when has_report then 1 else 0 end) as reports,
                sum(case when has_order_id then 1 else 0 end) as orders_created,
                sum(case when is_filled then 1 else 0 end) as filled_signals
            from signal_to_order_outcomes
            group by terminal_status, conversion_stage
            """
        )
        conn.execute(
            f"""
            create or replace view signal_to_order_examples as
            select *
            from signal_to_order_outcomes
            order by
                case root_cause
                    when 'missing_execution_report' then 1
                    when 'executor_reported_error' then 2
                    when 'order_created_unfilled' then 3
                    else 4
                end,
                signal_timestamp_ms desc nulls last,
                signal_id
            limit {config.examples_limit}
            """
        )
        conn.execute(
            """
            create or replace view signal_to_order_summary as
            select
                count(*) as signals,
                coalesce(sum(case when has_report then 1 else 0 end), 0) as reports,
                coalesce(sum(case when missing_report then 1 else 0 end), 0) as missing_reports,
                coalesce(sum(case when has_order_id then 1 else 0 end), 0) as orders_created,
                coalesce(sum(case when is_filled then 1 else 0 end), 0) as filled_signals,
                coalesce(sum(case when is_partial then 1 else 0 end), 0) as partial_signals,
                coalesce(sum(case when is_error then 1 else 0 end), 0) as error_reports,
                case when count(*) > 0 then sum(case when has_report then 1 else 0 end)::double / count(*) else 0 end as report_rate,
                case when count(*) > 0 then sum(case when has_order_id then 1 else 0 end)::double / count(*) else 0 end as order_creation_rate,
                case when count(*) > 0 then sum(case when is_filled then 1 else 0 end)::double / count(*) else 0 end as fill_rate,
                case when count(*) > 0 then sum(case when missing_report then 1 else 0 end)::double / count(*) else 0 end as missing_report_rate,
                case when count(*) > 0 then sum(case when is_error then 1 else 0 end)::double / count(*) else 0 end as error_rate,
                avg(report_latency_ms) as avg_report_latency_ms
            from signal_to_order_outcomes
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


def empty_summary() -> dict[str, object]:
    return {
        "signals": 0,
        "reports": 0,
        "missing_reports": 0,
        "orders_created": 0,
        "filled_signals": 0,
        "partial_signals": 0,
        "error_reports": 0,
        "report_rate": 0.0,
        "order_creation_rate": 0.0,
        "fill_rate": 0.0,
        "missing_report_rate": 0.0,
        "error_rate": 0.0,
        "avg_report_latency_ms": None,
    }


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
    parser = argparse.ArgumentParser(description="Measure signal to order conversion")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--missing-report-after-ms",
        type=int,
        default=SignalToOrderConversionConfig.missing_report_after_ms,
    )
    parser.add_argument(
        "--stale-book-ms",
        type=int,
        default=SignalToOrderConversionConfig.stale_book_ms,
    )
    parser.add_argument(
        "--examples-limit",
        type=int,
        default=SignalToOrderConversionConfig.examples_limit,
    )
    parser.add_argument(
        "--min-signals",
        type=int,
        default=SignalToOrderConversionConfig.min_signals,
    )
    args = parser.parse_args()
    report = create_signal_to_order_conversion_report(
        Path(args.duckdb),
        Path(args.output_dir),
        SignalToOrderConversionConfig(
            missing_report_after_ms=args.missing_report_after_ms,
            stale_book_ms=args.stale_book_ms,
            examples_limit=args.examples_limit,
            min_signals=args.min_signals,
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
