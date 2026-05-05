import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd  # type: ignore[import-untyped]

from src.research.backtest import (
    create_backtest_views,
    create_observed_execution_reports_view,
    duckdb_literal,
)
from src.research.game_theory import ensure_base_views, relation_exists
from src.research.synthetic_fills import (
    SyntheticFillConfig,
    create_synthetic_fill_views,
)


REPORT_VERSION = "quote_execution_diagnostics_v1"


@dataclass(frozen=True)
class QuoteExecutionDiagnosticsConfig:
    max_future_window_ms: int = 300_000
    examples_limit: int = 100

    def __post_init__(self) -> None:
        if self.max_future_window_ms <= 0:
            raise ValueError("max_future_window_ms must be positive")
        if self.examples_limit <= 0:
            raise ValueError("examples_limit must be positive")


def create_quote_execution_diagnostics_report(
    db_path: Path,
    output_dir: Path,
    config: QuoteExecutionDiagnosticsConfig = QuoteExecutionDiagnosticsConfig(),
) -> dict[str, object]:
    create_quote_execution_diagnostics_views(db_path, config)
    output_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
        counts = copy_views(
            conn,
            output_dir,
            (
                "quote_execution_signal_books",
                "quote_execution_lifecycle",
                "quote_execution_outcomes",
                "quote_execution_summary",
                "quote_execution_by_market_asset",
                "quote_execution_examples",
            ),
        )
        summary = normalize_records(
            conn.execute("select * from quote_execution_summary").fetch_df().to_dict(
                orient="records"
            )
        )
    report: dict[str, object] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_quote_execution_diagnostics_only",
        "config": asdict(config),
        "counts": counts,
        "summary": summary[0] if summary else {},
        "outputs": [
            "quote_execution_signal_books.parquet",
            "quote_execution_lifecycle.parquet",
            "quote_execution_outcomes.parquet",
            "quote_execution_summary.parquet",
            "quote_execution_by_market_asset.parquet",
            "quote_execution_examples.parquet",
            "quote_execution_diagnostics.json",
        ],
    }
    (output_dir / "quote_execution_diagnostics.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return report


def create_quote_execution_diagnostics_views(
    db_path: Path,
    config: QuoteExecutionDiagnosticsConfig = QuoteExecutionDiagnosticsConfig(),
) -> None:
    with duckdb.connect(str(db_path)) as conn:
        ensure_base_views(conn)
        create_observed_execution_reports_view(conn)
    ensure_synthetic_and_backtest_views(db_path)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            create or replace view quote_execution_signal_books as
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
                book.best_bid,
                book.best_ask,
                book.spread,
                book.bid_depth,
                book.ask_depth,
                book.event_timestamp_ms as book_timestamp_ms,
                case
                    when book.event_timestamp_ms is not null and s.event_timestamp_ms is not null
                    then s.event_timestamp_ms - book.event_timestamp_ms
                    else null
                end as book_age_ms,
                case
                    when book.best_bid is not null and book.best_ask is not null
                    then (book.best_bid + book.best_ask) / 2
                    else null
                end as mid_at_signal,
                case
                    when book.best_bid is not null and book.best_ask is not null
                    then abs(s.price - ((book.best_bid + book.best_ask) / 2))
                    else null
                end as distance_to_mid,
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
            create or replace view quote_execution_lifecycle as
            select
                books.*,
                coalesce(raw_reports.raw_observed_report_rows, 0) as raw_observed_report_rows,
                observed.order_id as observed_order_id,
                observed.status as observed_terminal_status,
                observed.filled_price as observed_filled_price,
                coalesce(observed.cumulative_filled_size, observed.filled_size, 0) as observed_filled_size,
                case
                    when books.signal_size > 0 then coalesce(observed.cumulative_filled_size, observed.filled_size, 0) / books.signal_size
                    else 0
                end as observed_fill_rate,
                coalesce(observed.order_id, '') like 'dry-run-%' as is_dry_run_order,
                coalesce(observed.order_id, '') like 'dry-run-%' as dry_run_created,
                coalesce(observed.order_id, '') like 'dry-run-%'
                    and coalesce(observed.cumulative_filled_size, observed.filled_size, 0) > 0 as dry_run_filled,
                case
                    when observed.event_timestamp_ms is not null and books.signal_timestamp_ms is not null
                    then observed.event_timestamp_ms - books.signal_timestamp_ms
                    else null
                end as dry_run_terminal_latency_ms,
                synthetic.order_id as synthetic_order_id,
                synthetic.status as synthetic_status,
                synthetic.filled_price as synthetic_filled_price,
                coalesce(synthetic.cumulative_filled_size, synthetic.filled_size, 0) as synthetic_filled_size,
                case
                    when books.signal_size > 0 then coalesce(synthetic.cumulative_filled_size, synthetic.filled_size, 0) / books.signal_size
                    else 0
                end as synthetic_fill_rate,
                case
                    when synthetic.event_timestamp_ms is not null and books.signal_timestamp_ms is not null
                    then synthetic.event_timestamp_ms - books.signal_timestamp_ms
                    else null
                end as synthetic_touch_latency_ms
            from quote_execution_signal_books books
            left join observed_execution_reports observed on observed.signal_id = books.signal_id
            left join synthetic_execution_reports synthetic on synthetic.signal_id = books.signal_id
            left join (
                select signal_id, count(*) as raw_observed_report_rows
                from execution_reports
                group by signal_id
            ) raw_reports on raw_reports.signal_id = books.signal_id
            """
        )
        conn.execute(
            f"""
            create or replace view quote_execution_outcomes as
            select
                lifecycle.*,
                case
                    when observed_filled_size > 0 then 'observed_fill'
                    when dry_run_created then 'dry_run_unfilled'
                    when synthetic_filled_size > 0 then 'synthetic_only'
                    else 'neither'
                end as execution_path,
                case
                    when observed_filled_size > 0 then 'observed'
                    when synthetic_filled_size > 0 then 'synthetic'
                    else 'none'
                end as fill_source,
                case
                    when signal_timestamp_ms is null or signal_price is null or signal_size is null then 'missing_signal_or_book_fields'
                    when observed_filled_size > 0 then 'observed_fill'
                    when dry_run_created and synthetic_filled_size > 0 then 'dry_run_created_unmatched_with_synthetic_touch'
                    when dry_run_created then 'dry_run_created_unmatched'
                    when synthetic_filled_size > 0 then 'synthetic_backtest_only'
                    when future_book_snapshots = 0 then 'no_future_orderbook_snapshot'
                    when future_touches = 0 then 'future_book_never_touched_limit'
                    else 'no_observed_report_no_synthetic_fill'
                end as root_cause,
                observed_filled_size > 0 as promotion_counted_fill,
                synthetic_filled_size > 0 or observed_filled_size > 0 as backtest_counted_fill,
                future_book_snapshots,
                future_touches,
                first_future_book_timestamp_ms,
                first_future_touch_timestamp_ms,
                case
                    when first_future_book_timestamp_ms is not null and signal_timestamp_ms is not null
                    then first_future_book_timestamp_ms - signal_timestamp_ms
                    else null
                end as ms_to_first_future_book,
                case
                    when first_future_touch_timestamp_ms is not null and signal_timestamp_ms is not null
                    then first_future_touch_timestamp_ms - signal_timestamp_ms
                    else null
                end as ms_to_first_future_touch
            from (
                select
                    lifecycle.*,
                    (
                        select count(*)
                        from orderbook_snapshots future_book
                        where future_book.market_id = lifecycle.market_id
                          and future_book.asset_id = lifecycle.asset_id
                          and future_book.event_timestamp_ms > lifecycle.signal_timestamp_ms
                          and future_book.event_timestamp_ms <= lifecycle.signal_timestamp_ms + {config.max_future_window_ms}
                    ) as future_book_snapshots,
                    (
                        select count(*)
                        from orderbook_snapshots future_book
                        where future_book.market_id = lifecycle.market_id
                          and future_book.asset_id = lifecycle.asset_id
                          and future_book.event_timestamp_ms > lifecycle.signal_timestamp_ms
                          and future_book.event_timestamp_ms <= lifecycle.signal_timestamp_ms + {config.max_future_window_ms}
                          and (
                            (lifecycle.side = 'BUY' and future_book.best_ask <= lifecycle.signal_price)
                            or (lifecycle.side = 'SELL' and future_book.best_bid >= lifecycle.signal_price)
                          )
                    ) as future_touches,
                    (
                        select min(future_book.event_timestamp_ms)
                        from orderbook_snapshots future_book
                        where future_book.market_id = lifecycle.market_id
                          and future_book.asset_id = lifecycle.asset_id
                          and future_book.event_timestamp_ms > lifecycle.signal_timestamp_ms
                          and future_book.event_timestamp_ms <= lifecycle.signal_timestamp_ms + {config.max_future_window_ms}
                    ) as first_future_book_timestamp_ms,
                    (
                        select min(future_book.event_timestamp_ms)
                        from orderbook_snapshots future_book
                        where future_book.market_id = lifecycle.market_id
                          and future_book.asset_id = lifecycle.asset_id
                          and future_book.event_timestamp_ms > lifecycle.signal_timestamp_ms
                          and future_book.event_timestamp_ms <= lifecycle.signal_timestamp_ms + {config.max_future_window_ms}
                          and (
                            (lifecycle.side = 'BUY' and future_book.best_ask <= lifecycle.signal_price)
                            or (lifecycle.side = 'SELL' and future_book.best_bid >= lifecycle.signal_price)
                          )
                    ) as first_future_touch_timestamp_ms
                from quote_execution_lifecycle lifecycle
            ) lifecycle
            """
        )
        conn.execute(
            """
            create or replace view quote_execution_summary as
            select
                count(*) as signals,
                coalesce(sum(raw_observed_report_rows), 0) as raw_observed_report_rows,
                sum(case when observed_order_id is not null then 1 else 0 end) as observed_signal_lifecycles,
                sum(case when dry_run_created then 1 else 0 end) as dry_run_signal_lifecycles,
                sum(case when dry_run_filled then 1 else 0 end) as dry_run_filled_signals,
                sum(case when synthetic_filled_size > 0 then 1 else 0 end) as synthetic_filled_signals,
                sum(case when backtest_counted_fill then 1 else 0 end) as backtest_filled_signals,
                sum(case when execution_path = 'synthetic_only' then 1 else 0 end) as synthetic_only_signals,
                sum(case when dry_run_created and synthetic_filled_size > 0 and observed_filled_size <= 0 then 1 else 0 end) as dry_run_unfilled_but_synthetic_available,
                sum(case when observed_order_id is null then 1 else 0 end) as signals_without_observed_report,
                avg(case when observed_filled_size > 0 then 1.0 else 0.0 end) as observed_fill_rate,
                avg(case when dry_run_filled then 1.0 else 0.0 end) as dry_run_fill_rate,
                avg(case when synthetic_filled_size > 0 then 1.0 else 0.0 end) as synthetic_fill_rate,
                avg(case when backtest_counted_fill then 1.0 else 0.0 end) as backtest_fill_rate,
                'synthetic fills are offline backtest evidence; promotion/go-no-go only count observed dry-run/live reports' as explanation
            from quote_execution_outcomes
            """
        )
        conn.execute(
            """
            create or replace view quote_execution_by_market_asset as
            select
                market_id,
                asset_id,
                side,
                strategy,
                coalesce(model_version, 'unknown') as model_version,
                count(*) as signals,
                sum(case when dry_run_created then 1 else 0 end) as dry_run_signal_lifecycles,
                sum(case when dry_run_filled then 1 else 0 end) as dry_run_filled_signals,
                sum(case when synthetic_filled_size > 0 then 1 else 0 end) as synthetic_filled_signals,
                sum(case when execution_path = 'synthetic_only' then 1 else 0 end) as synthetic_only_signals,
                sum(case when execution_path = 'neither' then 1 else 0 end) as neither_signals,
                avg(case when quote_relation = 'at_touch' then 1.0 else 0.0 end) as at_touch_rate,
                avg(case when quote_relation = 'inside_spread' then 1.0 else 0.0 end) as inside_spread_rate,
                avg(case when quote_relation = 'behind_touch' then 1.0 else 0.0 end) as behind_touch_rate,
                avg(spread) as avg_spread_at_signal,
                avg(distance_to_mid) as avg_distance_to_mid,
                avg(book_age_ms) as avg_book_age_ms,
                avg(dry_run_terminal_latency_ms) as avg_dry_run_terminal_latency_ms,
                avg(synthetic_touch_latency_ms) as avg_synthetic_touch_latency_ms
            from quote_execution_outcomes
            group by market_id, asset_id, side, strategy, model_version
            """
        )
        conn.execute(
            f"""
            create or replace view quote_execution_examples as
            select *
            from (
                select
                    *,
                    row_number() over (
                        partition by root_cause
                        order by signal_timestamp_ms, signal_id
                    ) as root_cause_rank
                from quote_execution_outcomes
            )
            where root_cause_rank <= {config.examples_limit}
            """
        )


def ensure_synthetic_and_backtest_views(db_path: Path) -> None:
    with duckdb.connect(str(db_path)) as conn:
        has_synthetic = relation_exists(conn, "synthetic_execution_reports")
        has_observed_vs_synthetic = relation_exists(conn, "observed_vs_synthetic_fills")
        has_unfilled_reasons = relation_exists(conn, "unfilled_signal_reasons")
    if not has_synthetic:
        create_synthetic_fill_views(db_path, SyntheticFillConfig())
    if not has_observed_vs_synthetic or not has_unfilled_reasons:
        create_backtest_views(db_path)


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
    parser = argparse.ArgumentParser(description="Export offline quote execution diagnostics")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--max-future-window-ms",
        type=int,
        default=QuoteExecutionDiagnosticsConfig.max_future_window_ms,
    )
    parser.add_argument(
        "--examples-limit",
        type=int,
        default=QuoteExecutionDiagnosticsConfig.examples_limit,
    )
    args = parser.parse_args()

    report = create_quote_execution_diagnostics_report(
        Path(args.duckdb),
        Path(args.output_dir),
        QuoteExecutionDiagnosticsConfig(
            max_future_window_ms=args.max_future_window_ms,
            examples_limit=args.examples_limit,
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
