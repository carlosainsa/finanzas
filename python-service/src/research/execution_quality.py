import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd  # type: ignore[import-untyped]

from src.research.backtest import create_observed_execution_reports_view, duckdb_literal
from src.research.game_theory import ensure_base_views


REPORT_VERSION = "execution_quality_v1"


@dataclass(frozen=True)
class ExecutionQualityConfig:
    min_signals: int = 5
    max_error_rate: float = 0.25
    max_unfilled_rate: float = 0.90
    max_abs_slippage: float = 0.05
    max_avg_report_latency_ms: int = 300_000
    limit: int = 20

    def __post_init__(self) -> None:
        if self.min_signals <= 0:
            raise ValueError("min_signals must be positive")
        if not 0 <= self.max_error_rate <= 1:
            raise ValueError("max_error_rate must be between 0 and 1")
        if not 0 <= self.max_unfilled_rate <= 1:
            raise ValueError("max_unfilled_rate must be between 0 and 1")
        if self.max_abs_slippage < 0:
            raise ValueError("max_abs_slippage must be non-negative")
        if self.max_avg_report_latency_ms <= 0:
            raise ValueError("max_avg_report_latency_ms must be positive")
        if self.limit <= 0:
            raise ValueError("limit must be positive")


def create_execution_quality_report(
    db_path: Path,
    output_dir: Path,
    config: ExecutionQualityConfig = ExecutionQualityConfig(),
) -> dict[str, object]:
    create_execution_quality_views(db_path, config)
    output_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
        counts = copy_views(
            conn,
            output_dir,
            (
                "execution_quality_signals",
                "execution_quality_by_asset",
                "execution_quality_ranking",
            ),
        )
        ranking = conn.execute(
            """
            select *
            from execution_quality_ranking
            order by rank
            """
        ).fetch_df()
    top_assets = normalize_records(ranking.head(config.limit).to_dict(orient="records"))
    report: dict[str, object] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_execution_quality_only",
        "config": asdict(config),
        "counts": counts,
        "top_assets": top_assets,
        "top_asset_ids": [str(row["asset_id"]) for row in top_assets],
        "outputs": [
            "execution_quality_signals.parquet",
            "execution_quality_by_asset.parquet",
            "execution_quality_ranking.parquet",
            "execution_quality.json",
        ],
    }
    (output_dir / "execution_quality.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return report


def create_execution_quality_views(
    db_path: Path,
    config: ExecutionQualityConfig = ExecutionQualityConfig(),
) -> None:
    with duckdb.connect(str(db_path)) as conn:
        ensure_base_views(conn)
        create_observed_execution_reports_view(conn)
        conn.execute(
            """
            create or replace view execution_quality_signal_books as
            select *
            from (
                select
                    s.signal_id,
                    book.best_bid,
                    book.best_ask,
                    book.spread,
                    book.bid_depth,
                    book.ask_depth,
                    book.event_timestamp_ms as book_timestamp_ms,
                    row_number() over (
                        partition by s.signal_id
                        order by book.event_timestamp_ms desc nulls last
                    ) as book_rank
                from signals s
                left join orderbook_snapshots book
                  on book.market_id = s.market_id
                 and book.asset_id = s.asset_id
                 and book.event_timestamp_ms <= s.event_timestamp_ms
                where s.signal_id is not null
            )
            where book_rank = 1
            """
        )
        conn.execute(
            """
            create or replace view execution_quality_signals as
            select
                s.signal_id,
                s.market_id,
                s.asset_id,
                s.side,
                coalesce(s.strategy, 'unknown') as strategy,
                s.model_version::varchar as model_version,
                s.price as signal_price,
                s.size as signal_size,
                s.confidence,
                s.event_timestamp_ms as signal_timestamp_ms,
                er.order_id,
                er.status,
                er.filled_price,
                coalesce(er.cumulative_filled_size, er.filled_size, 0) as filled_size,
                case
                    when s.size > 0 then coalesce(er.cumulative_filled_size, er.filled_size, 0) / s.size
                    else 0
                end as fill_fraction,
                case
                    when er.filled_price is null then null
                    when s.side = 'BUY' then er.filled_price - s.price
                    when s.side = 'SELL' then s.price - er.filled_price
                    else null
                end as slippage,
                case
                    when er.event_timestamp_ms is not null and s.event_timestamp_ms is not null
                    then er.event_timestamp_ms - s.event_timestamp_ms
                    else null
                end as report_latency_ms,
                book.best_bid,
                book.best_ask,
                book.spread as spread_at_signal,
                book.bid_depth,
                book.ask_depth,
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
                    when s.side = 'BUY' then book.ask_depth
                    when s.side = 'SELL' then book.bid_depth
                    else null
                end as touch_side_depth
            from signals s
            left join observed_execution_reports er on er.signal_id = s.signal_id
            left join execution_quality_signal_books book on book.signal_id = s.signal_id
            where s.signal_id is not null
            """
        )
        conn.execute(
            """
            create or replace view execution_quality_by_asset as
            select
                market_id,
                asset_id,
                count(*) as signals,
                count(order_id) as observed_reports,
                sum(case when filled_size > 0 then 1 else 0 end) as filled_signals,
                sum(case when status = 'MATCHED' then 1 else 0 end) as matched_reports,
                sum(case when status = 'PARTIAL' then 1 else 0 end) as partial_reports,
                sum(case when status = 'ERROR' then 1 else 0 end) as error_reports,
                avg(fill_fraction) as avg_fill_fraction,
                sum(case when filled_size > 0 then 1 else 0 end)::double / count(*) as observed_fill_rate,
                sum(case when status = 'MATCHED' then 1 else 0 end)::double / count(*) as matched_rate,
                sum(case when status = 'PARTIAL' then 1 else 0 end)::double / count(*) as partial_rate,
                sum(case when status = 'ERROR' then 1 else 0 end)::double / count(*) as error_rate,
                sum(case when coalesce(filled_size, 0) <= 0 then 1 else 0 end)::double / count(*) as unfilled_rate,
                avg(abs(slippage)) as avg_abs_slippage,
                avg(report_latency_ms) as avg_report_latency_ms,
                avg(spread_at_signal) as avg_spread_at_signal,
                avg(distance_to_mid) as avg_distance_to_mid,
                avg(touch_side_depth) as avg_touch_side_depth
            from execution_quality_signals
            group by market_id, asset_id
            """
        )
        conn.execute(
            f"""
            create or replace view execution_quality_ranking as
            with eligible as (
                select
                    *,
                    (
                        observed_fill_rate * 100
                        + matched_rate * 25
                        + partial_rate * 10
                        + least(signals, 1000) / 100
                        - error_rate * 100
                        - unfilled_rate * 40
                        - coalesce(avg_abs_slippage, {config.max_abs_slippage}) * 200
                        - least(coalesce(avg_report_latency_ms, {config.max_avg_report_latency_ms}), {config.max_avg_report_latency_ms}) / 10000
                    ) as execution_quality_score
                from execution_quality_by_asset
                where signals >= {config.min_signals}
                  and error_rate <= {config.max_error_rate}
                  and unfilled_rate <= {config.max_unfilled_rate}
                  and coalesce(avg_abs_slippage, 0) <= {config.max_abs_slippage}
                  and coalesce(avg_report_latency_ms, 0) <= {config.max_avg_report_latency_ms}
            )
            select
                row_number() over (
                    order by execution_quality_score desc, observed_fill_rate desc, signals desc, asset_id
                ) as rank,
                *
            from eligible
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
    parser = argparse.ArgumentParser(description="Export offline execution quality diagnostics")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--min-signals", type=int, default=ExecutionQualityConfig.min_signals)
    parser.add_argument("--max-error-rate", type=float, default=ExecutionQualityConfig.max_error_rate)
    parser.add_argument(
        "--max-unfilled-rate",
        type=float,
        default=ExecutionQualityConfig.max_unfilled_rate,
    )
    parser.add_argument(
        "--max-abs-slippage",
        type=float,
        default=ExecutionQualityConfig.max_abs_slippage,
    )
    parser.add_argument(
        "--max-avg-report-latency-ms",
        type=int,
        default=ExecutionQualityConfig.max_avg_report_latency_ms,
    )
    parser.add_argument("--limit", type=int, default=ExecutionQualityConfig.limit)
    args = parser.parse_args()

    report = create_execution_quality_report(
        Path(args.duckdb),
        Path(args.output_dir),
        ExecutionQualityConfig(
            min_signals=args.min_signals,
            max_error_rate=args.max_error_rate,
            max_unfilled_rate=args.max_unfilled_rate,
            max_abs_slippage=args.max_abs_slippage,
            max_avg_report_latency_ms=args.max_avg_report_latency_ms,
            limit=args.limit,
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
