import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd  # type: ignore[import-untyped]

from src.research.backtest import duckdb_literal
from src.research.quote_execution_diagnostics import (
    QuoteExecutionDiagnosticsConfig,
    create_quote_execution_diagnostics_views,
)


REPORT_VERSION = "ml_fill_dataset_v1"
DATASET_VERSION = "ml_fill_targets_v1"


@dataclass(frozen=True)
class MlFillDatasetConfig:
    max_future_window_ms: int = 300_000
    adverse_selection_window_ms: int = 30_000

    def __post_init__(self) -> None:
        if self.max_future_window_ms <= 0:
            raise ValueError("max_future_window_ms must be positive")
        if self.adverse_selection_window_ms <= 0:
            raise ValueError("adverse_selection_window_ms must be positive")


def create_ml_fill_dataset_report(
    db_path: Path,
    output_dir: Path,
    config: MlFillDatasetConfig = MlFillDatasetConfig(),
) -> dict[str, object]:
    create_ml_fill_dataset_views(db_path, config)
    output_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
        counts = copy_views(
            conn,
            output_dir,
            (
                "ml_fill_examples",
                "ml_fill_dataset_summary",
            ),
        )
        summary = normalize_records(
            conn.execute("select * from ml_fill_dataset_summary")
            .fetch_df()
            .to_dict(orient="records")
        )
    report: dict[str, object] = {
        "report_version": REPORT_VERSION,
        "dataset_version": DATASET_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_ml_dataset_only",
        "config": asdict(config),
        "counts": counts,
        "summary": summary,
        "targets": [
            "will_fill_within_5m",
            "future_touch",
            "adverse_selection_after_fill",
        ],
        "outputs": [
            "ml_fill_examples.parquet",
            "ml_fill_dataset_summary.parquet",
            "ml_fill_dataset.json",
        ],
    }
    (output_dir / "ml_fill_dataset.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return report


def create_ml_fill_dataset_views(
    db_path: Path,
    config: MlFillDatasetConfig = MlFillDatasetConfig(),
) -> None:
    create_quote_execution_diagnostics_views(
        db_path,
        QuoteExecutionDiagnosticsConfig(max_future_window_ms=config.max_future_window_ms),
    )
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            f"""
            create or replace view ml_fill_examples as
            select
                '{DATASET_VERSION}' as dataset_version,
                signal_id,
                market_id,
                asset_id,
                side,
                strategy,
                model_version,
                data_version,
                feature_version,
                signal_timestamp_ms,
                signal_price,
                signal_size,
                confidence,
                best_bid,
                best_ask,
                spread,
                bid_depth,
                ask_depth,
                book_age_ms,
                quote_relation,
                distance_to_touch,
                distance_to_mid,
                required_quote_move,
                future_touches,
                ms_to_first_future_touch,
                future_book_snapshots,
                observed_order_id,
                observed_filled_size,
                observed_fill_rate,
                synthetic_filled_size,
                synthetic_fill_rate,
                case
                    when observed_order_id is not null then 'observed_execution'
                    when future_book_snapshots > 0 then 'future_orderbook_only'
                    else 'unlabeled'
                end as label_source,
                case when observed_order_id is not null then true else false end
                    as can_train_fill_target,
                case when future_book_snapshots > 0 then true else false end
                    as can_train_touch_target,
                case
                    when observed_filled_size > 0 and adverse_mark_price is not null
                    then true else false
                end as can_train_adverse_selection_target,
                case
                    when observed_order_id is null then null
                    when observed_filled_size > 0
                     and dry_run_terminal_latency_ms <= {config.max_future_window_ms}
                    then 1 else 0
                end as will_fill_within_5m,
                case when future_touched_limit then 1 else 0 end as future_touch,
                case
                    when observed_filled_size <= 0 then null
                    when side = 'BUY' and adverse_mark_price is not null
                    then case when adverse_mark_price < observed_filled_price then 1 else 0 end
                    when side = 'SELL' and adverse_mark_price is not null
                    then case when adverse_mark_price > observed_filled_price then 1 else 0 end
                    else null
                end as adverse_selection_after_fill,
                adverse_mark_price,
                adverse_mark_timestamp_ms
            from (
                select
                    outcomes.*,
                    (
                        select
                            case
                                when outcomes.side = 'BUY' then (book.best_bid + book.best_ask) / 2
                                when outcomes.side = 'SELL' then (book.best_bid + book.best_ask) / 2
                                else null
                            end
                        from orderbook_snapshots book
                        where book.market_id = outcomes.market_id
                          and book.asset_id = outcomes.asset_id
                          and outcomes.observed_filled_size > 0
                          and book.event_timestamp_ms >= outcomes.signal_timestamp_ms + {config.adverse_selection_window_ms}
                          and book.best_bid is not null
                          and book.best_ask is not null
                        order by book.event_timestamp_ms
                        limit 1
                    ) as adverse_mark_price,
                    (
                        select book.event_timestamp_ms
                        from orderbook_snapshots book
                        where book.market_id = outcomes.market_id
                          and book.asset_id = outcomes.asset_id
                          and outcomes.observed_filled_size > 0
                          and book.event_timestamp_ms >= outcomes.signal_timestamp_ms + {config.adverse_selection_window_ms}
                        order by book.event_timestamp_ms
                        limit 1
                    ) as adverse_mark_timestamp_ms
                from quote_execution_outcomes outcomes
            )
            """
        )
        conn.execute(
            """
            create or replace view ml_fill_dataset_summary as
            select
                dataset_version,
                strategy,
                coalesce(model_version, 'unknown') as model_version,
                market_id,
                asset_id,
                side,
                count(*) as examples,
                avg(will_fill_within_5m) as will_fill_within_5m_rate,
                avg(future_touch) as future_touch_rate,
                avg(adverse_selection_after_fill) as adverse_selection_after_fill_rate,
                sum(case when can_train_fill_target then 1 else 0 end) as fill_target_examples,
                sum(case when can_train_touch_target then 1 else 0 end) as touch_target_examples,
                sum(case when can_train_adverse_selection_target then 1 else 0 end)
                    as adverse_selection_target_examples,
                avg(spread) as avg_spread,
                avg(distance_to_touch) as avg_distance_to_touch,
                avg(required_quote_move) as avg_required_quote_move
            from ml_fill_examples
            group by dataset_version, strategy, model_version, market_id, asset_id, side
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
    parser = argparse.ArgumentParser(description="Build offline ML fill dataset")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--max-future-window-ms",
        type=int,
        default=MlFillDatasetConfig.max_future_window_ms,
    )
    parser.add_argument(
        "--adverse-selection-window-ms",
        type=int,
        default=MlFillDatasetConfig.adverse_selection_window_ms,
    )
    args = parser.parse_args()
    report = create_ml_fill_dataset_report(
        Path(args.duckdb),
        Path(args.output_dir),
        MlFillDatasetConfig(
            max_future_window_ms=args.max_future_window_ms,
            adverse_selection_window_ms=args.adverse_selection_window_ms,
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
