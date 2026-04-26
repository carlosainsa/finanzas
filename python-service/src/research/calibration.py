import json
import math
from pathlib import Path

import duckdb

from src.research.backtest import create_backtest_views, duckdb_literal


CALIBRATION_OUTPUTS = (
    "walk_forward_splits",
    "walk_forward_metrics",
    "calibration_buckets",
    "realized_edge_by_confidence_bucket",
)


def create_calibration_views(db_path: Path, train_fraction: float = 0.70) -> None:
    if not 0.0 < train_fraction < 1.0:
        raise ValueError("train_fraction must be between 0 and 1")

    create_backtest_views(db_path)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            create or replace view labeled_backtest_trades as
            select
                *,
                case
                    when realized_edge_after_slippage is null then null
                    when realized_edge_after_slippage > 0 then 1.0
                    else 0.0
                end as profitable_label,
                least(greatest(confidence, 0.000001), 0.999999) as clipped_confidence
            from backtest_trades
            where signal_timestamp_ms is not null
              and confidence is not null
            """
        )
        conn.execute(
            f"""
            create or replace view walk_forward_splits as
            select
                *,
                case
                    when sample_index <= greatest(1, floor(sample_count * {train_fraction}))
                    then 'train'
                    else 'test'
                end as split
            from (
                select
                    *,
                    row_number() over (
                        order by signal_timestamp_ms, signal_id, coalesce(order_id, '')
                    ) as sample_index,
                    count(*) over () as sample_count
                from labeled_backtest_trades
                where profitable_label is not null
            )
            """
        )
        conn.execute(
            """
            create or replace view walk_forward_metrics as
            select
                split,
                count(*) as samples,
                avg(profitable_label) as positive_rate,
                avg(power(clipped_confidence - profitable_label, 2)) as brier_score,
                avg(
                    -1 * (
                        profitable_label * ln(clipped_confidence)
                        + (1 - profitable_label) * ln(1 - clipped_confidence)
                    )
                ) as log_loss,
                avg(realized_edge_after_slippage) as avg_realized_edge_after_slippage,
                avg(fill_rate) as avg_fill_rate
            from walk_forward_splits
            group by split
            """
        )
        conn.execute(
            """
            create or replace view calibration_buckets as
            select
                split,
                confidence_bucket,
                count(*) as samples,
                avg(confidence) as avg_confidence,
                avg(profitable_label) as empirical_positive_rate,
                avg(power(clipped_confidence - profitable_label, 2)) as brier_score,
                avg(
                    -1 * (
                        profitable_label * ln(clipped_confidence)
                        + (1 - profitable_label) * ln(1 - clipped_confidence)
                    )
                ) as log_loss
            from (
                select
                    *,
                    case
                        when confidence < 0.1 then '00_10'
                        when confidence < 0.2 then '10_20'
                        when confidence < 0.3 then '20_30'
                        when confidence < 0.4 then '30_40'
                        when confidence < 0.5 then '40_50'
                        when confidence < 0.6 then '50_60'
                        when confidence < 0.7 then '60_70'
                        when confidence < 0.8 then '70_80'
                        when confidence < 0.9 then '80_90'
                        else '90_100'
                    end as confidence_bucket
                from walk_forward_splits
            )
            group by split, confidence_bucket
            """
        )
        conn.execute(
            """
            create or replace view realized_edge_by_confidence_bucket as
            select
                split,
                confidence_bucket,
                count(*) as samples,
                avg(realized_edge_after_slippage) as avg_realized_edge_after_slippage,
                avg(fill_rate) as avg_fill_rate,
                sum(filled_size) as total_filled_size
            from (
                select
                    *,
                    case
                        when confidence < 0.1 then '00_10'
                        when confidence < 0.2 then '10_20'
                        when confidence < 0.3 then '20_30'
                        when confidence < 0.4 then '30_40'
                        when confidence < 0.5 then '40_50'
                        when confidence < 0.6 then '50_60'
                        when confidence < 0.7 then '60_70'
                        when confidence < 0.8 then '70_80'
                        when confidence < 0.9 then '80_90'
                        else '90_100'
                    end as confidence_bucket
                from walk_forward_splits
            )
            group by split, confidence_bucket
            """
        )


def export_calibration_report(
    db_path: Path, output_dir: Path, train_fraction: float = 0.70
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    create_calibration_views(db_path, train_fraction=train_fraction)
    counts: dict[str, int] = {}
    metrics: list[dict[str, object]] = []
    with duckdb.connect(str(db_path)) as conn:
        for view_name in CALIBRATION_OUTPUTS:
            target = output_dir / f"{view_name}.parquet"
            conn.execute(
                f"copy (select * from {view_name}) to '{duckdb_literal(target.as_posix())}' (format parquet)"
            )
            row = conn.execute(f"select count(*) from {view_name}").fetchone()
            counts[view_name] = int(row[0]) if row else 0
        metrics = [
            metric_row(row)
            for row in conn.execute(
                """
                select
                    split,
                    samples,
                    positive_rate,
                    brier_score,
                    log_loss,
                    avg_realized_edge_after_slippage,
                    avg_fill_rate
                from walk_forward_metrics
                order by split
                """
            ).fetchall()
        ]
    report: dict[str, object] = {
        "counts": counts,
        "train_fraction": train_fraction,
        "metrics": metrics,
        "passed": calibration_passed(metrics),
    }
    (output_dir / "calibration_summary.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return report


def metric_row(row: tuple[object, ...]) -> dict[str, object]:
    return {
        "split": row[0],
        "samples": row[1],
        "positive_rate": row[2],
        "brier_score": row[3],
        "log_loss": row[4],
        "avg_realized_edge_after_slippage": row[5],
        "avg_fill_rate": row[6],
    }


def calibration_passed(metrics: list[dict[str, object]]) -> bool:
    test_metrics = [item for item in metrics if item.get("split") == "test"]
    if not test_metrics:
        return False
    test = test_metrics[0]
    brier = as_float(test.get("brier_score"))
    log_loss = as_float(test.get("log_loss"))
    realized_edge = as_float(test.get("avg_realized_edge_after_slippage"))
    return (
        brier is not None
        and brier < 0.25
        and log_loss is not None
        and math.isfinite(log_loss)
        and realized_edge is not None
        and realized_edge > 0
    )


def as_float(value: object) -> float | None:
    if isinstance(value, (int, float)):
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    return None


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="research-calibration")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--train-fraction", type=float, default=0.70)
    args = parser.parse_args()

    report = export_calibration_report(
        Path(args.duckdb), Path(args.output_dir), train_fraction=args.train_fraction
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
