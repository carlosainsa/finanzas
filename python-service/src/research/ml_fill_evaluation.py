import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd  # type: ignore[import-untyped]

from src.research.backtest import duckdb_literal
from src.research.ml_fill_dataset import (
    DATASET_VERSION,
    MlFillDatasetConfig,
    create_ml_fill_dataset_views,
)


REPORT_VERSION = "ml_fill_evaluation_v1"
EVALUATOR_VERSION = "offline_fill_scorecard_v1"
TARGETS = (
    "will_fill_within_5m",
    "future_touch",
    "adverse_selection_after_fill",
)


@dataclass(frozen=True)
class MlFillEvaluationConfig:
    train_fraction: float = 0.70
    bucket_count: int = 10
    min_test_samples: int = 5
    min_train_samples: int = 20
    min_total_samples: int = 30
    require_two_classes: bool = True

    def __post_init__(self) -> None:
        if not 0 < self.train_fraction < 1:
            raise ValueError("train_fraction must be between 0 and 1")
        if self.bucket_count <= 1:
            raise ValueError("bucket_count must be greater than 1")
        if self.min_test_samples <= 0:
            raise ValueError("min_test_samples must be positive")
        if self.min_train_samples <= 0:
            raise ValueError("min_train_samples must be positive")
        if self.min_total_samples <= 0:
            raise ValueError("min_total_samples must be positive")


def create_ml_fill_evaluation_report(
    db_path: Path,
    output_dir: Path,
    config: MlFillEvaluationConfig = MlFillEvaluationConfig(),
) -> dict[str, object]:
    create_ml_fill_evaluation_views(db_path, config)
    output_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
        counts = copy_views(
            conn,
            output_dir,
            (
                "ml_fill_evaluation_examples",
                "ml_fill_evaluation_metrics",
                "ml_fill_evaluation_buckets",
                "ml_fill_evaluation_summary",
            ),
        )
        summary = normalize_records(
            conn.execute("select * from ml_fill_evaluation_summary order by target_name")
            .fetch_df()
            .to_dict(orient="records")
        )
    label_quality_gate = build_label_quality_gate(summary, config)
    report: dict[str, object] = {
        "report_version": REPORT_VERSION,
        "dataset_version": DATASET_VERSION,
        "evaluator_version": EVALUATOR_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_ml_fill_evaluation_only",
        "config": asdict(config),
        "counts": counts,
        "summary": summary,
        "label_quality_gate": label_quality_gate,
        "targets": list(TARGETS),
        "outputs": [
            "ml_fill_evaluation_examples.parquet",
            "ml_fill_evaluation_metrics.parquet",
            "ml_fill_evaluation_buckets.parquet",
            "ml_fill_evaluation_summary.parquet",
            "ml_fill_evaluation.json",
        ],
    }
    (output_dir / "ml_fill_evaluation.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return report


def create_ml_fill_evaluation_views(
    db_path: Path,
    config: MlFillEvaluationConfig = MlFillEvaluationConfig(),
) -> None:
    create_ml_fill_dataset_views(db_path, MlFillDatasetConfig())
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            f"""
            create or replace view ml_fill_evaluation_examples as
            with target_rows as (
                select
                    'will_fill_within_5m' as target_name,
                    can_train_fill_target as can_train_target,
                    will_fill_within_5m::double as target_label,
                    *
                from ml_fill_examples
                where can_train_fill_target and will_fill_within_5m is not null
                union all
                select
                    'future_touch' as target_name,
                    can_train_touch_target as can_train_target,
                    future_touch::double as target_label,
                    *
                from ml_fill_examples
                where can_train_touch_target and future_touch is not null
                union all
                select
                    'adverse_selection_after_fill' as target_name,
                    can_train_adverse_selection_target as can_train_target,
                    adverse_selection_after_fill::double as target_label,
                    *
                from ml_fill_examples
                where can_train_adverse_selection_target
                  and adverse_selection_after_fill is not null
            ),
            scored as (
                select
                    *,
                    case quote_relation
                        when 'crossing_touch' then 0.95
                        when 'at_touch' then 0.85
                        when 'inside_spread' then 0.65
                        when 'behind_touch' then 0.25
                        when 'no_book' then 0.05
                        else 0.10
                    end as quote_relation_score,
                    1 - least(
                        coalesce(distance_to_touch, spread, 1.0)
                        / greatest(coalesce(spread, 0.01), 0.01),
                        1
                    ) as distance_score,
                    1 - least(coalesce(book_age_ms, 60000) / 60000, 1)
                        as fresh_book_score,
                    case
                        when side = 'BUY'
                        then coalesce(ask_depth, 0) / greatest(coalesce(bid_depth, 0) + coalesce(ask_depth, 0), 1)
                        when side = 'SELL'
                        then coalesce(bid_depth, 0) / greatest(coalesce(bid_depth, 0) + coalesce(ask_depth, 0), 1)
                        else 0.5
                    end as depth_balance_score,
                    least(greatest(coalesce(confidence, 0.5), 0), 1) as clipped_confidence,
                    case quote_relation
                        when 'crossing_touch' then 0.85
                        when 'at_touch' then 0.70
                        when 'inside_spread' then 0.45
                        when 'behind_touch' then 0.25
                        else 0.50
                    end as quote_aggression_score,
                    least(coalesce(spread, 0) / 0.10, 1) as spread_risk_score
                from target_rows
            ),
            predictions as (
                select
                    '{EVALUATOR_VERSION}' as evaluator_version,
                    target_name,
                    dataset_version,
                    signal_id,
                    market_id,
                    asset_id,
                    side,
                    strategy,
                    model_version,
                    data_version,
                    feature_version,
                    signal_timestamp_ms,
                    row_number() over (
                        partition by target_name
                        order by signal_timestamp_ms, signal_id
                    ) as sample_index,
                    count(*) over (partition by target_name) as sample_count,
                    label_source,
                    target_label,
                    case target_name
                        when 'will_fill_within_5m' then least(greatest(
                            0.40 * quote_relation_score
                            + 0.25 * distance_score
                            + 0.20 * clipped_confidence
                            + 0.10 * fresh_book_score
                            + 0.05 * depth_balance_score,
                            0.000001), 0.999999)
                        when 'future_touch' then least(greatest(
                            0.50 * quote_relation_score
                            + 0.30 * distance_score
                            + 0.10 * fresh_book_score
                            + 0.10 * clipped_confidence,
                            0.000001), 0.999999)
                        when 'adverse_selection_after_fill' then least(greatest(
                            0.35 * quote_aggression_score
                            + 0.25 * spread_risk_score
                            + 0.20 * (1 - depth_balance_score)
                            + 0.20 * clipped_confidence,
                            0.000001), 0.999999)
                        else 0.5
                    end as prediction_score,
                    can_train_target,
                    confidence,
                    quote_relation,
                    spread,
                    distance_to_touch,
                    distance_to_mid,
                    bid_depth,
                    ask_depth,
                    book_age_ms
                from scored
            )
            select
                *,
                case
                    when sample_index <= floor(sample_count * {config.train_fraction})
                    then 'train' else 'test'
                end as split,
                lpad(
                    cast(cast(floor(prediction_score * {config.bucket_count}) as integer)
                        * cast(100 / {config.bucket_count} as integer) as varchar),
                    2,
                    '0'
                )
                || '_'
                || lpad(
                    cast((cast(floor(prediction_score * {config.bucket_count}) as integer) + 1)
                        * cast(100 / {config.bucket_count} as integer) as varchar),
                    2,
                    '0'
                ) as prediction_bucket
            from predictions
            """
        )
        conn.execute(
            """
            create or replace view ml_fill_evaluation_metrics as
            with ranked as (
                select
                    *,
                    row_number() over (
                        partition by target_name, split
                        order by prediction_score desc, signal_timestamp_ms, signal_id
                    ) as descending_rank,
                    rank() over (
                        partition by target_name, split
                        order by prediction_score
                    ) as auc_rank,
                    count(*) over (partition by target_name, split) as split_samples
                from ml_fill_evaluation_examples
            ),
            aggregate as (
                select
                    target_name,
                    split,
                    count(*) as samples,
                    sum(target_label) as positives,
                    count(*) - sum(target_label) as negatives,
                    avg(target_label) as positive_rate,
                    avg(prediction_score) as avg_prediction_score,
                    avg(power(prediction_score - target_label, 2)) as brier_score,
                    avg(
                        -1 * (
                            target_label * ln(prediction_score)
                            + (1 - target_label) * ln(1 - prediction_score)
                        )
                    ) as log_loss,
                    avg(case when (prediction_score >= 0.50)::int = target_label then 1 else 0 end)
                        as accuracy_at_050,
                    sum(case when prediction_score >= 0.50 and target_label = 1 then 1 else 0 end)
                        as true_positives_at_050,
                    sum(case when prediction_score >= 0.50 and target_label = 0 then 1 else 0 end)
                        as false_positives_at_050,
                    sum(case when prediction_score < 0.50 and target_label = 0 then 1 else 0 end)
                        as true_negatives_at_050,
                    sum(case when prediction_score < 0.50 and target_label = 1 then 1 else 0 end)
                        as false_negatives_at_050,
                    sum(case when target_label = 1 then auc_rank else 0 end) as positive_rank_sum,
                    avg(case when descending_rank <= ceil(split_samples * 0.10) then target_label else null end)
                        as top_decile_positive_rate
                from ranked
                group by target_name, split
            )
            select
                target_name,
                split,
                samples,
                positive_rate,
                avg_prediction_score,
                brier_score,
                log_loss,
                accuracy_at_050,
                true_positives_at_050 / nullif(true_positives_at_050 + false_positives_at_050, 0)
                    as precision_at_050,
                true_positives_at_050 / nullif(true_positives_at_050 + false_negatives_at_050, 0)
                    as recall_at_050,
                true_positives_at_050,
                false_positives_at_050,
                true_negatives_at_050,
                false_negatives_at_050,
                case
                    when positives > 0 and negatives > 0
                    then (positive_rank_sum - positives * (positives + 1) / 2)
                        / (positives * negatives)
                    else null
                end as roc_auc,
                top_decile_positive_rate / nullif(positive_rate, 0) as lift_top_decile
            from aggregate
            """
        )
        conn.execute(
            """
            create or replace view ml_fill_evaluation_buckets as
            select
                target_name,
                split,
                prediction_bucket,
                count(*) as samples,
                avg(prediction_score) as avg_prediction_score,
                avg(target_label) as empirical_positive_rate,
                avg(power(prediction_score - target_label, 2)) as brier_score,
                avg(
                    -1 * (
                        target_label * ln(prediction_score)
                        + (1 - target_label) * ln(1 - prediction_score)
                    )
                ) as log_loss
            from ml_fill_evaluation_examples
            group by target_name, split, prediction_bucket
            """
        )
        conn.execute(
            f"""
            create or replace view ml_fill_evaluation_summary as
            with train_metrics as (
                select
                    target_name,
                    samples as train_samples,
                    positive_rate as train_positive_rate
                from ml_fill_evaluation_metrics
                where split = 'train'
            ),
            test_metrics as (
                select *
                from ml_fill_evaluation_metrics
                where split = 'test'
            )
            select
                targets.target_name,
                coalesce(train_metrics.train_samples, 0) as train_samples,
                train_metrics.train_positive_rate as train_positive_rate,
                coalesce(test_metrics.samples, 0) as test_samples,
                test_metrics.positive_rate as test_positive_rate,
                test_metrics.brier_score as test_brier_score,
                test_metrics.log_loss as test_log_loss,
                test_metrics.roc_auc as test_roc_auc,
                test_metrics.lift_top_decile as test_lift_top_decile,
                case
                    when coalesce(test_metrics.samples, 0) = 0 then 'NO_TEST_SAMPLES'
                    when coalesce(test_metrics.samples, 0) < {config.min_test_samples}
                    then 'INSUFFICIENT_TEST_SAMPLES'
                    when test_metrics.positive_rate in (0, 1) then 'ONE_CLASS_TEST_LABELS'
                    else 'EVALUATED'
                end as status,
                case
                    when coalesce(test_metrics.samples, 0) = 0 then 'no chronological test rows'
                    when coalesce(test_metrics.samples, 0) < {config.min_test_samples}
                    then 'test sample below minimum'
                    when test_metrics.positive_rate in (0, 1) then 'test labels contain one class'
                    else 'scorecard evaluated on chronological test split'
                end as reason
            from (
                select 'will_fill_within_5m' as target_name
                union all select 'future_touch'
                union all select 'adverse_selection_after_fill'
            ) targets
            left join train_metrics on train_metrics.target_name = targets.target_name
            left join test_metrics on test_metrics.target_name = targets.target_name
            where coalesce(train_metrics.train_samples, 0) > 0
               or coalesce(test_metrics.samples, 0) > 0
            """
        )


def build_label_quality_gate(
    summary: list[dict[str, object]],
    config: MlFillEvaluationConfig,
) -> dict[str, object]:
    target_reports = [label_quality_target(row, config) for row in summary]
    blockers = [
        blocker
        for target in target_reports
        for blocker in list_of_dicts(target.get("blockers"))
    ]
    targets_passed = sum(1 for target in target_reports if target.get("status") == "passed")
    status = "passed" if target_reports and not blockers else "blocked"
    return {
        "schema_version": "ml_fill_label_quality_gate_v1",
        "status": status,
        "can_train_models": status == "passed",
        "decision_policy": "offline_label_quality_training_gate",
        "config": {
            "min_train_samples": config.min_train_samples,
            "min_test_samples": config.min_test_samples,
            "min_total_samples": config.min_total_samples,
            "require_two_classes": config.require_two_classes,
            "target_scope": list(TARGETS),
        },
        "summary": {
            "targets_total": len(TARGETS),
            "targets_evaluated": len(target_reports),
            "targets_passed": targets_passed,
            "targets_blocked": len(target_reports) - targets_passed,
            "blocker_count": len(blockers),
        },
        "blockers": blockers,
        "targets": target_reports,
    }


def label_quality_target(
    row: dict[str, object],
    config: MlFillEvaluationConfig,
) -> dict[str, object]:
    target_name = str(row.get("target_name") or "")
    train_samples = int_or_zero(row.get("train_samples"))
    test_samples = int_or_zero(row.get("test_samples"))
    test_positive_rate = numeric_or_none(row.get("test_positive_rate"))
    train_positive_rate = numeric_or_none(row.get("train_positive_rate"))
    total_samples = train_samples + test_samples
    train_positives = count_from_rate(train_samples, train_positive_rate)
    test_positives = count_from_rate(test_samples, test_positive_rate)
    total_positives = train_positives + test_positives
    train_negatives = train_samples - train_positives
    test_negatives = test_samples - test_positives
    total_negatives = total_samples - total_positives
    reasons: list[str] = []
    blockers: list[dict[str, object]] = []
    add_sample_blockers(
        blockers,
        reasons,
        target_name,
        "train",
        train_samples,
        config.min_train_samples,
        empty_reason="NO_TRAIN_SAMPLES",
        insufficient_reason="INSUFFICIENT_TRAIN_SAMPLES",
    )
    add_sample_blockers(
        blockers,
        reasons,
        target_name,
        "test",
        test_samples,
        config.min_test_samples,
        empty_reason="NO_TEST_SAMPLES",
        insufficient_reason="INSUFFICIENT_TEST_SAMPLES",
    )
    if total_samples < config.min_total_samples:
        reason = "INSUFFICIENT_TOTAL_SAMPLES"
        reasons.append(reason)
        blockers.append(
            {
                "target_name": target_name,
                "split": "all",
                "reason_code": reason,
                "samples": total_samples,
                "min_required": config.min_total_samples,
            }
        )
    if config.require_two_classes:
        add_class_blocker(
            blockers,
            reasons,
            target_name,
            "train",
            train_samples,
            train_positives,
            train_negatives,
            "ONE_CLASS_TRAIN_LABELS",
        )
        add_class_blocker(
            blockers,
            reasons,
            target_name,
            "test",
            test_samples,
            test_positives,
            test_negatives,
            "ONE_CLASS_TEST_LABELS",
        )
        add_class_blocker(
            blockers,
            reasons,
            target_name,
            "all",
            total_samples,
            total_positives,
            total_negatives,
            "ONE_CLASS_TOTAL_LABELS",
        )
    return {
        "target_name": target_name,
        "status": "blocked" if blockers else "passed",
        "train_samples": train_samples,
        "train_positives": train_positives,
        "train_negatives": train_negatives,
        "train_positive_rate": train_positive_rate,
        "test_samples": test_samples,
        "test_positives": test_positives,
        "test_negatives": test_negatives,
        "test_positive_rate": test_positive_rate,
        "total_samples": total_samples,
        "total_positives": total_positives,
        "total_negatives": total_negatives,
        "total_positive_rate": (
            total_positives / total_samples if total_samples > 0 else None
        ),
        "reasons": sorted(set(reasons)),
        "blockers": blockers,
    }


def add_sample_blockers(
    blockers: list[dict[str, object]],
    reasons: list[str],
    target_name: str,
    split: str,
    samples: int,
    min_required: int,
    *,
    empty_reason: str,
    insufficient_reason: str,
) -> None:
    reason = empty_reason if samples <= 0 else (
        insufficient_reason if samples < min_required else ""
    )
    if not reason:
        return
    reasons.append(reason)
    blockers.append(
        {
            "target_name": target_name,
            "split": split,
            "reason_code": reason,
            "samples": samples,
            "min_required": min_required,
        }
    )


def add_class_blocker(
    blockers: list[dict[str, object]],
    reasons: list[str],
    target_name: str,
    split: str,
    samples: int,
    positives: int,
    negatives: int,
    reason: str,
) -> None:
    if samples <= 0 or (positives > 0 and negatives > 0):
        return
    reasons.append(reason)
    blockers.append(
        {
            "target_name": target_name,
            "split": split,
            "reason_code": reason,
            "samples": samples,
            "positives": positives,
            "negatives": negatives,
        }
    )


def count_from_rate(samples: int, rate: float | None) -> int:
    if rate is None:
        return 0
    return int(round(samples * rate))


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


def list_of_dicts(value: object) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def int_or_zero(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    return 0


def numeric_or_none(value: object) -> float | None:
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate offline ML fill targets")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--train-fraction",
        type=float,
        default=MlFillEvaluationConfig.train_fraction,
    )
    parser.add_argument(
        "--bucket-count",
        type=int,
        default=MlFillEvaluationConfig.bucket_count,
    )
    parser.add_argument(
        "--min-test-samples",
        type=int,
        default=MlFillEvaluationConfig.min_test_samples,
    )
    parser.add_argument(
        "--min-train-samples",
        type=int,
        default=MlFillEvaluationConfig.min_train_samples,
    )
    parser.add_argument(
        "--min-total-samples",
        type=int,
        default=MlFillEvaluationConfig.min_total_samples,
    )
    args = parser.parse_args()
    report = create_ml_fill_evaluation_report(
        Path(args.duckdb),
        Path(args.output_dir),
        MlFillEvaluationConfig(
            train_fraction=args.train_fraction,
            bucket_count=args.bucket_count,
            min_test_samples=args.min_test_samples,
            min_train_samples=args.min_train_samples,
            min_total_samples=args.min_total_samples,
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
