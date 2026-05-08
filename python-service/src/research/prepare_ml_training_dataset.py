import argparse
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd  # type: ignore[import-untyped]

from src.research.backtest import duckdb_literal
from src.research.ml_fill_dataset import DATASET_VERSION
from src.research.ml_fill_evaluation import (
    EVALUATOR_VERSION,
    MlFillEvaluationConfig,
    build_label_quality_gate,
    create_ml_fill_evaluation_report,
    create_ml_fill_evaluation_views,
)


REPORT_VERSION = "ml_training_dataset_preparation_v1"
MANIFEST_VERSION = "ml_fill_training_dataset_manifest_v1"
TRAINING_EXAMPLES_FILE = "ml_fill_training_examples.parquet"
MANIFEST_FILE = "ml_training_dataset_manifest.json"
PREPARATION_FILE = "ml_training_dataset_preparation.json"
BLOCKED_EXIT_CODE = 20
ERROR_EXIT_CODE = 2
TRAINING_COLUMNS = (
    "dataset_version",
    "evaluator_version",
    "target_name",
    "signal_id",
    "market_id",
    "asset_id",
    "side",
    "strategy",
    "model_version",
    "data_version",
    "feature_version",
    "signal_timestamp_ms",
    "split",
    "target_label",
    "confidence",
    "quote_relation",
    "spread",
    "distance_to_touch",
    "distance_to_mid",
    "bid_depth",
    "ask_depth",
    "book_age_ms",
)


class TrainingDatasetBlockedError(RuntimeError):
    def __init__(self, report: dict[str, object]) -> None:
        self.report = report
        super().__init__(str(report.get("reason", "ML training dataset is blocked")))


def prepare_ml_training_dataset(
    db_path: Path,
    output_dir: Path,
    ml_fill_evaluation_json: Path | None = None,
    config: MlFillEvaluationConfig = MlFillEvaluationConfig(),
    *,
    refresh_views: bool = True,
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    evaluation_report = load_or_create_evaluation_report(
        db_path,
        output_dir,
        ml_fill_evaluation_json,
        config,
        refresh_views=refresh_views,
    )
    gate = extract_gate(evaluation_report)
    if gate.get("can_train_models") is not True:
        report = blocked_report(
            db_path,
            evaluation_report,
            gate,
            "ml_fill_label_quality_gate_blocked",
            config,
        )
        write_preparation_report(output_dir, report)
        raise TrainingDatasetBlockedError(report)
    if refresh_views:
        create_ml_fill_evaluation_views(db_path, config)
    try:
        manifest = export_training_dataset(
            db_path,
            output_dir,
            evaluation_report,
            gate,
            config,
        )
    except duckdb.Error as exc:
        report = blocked_report(
            db_path,
            evaluation_report,
            gate,
            f"training_examples_unavailable:{exc}",
            config,
        )
        write_preparation_report(output_dir, report)
        raise TrainingDatasetBlockedError(report) from exc
    write_preparation_report(output_dir, manifest)
    return manifest


def load_or_create_evaluation_report(
    db_path: Path,
    output_dir: Path,
    ml_fill_evaluation_json: Path | None,
    config: MlFillEvaluationConfig,
    *,
    refresh_views: bool,
) -> dict[str, object]:
    if ml_fill_evaluation_json is not None:
        if not ml_fill_evaluation_json.exists():
            raise FileNotFoundError(str(ml_fill_evaluation_json))
        loaded = json.loads(ml_fill_evaluation_json.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise ValueError("ml_fill_evaluation_json must contain a JSON object")
        return loaded
    if not refresh_views:
        return evaluation_report_from_existing_views(db_path, config)
    return create_ml_fill_evaluation_report(
        db_path,
        output_dir / "ml_fill_evaluation_source",
        config,
    )


def evaluation_report_from_existing_views(
    db_path: Path,
    config: MlFillEvaluationConfig,
) -> dict[str, object]:
    with duckdb.connect(str(db_path)) as conn:
        summary = normalize_records(
            conn.execute("select * from ml_fill_evaluation_summary order by target_name")
            .fetch_df()
            .to_dict(orient="records")
        )
    return {
        "report_version": "ml_fill_evaluation_existing_views",
        "dataset_version": DATASET_VERSION,
        "evaluator_version": EVALUATOR_VERSION,
        "config": asdict(config),
        "summary": summary,
        "label_quality_gate": build_label_quality_gate(summary, config),
    }


def extract_gate(evaluation_report: dict[str, object]) -> dict[str, object]:
    gate = evaluation_report.get("label_quality_gate")
    if isinstance(gate, dict):
        return gate
    return {
        "schema_version": "ml_fill_label_quality_gate_v1",
        "status": "blocked",
        "can_train_models": False,
        "decision_policy": "offline_label_quality_training_gate",
        "summary": {"blocker_count": 1},
        "blockers": [
            {
                "reason_code": "MISSING_LABEL_QUALITY_GATE",
                "reason": "ml_fill_evaluation.json does not include label_quality_gate",
            }
        ],
        "targets": [],
    }


def export_training_dataset(
    db_path: Path,
    output_dir: Path,
    evaluation_report: dict[str, object],
    gate: dict[str, object],
    config: MlFillEvaluationConfig,
) -> dict[str, object]:
    training_path = output_dir / TRAINING_EXAMPLES_FILE
    columns_sql = ", ".join(TRAINING_COLUMNS)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            f"""
            copy (
                select {columns_sql}
                from ml_fill_evaluation_examples
                order by target_name, split, signal_timestamp_ms, signal_id
            )
            to '{duckdb_literal(training_path.as_posix())}' (format parquet)
            """
        )
        counts = training_counts(conn)
    manifest: dict[str, object] = {
        "report_version": REPORT_VERSION,
        "manifest_version": MANIFEST_VERSION,
        "dataset_version": str(evaluation_report.get("dataset_version") or DATASET_VERSION),
        "source_report_version": str(evaluation_report.get("report_version") or ""),
        "source_evaluator_version": str(
            evaluation_report.get("evaluator_version") or EVALUATOR_VERSION
        ),
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "can_train_models": True,
        "status": "ready",
        "decision_policy": "fail_closed_ml_training_dataset_gate",
        "config": asdict(config),
        "source_duckdb": str(db_path),
        "gate": gate,
        "counts": counts,
        "targets": counts["targets"],
        "outputs": [TRAINING_EXAMPLES_FILE, MANIFEST_FILE, PREPARATION_FILE],
    }
    (output_dir / MANIFEST_FILE).write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def training_counts(conn: duckdb.DuckDBPyConnection) -> dict[str, object]:
    rows = normalize_records(
        conn.execute(
            """
            select
                target_name,
                count(*) as examples,
                sum(case when split = 'train' then 1 else 0 end) as train_examples,
                sum(case when split = 'test' then 1 else 0 end) as test_examples,
                avg(target_label) as positive_rate
            from ml_fill_evaluation_examples
            group by target_name
            order by target_name
            """
        )
        .fetch_df()
        .to_dict(orient="records")
    )
    return {
        "training_examples": sum(int_or_zero(row.get("examples")) for row in rows),
        "targets": [str(row["target_name"]) for row in rows],
        "target_count": len(rows),
        "train_examples": sum(int_or_zero(row.get("train_examples")) for row in rows),
        "test_examples": sum(int_or_zero(row.get("test_examples")) for row in rows),
        "by_target": rows,
    }


def blocked_report(
    db_path: Path,
    evaluation_report: dict[str, object],
    gate: dict[str, object],
    reason: str,
    config: MlFillEvaluationConfig,
) -> dict[str, object]:
    return {
        "report_version": REPORT_VERSION,
        "manifest_version": MANIFEST_VERSION,
        "dataset_version": str(evaluation_report.get("dataset_version") or DATASET_VERSION),
        "source_report_version": str(evaluation_report.get("report_version") or ""),
        "source_evaluator_version": str(
            evaluation_report.get("evaluator_version") or EVALUATOR_VERSION
        ),
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "can_train_models": False,
        "status": "blocked",
        "reason": reason,
        "decision_policy": "fail_closed_ml_training_dataset_gate",
        "config": asdict(config),
        "source_duckdb": str(db_path),
        "gate": gate,
        "outputs": [PREPARATION_FILE],
    }


def write_preparation_report(output_dir: Path, report: dict[str, object]) -> None:
    (output_dir / PREPARATION_FILE).write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def normalize_records(rows: list[dict[str, Any]]) -> list[dict[str, object]]:
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


def int_or_zero(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prepare ML fill training dataset after label quality gate passes"
    )
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--ml-fill-evaluation-json")
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
    parser.add_argument(
        "--use-existing-views",
        action="store_true",
        help="Use already materialized ml_fill_evaluation_* views instead of rebuilding them.",
    )
    args = parser.parse_args()
    config = MlFillEvaluationConfig(
        train_fraction=args.train_fraction,
        bucket_count=args.bucket_count,
        min_test_samples=args.min_test_samples,
        min_train_samples=args.min_train_samples,
        min_total_samples=args.min_total_samples,
    )
    try:
        report = prepare_ml_training_dataset(
            Path(args.duckdb),
            Path(args.output_dir),
            Path(args.ml_fill_evaluation_json)
            if args.ml_fill_evaluation_json
            else None,
            config,
            refresh_views=not args.use_existing_views,
        )
    except TrainingDatasetBlockedError as exc:
        print(json.dumps(exc.report, indent=2, sort_keys=True))
        return BLOCKED_EXIT_CODE
    except (FileNotFoundError, ValueError) as exc:
        print(
            json.dumps(
                {
                    "report_version": REPORT_VERSION,
                    "status": "error",
                    "can_execute_trades": False,
                    "can_train_models": False,
                    "reason": str(exc),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return ERROR_EXIT_CODE
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
