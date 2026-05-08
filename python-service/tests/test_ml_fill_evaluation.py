from pathlib import Path
from typing import Any, cast

import duckdb

from src.research.ml_fill_dataset import DATASET_VERSION
from src.research.ml_fill_evaluation import (
    EVALUATOR_VERSION,
    REPORT_VERSION,
    MlFillEvaluationConfig,
    create_ml_fill_evaluation_report,
)
from test_ml_fill_dataset import seed_ml_dataset_db


def test_ml_fill_evaluation_exports_offline_metrics_without_live_capability(
    tmp_path: Path,
) -> None:
    db_path = seed_ml_dataset_db(tmp_path)

    report = create_ml_fill_evaluation_report(
        db_path,
        tmp_path / "ml_eval",
        MlFillEvaluationConfig(train_fraction=0.5, min_test_samples=1),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["dataset_version"] == DATASET_VERSION
    assert report["evaluator_version"] == EVALUATOR_VERSION
    assert report["can_execute_trades"] is False
    assert report["decision_policy"] == "offline_ml_fill_evaluation_only"
    counts = cast(dict[str, int], report["counts"])
    assert counts["ml_fill_evaluation_examples"] == 6
    assert (tmp_path / "ml_eval" / "ml_fill_evaluation_examples.parquet").exists()
    assert (tmp_path / "ml_eval" / "ml_fill_evaluation_metrics.parquet").exists()
    assert (tmp_path / "ml_eval" / "ml_fill_evaluation_buckets.parquet").exists()
    assert (tmp_path / "ml_eval" / "ml_fill_evaluation_summary.parquet").exists()


def test_ml_fill_evaluation_uses_only_trainable_target_rows(tmp_path: Path) -> None:
    db_path = seed_ml_dataset_db(tmp_path)
    create_ml_fill_evaluation_report(db_path, tmp_path / "ml_eval")

    with duckdb.connect(str(db_path)) as conn:
        rows = dict(
            conn.execute(
                """
                select target_name, count(*)
                from ml_fill_evaluation_examples
                group by target_name
                """
            ).fetchall()
        )

    assert rows["will_fill_within_5m"] == 2
    assert rows["future_touch"] == 3
    assert rows["adverse_selection_after_fill"] == 1


def test_ml_fill_evaluation_splits_chronologically_by_target(tmp_path: Path) -> None:
    db_path = seed_ml_dataset_db(tmp_path)
    create_ml_fill_evaluation_report(
        db_path,
        tmp_path / "ml_eval",
        MlFillEvaluationConfig(train_fraction=0.5, min_test_samples=1),
    )

    with duckdb.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            select signal_id, split
            from ml_fill_evaluation_examples
            where target_name = 'future_touch'
            order by sample_index
            """
        ).fetchall()

    assert rows == [
        ("signal-fill", "train"),
        ("signal-no-touch", "test"),
        ("signal-late", "test"),
    ]


def test_ml_fill_evaluation_metrics_are_reproducible(tmp_path: Path) -> None:
    db_path = seed_ml_dataset_db(tmp_path)
    create_ml_fill_evaluation_report(
        db_path,
        tmp_path / "ml_eval",
        MlFillEvaluationConfig(train_fraction=0.5, min_test_samples=1),
    )

    with duckdb.connect(str(db_path)) as conn:
        metric = conn.execute(
            """
            select samples, positive_rate, brier_score, log_loss
            from ml_fill_evaluation_metrics
            where target_name = 'future_touch' and split = 'test'
            """
        ).fetchone()

    assert metric is not None
    assert metric[0] == 2
    assert metric[1] == 0.5
    assert metric[2] > 0
    assert metric[3] > 0


def test_ml_fill_evaluation_handles_empty_database(tmp_path: Path) -> None:
    db_path = tmp_path / "empty.duckdb"
    with duckdb.connect(str(db_path)):
        pass

    report = create_ml_fill_evaluation_report(db_path, tmp_path / "ml_eval")

    counts = cast(dict[str, int], report["counts"])
    assert counts["ml_fill_evaluation_examples"] == 0
    assert cast(list[dict[str, Any]], report["summary"]) == []


def test_ml_fill_evaluation_rejects_invalid_config() -> None:
    try:
        MlFillEvaluationConfig(train_fraction=1.0)
    except ValueError as exc:
        assert "train_fraction must be between 0 and 1" in str(exc)
    else:
        raise AssertionError("expected ValueError")
