import json
from pathlib import Path
from typing import Any, cast

import duckdb

from src.research.prepare_ml_training_dataset import (
    MANIFEST_FILE,
    PREPARATION_FILE,
    TRAINING_EXAMPLES_FILE,
    TrainingDatasetBlockedError,
    prepare_ml_training_dataset,
)
from test_ml_fill_dataset import seed_ml_dataset_db


def test_prepare_ml_training_dataset_blocks_when_gate_false(tmp_path: Path) -> None:
    db_path = seed_ml_dataset_db(tmp_path)
    evaluation_path = write_evaluation_json(tmp_path, can_train_models=False)

    try:
        prepare_ml_training_dataset(
            db_path,
            tmp_path / "training",
            evaluation_path,
        )
    except TrainingDatasetBlockedError as exc:
        assert exc.report["status"] == "blocked"
        assert exc.report["can_train_models"] is False
    else:
        raise AssertionError("expected TrainingDatasetBlockedError")

    assert not (tmp_path / "training" / TRAINING_EXAMPLES_FILE).exists()
    assert not (tmp_path / "training" / MANIFEST_FILE).exists()
    preparation = json.loads(
        (tmp_path / "training" / PREPARATION_FILE).read_text(encoding="utf-8")
    )
    assert preparation["reason"] == "ml_fill_label_quality_gate_blocked"


def test_prepare_ml_training_dataset_blocks_when_gate_missing(tmp_path: Path) -> None:
    db_path = seed_ml_dataset_db(tmp_path)
    evaluation_path = tmp_path / "ml_fill_evaluation.json"
    evaluation_path.write_text(
        json.dumps({"report_version": "ml_fill_evaluation_v1"}) + "\n",
        encoding="utf-8",
    )

    try:
        prepare_ml_training_dataset(db_path, tmp_path / "training", evaluation_path)
    except TrainingDatasetBlockedError as exc:
        gate = cast(dict[str, Any], exc.report["gate"])
        blockers = cast(list[dict[str, Any]], gate["blockers"])
        assert blockers[0]["reason_code"] == "MISSING_LABEL_QUALITY_GATE"
    else:
        raise AssertionError("expected TrainingDatasetBlockedError")


def test_prepare_ml_training_dataset_exports_parquet_and_manifest_when_gate_true(
    tmp_path: Path,
) -> None:
    db_path = seed_ready_training_view(tmp_path)
    evaluation_path = write_evaluation_json(tmp_path, can_train_models=True)

    report = prepare_ml_training_dataset(
        db_path,
        tmp_path / "training",
        evaluation_path,
        refresh_views=False,
    )

    assert report["status"] == "ready"
    assert report["can_train_models"] is True
    assert (tmp_path / "training" / TRAINING_EXAMPLES_FILE).exists()
    assert (tmp_path / "training" / MANIFEST_FILE).exists()
    manifest = json.loads(
        (tmp_path / "training" / MANIFEST_FILE).read_text(encoding="utf-8")
    )
    assert manifest["counts"]["training_examples"] == 2
    with duckdb.connect(str(db_path)) as conn:
        exported = conn.execute(
            f"""
            select target_name, split, target_label, signal_id
            from read_parquet('{(tmp_path / "training" / TRAINING_EXAMPLES_FILE).as_posix()}')
            order by signal_id
            """
        ).fetchall()
    assert exported == [
        ("future_touch", "train", 1.0, "signal-a"),
        ("future_touch", "test", 0.0, "signal-b"),
    ]


def test_prepare_ml_training_dataset_duckdb_mode_fails_closed_when_quality_not_passed(
    tmp_path: Path,
) -> None:
    db_path = seed_ml_dataset_db(tmp_path)

    try:
        prepare_ml_training_dataset(db_path, tmp_path / "training")
    except TrainingDatasetBlockedError as exc:
        assert exc.report["status"] == "blocked"
        assert exc.report["can_train_models"] is False
    else:
        raise AssertionError("expected TrainingDatasetBlockedError")


def seed_ready_training_view(tmp_path: Path) -> Path:
    db_path = tmp_path / "ready_training.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            create table ml_fill_evaluation_examples (
                dataset_version varchar,
                evaluator_version varchar,
                target_name varchar,
                signal_id varchar,
                market_id varchar,
                asset_id varchar,
                side varchar,
                strategy varchar,
                model_version varchar,
                data_version varchar,
                feature_version varchar,
                signal_timestamp_ms bigint,
                split varchar,
                target_label double,
                confidence double,
                quote_relation varchar,
                spread double,
                distance_to_touch double,
                distance_to_mid double,
                bid_depth double,
                ask_depth double,
                book_age_ms bigint
            )
            """
        )
        conn.executemany(
            "insert into ml_fill_evaluation_examples values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "ml_fill_targets_v1",
                    "offline_fill_scorecard_v1",
                    "future_touch",
                    "signal-a",
                    "market-a",
                    "asset-a",
                    "BUY",
                    "probe",
                    "model",
                    "data",
                    "feature",
                    1_000,
                    "train",
                    1.0,
                    0.8,
                    "inside_spread",
                    0.04,
                    0.01,
                    0.01,
                    10.0,
                    20.0,
                    100,
                ),
                (
                    "ml_fill_targets_v1",
                    "offline_fill_scorecard_v1",
                    "future_touch",
                    "signal-b",
                    "market-b",
                    "asset-b",
                    "BUY",
                    "probe",
                    "model",
                    "data",
                    "feature",
                    2_000,
                    "test",
                    0.0,
                    0.7,
                    "behind_touch",
                    0.05,
                    0.03,
                    0.02,
                    11.0,
                    21.0,
                    100,
                ),
            ],
        )
    return db_path


def write_evaluation_json(tmp_path: Path, *, can_train_models: bool) -> Path:
    evaluation_path = tmp_path / "ml_fill_evaluation.json"
    evaluation_path.write_text(
        json.dumps(
            {
                "report_version": "ml_fill_evaluation_v1",
                "dataset_version": "ml_fill_targets_v1",
                "evaluator_version": "offline_fill_scorecard_v1",
                "label_quality_gate": {
                    "schema_version": "ml_fill_label_quality_gate_v1",
                    "status": "passed" if can_train_models else "blocked",
                    "can_train_models": can_train_models,
                    "summary": {"blocker_count": 0 if can_train_models else 1},
                    "blockers": [],
                    "targets": [],
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return evaluation_path
