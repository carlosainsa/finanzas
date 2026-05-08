from pathlib import Path
from typing import Any, cast

import duckdb

from src.research.ml_fill_dataset import (
    DATASET_VERSION,
    REPORT_VERSION,
    MlFillDatasetConfig,
    create_ml_fill_dataset_report,
)


def test_ml_fill_dataset_exports_targets_without_live_capability(
    tmp_path: Path,
) -> None:
    db_path = seed_ml_dataset_db(tmp_path)

    report = create_ml_fill_dataset_report(db_path, tmp_path / "ml")

    assert report["report_version"] == REPORT_VERSION
    assert report["dataset_version"] == DATASET_VERSION
    assert report["can_execute_trades"] is False
    assert report["decision_policy"] == "offline_ml_dataset_only"
    counts = cast(dict[str, int], report["counts"])
    assert counts["ml_fill_examples"] == 3
    assert (tmp_path / "ml" / "ml_fill_examples.parquet").exists()

    with duckdb.connect(str(db_path)) as conn:
        rows = {
            row[0]: row[1:]
            for row in conn.execute(
                """
                select
                    signal_id,
                    label_source,
                    can_train_fill_target,
                    can_train_touch_target,
                    can_train_adverse_selection_target,
                    will_fill_within_5m,
                    future_touch,
                    adverse_selection_after_fill,
                    book_age_ms
                from ml_fill_examples
                order by signal_id
                """
            ).fetchall()
        }

    assert rows["signal-fill"] == (
        "observed_execution",
        True,
        True,
        True,
        1,
        1,
        1,
        100,
    )
    assert rows["signal-late"] == (
        "observed_execution",
        True,
        True,
        False,
        0,
        1,
        None,
        100,
    )
    assert rows["signal-no-touch"] == (
        "future_orderbook_only",
        False,
        True,
        False,
        None,
        0,
        None,
        100,
    )


def test_ml_fill_dataset_handles_empty_database(tmp_path: Path) -> None:
    db_path = tmp_path / "empty.duckdb"
    with duckdb.connect(str(db_path)):
        pass

    report = create_ml_fill_dataset_report(db_path, tmp_path / "ml")

    counts = cast(dict[str, int], report["counts"])
    assert counts["ml_fill_examples"] == 0
    assert cast(list[dict[str, Any]], report["summary"]) == []


def test_ml_fill_dataset_rejects_invalid_config() -> None:
    try:
        MlFillDatasetConfig(max_future_window_ms=0)
    except ValueError as exc:
        assert "max_future_window_ms must be positive" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def seed_ml_dataset_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "research.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            create table signals (
                signal_id varchar,
                market_id varchar,
                asset_id varchar,
                side varchar,
                price double,
                size double,
                confidence double,
                strategy varchar,
                model_version varchar,
                data_version varchar,
                feature_version varchar,
                event_timestamp_ms bigint
            )
            """
        )
        conn.execute(
            """
            create table orderbook_snapshots (
                market_id varchar,
                asset_id varchar,
                event_timestamp_ms bigint,
                best_bid double,
                best_ask double,
                spread double,
                bid_depth double,
                ask_depth double
            )
            """
        )
        conn.execute(
            """
            create table execution_reports (
                signal_id varchar,
                order_id varchar,
                status varchar,
                filled_price double,
                filled_size double,
                cumulative_filled_size double,
                remaining_size double,
                error varchar,
                event_timestamp_ms bigint
            )
            """
        )
        conn.executemany(
            "insert into signals values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "signal-fill",
                    "market-1",
                    "asset-1",
                    "BUY",
                    0.50,
                    1.0,
                    0.65,
                    "probe",
                    "model",
                    "data",
                    "feature",
                    1_000,
                ),
                (
                    "signal-late",
                    "market-1",
                    "asset-1",
                    "BUY",
                    0.50,
                    1.0,
                    0.65,
                    "probe",
                    "model",
                    "data",
                    "feature",
                    2_000,
                ),
                (
                    "signal-no-touch",
                    "market-2",
                    "asset-2",
                    "BUY",
                    0.40,
                    1.0,
                    0.65,
                    "probe",
                    "model",
                    "data",
                    "feature",
                    1_000,
                ),
            ],
        )
        conn.executemany(
            "insert into orderbook_snapshots values (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("market-1", "asset-1", 900, 0.45, 0.50, 0.05, 10.0, 10.0),
                ("market-1", "asset-1", 1_100, 0.45, 0.50, 0.05, 10.0, 10.0),
                ("market-1", "asset-1", 1_900, 0.45, 0.50, 0.05, 10.0, 10.0),
                ("market-1", "asset-1", 31_000, 0.40, 0.45, 0.05, 10.0, 10.0),
                ("market-2", "asset-2", 900, 0.35, 0.50, 0.15, 10.0, 10.0),
                ("market-2", "asset-2", 1_100, 0.35, 0.50, 0.15, 10.0, 10.0),
            ],
        )
        conn.executemany(
            "insert into execution_reports values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "signal-fill",
                    "dry-run-signal-fill",
                    "MATCHED",
                    0.50,
                    1.0,
                    1.0,
                    0.0,
                    None,
                    2_000,
                ),
                (
                    "signal-late",
                    "dry-run-signal-late",
                    "MATCHED",
                    0.50,
                    1.0,
                    1.0,
                    0.0,
                    None,
                    400_000,
                ),
            ],
        )
    return db_path
