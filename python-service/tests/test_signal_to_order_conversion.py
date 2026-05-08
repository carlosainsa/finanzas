from pathlib import Path
from typing import Any, cast

import duckdb

from src.research.signal_to_order_conversion import (
    REPORT_VERSION,
    SignalToOrderConversionConfig,
    create_signal_to_order_conversion_report,
)


def test_signal_to_order_conversion_classifies_missing_and_filled(
    tmp_path: Path,
) -> None:
    db_path = seed_conversion_db(tmp_path)

    report = create_signal_to_order_conversion_report(
        db_path,
        tmp_path / "conversion",
        SignalToOrderConversionConfig(examples_limit=10),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert report["decision_policy"] == "offline_signal_to_order_conversion_only"
    summary = cast(dict[str, Any], report["summary"])
    assert summary["signals"] == 4
    assert summary["reports"] == 3
    assert summary["missing_reports"] == 1
    assert summary["orders_created"] == 2
    assert summary["filled_signals"] == 1
    assert summary["report_rate"] == 0.75
    root_causes = {
        str(row["root_cause"])
        for row in cast(list[dict[str, Any]], report["top_root_causes"])
    }
    assert "missing_execution_report" in root_causes
    assert "order_created_filled" in root_causes
    assert "order_created_unfilled" in root_causes
    assert "executor_reported_error" in root_causes
    assert (tmp_path / "conversion" / "signal_to_order_outcomes.parquet").exists()


def test_signal_to_order_conversion_handles_empty_database(tmp_path: Path) -> None:
    db_path = tmp_path / "empty.duckdb"
    with duckdb.connect(str(db_path)):
        pass

    report = create_signal_to_order_conversion_report(db_path, tmp_path / "conversion")

    summary = cast(dict[str, Any], report["summary"])
    assert summary["signals"] == 0
    assert summary["missing_reports"] == 0
    counts = cast(dict[str, int], report["counts"])
    assert counts["signal_to_order_outcomes"] == 0


def test_signal_to_order_conversion_rejects_invalid_config() -> None:
    try:
        SignalToOrderConversionConfig(missing_report_after_ms=0)
    except ValueError as exc:
        assert "missing_report_after_ms must be positive" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def seed_conversion_db(tmp_path: Path) -> Path:
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
        conn.executemany(
            "insert into signals values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("s-missing", "m1", "a1", "BUY", 0.45, 1.0, 0.7, "probe", "m", "d", "f", 1_000),
                ("s-filled", "m1", "a1", "BUY", 0.46, 1.0, 0.7, "probe", "m", "d", "f", 2_000),
                ("s-unfilled", "m2", "a2", "BUY", 0.50, 1.0, 0.7, "probe", "m", "d", "f", 3_000),
                ("s-error", "m2", "a2", "BUY", 0.51, 1.0, 0.7, "probe", "m", "d", "f", 4_000),
            ],
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
            "insert into execution_reports values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("s-filled", "dry-run-filled", "MATCHED", 0.46, 1.0, 1.0, 0.0, None, 2_050),
                ("s-unfilled", "dry-run-unfilled", "UNMATCHED", None, 0.0, 0.0, 1.0, None, 3_050),
                ("s-error", "", "ERROR", None, 0.0, 0.0, 1.0, "risk rejected", 4_050),
            ],
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
        conn.executemany(
            "insert into orderbook_snapshots values (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("m1", "a1", 900, 0.44, 0.46, 0.02, 10.0, 10.0),
                ("m2", "a2", 2_900, 0.49, 0.51, 0.02, 10.0, 10.0),
            ],
        )
    return db_path
