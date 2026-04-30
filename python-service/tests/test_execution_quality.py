from pathlib import Path
from typing import Any, cast

import duckdb

from src.research.execution_quality import (
    REPORT_VERSION,
    ExecutionQualityConfig,
    create_execution_quality_report,
)


def test_execution_quality_ranks_executable_asset_above_apparent_spread(
    tmp_path: Path,
) -> None:
    db_path = seed_quality_db(tmp_path)
    output_dir = tmp_path / "execution_quality"

    report = create_execution_quality_report(
        db_path,
        output_dir,
        ExecutionQualityConfig(
            min_signals=2,
            max_error_rate=1.0,
            max_unfilled_rate=1.0,
            limit=2,
        ),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert report["decision_policy"] == "offline_execution_quality_only"
    counts = cast(dict[str, Any], report["counts"])
    assert counts["execution_quality_signals"] == 4
    assert counts["execution_quality_by_asset"] == 2
    assert counts["execution_quality_ranking"] == 2
    top_assets = cast(list[dict[str, Any]], report["top_assets"])
    assert top_assets[0]["asset_id"] == "asset-executable"
    assert top_assets[0]["observed_fill_rate"] == 1.0
    assert top_assets[1]["asset_id"] == "asset-wide-spread"
    assert report["top_asset_ids"] == ["asset-executable", "asset-wide-spread"]
    assert (output_dir / "execution_quality_by_asset.parquet").exists()
    assert (output_dir / "execution_quality_ranking.parquet").exists()


def test_execution_quality_handles_empty_database(tmp_path: Path) -> None:
    db_path = tmp_path / "empty.duckdb"
    with duckdb.connect(str(db_path)):
        pass

    report = create_execution_quality_report(db_path, tmp_path / "quality")

    counts = cast(dict[str, Any], report["counts"])
    assert counts["execution_quality_signals"] == 0
    assert counts["execution_quality_by_asset"] == 0
    assert counts["execution_quality_ranking"] == 0
    assert report["top_asset_ids"] == []


def test_execution_quality_rejects_invalid_config() -> None:
    try:
        ExecutionQualityConfig(min_signals=0)
    except ValueError as exc:
        assert "min_signals must be positive" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def seed_quality_db(tmp_path: Path) -> Path:
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
                ("s1", "m1", "asset-executable", "BUY", 0.45, 10.0, 0.65, "near", "m", "d", "f", 1_000),
                ("s2", "m1", "asset-executable", "BUY", 0.46, 10.0, 0.65, "near", "m", "d", "f", 2_000),
                ("s3", "m2", "asset-wide-spread", "BUY", 0.45, 10.0, 0.65, "near", "m", "d", "f", 1_000),
                ("s4", "m2", "asset-wide-spread", "BUY", 0.45, 10.0, 0.65, "near", "m", "d", "f", 2_000),
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
                ("s1", "o1", "MATCHED", 0.45, 10.0, 10.0, 0.0, None, 1_050),
                ("s2", "o2", "MATCHED", 0.46, 10.0, 10.0, 0.0, None, 2_050),
                ("s3", "o3", "UNMATCHED", None, 0.0, 0.0, 10.0, None, 1_100),
                ("s4", "o4", "ERROR", None, 0.0, 0.0, 10.0, "rejected", 2_100),
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
                ("m1", "asset-executable", 900, 0.44, 0.45, 0.01, 100.0, 100.0),
                ("m1", "asset-executable", 1_900, 0.45, 0.46, 0.01, 100.0, 100.0),
                ("m2", "asset-wide-spread", 900, 0.30, 0.45, 0.15, 100.0, 1.0),
                ("m2", "asset-wide-spread", 1_900, 0.30, 0.45, 0.15, 100.0, 1.0),
            ],
        )
    return db_path
