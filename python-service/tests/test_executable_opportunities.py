from pathlib import Path
from typing import Any, cast

import duckdb
import pytest

from src.research.executable_opportunities import (
    REPORT_VERSION,
    ExecutableOpportunitiesConfig,
    create_executable_opportunities_report,
)


def test_executable_opportunities_export_signal_execution_context(
    tmp_path: Path,
) -> None:
    db_path = seed_db(tmp_path)

    report = create_executable_opportunities_report(
        db_path,
        tmp_path / "opportunities",
        ExecutableOpportunitiesConfig(max_future_window_ms=5_000),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert report["decision_policy"] == "offline_executable_opportunity_dataset_only"
    summary = cast(dict[str, Any], report["summary"])
    assert summary["opportunities"] == 1
    assert summary["executable_opportunities"] == 1
    assert summary["observed_fills"] == 1
    assert summary["avg_expected_edge"] == pytest.approx(0.1)
    assert (tmp_path / "opportunities" / "executable_opportunities.parquet").exists()

    with duckdb.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            select
                market_id,
                asset_id,
                spread_bucket,
                distance_to_touch_bucket,
                timing_bucket,
                observed_filled,
                is_executable
            from executable_opportunities
            """
        ).fetchone()
    assert row == (
        "market-1",
        "asset-1",
        "250_500bps",
        "at_touch",
        "volatile",
        True,
        True,
    )


def seed_db(tmp_path: Path) -> Path:
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
            insert into signals values (
                'signal-1', 'market-1', 'asset-1', 'BUY', 0.50, 2.0, 0.60,
                'probe', 'model', 'data', 'feature', 1_000
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
        conn.executemany(
            "insert into orderbook_snapshots values (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("market-1", "asset-1", 800, 0.46, 0.50, 0.04, 40.0, 50.0),
                ("market-1", "asset-1", 900, 0.45, 0.50, 0.05, 40.0, 50.0),
                ("market-1", "asset-1", 1_100, 0.45, 0.50, 0.05, 40.0, 50.0),
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
        conn.execute(
            """
            insert into execution_reports values (
                'signal-1', 'dry-run-signal-1', 'MATCHED',
                0.50, 2.0, 2.0, 0.0, null, 1_500
            )
            """
        )
    return db_path
