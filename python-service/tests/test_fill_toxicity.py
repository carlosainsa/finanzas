from pathlib import Path
from typing import Any, cast

import duckdb
import pytest

from src.research.fill_toxicity import (
    FillToxicityConfig,
    create_fill_toxicity_report,
)


def test_fill_toxicity_rejects_adverse_asset_strategy(tmp_path: Path) -> None:
    db_path = seed_fill_toxicity_db(tmp_path)
    output_dir = tmp_path / "fill_toxicity"

    report = create_fill_toxicity_report(
        db_path,
        output_dir,
        FillToxicityConfig(min_filled_events=1, max_adverse_30s_rate=0.50),
    )

    assert report["can_execute_trades"] is False
    assert report["counts"] == {
        "fill_toxicity_by_asset_strategy": 1,
        "fill_toxicity_by_segment": 1,
        "fill_toxicity_events": 1,
        "fill_toxicity_summary": 1,
    }
    with duckdb.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            select filled_events, adverse_30s_rate, avg_pnl_30s, decision
            from fill_toxicity_by_asset_strategy
            """
        ).fetchone()

    assert row == (1, pytest.approx(1.0), pytest.approx(-0.04), "REJECT_TOXICITY")
    assert (output_dir / "fill_toxicity_by_asset_strategy.parquet").exists()
    assert (output_dir / "fill_toxicity_by_segment.parquet").exists()
    assert (output_dir / "blocked_segments.json").exists()
    assert (output_dir / "fill_toxicity.json").exists()


def test_fill_toxicity_handles_empty_database(tmp_path: Path) -> None:
    db_path = tmp_path / "research.duckdb"

    report = create_fill_toxicity_report(db_path, tmp_path / "fill_toxicity")

    counts = cast(dict[str, Any], report["counts"])
    summary = cast(dict[str, Any], report["summary"])
    assert counts["fill_toxicity_events"] == 0
    assert counts["fill_toxicity_by_asset_strategy"] == 0
    assert summary["segments"] == 0


def test_fill_toxicity_rejects_invalid_config() -> None:
    try:
        FillToxicityConfig(max_adverse_30s_rate=1.5)
    except ValueError as exc:
        assert "max_adverse_30s_rate" in str(exc)
    else:
        raise AssertionError("expected invalid config to fail")


def seed_fill_toxicity_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "research.duckdb"
    with duckdb.connect(str(db_path)) as conn:
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
                ("market-1", "asset-1", 1_000, 0.44, 0.46, 0.02, 10.0, 8.0),
                ("market-1", "asset-1", 1_010, 0.45, 0.47, 0.02, 9.0, 7.0),
                ("market-1", "asset-1", 31_010, 0.42, 0.44, 0.02, 12.0, 5.0),
            ],
        )
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
            insert into signals values
            ('signal-1', 'market-1', 'asset-1', 'BUY', 0.47, 1.0, 0.8,
             'execution_probe_v9_test', 'model-v9', 'redis', 'features', 1000)
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
        conn.execute(
            """
            insert into execution_reports values
            ('signal-1', 'order-1', 'MATCHED', 0.47, 1.0, 1.0, 0.0, null, 1010)
            """
        )
    return db_path
