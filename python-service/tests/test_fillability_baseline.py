from pathlib import Path
from typing import Any, cast

import duckdb

from src.research.fillability_baseline import (
    REPORT_VERSION,
    FillabilityBaselineConfig,
    create_fillability_baseline_report,
)


def test_fillability_baseline_prioritizes_future_touch_evidence(
    tmp_path: Path,
) -> None:
    db_path = seed_fillability_db(tmp_path)

    report = create_fillability_baseline_report(
        db_path,
        tmp_path / "fillability",
        FillabilityBaselineConfig(min_signals=1, min_future_touch_rate=0.05),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert report["decision_policy"] == "offline_fillability_ranking_only"
    selected = cast(list[dict[str, Any]], report["selected"])
    assert selected[0]["asset_id"] == "asset-touch"
    assert selected[0]["recommendation"] == "PROMOTE_TO_OBSERVATION"
    assert (tmp_path / "fillability" / "fillability_market_ranking.parquet").exists()


def test_fillability_baseline_handles_empty_database(tmp_path: Path) -> None:
    db_path = tmp_path / "empty.duckdb"
    with duckdb.connect(str(db_path)):
        pass

    report = create_fillability_baseline_report(db_path, tmp_path / "fillability")

    counts = cast(dict[str, int], report["counts"])
    assert counts["fillability_market_features"] == 0
    assert counts["selected_fillability_markets"] == 0
    assert report["selected_market_asset_ids"] == []


def test_fillability_baseline_rejects_invalid_config() -> None:
    try:
        FillabilityBaselineConfig(min_future_touch_rate=2)
    except ValueError as exc:
        assert "min_future_touch_rate must be between 0 and 1" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def seed_fillability_db(tmp_path: Path) -> Path:
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
        signals = [
            (
                "signal-touch",
                "market-touch",
                "asset-touch",
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
                "signal-stale",
                "market-stale",
                "asset-stale",
                "BUY",
                0.45,
                1.0,
                0.65,
                "probe",
                "model",
                "data",
                "feature",
                1_000,
            ),
        ]
        conn.executemany("insert into signals values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", signals)
        conn.executemany(
            "insert into orderbook_snapshots values (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("market-touch", "asset-touch", 900, 0.45, 0.50, 0.05, 10.0, 10.0),
                ("market-touch", "asset-touch", 1_100, 0.45, 0.50, 0.05, 10.0, 10.0),
                ("market-stale", "asset-stale", 900, 0.40, 0.50, 0.10, 10.0, 10.0),
                ("market-stale", "asset-stale", 1_100, 0.40, 0.50, 0.10, 10.0, 10.0),
            ],
        )
    return db_path
