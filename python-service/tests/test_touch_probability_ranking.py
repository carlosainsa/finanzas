from pathlib import Path
from typing import Any, cast

import duckdb

from src.research.touch_probability_ranking import (
    REPORT_VERSION,
    TouchProbabilityRankingConfig,
    create_touch_probability_ranking_report,
)


def test_touch_probability_ranking_prioritizes_future_touch_evidence(
    tmp_path: Path,
) -> None:
    db_path = seed_touch_probability_db(tmp_path)

    report = create_touch_probability_ranking_report(
        db_path,
        tmp_path / "touch",
        TouchProbabilityRankingConfig(min_signals=1, min_future_touch_rate=0.05),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert report["decision_policy"] == "offline_touch_probability_ranking_only"
    selected = cast(list[dict[str, Any]], report["selected"])
    assert selected[0]["asset_id"] == "asset-touch"
    assert selected[0]["recommendation"] == "PROMOTE_TO_OBSERVATION"
    assert (tmp_path / "touch" / "touch_probability_ranking.parquet").exists()


def test_touch_probability_ranking_rejects_invalid_config() -> None:
    try:
        TouchProbabilityRankingConfig(min_future_touch_rate=2)
    except ValueError as exc:
        assert "min_future_touch_rate must be between 0 and 1" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def seed_touch_probability_db(tmp_path: Path) -> Path:
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
                    "signal-touch",
                    "market-touch",
                    "asset-touch",
                    "BUY",
                    0.49,
                    1.0,
                    0.65,
                    "probe",
                    "model",
                    "data",
                    "feature",
                    1_000,
                ),
                (
                    "signal-never",
                    "market-never",
                    "asset-never",
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
                ("market-touch", "asset-touch", 900, 0.45, 0.50, 0.05, 10.0, 10.0),
                ("market-touch", "asset-touch", 1_100, 0.45, 0.49, 0.04, 10.0, 10.0),
                ("market-never", "asset-never", 900, 0.35, 0.50, 0.15, 10.0, 10.0),
                ("market-never", "asset-never", 1_100, 0.35, 0.50, 0.15, 10.0, 10.0),
            ],
        )
    return db_path
