from pathlib import Path
from typing import Any, cast

import duckdb

from src.research.runtime_touch_ranking import (
    REPORT_VERSION,
    RuntimeTouchRankingConfig,
    create_runtime_touch_ranking_report,
)


def test_runtime_touch_ranking_selects_fresh_top_of_book_activity(
    tmp_path: Path,
) -> None:
    db_path = seed_runtime_touch_db(tmp_path)

    report = create_runtime_touch_ranking_report(
        db_path,
        tmp_path / "runtime-touch",
        RuntimeTouchRankingConfig(
            min_snapshots=3,
            min_active_minutes=1,
            min_touch_change_rate=0.10,
            limit=2,
        ),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    selected = cast(list[dict[str, Any]], report["selected"])
    assert selected[0]["asset_id"] == "asset-active"
    assert selected[0]["recommendation"] == "PROMOTE_TO_OBSERVATION"
    assert (
        tmp_path / "runtime-touch" / "selected_runtime_touch_markets.parquet"
    ).exists()


def test_runtime_touch_ranking_rejects_invalid_config() -> None:
    try:
        RuntimeTouchRankingConfig(min_touch_change_rate=1.5)
    except ValueError as exc:
        assert "min_touch_change_rate must be between 0 and 1" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def seed_runtime_touch_db(tmp_path: Path) -> Path:
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
        conn.execute(
            """
            create table market_metadata (
                market_id varchar,
                asset_id varchar,
                outcome varchar,
                question varchar,
                slug varchar,
                active boolean,
                closed boolean,
                archived boolean,
                enable_order_book boolean,
                liquidity double,
                volume double,
                ingested_at_ms bigint
            )
            """
        )
        conn.executemany(
            "insert into orderbook_snapshots values (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("market-active", "asset-active", 1_000, 0.40, 0.45, 0.05, 10.0, 10.0),
                ("market-active", "asset-active", 61_000, 0.41, 0.45, 0.04, 10.0, 10.0),
                ("market-active", "asset-active", 121_000, 0.41, 0.44, 0.03, 10.0, 10.0),
                ("market-stale", "asset-stale", 1_000, 0.40, 0.45, 0.05, 10.0, 10.0),
                ("market-stale", "asset-stale", 61_000, 0.40, 0.45, 0.05, 10.0, 10.0),
                ("market-stale", "asset-stale", 121_000, 0.40, 0.45, 0.05, 10.0, 10.0),
            ],
        )
        conn.executemany(
            "insert into market_metadata values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "market-active",
                    "asset-active",
                    "YES",
                    "Active question",
                    "active-question",
                    True,
                    False,
                    False,
                    True,
                    1000.0,
                    2000.0,
                    1,
                ),
                (
                    "market-stale",
                    "asset-stale",
                    "YES",
                    "Stale question",
                    "stale-question",
                    True,
                    False,
                    False,
                    True,
                    1000.0,
                    2000.0,
                    1,
                ),
            ],
        )
    return db_path
