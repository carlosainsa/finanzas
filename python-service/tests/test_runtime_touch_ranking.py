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
    assert selected[0]["current_is_signalable"] is True
    assert selected[0]["last_signalable_timestamp_ms"] == 121_000
    assert selected[0]["recent_signalable_density"] == 1.0
    assert selected[0]["current_spread"] == 0.03
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


def test_runtime_touch_ranking_filters_non_signalable_activity(
    tmp_path: Path,
) -> None:
    db_path = seed_runtime_touch_db(tmp_path)
    with duckdb.connect(str(db_path)) as conn:
        conn.executemany(
            "insert into orderbook_snapshots values (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("market-thin", "asset-thin", 1_000, 0.40, 0.405, 0.005, 10.0, 10.0),
                ("market-thin", "asset-thin", 61_000, 0.401, 0.405, 0.004, 10.0, 10.0),
                ("market-thin", "asset-thin", 121_000, 0.401, 0.404, 0.003, 10.0, 10.0),
            ],
        )
        conn.execute(
            """
            insert into market_metadata values
            ('market-thin', 'asset-thin', 'YES', 'Thin question', 'thin-question',
             true, false, false, true, 1000.0, 2000.0, 1)
            """
        )

    report = create_runtime_touch_ranking_report(
        db_path,
        tmp_path / "runtime-touch",
        RuntimeTouchRankingConfig(
            min_snapshots=3,
            min_active_minutes=1,
            min_touch_change_rate=0.10,
            min_signalable_snapshots=2,
            min_signalable_density=0.50,
            signal_min_spread=0.01,
            signal_min_depth=1.5,
            limit=3,
        ),
    )

    selected = cast(list[dict[str, Any]], report["selected"])
    selected_assets = {str(row["asset_id"]) for row in selected}
    assert "asset-active" in selected_assets
    assert "asset-thin" not in selected_assets
    ranked = cast(list[dict[str, Any]], report["selected"])
    assert ranked[0]["signalable_snapshots"] >= 2


def test_runtime_touch_ranking_can_order_freshest_signalable_asset_first(
    tmp_path: Path,
) -> None:
    db_path = seed_runtime_touch_db(tmp_path)
    with duckdb.connect(str(db_path)) as conn:
        conn.executemany(
            "insert into orderbook_snapshots values (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("market-fresh", "asset-fresh", 301_000, 0.30, 0.33, 0.03, 5.0, 5.0),
                ("market-fresh", "asset-fresh", 361_000, 0.31, 0.34, 0.03, 5.0, 5.0),
                ("market-fresh", "asset-fresh", 421_000, 0.32, 0.35, 0.03, 5.0, 5.0),
            ],
        )
        conn.execute(
            """
            insert into market_metadata values
            ('market-fresh', 'asset-fresh', 'YES', 'Fresh question', 'fresh-question',
             true, false, false, true, 1000.0, 2000.0, 1)
            """
        )

    report = create_runtime_touch_ranking_report(
        db_path,
        tmp_path / "runtime-touch",
        RuntimeTouchRankingConfig(
            min_snapshots=3,
            min_active_minutes=1,
            min_touch_change_rate=0.10,
            freshness_ordering="freshest_first",
            recent_signalable_window_ms=180_000,
            limit=2,
        ),
    )

    selected = cast(list[dict[str, Any]], report["selected"])
    assert selected[0]["asset_id"] == "asset-fresh"
    assert selected[0]["current_is_signalable"] is True
    assert selected[0]["last_signalable_age_ms"] == 0
    assert selected[0]["book_age_ms"] == 0


def test_runtime_touch_ranking_prefers_assets_with_predictor_acceptance(
    tmp_path: Path,
) -> None:
    db_path = seed_runtime_touch_db(tmp_path)
    with duckdb.connect(str(db_path)) as conn:
        conn.executemany(
            "insert into orderbook_snapshots values (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("market-pass", "asset-pass", 1_000, 0.50, 0.55, 0.05, 10.0, 10.0),
                ("market-pass", "asset-pass", 61_000, 0.51, 0.55, 0.04, 10.0, 10.0),
                ("market-pass", "asset-pass", 121_000, 0.51, 0.54, 0.03, 10.0, 10.0),
            ],
        )
        conn.execute(
            """
            insert into market_metadata values
            ('market-pass', 'asset-pass', 'YES', 'Pass question', 'pass-question',
             true, false, false, true, 1000.0, 2000.0, 1)
            """
        )
        conn.execute(
            """
            create table predictor_decisions (
                market_id varchar,
                asset_id varchar,
                accepted boolean,
                rejection_reason varchar
            )
            """
        )
        conn.executemany(
            "insert into predictor_decisions values (?, ?, ?, ?)",
            [
                ("market-active", "asset-active", False, "top_rotation"),
                ("market-active", "asset-active", False, "top_rotation"),
                ("market-pass", "asset-pass", True, "accepted"),
                ("market-pass", "asset-pass", False, "rate_limited"),
            ],
        )

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

    selected = cast(list[dict[str, Any]], report["selected"])
    selected_assets = {str(row["asset_id"]) for row in selected}
    assert "asset-pass" in selected_assets
    assert "asset-active" not in selected_assets
    first = selected[0]
    assert first["predictor_accepted_count"] == 1
    assert first["predictor_accept_density"] == 0.5


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
