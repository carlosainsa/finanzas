from pathlib import Path

import duckdb

from src.research.runtime_touch_market_timing_scout import (
    REPORT_VERSION,
    RuntimeTouchMarketTimingScoutConfig,
    create_runtime_touch_market_timing_scout,
)


def test_market_timing_scout_selects_comparable_active_window(
    tmp_path: Path,
) -> None:
    db_path = seed_market_timing_scout_db(tmp_path)

    report = create_runtime_touch_market_timing_scout(
        db_path,
        tmp_path / "scout",
        RuntimeTouchMarketTimingScoutConfig(
            window_ms=900_000,
            min_assets=2,
            min_window_snapshots=2,
            min_signalable_snapshots=1,
            min_signalable_density=0.25,
        ),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert report["decision_policy"] == "offline_runtime_touch_market_timing_scout_only"
    assert report["status"] == "ready"
    assert report["market_asset_ids"] == ["asset-a", "asset-b"]
    assert report["market_asset_ids_count"] == 2
    assert report["counts"]["eligible_windows"] == 1
    assert report["risk_contract"]["does_not_publish_signals"] is True
    assert (
        tmp_path / "scout" / "runtime_touch_market_timing_windows.parquet"
    ).exists()
    assert (
        tmp_path / "scout" / "selected_runtime_touch_market_timing_assets.parquet"
    ).exists()


def test_market_timing_scout_reports_insufficient_assets(
    tmp_path: Path,
) -> None:
    db_path = seed_market_timing_scout_db(tmp_path, second_asset_signalable=False)

    report = create_runtime_touch_market_timing_scout(
        db_path,
        tmp_path / "scout",
        RuntimeTouchMarketTimingScoutConfig(
            window_ms=900_000,
            min_assets=2,
            min_window_snapshots=2,
            min_signalable_snapshots=1,
            min_signalable_density=0.25,
        ),
    )

    assert report["status"] == "insufficient_assets"
    assert report["market_asset_ids"] == []
    assert "below_minimum_2" in report["selection_reason"]


def test_market_timing_scout_config_rejects_invalid_density() -> None:
    try:
        RuntimeTouchMarketTimingScoutConfig(min_signalable_density=1.5)
    except ValueError as exc:
        assert str(exc) == "min_signalable_density must be between 0 and 1"
    else:
        raise AssertionError("expected invalid density to fail")


def seed_market_timing_scout_db(
    tmp_path: Path,
    *,
    second_asset_signalable: bool = True,
) -> Path:
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
        rows = []
        metadata = []
        for index, asset_id in enumerate(("asset-a", "asset-b", "asset-thin")):
            market_id = f"market-{asset_id}"
            spread = 0.04
            if asset_id == "asset-b" and not second_asset_signalable:
                spread = 0.01
            if asset_id == "asset-thin":
                spread = 0.01
            depth = 8.0 if asset_id != "asset-thin" else 0.5
            for offset in range(3):
                best_bid = 0.40 + offset * 0.001
                rows.append(
                    (
                        market_id,
                        asset_id,
                        10_000_000 + offset * 60_000,
                        best_bid,
                        best_bid + spread,
                        spread,
                        depth,
                        depth,
                    )
                )
            metadata.append(
                (
                    market_id,
                    asset_id,
                    "YES",
                    f"Question {index}",
                    f"question-{index}",
                    True,
                    False,
                    False,
                    True,
                    10_000.0 - index * 1_000,
                    20_000.0 - index * 1_000,
                    1,
                )
            )
        conn.executemany(
            "insert into orderbook_snapshots values (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.executemany(
            "insert into market_metadata values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            metadata,
        )
    return db_path
