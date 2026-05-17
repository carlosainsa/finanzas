from pathlib import Path

import duckdb

from src.research.runtime_touch_opportunity_windows import (
    REPORT_VERSION,
    RuntimeTouchOpportunityWindowConfig,
    create_runtime_touch_opportunity_windows,
)


def test_opportunity_windows_selects_assets_with_signalable_windows(
    tmp_path: Path,
) -> None:
    db_path = seed_opportunity_window_db(tmp_path)

    report = create_runtime_touch_opportunity_windows(
        db_path,
        tmp_path / "windows",
        RuntimeTouchOpportunityWindowConfig(
            window_ms=300_000,
            min_assets=2,
            limit=3,
            min_window_snapshots=2,
            min_window_signalable_snapshots=1,
            min_window_signalable_density=0.25,
        ),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert (
        report["decision_policy"]
        == "offline_runtime_touch_opportunity_window_selection_only"
    )
    assert report["status"] == "ready"
    assert report["selection_source"] == "runtime_touch_opportunity_windows"
    assert report["market_asset_ids"] == ["asset-a", "asset-b"]
    assert report["market_asset_ids_count"] == 2
    assert report["risk_contract"]["does_not_modify_quote_policy"] is True
    assert report["risk_contract"]["does_not_modify_risk_limits"] is True
    assert (tmp_path / "windows" / "runtime_touch_opportunity_windows.parquet").exists()
    assert (
        tmp_path / "windows" / "selected_runtime_touch_opportunity_window_assets.parquet"
    ).exists()


def test_opportunity_windows_respects_exclusions(
    tmp_path: Path,
) -> None:
    db_path = seed_opportunity_window_db(tmp_path)

    report = create_runtime_touch_opportunity_windows(
        db_path,
        tmp_path / "windows",
        RuntimeTouchOpportunityWindowConfig(
            window_ms=300_000,
            min_assets=2,
            limit=3,
            min_window_snapshots=2,
            min_window_signalable_snapshots=1,
            min_window_signalable_density=0.25,
            excluded_asset_ids=("asset-a",),
        ),
    )

    assert report["status"] == "insufficient_assets"
    assert report["market_asset_ids"] == ["asset-b"]
    assert "only_1_assets_available_below_minimum_2" in report["selection_reason"]


def test_opportunity_window_config_rejects_invalid_limit() -> None:
    try:
        RuntimeTouchOpportunityWindowConfig(min_assets=2, limit=1)
    except ValueError as exc:
        assert str(exc) == "limit must be >= min_assets"
    else:
        raise AssertionError("expected invalid limit to fail")


def seed_opportunity_window_db(tmp_path: Path) -> Path:
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
            spread = 0.04 if asset_id != "asset-thin" else 0.01
            bid_depth = 8.0 if asset_id != "asset-thin" else 0.5
            ask_depth = 8.0 if asset_id != "asset-thin" else 0.5
            for offset in range(3):
                rows.append(
                    (
                        market_id,
                        asset_id,
                        1_000_000 + offset * 60_000,
                        0.40,
                        0.40 + spread,
                        spread,
                        bid_depth,
                        ask_depth,
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
