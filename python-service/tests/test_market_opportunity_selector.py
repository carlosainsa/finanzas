from pathlib import Path
from typing import Any, cast

import duckdb

from src.research.market_opportunity_selector import (
    REPORT_VERSION,
    MarketOpportunityConfig,
    create_market_opportunity_report,
)


def test_market_opportunity_selector_ranks_spread_dense_markets(
    tmp_path: Path,
) -> None:
    db_path = seed_db(tmp_path)
    output_dir = tmp_path / "market_opportunity"

    report = create_market_opportunity_report(
        db_path,
        output_dir,
        MarketOpportunityConfig(
            min_spread=0.03,
            max_spread=0.30,
            min_snapshots=2,
            min_opportunity_density=0.25,
            min_liquidity=100.0,
            limit=2,
        ),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    counts = cast(dict[str, Any], report["counts"])
    assert counts["ranked_markets"] == 2
    assert counts["selected_markets"] == 2
    selected = cast(list[dict[str, Any]], report["selected"])
    assert selected[0]["asset_id"] == "asset-a"
    assert selected[0]["opportunity_snapshots"] == 3
    assert selected[0]["spread_opportunity_density"] == 1.0
    assert report["selected_market_asset_ids_csv"] == "asset-a,asset-b"
    assert (output_dir / "market_opportunity_ranking.parquet").exists()
    assert (output_dir / "selected_market_opportunities.parquet").exists()


def test_market_opportunity_selector_handles_empty_inputs(tmp_path: Path) -> None:
    db_path = tmp_path / "empty.duckdb"
    with duckdb.connect(str(db_path)):
        pass

    report = create_market_opportunity_report(db_path, tmp_path / "selector")

    assert cast(dict[str, Any], report["counts"])["ranked_markets"] == 0
    assert report["selected_market_asset_ids"] == []


def seed_db(tmp_path: Path) -> Path:
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
                ("m1", "asset-a", 1_000, 0.40, 0.45, 0.05, 10.0, 10.0),
                ("m1", "asset-a", 2_000, 0.41, 0.46, 0.05, 10.0, 10.0),
                ("m1", "asset-a", 3_000, 0.42, 0.47, 0.05, 10.0, 10.0),
                ("m2", "asset-b", 1_000, 0.40, 0.45, 0.05, 10.0, 10.0),
                ("m2", "asset-b", 2_000, 0.44, 0.45, 0.01, 10.0, 10.0),
                ("m2", "asset-b", 3_000, 0.45, 0.46, 0.01, 10.0, 10.0),
                ("m3", "asset-c", 1_000, 0.40, 0.45, 0.05, 10.0, 10.0),
                ("m3", "asset-c", 2_000, 0.40, 0.45, 0.05, 10.0, 10.0),
            ],
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
            "insert into market_metadata values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("m1", "asset-a", "YES", "A", "a", True, False, False, True, 1_000.0, 2_000.0, 1),
                ("m2", "asset-b", "YES", "B", "b", True, False, False, True, 1_000.0, 2_000.0, 1),
                ("m3", "asset-c", "YES", "C", "c", True, False, False, True, 10.0, 2_000.0, 1),
            ],
        )
    return db_path
