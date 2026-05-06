from pathlib import Path
from typing import Any, cast

import duckdb

from src.research.execution_probe_universe_selection import (
    REPORT_VERSION,
    ExecutionProbeUniverseConfig,
    create_execution_probe_universe_selection,
)


def test_execution_probe_universe_selection_exports_ready_contract(
    tmp_path: Path,
) -> None:
    db_path = seed_universe_db(tmp_path, asset_count=6)

    report = create_execution_probe_universe_selection(
        db_path,
        tmp_path / "universe",
        ExecutionProbeUniverseConfig(limit=5, min_assets=5),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert report["decision_policy"] == "offline_multi_market_observation_universe_only"
    assert report["profile"] == "execution_probe_v5"
    assert report["status"] == "ready"
    assert report["market_asset_ids_count"] == 5
    assert len(str(report["market_asset_ids_sha256"])) == 64
    assert (tmp_path / "universe" / "execution_probe_universe_selection.parquet").exists()
    selected = cast(list[dict[str, Any]], report["selected"])
    assert selected[0]["asset_id"] == "asset-5"


def test_execution_probe_universe_selection_marks_insufficient_assets(
    tmp_path: Path,
) -> None:
    db_path = seed_universe_db(tmp_path, asset_count=3)

    report = create_execution_probe_universe_selection(
        db_path,
        tmp_path / "universe",
        ExecutionProbeUniverseConfig(limit=5, min_assets=5),
    )

    assert report["status"] == "insufficient_assets"
    assert report["market_asset_ids_count"] == 3
    assert "repeat_collection" in str(report["selection_reason"])


def test_execution_probe_universe_selection_rejects_invalid_profile() -> None:
    try:
        ExecutionProbeUniverseConfig(profile="live")
    except ValueError as exc:
        assert "profile must be execution_probe_v5 or execution_probe_v6" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def seed_universe_db(tmp_path: Path, asset_count: int) -> Path:
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
        snapshots = []
        metadata = []
        for asset_index in range(asset_count):
            asset_id = f"asset-{asset_index}"
            market_id = f"market-{asset_index}"
            for offset in range(10):
                snapshots.append(
                    (
                        market_id,
                        asset_id,
                        1_000 + offset * 1_000,
                        0.40,
                        0.45,
                        0.05,
                        10.0 + asset_index,
                        10.0 + asset_index,
                    )
                )
            metadata.append(
                (
                    market_id,
                    asset_id,
                    "YES",
                    f"Question {asset_index}",
                    f"question-{asset_index}",
                    True,
                    False,
                    False,
                    True,
                    1_000.0 + asset_index,
                    2_000.0 + asset_index,
                    1,
                )
            )
        conn.executemany(
            "insert into orderbook_snapshots values (?, ?, ?, ?, ?, ?, ?, ?)",
            snapshots,
        )
        conn.executemany(
            "insert into market_metadata values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            metadata,
        )
    return db_path
