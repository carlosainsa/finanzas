from pathlib import Path

import duckdb

from src.research.runtime_touch_candidate_expansion import (
    REPORT_VERSION,
    RuntimeTouchCandidateExpansionConfig,
    create_runtime_touch_candidate_expansion,
)


def test_candidate_expansion_selects_signalable_liquid_assets(
    tmp_path: Path,
) -> None:
    db_path = seed_candidate_expansion_db(tmp_path)

    report = create_runtime_touch_candidate_expansion(
        db_path,
        tmp_path / "expansion",
        RuntimeTouchCandidateExpansionConfig(
            min_assets=2,
            limit=3,
            min_snapshots=3,
            min_active_minutes=1,
            min_signalable_snapshots=1,
            min_signalable_density=0.01,
        ),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert report["decision_policy"] == "offline_runtime_touch_selection_only"
    assert report["status"] == "ready"
    assert report["market_asset_ids_count"] == 2
    assert report["risk_contract"]["does_not_modify_risk_limits"] is True
    assert report["market_asset_ids"] == ["asset-a", "asset-b"]
    assert (
        tmp_path / "expansion" / "runtime_touch_candidate_expansion.parquet"
    ).exists()


def test_candidate_expansion_respects_exclusions(
    tmp_path: Path,
) -> None:
    db_path = seed_candidate_expansion_db(tmp_path)

    report = create_runtime_touch_candidate_expansion(
        db_path,
        tmp_path / "expansion",
        RuntimeTouchCandidateExpansionConfig(
            min_assets=2,
            limit=3,
            min_snapshots=3,
            min_active_minutes=1,
            min_signalable_snapshots=1,
            min_signalable_density=0.01,
            excluded_asset_ids=("asset-a",),
        ),
    )

    assert report["status"] == "insufficient_assets"
    assert report["market_asset_ids"] == ["asset-b"]


def seed_candidate_expansion_db(tmp_path: Path) -> Path:
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
            spread = 0.03 if asset_id != "asset-thin" else 0.01
            for offset in range(3):
                rows.append(
                    (
                        market_id,
                        asset_id,
                        1_000 + offset * 60_000,
                        0.40,
                        0.40 + spread,
                        spread,
                        10.0,
                        10.0,
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
