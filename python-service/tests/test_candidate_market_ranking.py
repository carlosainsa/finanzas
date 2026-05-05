from pathlib import Path
from typing import Any, cast

import duckdb

from src.research.candidate_market_ranking import (
    REPORT_VERSION,
    CandidateMarketRankingConfig,
    create_candidate_market_ranking_report,
)


def test_candidate_market_ranking_promotes_assets_with_spread_and_execution(
    tmp_path: Path,
) -> None:
    db_path = seed_candidate_db(tmp_path)
    output_dir = tmp_path / "candidate_market_ranking"

    report = create_candidate_market_ranking_report(
        db_path,
        output_dir,
        CandidateMarketRankingConfig(
            max_stale_rate=1.0,
            min_execution_fill_rate=0.01,
            max_unfilled_rate=0.95,
        ),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert report["decision_policy"] == "offline_combined_market_ranking_only"
    counts = cast(dict[str, Any], report["counts"])
    assert counts["candidate_market_ranking"] == 2
    assert counts["selected_candidate_markets"] == 1
    assert cast(dict[str, Any], counts["recommendations"]) == {
        "NEEDS_EXECUTION_EVIDENCE": 1,
        "PROMOTE_TO_OBSERVATION": 1,
    }
    selected = cast(list[dict[str, Any]], report["selected"])
    assert selected[0]["asset_id"] == "asset-good"
    assert selected[0]["recommendation"] == "PROMOTE_TO_OBSERVATION"
    assert selected[0]["observed_fill_rate"] == 1.0
    assert report["selected_market_asset_ids"] == ["asset-good"]
    assert (output_dir / "candidate_market_ranking.parquet").exists()
    assert (output_dir / "selected_candidate_markets.parquet").exists()


def test_candidate_market_ranking_keeps_spread_only_assets_diagnostic(
    tmp_path: Path,
) -> None:
    db_path = seed_spread_only_db(tmp_path)

    report = create_candidate_market_ranking_report(
        db_path,
        tmp_path / "ranking",
        CandidateMarketRankingConfig(max_stale_rate=1.0),
    )

    counts = cast(dict[str, Any], report["counts"])
    assert counts["candidate_market_ranking"] == 1
    assert counts["selected_candidate_markets"] == 0
    assert cast(dict[str, Any], counts["recommendations"]) == {
        "NEEDS_EXECUTION_EVIDENCE": 1
    }
    assert report["selected_market_asset_ids"] == []


def test_candidate_market_ranking_rejects_invalid_config() -> None:
    try:
        CandidateMarketRankingConfig(opportunity_weight=0.0, execution_weight=0.0)
    except ValueError as exc:
        assert "at least one weight must be positive" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def seed_candidate_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "research.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        seed_tables(conn)
        conn.executemany(
            "insert into execution_reports values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("s1", "o1", "MATCHED", 0.45, 10.0, 10.0, 0.0, None, 1_100),
                ("s2", "o2", "MATCHED", 0.46, 10.0, 10.0, 0.0, None, 2_100),
                ("s3", "o3", "MATCHED", 0.47, 10.0, 10.0, 0.0, None, 3_100),
                ("s4", "o4", "MATCHED", 0.48, 10.0, 10.0, 0.0, None, 4_100),
                ("s5", "o5", "MATCHED", 0.49, 10.0, 10.0, 0.0, None, 5_100),
            ],
        )
    return db_path


def seed_spread_only_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "spread-only.duckdb"
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
                ("m1", "asset-spread-only", 1_000, 0.40, 0.45, 0.05, 10.0, 10.0),
                ("m1", "asset-spread-only", 2_000, 0.40, 0.45, 0.05, 10.0, 10.0),
                ("m1", "asset-spread-only", 3_000, 0.40, 0.45, 0.05, 10.0, 10.0),
                ("m1", "asset-spread-only", 4_000, 0.40, 0.45, 0.05, 10.0, 10.0),
                ("m1", "asset-spread-only", 5_000, 0.40, 0.45, 0.05, 10.0, 10.0),
                ("m1", "asset-spread-only", 6_000, 0.40, 0.45, 0.05, 10.0, 10.0),
                ("m1", "asset-spread-only", 7_000, 0.40, 0.45, 0.05, 10.0, 10.0),
                ("m1", "asset-spread-only", 8_000, 0.40, 0.45, 0.05, 10.0, 10.0),
                ("m1", "asset-spread-only", 9_000, 0.40, 0.45, 0.05, 10.0, 10.0),
                ("m1", "asset-spread-only", 10_000, 0.40, 0.45, 0.05, 10.0, 10.0),
            ],
        )
    return db_path


def seed_tables(conn: duckdb.DuckDBPyConnection) -> None:
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
    conn.executemany(
        "insert into signals values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("s1", "m1", "asset-good", "BUY", 0.45, 10.0, 0.65, "near", "m", "d", "f", 1_000),
            ("s2", "m1", "asset-good", "BUY", 0.46, 10.0, 0.65, "near", "m", "d", "f", 2_000),
            ("s3", "m1", "asset-good", "BUY", 0.47, 10.0, 0.65, "near", "m", "d", "f", 3_000),
            ("s4", "m1", "asset-good", "BUY", 0.48, 10.0, 0.65, "near", "m", "d", "f", 4_000),
            ("s5", "m1", "asset-good", "BUY", 0.49, 10.0, 0.65, "near", "m", "d", "f", 5_000),
        ],
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
            ("m1", "asset-good", 900, 0.40, 0.45, 0.05, 50.0, 50.0),
            ("m1", "asset-good", 1_900, 0.41, 0.46, 0.05, 50.0, 50.0),
            ("m1", "asset-good", 2_900, 0.42, 0.47, 0.05, 50.0, 50.0),
            ("m1", "asset-good", 3_900, 0.43, 0.48, 0.05, 50.0, 50.0),
            ("m1", "asset-good", 4_900, 0.44, 0.49, 0.05, 50.0, 50.0),
            ("m1", "asset-good", 5_900, 0.45, 0.50, 0.05, 50.0, 50.0),
            ("m1", "asset-good", 6_900, 0.45, 0.50, 0.05, 50.0, 50.0),
            ("m1", "asset-good", 7_900, 0.45, 0.50, 0.05, 50.0, 50.0),
            ("m1", "asset-good", 8_900, 0.45, 0.50, 0.05, 50.0, 50.0),
            ("m1", "asset-good", 9_900, 0.45, 0.50, 0.05, 50.0, 50.0),
            ("m2", "asset-spread-only", 900, 0.40, 0.45, 0.05, 50.0, 50.0),
            ("m2", "asset-spread-only", 1_900, 0.40, 0.45, 0.05, 50.0, 50.0),
            ("m2", "asset-spread-only", 2_900, 0.40, 0.45, 0.05, 50.0, 50.0),
            ("m2", "asset-spread-only", 3_900, 0.40, 0.45, 0.05, 50.0, 50.0),
            ("m2", "asset-spread-only", 4_900, 0.40, 0.45, 0.05, 50.0, 50.0),
            ("m2", "asset-spread-only", 5_900, 0.40, 0.45, 0.05, 50.0, 50.0),
            ("m2", "asset-spread-only", 6_900, 0.40, 0.45, 0.05, 50.0, 50.0),
            ("m2", "asset-spread-only", 7_900, 0.40, 0.45, 0.05, 50.0, 50.0),
            ("m2", "asset-spread-only", 8_900, 0.40, 0.45, 0.05, 50.0, 50.0),
            ("m2", "asset-spread-only", 9_900, 0.40, 0.45, 0.05, 50.0, 50.0),
            ("m2", "asset-spread-only", 10_900, 0.40, 0.45, 0.05, 50.0, 50.0),
        ],
    )
