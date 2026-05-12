from pathlib import Path
from typing import Any, cast

import duckdb
def test_segment_opportunity_ranking_prefers_executable_opportunities(
    tmp_path: Path,
) -> None:
    from src.research.segment_opportunity_ranking import (
        REPORT_VERSION,
        SegmentOpportunityRankingConfig,
        create_segment_opportunity_ranking_report,
    )

    db_path = seed_executable_opportunity_db(tmp_path)
    output_dir = tmp_path / "segment_opportunity_ranking"

    report = create_segment_opportunity_ranking_report(
        db_path,
        output_dir,
        SegmentOpportunityRankingConfig(
            min_executable_opportunities=2,
            min_avg_expected_edge=0.01,
            min_avg_available_depth=20.0,
            limit=2,
        ),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert report["decision_policy"] == "offline_segment_opportunity_ranking_only"

    counts = cast(dict[str, Any], report["counts"])
    assert counts["ranked_segments"] == 3
    assert counts["selected_segments"] == 1

    selected = cast(list[dict[str, Any]], report["selected"])
    assert selected == [
        {
            "rank": 1,
            "market_id": "m1",
            "asset_id": "asset-executable",
            "side": "BUY",
            "strategy": "near_touch",
            "model_version": "research-model",
            "spread_bucket": "250_500bps",
            "timing_bucket": "stable",
            "executable_opportunities": 3,
            "avg_expected_edge": 0.04,
            "avg_available_depth": 50.0,
            "observed_fill_rate": 1.0,
            "synthetic_fill_rate": 1.0,
            "synthetic_observed_gap": 0.0,
            "adverse_30s_rate": 0.0,
            "avg_pnl_30s": 0.02,
            "recommendation": "PROMOTE_TO_OBSERVATION",
        }
    ]
    assert report["selected_segment_keys"] == [
        "m1|asset-executable|BUY|near_touch|research-model"
    ]
    assert (output_dir / "segment_opportunity_ranking.parquet").exists()
    assert (output_dir / "selected_segment_opportunities.parquet").exists()
    assert (output_dir / "allowed_segments.json").exists()
    assert (output_dir / "segment_opportunity_ranking.json").exists()


def seed_executable_opportunity_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "research.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            create table executable_opportunities (
                signal_id varchar,
                market_id varchar,
                asset_id varchar,
                side varchar,
                strategy varchar,
                model_version varchar,
                feature_version varchar,
                event_timestamp_ms bigint,
                spread_bucket varchar,
                timing_bucket varchar,
                expected_edge double,
                available_depth double,
                observed_filled boolean,
                synthetic_filled boolean,
                observed_fill_rate double,
                synthetic_fill_rate double,
                pnl_30s double,
                adverse_30s double,
                executable_score double,
                is_executable boolean
            )
            """
        )
        conn.executemany(
            "insert into executable_opportunities values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "s1",
                    "m1",
                    "asset-executable",
                    "BUY",
                    "near_touch",
                    "research-model",
                    "features",
                    1_000,
                    "250_500bps",
                    "stable",
                    0.03,
                    40.0,
                    True,
                    True,
                    1.0,
                    1.0,
                    0.02,
                    0.0,
                    2.0,
                    True,
                ),
                (
                    "s2",
                    "m1",
                    "asset-executable",
                    "BUY",
                    "near_touch",
                    "research-model",
                    "features",
                    2_000,
                    "250_500bps",
                    "stable",
                    0.04,
                    50.0,
                    True,
                    True,
                    1.0,
                    1.0,
                    0.02,
                    0.0,
                    2.0,
                    True,
                ),
                (
                    "s3",
                    "m1",
                    "asset-executable",
                    "BUY",
                    "near_touch",
                    "research-model",
                    "features",
                    3_000,
                    "250_500bps",
                    "stable",
                    0.05,
                    60.0,
                    True,
                    True,
                    1.0,
                    1.0,
                    0.02,
                    0.0,
                    2.0,
                    True,
                ),
                (
                    "s4",
                    "m2",
                    "asset-thin",
                    "BUY",
                    "near_touch",
                    "research-model",
                    "features",
                    1_000,
                    "250_500bps",
                    "stable",
                    0.04,
                    5.0,
                    True,
                    True,
                    1.0,
                    1.0,
                    0.02,
                    0.0,
                    2.0,
                    True,
                ),
                (
                    "s5",
                    "m2",
                    "asset-thin",
                    "BUY",
                    "near_touch",
                    "research-model",
                    "features",
                    2_000,
                    "250_500bps",
                    "stable",
                    0.05,
                    5.0,
                    True,
                    True,
                    1.0,
                    1.0,
                    0.02,
                    0.0,
                    2.0,
                    True,
                ),
                (
                    "s6",
                    "m3",
                    "asset-not-executable",
                    "SELL",
                    "near_touch",
                    "research-model",
                    "features",
                    1_000,
                    "250_500bps",
                    "stable",
                    0.20,
                    100.0,
                    False,
                    False,
                    0.0,
                    0.0,
                    None,
                    None,
                    0.0,
                    False,
                ),
            ],
        )
    return db_path
