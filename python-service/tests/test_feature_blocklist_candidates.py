from pathlib import Path

import duckdb

from src.research.feature_blocklist_candidates import (
    create_feature_blocklist_candidate_views,
    export_feature_blocklist_candidate_report,
)
from test_market_regime import seed_market_regime_db
from test_sentiment_lift import seed_sentiment_lift_db


def test_feature_blocklist_candidates_mark_bad_sentiment_bucket(
    tmp_path: Path,
) -> None:
    db_path = seed_sentiment_lift_db(tmp_path)

    create_feature_blocklist_candidate_views(
        db_path,
        lookback_ms=900,
        min_samples=1,
        min_adverse_edge_rate=0.5,
        max_sentiment_disagreement=0.0,
    )

    with duckdb.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            select
                feature_family,
                feature_name,
                bucket,
                candidate_reason,
                should_block_candidate,
                can_apply_live
            from research_feature_blocklist_candidates
            where feature_family = 'sentiment'
              and bucket = 'opposed:strong_negative'
            """
        ).fetchone()

    assert row == (
        "sentiment",
        "sentiment_alignment",
        "opposed:strong_negative",
        "negative_edge_high_adverse_rate",
        True,
        False,
    )


def test_feature_blocklist_candidates_include_regime_buckets(tmp_path: Path) -> None:
    db_path = seed_market_regime_db(tmp_path)

    create_feature_blocklist_candidate_views(db_path, min_samples=1)

    with duckdb.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            select distinct feature_family, feature_name
            from research_feature_bucket_performance
            where feature_family = 'regime'
            order by feature_name
            """
        ).fetchall()

    assert ("regime", "tail_risk") in rows
    assert ("regime", "whale_pressure") in rows


def test_export_feature_blocklist_candidate_report_writes_outputs(
    tmp_path: Path,
) -> None:
    db_path = seed_sentiment_lift_db(tmp_path)
    output_dir = tmp_path / "feature_blocklist_candidates"

    report = export_feature_blocklist_candidate_report(
        db_path,
        output_dir,
        lookback_ms=900,
        min_samples=1,
        min_adverse_edge_rate=0.5,
    )

    counts = report["counts"]
    assert isinstance(counts, dict)
    assert counts["research_feature_bucket_performance"] >= 2
    assert counts["research_feature_blocklist_candidates"] >= 2
    assert counts["blocked_segment_candidates"] >= 1
    assert report["can_execute_trades"] is False
    assert report["can_apply_live"] is False
    assert (output_dir / "research_feature_bucket_performance.parquet").exists()
    assert (output_dir / "research_feature_blocklist_candidates.parquet").exists()
    assert (output_dir / "blocked_segments_candidates.json").exists()
    assert (output_dir / "feature_blocklist_candidates.json").exists()
