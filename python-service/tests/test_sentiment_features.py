from pathlib import Path

import duckdb
import pytest

from src.research.data_lake import (
    create_duckdb_views,
    export_external_evidence,
)
from src.research.sentiment_features import (
    create_sentiment_feature_views,
    export_sentiment_feature_report,
)


def test_sentiment_feature_builder_aggregates_lookback_without_future_evidence(
    tmp_path: Path,
) -> None:
    db_path = seed_sentiment_db(tmp_path)

    create_sentiment_feature_views(db_path, lookback_ms=1_000)

    with duckdb.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            select
                feature_timestamp_ms,
                evidence_count,
                source_count,
                direction,
                sentiment_score,
                available_at_ms
            from sentiment_feature_candidates
            order by feature_timestamp_ms
            """
        ).fetchall()

    assert rows[0] == (1_100, 1, 1, "YES", pytest.approx(0.6), 1_100)
    assert rows[1] == (1_900, 2, 2, "YES", pytest.approx(0.2), 1_900)
    assert rows[2] == (3_500, 1, 1, "YES", pytest.approx(0.9), 3_500)


def test_export_sentiment_feature_report_writes_outputs(tmp_path: Path) -> None:
    db_path = seed_sentiment_db(tmp_path)
    output_dir = tmp_path / "sentiment_features"

    report = export_sentiment_feature_report(db_path, output_dir, lookback_ms=1_000)

    counts = report["counts"]
    assert isinstance(counts, dict)
    assert counts["sentiment_feature_candidates"] == 3
    assert report["can_execute_trades"] is False
    assert (output_dir / "sentiment_feature_candidates.parquet").exists()
    assert (output_dir / "sentiment_features.json").exists()


def seed_sentiment_db(tmp_path: Path) -> Path:
    export_external_evidence(
        tmp_path,
        [
            evidence("evidence-1", "source-a", 1_000, 1_100, 0.6),
            evidence("evidence-2", "source-b", 1_800, 1_900, -0.2),
            evidence("evidence-3", "source-c", 3_400, 3_500, 0.9),
        ],
    )
    db_path = tmp_path / "research.duckdb"
    create_duckdb_views(tmp_path, db_path)
    return db_path


def evidence(
    evidence_id: str,
    source: str,
    published_at_ms: int,
    available_at_ms: int,
    sentiment_score: float,
) -> dict[str, object]:
    return {
        "evidence_id": evidence_id,
        "source": source,
        "source_type": "news",
        "published_at_ms": published_at_ms,
        "observed_at_ms": available_at_ms,
        "available_at_ms": available_at_ms,
        "market_id": "market-1",
        "asset_id": "asset-yes",
        "raw_reference_hash": f"sha256:{evidence_id}",
        "direction": "YES" if sentiment_score > 0 else "NO",
        "sentiment_score": sentiment_score,
        "source_quality": 0.8,
        "confidence": 0.7,
        "data_version": "external_evidence_v1",
    }
