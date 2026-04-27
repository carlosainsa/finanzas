import asyncio
from pathlib import Path

import duckdb
import pytest

from src.config import settings
from src.research.data_lake import create_duckdb_views, export_data_lake, export_external_evidence
from src.research.sentiment_lift import create_sentiment_lift_views, export_sentiment_lift_report
from test_game_theory import FakeRedis
from test_sentiment_features import evidence


def test_sentiment_lift_uses_latest_available_feature_before_signal(
    tmp_path: Path,
) -> None:
    db_path = seed_sentiment_lift_db(tmp_path)

    create_sentiment_lift_views(db_path, lookback_ms=900)

    with duckdb.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            select
                signal_id,
                sentiment_available_at_ms,
                sentiment_direction,
                sentiment_bucket,
                signal_sentiment_alignment,
                realized_edge_after_slippage
            from sentiment_lift_trade_context
            order by signal_id
            """
        ).fetchall()

    assert rows == [
        ("signal-1", 1_500, "YES", "strong_positive", "aligned", pytest.approx(0.10)),
        ("signal-2", 2_500, "NO", "strong_negative", "opposed", pytest.approx(-0.10)),
    ]


def test_sentiment_lift_summary_reports_bucket_lift(tmp_path: Path) -> None:
    db_path = seed_sentiment_lift_db(tmp_path)

    create_sentiment_lift_views(db_path, lookback_ms=900)

    with duckdb.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            select
                signal_sentiment_alignment,
                sentiment_bucket,
                signals,
                avg_realized_edge_after_slippage,
                realized_edge_lift,
                adverse_edge_rate
            from sentiment_lift_summary
            order by signal_sentiment_alignment
            """
        ).fetchall()

    assert rows == [
        ("aligned", "strong_positive", 1, pytest.approx(0.10), pytest.approx(0.10), pytest.approx(0.0)),
        ("opposed", "strong_negative", 1, pytest.approx(-0.10), pytest.approx(-0.10), pytest.approx(1.0)),
    ]


def test_export_sentiment_lift_report_writes_outputs(tmp_path: Path) -> None:
    db_path = seed_sentiment_lift_db(tmp_path)
    output_dir = tmp_path / "sentiment_lift"

    report = export_sentiment_lift_report(db_path, output_dir, lookback_ms=900)

    counts = report["counts"]
    assert isinstance(counts, dict)
    assert counts["sentiment_lift_trade_context"] == 2
    assert counts["sentiment_lift_summary"] == 2
    assert report["can_execute_trades"] is False
    assert (output_dir / "sentiment_lift_trade_context.parquet").exists()
    assert (output_dir / "sentiment_lift_summary.parquet").exists()
    assert (output_dir / "sentiment_lift.json").exists()


def seed_sentiment_lift_db(tmp_path: Path) -> Path:
    redis = FakeRedis()
    redis.add_payload(
        settings.signals_stream,
        {
            "signal_id": "signal-1",
            "market_id": "market-1",
            "asset_id": "asset-yes",
            "side": "BUY",
            "price": 0.50,
            "size": 1.0,
            "confidence": 0.60,
            "timestamp_ms": 2_000,
            "strategy": "sentiment-test",
        },
    )
    redis.add_payload(
        settings.execution_reports_stream,
        {
            "signal_id": "signal-1",
            "order_id": "order-1",
            "status": "MATCHED",
            "filled_price": 0.50,
            "filled_size": 1.0,
            "cumulative_filled_size": 1.0,
            "remaining_size": 0.0,
            "timestamp_ms": 2_100,
        },
    )
    redis.add_payload(
        settings.signals_stream,
        {
            "signal_id": "signal-2",
            "market_id": "market-1",
            "asset_id": "asset-yes",
            "side": "BUY",
            "price": 0.50,
            "size": 1.0,
            "confidence": 0.40,
            "timestamp_ms": 3_000,
            "strategy": "sentiment-test",
        },
    )
    redis.add_payload(
        settings.execution_reports_stream,
        {
            "signal_id": "signal-2",
            "order_id": "order-2",
            "status": "MATCHED",
            "filled_price": 0.50,
            "filled_size": 1.0,
            "cumulative_filled_size": 1.0,
            "remaining_size": 0.0,
            "timestamp_ms": 3_100,
        },
    )
    asyncio.run(export_data_lake(redis, tmp_path, count=100))
    export_external_evidence(
        tmp_path,
        [
            evidence("evidence-1", "source-a", 1_400, 1_500, 0.6),
            evidence("evidence-2", "source-b", 2_400, 2_500, -0.6),
            evidence("future-evidence", "source-c", 3_400, 3_500, 0.9),
        ],
    )
    db_path = tmp_path / "research.duckdb"
    create_duckdb_views(tmp_path, db_path)
    return db_path
