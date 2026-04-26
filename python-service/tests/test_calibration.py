import asyncio
import json
from pathlib import Path

import duckdb
import pytest

from src.config import settings
from src.research.calibration import create_calibration_views, export_calibration_report
from src.research.data_lake import create_duckdb_views, export_data_lake


class FakeRedis:
    def __init__(self) -> None:
        self.streams: dict[str, list[tuple[str, dict[str, str]]]] = {}

    async def xrange(
        self,
        name: str,
        min: str = "-",
        max: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, str]]]:
        entries = self.streams.get(name, [])
        return entries if count is None else entries[:count]

    def add_payload(self, stream: str, payload: dict[str, object]) -> None:
        entries = self.streams.setdefault(stream, [])
        entries.append((f"{len(entries) + 1}-0", {"payload": json.dumps(payload)}))


def test_walk_forward_metrics_and_buckets_are_reproducible(tmp_path: Path) -> None:
    db_path = seed_calibration_db(tmp_path)

    create_calibration_views(db_path, train_fraction=0.5)

    with duckdb.connect(str(db_path)) as conn:
        splits = conn.execute(
            """
            select split, count(*)
            from walk_forward_splits
            group by split
            order by split
            """
        ).fetchall()
        metrics = conn.execute(
            """
            select split, samples, brier_score, log_loss
            from walk_forward_metrics
            where split = 'test'
            """
        ).fetchone()
        buckets = conn.execute(
            """
            select confidence_bucket, samples, avg_confidence, empirical_positive_rate
            from calibration_buckets
            where split = 'test'
            order by confidence_bucket
            """
        ).fetchall()
        edge_bucket = conn.execute(
            """
            select confidence_bucket, avg_realized_edge_after_slippage
            from realized_edge_by_confidence_bucket
            where split = 'test' and confidence_bucket = '80_90'
            """
        ).fetchone()

    assert splits == [("test", 2), ("train", 2)]
    assert metrics == ("test", 2, pytest.approx(0.04), pytest.approx(0.2231435513))
    assert buckets == [("20_30", 1, pytest.approx(0.2), pytest.approx(0.0)), ("80_90", 1, pytest.approx(0.8), pytest.approx(1.0))]
    assert edge_bucket == ("80_90", pytest.approx(0.34))


def test_export_calibration_report_writes_outputs(tmp_path: Path) -> None:
    db_path = seed_calibration_db(tmp_path)
    output_dir = tmp_path / "calibration"

    report = export_calibration_report(db_path, output_dir, train_fraction=0.5)

    assert report["passed"] is True
    assert report["counts"] == {
        "walk_forward_splits": 4,
        "walk_forward_metrics": 2,
        "calibration_buckets": 4,
        "realized_edge_by_confidence_bucket": 4,
    }
    assert (output_dir / "calibration_summary.json").exists()
    assert (output_dir / "walk_forward_metrics.parquet").exists()


def test_calibration_rejects_invalid_train_fraction(tmp_path: Path) -> None:
    db_path = seed_calibration_db(tmp_path)

    with pytest.raises(ValueError, match="train_fraction"):
        create_calibration_views(db_path, train_fraction=1.0)


def seed_calibration_db(tmp_path: Path) -> Path:
    redis = FakeRedis()
    add_signal_and_report(redis, "signal-1", 1_000, confidence=0.8, filled_price=0.46)
    add_signal_and_report(redis, "signal-2", 2_000, confidence=0.2, filled_price=0.46)
    add_signal_and_report(redis, "signal-3", 3_000, confidence=0.8, filled_price=0.46)
    add_signal_and_report(redis, "signal-4", 4_000, confidence=0.2, filled_price=0.46)
    asyncio.run(export_data_lake(redis, tmp_path, count=100))
    db_path = tmp_path / "research.duckdb"
    create_duckdb_views(tmp_path, db_path)
    return db_path


def add_signal_and_report(
    redis: FakeRedis, signal_id: str, timestamp_ms: int, confidence: float, filled_price: float
) -> None:
    redis.add_payload(
        settings.signals_stream,
        {
            "signal_id": signal_id,
            "market_id": "market-1",
            "asset_id": "asset-1",
            "side": "BUY",
            "price": 0.45,
            "size": 1.0,
            "confidence": confidence,
            "timestamp_ms": timestamp_ms,
            "strategy": "calibration-test",
        },
    )
    redis.add_payload(
        settings.execution_reports_stream,
        {
            "signal_id": signal_id,
            "order_id": f"order-{signal_id}",
            "status": "MATCHED",
            "timestamp_ms": timestamp_ms + 10,
            "filled_price": filled_price,
            "filled_size": 1.0,
            "cumulative_filled_size": 1.0,
            "remaining_size": 0.0,
        },
    )
