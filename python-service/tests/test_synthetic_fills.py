import asyncio
import json
from pathlib import Path

import duckdb
import pytest

from src.config import settings
from src.research.backtest import create_backtest_views
from src.research.data_lake import create_duckdb_views, export_data_lake
from src.research.synthetic_fills import (
    SYNTHETIC_FILL_MODEL_VERSION,
    create_synthetic_fill_views,
    export_synthetic_fill_report,
)


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


def test_synthetic_fills_create_conservative_buy_fill(tmp_path: Path) -> None:
    db_path = seed_synthetic_fill_db(tmp_path)

    create_synthetic_fill_views(db_path)

    with duckdb.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            select order_id, status, filled_price, filled_size, remaining_size, synthetic_model_version
            from synthetic_execution_reports
            """
        ).fetchone()

    assert row == (
        "synthetic-fill-signal-1",
        "MATCHED",
        pytest.approx(0.45),
        pytest.approx(2.0),
        pytest.approx(0.0),
        SYNTHETIC_FILL_MODEL_VERSION,
    )


def test_backtest_uses_synthetic_reports_when_available(tmp_path: Path) -> None:
    db_path = seed_synthetic_fill_db(tmp_path)

    create_synthetic_fill_views(db_path)
    create_backtest_views(db_path)

    with duckdb.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            select order_id, status, fill_rate, slippage, realized_edge_after_slippage
            from backtest_trades
            """
        ).fetchone()

    assert row == (
        "synthetic-fill-signal-1",
        "MATCHED",
        pytest.approx(1.0),
        pytest.approx(0.0),
        pytest.approx(0.35),
    )


def test_synthetic_fills_do_not_fill_without_future_touch(tmp_path: Path) -> None:
    db_path = seed_synthetic_fill_db(tmp_path, touch_limit=False)

    create_synthetic_fill_views(db_path)

    with duckdb.connect(str(db_path)) as conn:
        row = conn.execute("select count(*) from synthetic_execution_reports").fetchone()

    assert row == (0,)


def test_export_synthetic_fill_report_writes_artifacts(tmp_path: Path) -> None:
    db_path = seed_synthetic_fill_db(tmp_path)
    output_dir = tmp_path / "synthetic_fills"

    report = export_synthetic_fill_report(db_path, output_dir)

    assert report["model_version"] == SYNTHETIC_FILL_MODEL_VERSION
    assert report["counts"] == {
        "synthetic_fill_candidates": 1,
        "synthetic_execution_reports": 1,
        "synthetic_fill_summary": 1,
    }
    assert (output_dir / "synthetic_fills.json").exists()
    assert (output_dir / "synthetic_execution_reports.parquet").exists()


def seed_synthetic_fill_db(tmp_path: Path, touch_limit: bool = True) -> Path:
    redis = FakeRedis()
    redis.add_payload(
        settings.signals_stream,
        {
            "signal_id": "signal-1",
            "market_id": "market-1",
            "asset_id": "asset-1",
            "side": "BUY",
            "price": 0.45,
            "size": 2.0,
            "confidence": 0.8,
            "timestamp_ms": 1_000,
            "strategy": "test-strategy",
        },
    )
    redis.add_payload(
        settings.orderbook_stream,
        {
            "market_id": "market-1",
            "asset_id": "asset-1",
            "bids": [{"price": 0.43, "size": 10.0}],
            "asks": [{"price": 0.46, "size": 10.0}],
            "timestamp_ms": 1_500,
        },
    )
    redis.add_payload(
        settings.orderbook_stream,
        {
            "market_id": "market-1",
            "asset_id": "asset-1",
            "bids": [{"price": 0.44, "size": 10.0}],
            "asks": [{"price": 0.45 if touch_limit else 0.46, "size": 5.0}],
            "timestamp_ms": 2_000,
        },
    )
    asyncio.run(export_data_lake(redis, tmp_path, count=100))
    db_path = tmp_path / "research.duckdb"
    create_duckdb_views(tmp_path, db_path)
    return db_path
