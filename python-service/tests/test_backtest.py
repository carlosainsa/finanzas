import asyncio
import json
from pathlib import Path

import duckdb
import pytest

from src.config import settings
from src.research.backtest import create_backtest_views, export_backtest_report
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


def test_backtest_creates_trade_metrics(tmp_path: Path) -> None:
    db_path = seed_research_db(tmp_path, with_fill=True)

    create_backtest_views(db_path)

    with duckdb.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            select fill_rate, slippage, model_edge, realized_edge_after_slippage
            from backtest_trades
            """
        ).fetchone()

    assert row == (
        pytest.approx(0.5),
        pytest.approx(0.02),
        pytest.approx(0.35),
        pytest.approx(0.33),
    )


def test_backtest_counts_unfilled_signals(tmp_path: Path) -> None:
    db_path = seed_research_db(tmp_path, with_fill=False)

    create_backtest_views(db_path)

    with duckdb.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            select signals, filled_signals, fill_rate, total_filled_size
            from backtest_summary
            """
        ).fetchone()

    assert row == (1, 0, 0.0, 0.0)


def test_export_backtest_report_writes_outputs(tmp_path: Path) -> None:
    db_path = seed_research_db(tmp_path, with_fill=True)
    output_dir = tmp_path / "backtest"

    counts = export_backtest_report(db_path, output_dir)

    assert counts == {"backtest_trades": 1, "backtest_summary": 1}
    assert (output_dir / "backtest_trades.parquet").exists()
    assert (output_dir / "backtest_summary.parquet").exists()


def seed_research_db(tmp_path: Path, with_fill: bool) -> Path:
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
            "timestamp_ms": 1760000000000,
            "strategy": "test-strategy",
        },
    )
    if with_fill:
        redis.add_payload(
            settings.execution_reports_stream,
            {
                "signal_id": "signal-1",
                "order_id": "order-1",
                "status": "PARTIAL",
                "filled_price": 0.47,
                "filled_size": 1.0,
                "timestamp_ms": 1760000000010,
            },
        )

    asyncio.run(export_data_lake(redis, tmp_path, count=100))
    db_path = tmp_path / "research.duckdb"
    create_duckdb_views(tmp_path, db_path)
    return db_path
