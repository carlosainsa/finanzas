import asyncio
import json
from pathlib import Path

import duckdb

from src.config import settings
from src.research.data_lake import STREAM_DATASETS, create_duckdb_views, export_data_lake


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


def test_export_writes_partitioned_parquet_and_duckdb_view(tmp_path: Path) -> None:
    redis = FakeRedis()
    redis.add_payload(
        settings.orderbook_stream,
        {
            "market_id": "0xabc",
            "asset_id": "123",
            "bids": [{"price": 0.45, "size": 2}],
            "asks": [{"price": 0.50, "size": 3}],
            "timestamp_ms": 1760000000000,
        },
    )

    exported = asyncio.run(export_data_lake(redis, tmp_path, count=100))
    db_path = tmp_path / "research.duckdb"
    create_duckdb_views(tmp_path, db_path)

    assert exported["orderbook_snapshots"] == 1
    assert exported["orderbook_levels"] == 2
    assert list((tmp_path / "orderbook_snapshots").glob("**/*.parquet"))
    assert list((tmp_path / "orderbook_levels").glob("**/*.parquet"))
    with duckdb.connect(str(db_path)) as conn:
        rows = conn.execute("select count(*) from orderbook_snapshots").fetchone()
        levels = conn.execute("select count(*) from orderbook_levels").fetchone()
    assert rows == (1,)
    assert levels == (2,)


def test_export_validates_known_payload_schemas(tmp_path: Path) -> None:
    redis = FakeRedis()
    redis.add_payload(
        settings.signals_stream,
        {
            "signal_id": "signal-1",
            "market_id": "0xabc",
            "asset_id": "123",
            "side": "BUY",
            "price": 0.45,
            "size": 1,
            "confidence": 0.8,
            "timestamp_ms": 1760000000000,
        },
    )

    exported = asyncio.run(export_data_lake(redis, tmp_path, count=100, datasets=STREAM_DATASETS))

    assert exported["signals"] == 1
