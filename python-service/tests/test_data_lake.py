import asyncio
import json
from pathlib import Path

import duckdb

from src.config import settings
from src.discovery.markets import MarketCandidate
from src.research.data_lake import (
    STREAM_DATASETS,
    create_duckdb_views,
    export_data_lake,
    export_external_evidence,
    export_market_metadata,
    export_sentiment_features,
    market_metadata_rows,
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
        if min.startswith("("):
            last_id = min[1:]
            entries = [entry for entry in entries if entry[0] > last_id]
        elif min != "-":
            entries = [entry for entry in entries if entry[0] >= min]
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
            "model_version": "model-v1",
            "data_version": "data-v1",
            "feature_version": "features-v1",
        },
    )

    exported = asyncio.run(export_data_lake(redis, tmp_path, count=100, datasets=STREAM_DATASETS))

    assert exported["signals"] == 1
    db_path = tmp_path / "research.duckdb"
    create_duckdb_views(tmp_path, db_path)
    with duckdb.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            select model_version, data_version, feature_version
            from signals
            """
        ).fetchone()
    assert row == ("model-v1", "data-v1", "features-v1")


def test_incremental_export_tracks_last_stream_id(tmp_path: Path) -> None:
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

    first = asyncio.run(export_data_lake(redis, tmp_path, count=100, incremental=True))
    second = asyncio.run(export_data_lake(redis, tmp_path, count=100, incremental=True))
    redis.add_payload(
        settings.signals_stream,
        {
            "signal_id": "signal-2",
            "market_id": "0xabc",
            "asset_id": "123",
            "side": "SELL",
            "price": 0.55,
            "size": 1,
            "confidence": 0.8,
            "timestamp_ms": 1760000000100,
        },
    )
    third = asyncio.run(export_data_lake(redis, tmp_path, count=100, incremental=True))

    assert first["signals"] == 1
    assert second["signals"] == 0
    assert third["signals"] == 1
    state = json.loads((tmp_path / "_export_state.json").read_text(encoding="utf-8"))
    assert state["signals"] == "2-0"


def test_market_metadata_exports_asset_outcome_mapping(tmp_path: Path) -> None:
    market = MarketCandidate(
        market_id="market-1",
        question="Will metadata map correctly?",
        active=True,
        closed=False,
        archived=False,
        enable_order_book=True,
        liquidity=1_000,
        volume=2_000,
        outcomes=["Yes", "No"],
        outcome_prices=[0.48, 0.52],
        clob_token_ids=["asset-z-yes", "asset-a-no"],
        tags=["Politics"],
    )

    rows = market_metadata_rows([market])
    exported = export_market_metadata(tmp_path, [market])
    db_path = tmp_path / "research.duckdb"
    create_duckdb_views(tmp_path, db_path)

    assert exported == 2
    assert rows[0]["asset_id"] == "asset-z-yes"
    assert rows[0]["outcome"] == "Yes"
    assert rows[1]["asset_id"] == "asset-a-no"
    assert rows[1]["outcome"] == "No"
    with duckdb.connect(str(db_path)) as conn:
        db_rows = conn.execute(
            """
            select asset_id, outcome, outcome_price
            from market_metadata
            order by outcome_index
            """
        ).fetchall()
    assert db_rows == [("asset-z-yes", "Yes", 0.48), ("asset-a-no", "No", 0.52)]


def test_sentiment_contract_exports_timestamped_offline_features(tmp_path: Path) -> None:
    evidence_count = export_external_evidence(
        tmp_path,
        [
            {
                "evidence_id": "evidence-1",
                "source": "newswire",
                "source_type": "news",
                "published_at_ms": 1_000,
                "observed_at_ms": 1_100,
                "market_id": "market-1",
                "asset_id": "asset-yes",
                "raw_reference_hash": "sha256:abc",
                "data_version": "external_evidence_v1",
            }
        ],
    )
    feature_count = export_sentiment_features(
        tmp_path,
        [
            {
                "feature_id": "sentiment-1",
                "evidence_id": "evidence-1",
                "market_id": "market-1",
                "asset_id": "asset-yes",
                "observed_at_ms": 1_100,
                "feature_timestamp_ms": 1_200,
                "direction": "YES",
                "sentiment_score": 0.4,
                "source_quality": 0.8,
                "confidence": 0.7,
                "model_version": "sentiment_baseline_v1",
                "data_version": "external_evidence_v1",
                "feature_version": "sentiment_features_v1",
            }
        ],
    )
    db_path = tmp_path / "research.duckdb"
    create_duckdb_views(tmp_path, db_path)

    assert evidence_count == 1
    assert feature_count == 1
    with duckdb.connect(str(db_path)) as conn:
        evidence = conn.execute(
            "select evidence_id, observed_at_ms >= published_at_ms from external_evidence"
        ).fetchone()
        feature = conn.execute(
            """
            select feature_id, feature_timestamp_ms >= observed_at_ms, sentiment_score
            from sentiment_features
            """
        ).fetchone()
    assert evidence == ("evidence-1", True)
    assert feature == ("sentiment-1", True, 0.4)
