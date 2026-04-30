import asyncio
import json
from pathlib import Path
from typing import cast

import duckdb
import pytest

from src.config import settings
from src.research.data_lake import create_duckdb_views, export_data_lake
from src.research.deterministic_baseline import (
    BASELINE_FEATURE_VERSION,
    BASELINE_MODEL_VERSION,
    NEAR_TOUCH_BASELINE_FEATURE_VERSION,
    NEAR_TOUCH_BASELINE_MODEL_VERSION,
    BaselineConfig,
    create_baseline_views,
    export_baseline_report,
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


def test_baseline_generates_deterministic_versioned_signals(tmp_path: Path) -> None:
    db_path = seed_baseline_db(tmp_path)

    create_baseline_views(db_path, BaselineConfig(min_depth=1.0, max_stale_gap_ms=60_000))

    with duckdb.connect(str(db_path)) as conn:
        features = conn.execute(
            """
            select spread, total_depth, imbalance
            from baseline_market_features
            where event_timestamp_ms = 31_000
            """
        ).fetchone()
        signals = conn.execute(
            """
            select signal_id, side, price, size, model_version, feature_version
            from baseline_signals
            order by timestamp_ms
            """
        ).fetchall()

    assert features == (pytest.approx(0.08), pytest.approx(10.0), pytest.approx(0.0))
    assert len(signals) == 1
    assert signals[0][1:] == ("BUY", pytest.approx(0.44), pytest.approx(1.0), BASELINE_MODEL_VERSION, BASELINE_FEATURE_VERSION)


def test_baseline_near_touch_quote_generates_versioned_signals(tmp_path: Path) -> None:
    db_path = seed_baseline_db(tmp_path)

    create_baseline_views(
        db_path,
        BaselineConfig(
            min_depth=1.0,
            max_stale_gap_ms=60_000,
            quote_placement="near_touch",
            near_touch_tick_size=0.01,
            near_touch_offset_ticks=0,
            near_touch_max_spread_fraction=1.0,
        ),
    )

    with duckdb.connect(str(db_path)) as conn:
        signals = conn.execute(
            """
            select side, price, model_version, feature_version
            from baseline_signals
            order by timestamp_ms
            """
        ).fetchall()

    assert len(signals) == 1
    assert signals[0] == (
        "BUY",
        pytest.approx(0.52),
        NEAR_TOUCH_BASELINE_MODEL_VERSION,
        NEAR_TOUCH_BASELINE_FEATURE_VERSION,
    )


def test_baseline_filters_stale_momentum_and_depth(tmp_path: Path) -> None:
    db_path = seed_baseline_db(tmp_path)

    create_baseline_views(
        db_path,
        BaselineConfig(min_depth=20.0, max_abs_momentum=0.001, max_stale_gap_ms=1_000),
    )

    with duckdb.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            select signals, depth_passes, momentum_passes, stale_passes
            from baseline_summary
            """
        ).fetchone()

    assert row == (0, 0, 0, 1)


def test_baseline_can_limit_snapshots_per_asset(tmp_path: Path) -> None:
    db_path = seed_baseline_db(tmp_path)

    create_baseline_views(
        db_path,
        BaselineConfig(min_depth=1.0, max_stale_gap_ms=60_000, max_snapshots_per_asset=1),
    )

    with duckdb.connect(str(db_path)) as conn:
        row = conn.execute("select snapshots from baseline_summary").fetchone()

    assert row == (1,)


def test_export_baseline_report_writes_outputs(tmp_path: Path) -> None:
    db_path = seed_baseline_db(tmp_path)
    output_dir = tmp_path / "baseline"

    report = export_baseline_report(db_path, output_dir)
    counts = cast(dict[str, int], report["counts"])

    assert counts["baseline_signals"] == 1
    assert (output_dir / "baseline_signals.parquet").exists()
    assert (output_dir / "baseline_summary.json").exists()


def seed_baseline_db(tmp_path: Path) -> Path:
    redis = FakeRedis()
    add_orderbook(redis, 1_000, 0.45, 0.49, bid_size=5.0, ask_size=5.0)
    add_orderbook(redis, 31_000, 0.44, 0.52, bid_size=5.0, ask_size=5.0)
    asyncio.run(export_data_lake(redis, tmp_path, count=100))
    db_path = tmp_path / "research.duckdb"
    create_duckdb_views(tmp_path, db_path)
    return db_path


def add_orderbook(
    redis: FakeRedis,
    timestamp_ms: int,
    bid: float,
    ask: float,
    bid_size: float,
    ask_size: float,
) -> None:
    redis.add_payload(
        settings.orderbook_stream,
        {
            "market_id": "market-1",
            "asset_id": "asset-1",
            "bids": [{"price": bid, "size": bid_size}],
            "asks": [{"price": ask, "size": ask_size}],
            "timestamp_ms": timestamp_ms,
        },
    )
