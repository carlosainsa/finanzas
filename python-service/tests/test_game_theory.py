import asyncio
import json
from pathlib import Path

import duckdb
import pytest

from src.config import settings
from src.research.data_lake import create_duckdb_views, export_data_lake
from src.research.game_theory import create_game_theory_views, export_game_theory_report


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


def test_game_theory_views_measure_post_fill_pnl_and_competition(tmp_path: Path) -> None:
    db_path = seed_game_theory_db(tmp_path)

    create_game_theory_views(db_path)

    with duckdb.connect(str(db_path)) as conn:
        pnl = conn.execute(
            """
            select mark_5s, pnl_5s, mark_30s, pnl_30s, mark_300s, pnl_300s
            from post_fill_pnl_horizons
            """
        ).fetchone()
        adverse = conn.execute(
            """
            select filled_events, adverse_30s_count, adverse_30s_rate
            from adverse_selection_by_strategy
            """
        ).fetchone()
        fill_rate = conn.execute(
            """
            select filled_signals, fill_rate
            from fill_rate_by_distance_to_mid
            where distance_bucket = '100_250bps'
            """
        ).fetchone()
        competition = conn.execute(
            """
            select snapshots, quote_changes, quote_change_rate
            from quote_competition
            where asset_id = 'asset-yes'
            """
        ).fetchone()
        no_arb = conn.execute(
            """
            select yes_asset_id, no_asset_id, probability_sum, no_arbitrage_gap
            from binary_no_arbitrage
            order by event_timestamp_ms
            limit 1
            """
        ).fetchone()

    assert pnl == (
        pytest.approx(0.48),
        pytest.approx(0.01),
        pytest.approx(0.46),
        pytest.approx(-0.01),
        pytest.approx(0.50),
        pytest.approx(0.03),
    )
    assert adverse == (1, 1, pytest.approx(1.0))
    assert fill_rate == (1, pytest.approx(0.5))
    assert competition == (5, 5, pytest.approx(1.0))
    assert no_arb == ("asset-yes", "asset-no", pytest.approx(1.01), pytest.approx(0.01))


def test_export_game_theory_report_writes_parquet_outputs(tmp_path: Path) -> None:
    db_path = seed_game_theory_db(tmp_path)
    output_dir = tmp_path / "game_theory"

    counts = export_game_theory_report(db_path, output_dir)

    assert counts["post_fill_pnl_horizons"] == 1
    assert counts["fill_rate_by_distance_to_mid"] >= 1
    assert counts["adverse_selection_by_strategy"] == 1
    assert counts["quote_competition"] >= 1
    assert counts["binary_no_arbitrage"] >= 1
    assert (output_dir / "post_fill_pnl_horizons.parquet").exists()
    assert (output_dir / "binary_no_arbitrage.parquet").exists()


def test_binary_no_arbitrage_uses_metadata_not_asset_lexicographic_order(tmp_path: Path) -> None:
    redis = FakeRedis()
    add_orderbook(redis, "z-yes", 1_000, 0.44, 0.46)
    add_orderbook(redis, "a-no", 1_000, 0.55, 0.57)
    asyncio.run(export_data_lake(redis, tmp_path, count=100))
    from src.discovery.markets import MarketCandidate
    from src.research.data_lake import export_market_metadata

    export_market_metadata(
        tmp_path,
        [
            MarketCandidate(
                market_id="market-1",
                question="Will mapping win?",
                active=True,
                closed=False,
                archived=False,
                enable_order_book=True,
                liquidity=1_000,
                volume=2_000,
                outcomes=["Yes", "No"],
                outcome_prices=[0.46, 0.56],
                clob_token_ids=["z-yes", "a-no"],
            )
        ],
    )
    db_path = tmp_path / "research.duckdb"
    create_duckdb_views(tmp_path, db_path)

    create_game_theory_views(db_path)

    with duckdb.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            select yes_asset_id, no_asset_id, probability_sum
            from binary_no_arbitrage
            """
        ).fetchone()

    assert row == ("z-yes", "a-no", pytest.approx(1.01))


def seed_game_theory_db(tmp_path: Path) -> Path:
    redis = FakeRedis()
    add_orderbook(redis, "asset-yes", 1_000, 0.44, 0.46)
    add_orderbook(redis, "asset-no", 1_000, 0.55, 0.57)
    add_orderbook(redis, "asset-yes", 1_500, 0.45, 0.47)
    add_orderbook(redis, "asset-yes", 6_010, 0.47, 0.49)
    add_orderbook(redis, "asset-yes", 31_010, 0.45, 0.47)
    add_orderbook(redis, "asset-yes", 301_010, 0.49, 0.51)
    redis.add_payload(
        settings.signals_stream,
        {
            "signal_id": "signal-1",
            "market_id": "market-1",
            "asset_id": "asset-yes",
            "side": "BUY",
            "price": 0.47,
            "size": 2.0,
            "confidence": 0.8,
            "timestamp_ms": 1_500,
            "strategy": "passive-game-test",
        },
    )
    redis.add_payload(
        settings.execution_reports_stream,
        {
            "signal_id": "signal-1",
            "order_id": "order-1",
            "status": "PARTIAL",
            "filled_price": 0.47,
            "filled_size": 1.0,
            "cumulative_filled_size": 1.0,
            "remaining_size": 1.0,
            "timestamp_ms": 1_010,
        },
    )

    asyncio.run(export_data_lake(redis, tmp_path, count=100))
    from src.discovery.markets import MarketCandidate
    from src.research.data_lake import export_market_metadata

    export_market_metadata(
        tmp_path,
        [
            MarketCandidate(
                market_id="market-1",
                question="Will the binary market resolve?",
                active=True,
                closed=False,
                archived=False,
                enable_order_book=True,
                liquidity=1_000,
                volume=2_000,
                outcomes=["Yes", "No"],
                outcome_prices=[0.46, 0.55],
                clob_token_ids=["asset-yes", "asset-no"],
            )
        ],
    )
    db_path = tmp_path / "research.duckdb"
    create_duckdb_views(tmp_path, db_path)
    return db_path


def add_orderbook(redis: FakeRedis, asset_id: str, timestamp_ms: int, bid: float, ask: float) -> None:
    redis.add_payload(
        settings.orderbook_stream,
        {
            "market_id": "market-1",
            "asset_id": asset_id,
            "bids": [{"price": bid, "size": 10.0}],
            "asks": [{"price": ask, "size": 10.0}],
            "timestamp_ms": timestamp_ms,
        },
    )
