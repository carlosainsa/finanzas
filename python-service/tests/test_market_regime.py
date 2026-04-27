import asyncio
import json
from pathlib import Path

import duckdb
import pytest

from src.config import settings
from src.research.data_lake import export_data_lake
from src.research.market_regime import (
    create_market_regime_views,
    export_market_regime_report,
)
from test_game_theory import FakeRedis


def test_market_regime_views_measure_tail_fractal_and_whale_pressure(
    tmp_path: Path,
) -> None:
    db_path = seed_market_regime_db(tmp_path)

    create_market_regime_views(db_path)

    with duckdb.connect(str(db_path)) as conn:
        summary = conn.execute(
            """
            select
                return_observations,
                max_abs_return,
                hurst_proxy,
                fractal_dimension_proxy,
                tail_events
            from market_regime_summary
            where asset_id = 'asset-yes'
            """
        ).fetchone()
        tail = conn.execute(
            """
            select tail_events, hill_tail_index
            from market_tail_risk
            where asset_id = 'asset-yes'
            """
        ).fetchone()
        whale = conn.execute(
            """
            select large_level_updates, depth_withdrawal_events, whale_pressure_score
            from whale_pressure
            where asset_id = 'asset-yes'
            """
        ).fetchone()
        performance = conn.execute(
            """
            select
                bucket_type,
                bucket,
                signals,
                filled_signals,
                avg_realized_edge_after_slippage,
                max_drawdown
            from market_regime_bucket_performance
            where bucket_type = 'whale_pressure'
            """
        ).fetchone()
        context = conn.execute(
            """
            select regime_timestamp_ms, signal_timestamp_ms
            from market_regime_trade_context
            where signal_id = 'signal-1'
            """
        ).fetchone()

    assert summary is not None
    assert tail is not None
    assert whale is not None
    assert summary[0] >= 5
    assert summary[1] > 0
    assert 0.05 <= summary[2] <= 0.95
    assert 1.05 <= summary[3] <= 1.95
    assert summary[4] >= 1
    assert tail[0] >= 1
    assert tail[1] is None or tail[1] > 0
    assert whale[0] >= 1
    assert whale[1] >= 1
    assert whale[2] > 0
    assert performance is not None
    assert performance[0] == "whale_pressure"
    assert performance[2] == 1
    assert performance[3] == 1
    assert performance[4] == pytest.approx(0.10)
    assert performance[5] == pytest.approx(0.0)
    assert context is not None
    assert context[0] <= context[1]


def test_export_market_regime_report_writes_parquet_outputs(tmp_path: Path) -> None:
    db_path = seed_market_regime_db(tmp_path)
    output_dir = tmp_path / "market_regime"

    report = export_market_regime_report(db_path, output_dir)

    counts = report["counts"]
    assert isinstance(counts, dict)
    assert counts["market_regime_summary"] == 1
    assert counts["market_tail_risk"] == 1
    assert counts["whale_pressure"] == 1
    assert counts["market_regime_trade_context"] == 1
    assert counts["market_regime_trade_buckets"] >= 4
    assert counts["market_regime_bucket_drawdown"] >= 4
    assert counts["market_regime_bucket_performance"] >= 4
    assert report["can_execute_trades"] is False
    assert (output_dir / "market_regime_summary.parquet").exists()
    assert (output_dir / "market_tail_risk.parquet").exists()
    assert (output_dir / "whale_pressure.parquet").exists()
    assert (output_dir / "market_regime_trade_context.parquet").exists()
    assert (output_dir / "market_regime_trade_buckets.parquet").exists()
    assert (output_dir / "market_regime_bucket_drawdown.parquet").exists()
    assert (output_dir / "market_regime_bucket_performance.parquet").exists()
    assert (output_dir / "market_regime.json").exists()


def seed_market_regime_db(tmp_path: Path) -> Path:
    redis = FakeRedis()
    snapshots = [
        (1_000, 0.49, 0.51, 10.0, 10.0),
        (2_000, 0.50, 0.52, 11.0, 10.0),
        (3_000, 0.48, 0.50, 35.0, 9.0),
        (4_000, 0.47, 0.49, 36.0, 8.0),
        (5_000, 0.58, 0.60, 7.0, 6.0),
        (6_000, 0.57, 0.59, 6.0, 6.0),
        (7_000, 0.20, 0.22, 100.0, 5.0),
    ]
    for timestamp_ms, bid, ask, bid_size, ask_size in snapshots:
        redis.add_payload(
            settings.orderbook_stream,
            {
                "market_id": "market-1",
                "asset_id": "asset-yes",
                "bids": [
                    {"price": bid, "size": bid_size},
                    {"price": bid - 0.01, "size": 2.0},
                ],
                "asks": [
                    {"price": ask, "size": ask_size},
                    {"price": ask + 0.01, "size": 2.0},
                ],
                "timestamp_ms": timestamp_ms,
            },
        )
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
            "timestamp_ms": 3_000,
            "strategy": "regime-test",
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
            "timestamp_ms": 3_100,
        },
    )
    asyncio.run(export_data_lake(redis, tmp_path, count=100))
    db_path = tmp_path / "research.duckdb"
    from src.research.data_lake import create_duckdb_views

    create_duckdb_views(tmp_path, db_path)
    return db_path
