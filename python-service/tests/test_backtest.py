import asyncio
import json
from pathlib import Path

import duckdb
import pytest

from src.config import settings
from src.research.backtest import (
    create_pre_live_gate_report,
    create_backtest_views,
    export_backtest_report,
    export_pre_live_gate_report,
)
from src.research.data_lake import create_duckdb_views, export_data_lake
from src.research.synthetic_fills import create_synthetic_fill_views


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
            select signals, orders, filled_signals, filled_orders, fill_rate, total_filled_size
            from backtest_summary
            """
        ).fetchone()

    assert row == (1, 1, 0, 0, 0.0, 0.0)


def test_backtest_uses_canonical_execution_report_without_double_counting(tmp_path: Path) -> None:
    db_path = seed_research_db(tmp_path, with_fill=False)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            create table execution_reports as
            select
                'execution:reports:stream' as stream,
                '10-0' as stream_id,
                'execution_report' as schema_name,
                1760000000010 as event_timestamp_ms,
                1760000000010 as ingested_at_ms,
                '{}' as payload_json,
                'signal-1' as signal_id,
                'order-1' as order_id,
                'PARTIAL' as status,
                0.47 as filled_price,
                1.0 as filled_size,
                1.0 as cumulative_filled_size,
                1.0 as remaining_size,
                null as error
            union all
            select
                'execution:reports:stream',
                '11-0',
                'execution_report',
                1760000000020,
                1760000000020,
                '{}',
                'signal-1',
                'order-1',
                'MATCHED',
                0.47,
                1.0,
                2.0,
                0.0,
                null
            """
        )

    create_backtest_views(db_path)

    with duckdb.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            select count(*), sum(filled_size), avg(fill_rate)
            from backtest_trades
            """
        ).fetchone()

    assert row == (1, pytest.approx(2.0), pytest.approx(1.0))


def test_backtest_summary_counts_unique_signals_separately_from_orders(tmp_path: Path) -> None:
    db_path = seed_research_db(tmp_path, with_fill=False)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            create table execution_reports as
            select
                'execution:reports:stream' as stream,
                '10-0' as stream_id,
                'execution_report' as schema_name,
                1760000000010 as event_timestamp_ms,
                1760000000010 as ingested_at_ms,
                '{}' as payload_json,
                'signal-1' as signal_id,
                'order-1' as order_id,
                'PARTIAL' as status,
                0.47 as filled_price,
                1.0 as filled_size,
                1.0 as cumulative_filled_size,
                1.0 as remaining_size,
                null as error
            union all
            select
                'execution:reports:stream',
                '11-0',
                'execution_report',
                1760000000020,
                1760000000020,
                '{}',
                'signal-1',
                'order-2',
                'PARTIAL',
                0.46,
                0.5,
                0.5,
                1.5,
                null
            """
        )

    create_backtest_views(db_path)

    with duckdb.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            select signals, orders, filled_signals, filled_orders, total_filled_size
            from backtest_summary
            """
        ).fetchone()

    assert row == (1, 2, 1, 2, pytest.approx(1.5))


def test_export_backtest_report_writes_outputs(tmp_path: Path) -> None:
    db_path = seed_research_db(tmp_path, with_fill=True)
    output_dir = tmp_path / "backtest"

    counts = export_backtest_report(db_path, output_dir)

    assert counts == {
        "backtest_trades": 1,
        "backtest_summary": 1,
        "observed_vs_synthetic_fills": 1,
        "observed_vs_synthetic_fill_summary": 1,
        "unfilled_signal_reasons": 0,
        "unfilled_reason_summary": 0,
    }
    assert (output_dir / "backtest_trades.parquet").exists()
    assert (output_dir / "backtest_summary.parquet").exists()
    assert (output_dir / "observed_vs_synthetic_fill_summary.parquet").exists()
    assert (output_dir / "unfilled_signal_reasons.parquet").exists()
    assert (output_dir / "unfilled_reason_summary.parquet").exists()


def test_backtest_compares_observed_and_synthetic_fills(tmp_path: Path) -> None:
    db_path = seed_research_db(tmp_path, with_fill=True, with_touching_book=True)

    create_synthetic_fill_views(db_path)
    create_backtest_views(db_path)

    with duckdb.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            select
                signals,
                observed_filled_signals,
                synthetic_filled_signals,
                observed_fill_rate,
                synthetic_fill_rate,
                fill_rate_delta,
                both_filled
            from observed_vs_synthetic_fill_summary
            """
        ).fetchone()

    assert row == (
        1,
        1,
        1,
        pytest.approx(0.5),
        pytest.approx(1.0),
        pytest.approx(0.5),
        1,
    )


def test_backtest_classifies_unfilled_with_synthetic_fill_available(
    tmp_path: Path,
) -> None:
    db_path = seed_research_db(tmp_path, with_fill=False, with_touching_book=True)

    create_synthetic_fill_views(db_path)
    create_backtest_views(db_path)

    with duckdb.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            select unfilled_reason, market_evidence_reason, unfilled_signals
            from unfilled_reason_summary
            """
        ).fetchone()

    assert row == (
        "no_observed_report_but_synthetic_fill",
        "synthetic_fill_available",
        1,
    )


def test_backtest_classifies_unfilled_when_future_book_never_touches_limit(
    tmp_path: Path,
) -> None:
    db_path = seed_research_db(
        tmp_path,
        with_fill=False,
        with_touching_book=True,
        touch_limit=False,
    )

    create_synthetic_fill_views(db_path)
    create_backtest_views(db_path)

    with duckdb.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            select unfilled_reason, market_evidence_reason, unfilled_signals
            from unfilled_reason_summary
            """
        ).fetchone()

    assert row == (
        "no_observed_report_no_synthetic_fill",
        "future_book_never_touched_limit",
        1,
    )


def test_backtest_classifies_unfilled_when_no_future_orderbook(
    tmp_path: Path,
) -> None:
    db_path = seed_research_db(tmp_path, with_fill=False)

    create_synthetic_fill_views(db_path)
    create_backtest_views(db_path)

    with duckdb.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            select unfilled_reason, market_evidence_reason, unfilled_signals
            from unfilled_reason_summary
            """
        ).fetchone()

    assert row == (
        "no_observed_report_no_synthetic_fill",
        "no_future_orderbook_snapshot",
        1,
    )


def test_backtest_classifies_observed_unmatched(tmp_path: Path) -> None:
    db_path = seed_research_db(tmp_path, with_fill=False, report_status="UNMATCHED")

    create_synthetic_fill_views(db_path)
    create_backtest_views(db_path)

    with duckdb.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            select unfilled_reason, market_evidence_reason, observed_unmatched_signals
            from unfilled_reason_summary
            """
        ).fetchone()

    assert row == ("observed_unmatched", "no_future_orderbook_snapshot", 1)


def test_pre_live_gate_report_requires_positive_realized_edge(tmp_path: Path) -> None:
    db_path = seed_research_db(tmp_path, with_fill=True)

    report = create_pre_live_gate_report(db_path)

    assert report["signals"] == 1
    assert report["filled_signals"] == 1
    assert report["passed"] is True
    assert report["checks"] == {
        "has_signals": True,
        "has_fills": True,
        "positive_realized_edge_after_slippage": True,
        "acceptable_error_rate": True,
        "no_persistent_adverse_selection": True,
    }


def test_export_pre_live_gate_report_writes_json(tmp_path: Path) -> None:
    db_path = seed_research_db(tmp_path, with_fill=True)
    output_dir = tmp_path / "backtest"

    report = export_pre_live_gate_report(db_path, output_dir)

    assert report["passed"] is True
    assert (output_dir / "pre_live_gate.json").exists()


def seed_research_db(
    tmp_path: Path,
    with_fill: bool,
    with_touching_book: bool = False,
    touch_limit: bool = True,
    report_status: str | None = None,
) -> Path:
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
    if with_fill or report_status is not None:
        status = report_status or "PARTIAL"
        filled_size = 1.0 if with_fill else 0.0
        redis.add_payload(
            settings.execution_reports_stream,
            {
                "signal_id": "signal-1",
                "order_id": "order-1",
                "status": status,
                "filled_price": 0.47 if with_fill else None,
                "filled_size": filled_size,
                "cumulative_filled_size": filled_size,
                "remaining_size": 2.0 - filled_size,
                "timestamp_ms": 1760000000010,
            },
        )
    if with_touching_book:
        redis.add_payload(
            settings.orderbook_stream,
            {
                "market_id": "market-1",
                "asset_id": "asset-1",
                "bids": [{"price": 0.44, "size": 10.0}],
                "asks": [{"price": 0.45 if touch_limit else 0.46, "size": 5.0}],
                "timestamp_ms": 1760000000020,
            },
        )

    asyncio.run(export_data_lake(redis, tmp_path, count=100))
    db_path = tmp_path / "research.duckdb"
    create_duckdb_views(tmp_path, db_path)
    return db_path
