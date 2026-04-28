import asyncio
import json
from pathlib import Path
from typing import Any, cast

import duckdb
import pytest

from src.config import settings
from src.research.data_lake import create_duckdb_views, export_data_lake
from src.research.pre_live_promotion import (
    PROMOTION_REPORT_VERSION,
    PromotionConfig,
    create_promotion_report,
    create_promotion_views,
    export_promotion_report,
)
from src.research.go_no_go import GO_NO_GO_REPORT_VERSION, create_go_no_go_report, export_go_no_go_report


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


def test_promotion_report_combines_required_pre_live_metrics(tmp_path: Path) -> None:
    db_path = seed_promotion_db(tmp_path)

    report = create_promotion_report(
        db_path, PromotionConfig(max_drawdown=1.0, max_stale_data_rate=1.0)
    )

    metrics = cast(dict[str, object], report["metrics"])
    checks = {
        str(item["check_name"]): bool(item["passed"])
        for item in cast(list[dict[str, Any]], report["checks"])
    }
    assert report["report_version"] == PROMOTION_REPORT_VERSION
    assert metrics["signals"] == 4.0
    assert metrics["filled_signals"] == 4.0
    assert metrics["fill_rate"] == pytest.approx(1.0)
    assert metrics["realized_edge"] == pytest.approx(0.04)
    assert metrics["capture_duration_ms"] == 3000.0
    assert metrics["dry_run_observed_fill_rate"] is None
    assert metrics["max_abs_simulator_fill_rate_delta"] == 0.0
    assert metrics["reconciliation_divergence_rate"] == pytest.approx(0.0)
    assert checks["has_signals"] is True
    assert checks["sufficient_capture_duration"] is True
    assert checks["acceptable_dry_run_observed_fill_rate"] is True
    assert checks["bounded_simulator_fill_rate_delta"] is True
    assert checks["positive_realized_edge"] is True
    assert checks["calibration_available"] is True
    assert report["passed"] is True


def test_go_no_go_report_summarizes_quantitative_gate(tmp_path: Path) -> None:
    db_path = seed_promotion_db(tmp_path)

    report = create_go_no_go_report(
        db_path, PromotionConfig(max_drawdown=1.0, max_stale_data_rate=1.0)
    )

    assert report["report_version"] == GO_NO_GO_REPORT_VERSION
    assert report["decision"] == "GO"
    assert report["passed"] is True
    assert report["can_execute_trades"] is False
    assert report["blockers"] == []


def test_go_no_go_report_blocks_failed_quantitative_gate(tmp_path: Path) -> None:
    db_path = seed_promotion_db(tmp_path)

    report = create_go_no_go_report(
        db_path,
        PromotionConfig(
            min_signals=10,
            max_drawdown=1.0,
            max_stale_data_rate=1.0,
        ),
    )

    blockers = cast(list[dict[str, object]], report["blockers"])
    assert report["decision"] == "NO_GO"
    assert report["passed"] is False
    assert any(blocker["check_name"] == "has_signals" for blocker in blockers)


def test_export_go_no_go_report_writes_json(tmp_path: Path) -> None:
    db_path = seed_promotion_db(tmp_path)
    output_dir = tmp_path / "go_no_go"

    report = export_go_no_go_report(
        db_path, output_dir, PromotionConfig(max_drawdown=1.0, max_stale_data_rate=1.0)
    )

    assert report["passed"] is True
    assert (output_dir / "go_no_go.json").exists()


def test_promotion_report_enforces_explicit_real_dry_run_gates(
    tmp_path: Path,
) -> None:
    db_path = seed_promotion_db(tmp_path)

    report = create_promotion_report(
        db_path,
        PromotionConfig(
            min_capture_duration_ms=10_000,
            min_signals=10,
            min_dry_run_observed_fill_rate=0.50,
            max_drawdown=1.0,
            max_stale_data_rate=1.0,
        ),
    )

    checks = {
        str(item["check_name"]): bool(item["passed"])
        for item in cast(list[dict[str, Any]], report["checks"])
    }
    assert checks["sufficient_capture_duration"] is False
    assert checks["has_signals"] is False
    assert checks["acceptable_dry_run_observed_fill_rate"] is False
    assert report["passed"] is False


def test_promotion_views_expose_drawdown_and_stale_data(tmp_path: Path) -> None:
    db_path = seed_promotion_db(tmp_path)

    create_promotion_views(
        db_path, PromotionConfig(max_drawdown=1.0, max_stale_data_rate=1.0)
    )

    with duckdb.connect(str(db_path)) as conn:
        drawdown = conn.execute("select max(drawdown) from pre_live_drawdown").fetchone()
        stale = conn.execute(
            """
            select count(*), avg(case when is_stale_gap then 1.0 else 0.0 end)
            from pre_live_stale_data
            """
        ).fetchone()
        relation_types = {
            str(row[0]): str(row[1])
            for row in conn.execute(
                """
                select table_name, table_type
                from information_schema.tables
                where table_name in (
                    'pre_live_drawdown',
                    'pre_live_stale_data',
                    'pre_live_promotion_metrics',
                    'pre_live_promotion_checks',
                    'pre_live_promotion_segments',
                    'pre_live_promotion_segment_checks',
                    'pre_live_promotion_segment_summary'
                )
                """
            ).fetchall()
        }
        segment = conn.execute(
            """
            select
                signals,
                filled_signals,
                fill_rate,
                realized_edge,
                filled_notional,
                pnl,
                pnl_per_signal,
                pnl_per_filled_signal,
                pnl_per_filled_notional,
                max_drawdown,
                drawdown_per_signal,
                drawdown_per_filled_signal,
                drawdown_per_filled_notional
            from pre_live_promotion_segments
            where market_id = 'market-1'
              and asset_id = 'asset-1'
              and side = 'BUY'
            """
        ).fetchone()
        segment_checks = {
            str(row[0]): bool(row[1])
            for row in conn.execute(
                """
                select check_name, passed
                from pre_live_promotion_segment_checks
                where market_id = 'market-1'
                  and asset_id = 'asset-1'
                  and side = 'BUY'
                """
            ).fetchall()
        }

    assert drawdown == (pytest.approx(0.26),)
    assert stale == (4, pytest.approx(0.0))
    assert segment == (
        4,
        4,
        pytest.approx(1.0),
        pytest.approx(0.04),
        pytest.approx(1.84),
        pytest.approx(0.16),
        pytest.approx(0.04),
        pytest.approx(0.04),
        pytest.approx(0.0869565),
        pytest.approx(0.26),
        pytest.approx(0.065),
        pytest.approx(0.065),
        pytest.approx(0.1413043),
    )
    assert segment_checks["positive_realized_edge"] is True
    assert segment_checks["bounded_drawdown"] is True
    assert relation_types == {
        "pre_live_drawdown": "BASE TABLE",
        "pre_live_promotion_segment_checks": "BASE TABLE",
        "pre_live_promotion_segment_summary": "BASE TABLE",
        "pre_live_promotion_segments": "BASE TABLE",
        "pre_live_promotion_checks": "BASE TABLE",
        "pre_live_promotion_metrics": "BASE TABLE",
        "pre_live_stale_data": "BASE TABLE",
    }


def test_export_promotion_report_writes_json_and_parquet(tmp_path: Path) -> None:
    db_path = seed_promotion_db(tmp_path)
    output_dir = tmp_path / "promotion"

    report = export_promotion_report(
        db_path, output_dir, PromotionConfig(max_drawdown=1.0, max_stale_data_rate=1.0)
    )

    assert report["passed"] is True
    assert (output_dir / "pre_live_promotion.json").exists()
    assert (output_dir / "pre_live_promotion_metrics.parquet").exists()
    assert (output_dir / "pre_live_promotion_checks.parquet").exists()
    assert (output_dir / "pre_live_promotion_segments.parquet").exists()
    assert (output_dir / "pre_live_blocked_segments.parquet").exists()
    assert (output_dir / "blocked_segments.json").exists()


def test_export_promotion_report_writes_blocked_segments_json(tmp_path: Path) -> None:
    db_path = seed_promotion_db(tmp_path)
    output_dir = tmp_path / "promotion"

    export_promotion_report(
        db_path, output_dir, PromotionConfig(max_drawdown=0.10, max_stale_data_rate=1.0)
    )

    payload = json.loads((output_dir / "blocked_segments.json").read_text())

    assert payload["version"] == "blocked_segments_v1"
    assert payload["source_report_version"] == PROMOTION_REPORT_VERSION
    assert len(payload["segments"]) == 1
    segment = payload["segments"][0]
    assert segment["market_id"] == "market-1"
    assert segment["asset_id"] == "asset-1"
    assert segment["side"] == "BUY"
    assert segment["reason"] == "bounded_drawdown"
    assert segment["metrics"]["drawdown_per_signal"] == pytest.approx(0.065)


def test_promotion_report_handles_missing_data_without_crashing(tmp_path: Path) -> None:
    db_path = tmp_path / "empty.duckdb"

    report = create_promotion_report(db_path)
    metrics = cast(dict[str, object], report["metrics"])
    checks = {
        str(item["check_name"]): bool(item["passed"])
        for item in cast(list[dict[str, Any]], report["checks"])
    }

    assert report["passed"] is False
    assert metrics["signals"] == 0.0
    assert metrics["filled_signals"] == 0.0
    assert metrics["realized_edge"] is None
    assert metrics["adverse_selection"] is None
    assert metrics["stale_data_rate"] == 0.0
    assert metrics["reconciliation_divergence_rate"] == 0.0
    assert checks["has_signals"] is False


def seed_promotion_db(tmp_path: Path) -> Path:
    redis = FakeRedis()
    for timestamp_ms in (1_000, 2_000, 3_000, 4_000):
        redis.add_payload(
            settings.orderbook_stream,
            {
                "market_id": "market-1",
                "asset_id": "asset-1",
                "bids": [{"price": 0.44, "size": 10.0}],
                "asks": [{"price": 0.46, "size": 10.0}],
                "timestamp_ms": timestamp_ms,
            },
        )
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
            "strategy": "promotion-test",
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
