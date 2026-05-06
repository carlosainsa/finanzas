from pathlib import Path
from typing import Any, cast

import duckdb

from src.research.quote_execution_diagnostics import (
    REPORT_VERSION,
    QuoteExecutionDiagnosticsConfig,
    create_quote_execution_diagnostics_report,
)


def test_quote_execution_diagnostics_explains_synthetic_only_gap(
    tmp_path: Path,
) -> None:
    db_path = seed_db(tmp_path, synthetic=True, observed=False)

    report = create_quote_execution_diagnostics_report(db_path, tmp_path / "diagnostics")

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    summary = cast(dict[str, Any], report["summary"])
    assert summary["signals"] == 1
    assert summary["synthetic_filled_signals"] == 1
    assert summary["synthetic_only_signals"] == 1
    assert summary["dry_run_filled_signals"] == 0
    assert summary["observed_fill_rate"] == 0.0
    assert summary["adjusted_synthetic_fill_rate"] == 0.25
    assert summary["adjusted_fill_rate_gap"] == 0.25
    assert "synthetic fills are offline backtest evidence" in summary["explanation"]
    gap = cast(list[dict[str, Any]], report["synthetic_vs_observed_gap"])
    assert gap[0]["synthetic_optimism_flag"] is True
    assert gap[0]["dominant_gap_type"] == "synthetic_only_dominates"
    assert gap[0]["fill_rate_gap"] == 1.0
    assert gap[0]["adjusted_synthetic_fill_rate"] == 0.25
    assert gap[0]["adjusted_fill_rate_gap"] == 0.25
    assert gap[0]["avg_synthetic_evidence_weight"] == 0.25


def test_quote_execution_diagnostics_explains_dry_run_unmatched_with_synthetic(
    tmp_path: Path,
) -> None:
    db_path = seed_db(tmp_path, synthetic=True, observed=True)

    report = create_quote_execution_diagnostics_report(db_path, tmp_path / "diagnostics")

    summary = cast(dict[str, Any], report["summary"])
    assert summary["raw_observed_report_rows"] == 2
    assert summary["dry_run_signal_lifecycles"] == 1
    assert summary["dry_run_filled_signals"] == 0
    assert summary["dry_run_unfilled_but_synthetic_available"] == 1
    assert summary["adjusted_synthetic_fill_rate"] == 0.5

    with duckdb.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            select root_cause, execution_path, quote_relation, synthetic_evidence_weight
            from quote_execution_outcomes
            """
        ).fetchone()
    assert row == (
        "dry_run_created_unmatched_with_synthetic_touch",
        "dry_run_unfilled",
        "at_touch",
        0.5,
    )


def test_quote_execution_diagnostics_handles_empty_database(tmp_path: Path) -> None:
    db_path = tmp_path / "empty.duckdb"
    with duckdb.connect(str(db_path)):
        pass

    report = create_quote_execution_diagnostics_report(db_path, tmp_path / "diagnostics")

    summary = cast(dict[str, Any], report["summary"])
    assert summary["signals"] == 0
    assert report["counts"] == {
        "quote_execution_signal_books": 0,
        "quote_execution_lifecycle": 0,
        "quote_execution_outcomes": 0,
        "quote_execution_summary": 1,
        "quote_execution_by_market_asset": 0,
        "quote_execution_synthetic_gap": 0,
        "quote_execution_examples": 0,
    }


def test_quote_execution_diagnostics_rejects_invalid_config() -> None:
    try:
        QuoteExecutionDiagnosticsConfig(max_future_window_ms=0)
    except ValueError as exc:
        assert "max_future_window_ms must be positive" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def seed_db(tmp_path: Path, *, synthetic: bool, observed: bool) -> Path:
    db_path = tmp_path / "research.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            create table signals (
                signal_id varchar,
                market_id varchar,
                asset_id varchar,
                side varchar,
                price double,
                size double,
                confidence double,
                strategy varchar,
                model_version varchar,
                data_version varchar,
                feature_version varchar,
                event_timestamp_ms bigint
            )
            """
        )
        conn.execute(
            """
            insert into signals values (
                'signal-1', 'market-1', 'asset-1', 'BUY', 0.50, 2.0, 0.70,
                'probe', 'model', 'data', 'feature', 1_000
            )
            """
        )
        conn.execute(
            """
            create table orderbook_snapshots (
                market_id varchar,
                asset_id varchar,
                event_timestamp_ms bigint,
                best_bid double,
                best_ask double,
                spread double,
                bid_depth double,
                ask_depth double
            )
            """
        )
        conn.executemany(
            "insert into orderbook_snapshots values (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("market-1", "asset-1", 900, 0.45, 0.50, 0.05, 5.0, 3.0),
                ("market-1", "asset-1", 1_100, 0.45, 0.50, 0.05, 5.0, 3.0),
            ],
        )
        conn.execute(
            """
            create table execution_reports (
                signal_id varchar,
                order_id varchar,
                status varchar,
                filled_price double,
                filled_size double,
                cumulative_filled_size double,
                remaining_size double,
                error varchar,
                event_timestamp_ms bigint
            )
            """
        )
        if observed:
            conn.executemany(
                "insert into execution_reports values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    ("signal-1", "dry-run-signal-1", "DELAYED", None, None, None, 2.0, None, 1_010),
                    ("signal-1", "dry-run-signal-1", "UNMATCHED", None, None, None, 2.0, None, 2_010),
                ],
            )
        if synthetic:
            conn.execute(
                """
                create table synthetic_execution_reports (
                    signal_id varchar,
                    order_id varchar,
                    status varchar,
                    filled_price double,
                    filled_size double,
                    cumulative_filled_size double,
                    remaining_size double,
                    error varchar,
                    event_timestamp_ms bigint
                )
                """
            )
            conn.execute(
                """
                insert into synthetic_execution_reports values (
                    'signal-1', 'synthetic-fill-signal-1', 'MATCHED',
                    0.50, 2.0, 2.0, 0.0, null, 1_100
                )
                """
            )
    return db_path
