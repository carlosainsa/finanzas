import json
from pathlib import Path
from typing import Any, cast

from src.research.execution_failure_diagnostics import (
    REPORT_VERSION,
    create_execution_failure_diagnostics,
)


def test_execution_failure_diagnostics_separates_errors_from_unmatched(
    tmp_path: Path,
) -> None:
    report_root = tmp_path / "reports" / "run"
    report_root.mkdir(parents=True)
    write_json(
        report_root / "signal_to_order_conversion.json",
        {
            "summary": {
                "signals": 36,
                "reports": 36,
                "orders_created": 32,
                "error_reports": 4,
                "missing_reports": 0,
                "filled_signals": 0,
                "partial_signals": 0,
            },
            "top_root_causes": [
                {
                    "asset_id": "asset-1",
                    "strategy": "probe-v12",
                    "terminal_status": "UNMATCHED",
                    "root_cause": "order_created_unfilled",
                    "signals": 32,
                    "orders_created": 32,
                },
                {
                    "asset_id": "asset-1",
                    "strategy": "probe-v12",
                    "terminal_status": "ERROR",
                    "root_cause": "executor_reported_error",
                    "signals": 4,
                    "reports": 4,
                    "error_reports": 4,
                    "orders_created": 0,
                    "avg_report_latency_ms": 182_065.5,
                },
            ],
        },
    )
    write_json(
        report_root / "quote_execution_diagnostics.json",
        {
            "summary": {
                "no_fill_future_touch_rate": 0.0,
                "avg_required_quote_move": 0.0015,
            },
            "no_fill_diagnostics": [
                {
                    "asset_id": "asset-1",
                    "strategy": "probe-v12",
                    "model_version": "probe-v12",
                    "feature_version": "features-v12",
                    "quote_relation": "inside_spread",
                    "root_cause": "dry_run_created_unmatched",
                    "signals": 32,
                    "avg_distance_to_touch": 0.0015,
                    "avg_required_quote_move": 0.0015,
                    "future_touch_rate": 0.0,
                },
                {
                    "asset_id": "asset-1",
                    "strategy": "probe-v12",
                    "root_cause": "future_book_never_touched_limit",
                    "signals": 4,
                },
            ],
        },
    )

    report = create_execution_failure_diagnostics(report_root)

    assert report["report_version"] == REPORT_VERSION
    counts = cast(dict[str, Any], report["counts"])
    assert counts["errors"] == 4
    assert counts["unmatched"] == 32
    summary = cast(dict[str, Any], report["summary"])
    assert summary["dominant_failure_family"] == "UNMATCHED"
    assert summary["error_rate"] == 4 / 36
    assert report["recommended_next_action"] == "CHANGE_MARKET_OR_TIMING_FILTERS"
    error_rows = cast(list[dict[str, Any]], report["error_diagnostics"])
    unmatched_rows = cast(list[dict[str, Any]], report["unmatched_diagnostics"])
    assert error_rows[0]["root_cause"] == "executor_reported_error"
    assert unmatched_rows[0]["root_cause"] == "dry_run_created_unmatched"
    assert len(unmatched_rows) == 1


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
