from src.research.real_dry_run_preflight import build_preflight_report
from typing import cast


def test_preflight_report_passes_when_required_streams_progress() -> None:
    payload = build_preflight_report(
        run_id="run-1",
        started_at="2026-04-28T00:00:00+00:00",
        finished_at="2026-04-28T00:01:00+00:00",
        elapsed_seconds=60,
        stream_names=stream_names(),
        start_lengths={"orderbook": 1, "signals": 2, "reports": 3},
        end_lengths={"orderbook": 2, "signals": 3, "reports": 3},
        recent_reports=[],
        require_reports=False,
        market_asset_ids=["asset-1", "asset-2"],
        blocked_segments_path=None,
        check_seconds=60,
        capture_seconds=900,
    )

    assert payload["status"] == "ok"
    assert payload["blockers"] == []
    assert payload["recommendation"] == "continue_capture"
    assert payload["can_execute_trades"] is False


def test_preflight_report_fails_when_required_reports_do_not_progress() -> None:
    payload = build_preflight_report(
        run_id="run-1",
        started_at="2026-04-28T00:00:00+00:00",
        finished_at="2026-04-28T00:01:00+00:00",
        elapsed_seconds=60,
        stream_names=stream_names(),
        start_lengths={"orderbook": 1, "signals": 2, "reports": 3},
        end_lengths={"orderbook": 2, "signals": 3, "reports": 3},
        recent_reports=[],
        require_reports=True,
        market_asset_ids=["asset-1"],
        blocked_segments_path="/tmp/blocked_segments.json",
        check_seconds=60,
        capture_seconds=900,
    )

    blockers = cast(list[str], payload["blockers"])
    assert payload["status"] == "failed"
    assert payload["classification"] == "preflight_no_stream_progress"
    assert "missing_execution_reports_stream_progress" in blockers
    assert "missing_dry_run_execution_report" in blockers
    assert payload["recommendation"] == "repair_executor_pipeline_before_repeat"


def test_preflight_report_fails_when_signals_do_not_progress() -> None:
    payload = build_preflight_report(
        run_id="run-1",
        started_at="2026-04-28T00:00:00+00:00",
        finished_at="2026-04-28T00:01:00+00:00",
        elapsed_seconds=60,
        stream_names=stream_names(),
        start_lengths={"orderbook": 1, "signals": 2, "reports": 3},
        end_lengths={"orderbook": 2, "signals": 2, "reports": 4},
        recent_reports=[{"order_id": "dry-run-1", "status": "UNMATCHED"}],
        require_reports=True,
        market_asset_ids=[],
        blocked_segments_path=None,
        check_seconds=60,
        capture_seconds=900,
    )

    assert "missing_signals_stream_progress" in cast(list[str], payload["blockers"])
    assert payload["recommendation"] == "repair_predictor_pipeline_before_repeat"


def stream_names() -> dict[str, str]:
    return {
        "orderbook": "orderbook:stream",
        "signals": "signals:stream",
        "reports": "execution:reports:stream",
    }
