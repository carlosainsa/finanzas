import json
from pathlib import Path
from typing import Any

from src.research.runtime_touch_strict_signalable_collector import (
    REPORT_VERSION,
    create_strict_signalable_collector_summary,
)


def test_collector_selects_first_strict_ready_window(tmp_path: Path) -> None:
    relaxed = write_ladder_report(
        tmp_path / "window-01.json",
        route_result="DEPTH_STRICT_UNIVERSE_TOO_NARROW",
        strict_count=1,
        selected_label="runtime_hybrid_backfill",
        selected_count=2,
    )
    strict = write_ladder_report(
        tmp_path / "window-02.json",
        route_result="READY_FOR_STRICT_AB_RETRY",
        strict_count=2,
        selected_label="strict_signalable",
        selected_count=2,
    )

    summary = create_strict_signalable_collector_summary(
        [relaxed, strict],
        output_path=tmp_path / "summary.json",
        max_windows=3,
        min_assets=2,
    )

    assert summary["report_version"] == REPORT_VERSION
    assert summary["can_execute_trades"] is False
    assert summary["status"] == "ready"
    assert summary["next_action"] == "RUN_STRICT_RUNTIME_TOUCH_AB"
    assert summary["blockers"] == []
    selected = summary["selected_window"]
    assert isinstance(selected, dict)
    assert selected["window_index"] == 2
    assert selected["selected_attempt_label"] == "strict_signalable"
    assert selected["strict_asset_count"] == 2
    assert summary["recommended_next_run"] == {"script": "scripts/run_runtime_touch_ab_cycle.sh"}
    assert (tmp_path / "summary.json").exists()


def test_collector_blocks_when_only_non_strict_attempts_are_ready(
    tmp_path: Path,
) -> None:
    report = write_ladder_report(
        tmp_path / "window-01.json",
        route_result="DEPTH_STRICT_UNIVERSE_TOO_NARROW",
        strict_count=1,
        selected_label="runtime_hybrid_backfill",
        selected_count=2,
    )

    summary = create_strict_signalable_collector_summary(
        [report],
        max_windows=3,
        min_assets=2,
    )

    assert summary["status"] == "collect_more_windows"
    assert summary["next_action"] == "EXPAND_MARKET_DISCOVERY_OR_CHANGE_TIME_WINDOW"
    assert "strict_signalable_assets_below_minimum" in summary["blockers"]
    assert "depth_strict_universe_too_narrow" in summary["blockers"]
    assert "non_strict_attempt_selected_only" in summary["blockers"]
    assert summary["recommended_next_run"] is None


def test_collector_handles_empty_window_set(tmp_path: Path) -> None:
    summary = create_strict_signalable_collector_summary(
        [],
        output_path=tmp_path / "summary.json",
        max_windows=2,
        min_assets=2,
    )

    assert summary["status"] == "collect_more_windows"
    assert summary["windows_evaluated"] == 0
    assert summary["blockers"] == ["no_windows_evaluated"]


def write_ladder_report(
    path: Path,
    *,
    route_result: str,
    strict_count: int,
    selected_label: str | None,
    selected_count: int,
) -> Path:
    strict_attempt = {
        "label": "strict_signalable",
        "status": "ready" if strict_count >= 2 else "insufficient_assets",
        "market_asset_ids_count": strict_count,
        "selection_reason": f"strict_count={strict_count}",
    }
    selected_attempt: dict[str, Any] | None = None
    if selected_label is not None:
        selected_attempt = {
            "label": selected_label,
            "status": "ready",
            "market_asset_ids_count": selected_count,
        }
    payload = {
        "can_execute_trades": False,
        "can_promote_live": False,
        "can_run_selected_ab": route_result == "READY_FOR_STRICT_AB_RETRY",
        "recommendation": "test",
        "route_result": route_result,
        "strict_attempt": strict_attempt,
        "attempts": [strict_attempt],
        "selected_attempt": selected_attempt,
        "requires_allow_gate_bypass": route_result != "READY_FOR_STRICT_AB_RETRY",
        "next_run": {"script": "scripts/run_runtime_touch_ab_cycle.sh"},
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path
