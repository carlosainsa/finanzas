import json
from pathlib import Path

from src.research.runtime_touch_route_decision import (
    BLOCK_LIVE,
    CHANGE_TIME_WINDOW,
    EXPAND_MARKET_DISCOVERY,
    READY_FOR_AB_RETRY,
    REPORT_VERSION,
    RuntimeTouchRouteDecisionConfig,
    create_runtime_touch_route_decision,
)


def test_route_decision_ready_for_ab_retry(tmp_path: Path) -> None:
    summary = write_json(
        tmp_path / "summary.json",
        selection_summary(candidate_status="ready", candidate_assets=2),
    )
    scout = write_json(tmp_path / "scout.json", scout_report(status="ready", assets=2))

    report = create_runtime_touch_route_decision(
        tmp_path / "decision.json",
        summary,
        scout,
        RuntimeTouchRouteDecisionConfig(min_assets=2),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert report["can_promote_live"] is False
    assert report["live_gate"] == "BLOCK_LIVE"
    assert report["next_action"] == READY_FOR_AB_RETRY


def test_route_decision_expands_market_discovery_when_assets_are_missing(
    tmp_path: Path,
) -> None:
    summary = write_json(
        tmp_path / "summary.json",
        selection_summary(candidate_status="insufficient_assets", candidate_assets=1),
    )
    scout = write_json(
        tmp_path / "scout.json",
        scout_report(status="insufficient_assets", assets=1),
    )

    report = create_runtime_touch_route_decision(tmp_path / "decision.json", summary, scout)

    assert report["next_action"] == EXPAND_MARKET_DISCOVERY
    assert "candidate_expansion_has_insufficient_assets" in report["rationale"]


def test_route_decision_changes_time_window_when_scout_is_ready(
    tmp_path: Path,
) -> None:
    summary = write_json(
        tmp_path / "summary.json",
        selection_summary(candidate_status="insufficient_assets", candidate_assets=1),
    )
    scout = write_json(tmp_path / "scout.json", scout_report(status="ready", assets=2))

    report = create_runtime_touch_route_decision(tmp_path / "decision.json", summary, scout)

    assert report["next_action"] == CHANGE_TIME_WINDOW
    assert "market_timing_scout_found_a_comparable_active_window" in report["rationale"]


def test_route_decision_blocks_live_on_unsafe_contract(tmp_path: Path) -> None:
    summary = selection_summary(candidate_status="ready", candidate_assets=2)
    summary["can_execute_trades"] = True
    summary_path = write_json(tmp_path / "summary.json", summary)
    scout_path = write_json(tmp_path / "scout.json", scout_report(status="ready", assets=2))

    report = create_runtime_touch_route_decision(
        tmp_path / "decision.json",
        summary_path,
        scout_path,
    )

    assert report["next_action"] == BLOCK_LIVE
    assert "selection_probe_summary_can_execute_trades_not_false" in report["rationale"]


def selection_summary(
    *,
    candidate_status: str,
    candidate_assets: int,
    opportunity_status: str = "ready",
    opportunity_assets: int = 2,
) -> dict[str, object]:
    return {
        "can_execute_trades": False,
        "decision_policy": "offline_runtime_touch_selection_probe_only",
        "candidate_expansion_status": candidate_status,
        "candidate_expansion_assets": candidate_assets,
        "opportunity_window_status": opportunity_status,
        "opportunity_window_assets": opportunity_assets,
    }


def scout_report(*, status: str, assets: int) -> dict[str, object]:
    return {
        "can_execute_trades": False,
        "decision_policy": "offline_runtime_touch_market_timing_scout_only",
        "status": status,
        "market_asset_ids_count": assets,
    }


def write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path
