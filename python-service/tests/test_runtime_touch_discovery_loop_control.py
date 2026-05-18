import json
from pathlib import Path

from src.research.runtime_touch_discovery_loop_control import (
    evaluate_discovery_loop_batch,
)


def test_discovery_loop_control_stops_for_ab_retry(tmp_path: Path) -> None:
    batch_root = write_batch(
        tmp_path,
        route_next_action="READY_FOR_AB_RETRY",
        selected_assets=2,
        eligible_windows=1,
    )

    report = evaluate_discovery_loop_batch(batch_root, min_assets=2)

    assert report["can_execute_trades"] is False
    assert report["should_stop"] is True
    assert report["reason"] == "ready_for_ab_retry"


def test_discovery_loop_control_stops_for_time_window_candidate(tmp_path: Path) -> None:
    batch_root = write_batch(
        tmp_path,
        route_next_action="CHANGE_TIME_WINDOW",
        selected_assets=2,
        eligible_windows=1,
    )

    report = evaluate_discovery_loop_batch(batch_root, min_assets=2)

    assert report["should_stop"] is True
    assert report["reason"] == "time_window_candidate_ready"


def test_discovery_loop_control_continues_for_sparse_batch(tmp_path: Path) -> None:
    batch_root = write_batch(
        tmp_path,
        route_next_action="EXPAND_MARKET_DISCOVERY",
        selected_assets=1,
        eligible_windows=0,
    )

    report = evaluate_discovery_loop_batch(batch_root, min_assets=2)

    assert report["should_stop"] is False
    assert report["reason"] == "insufficient_selected_assets"


def write_batch(
    tmp_path: Path,
    *,
    route_next_action: str,
    selected_assets: int,
    eligible_windows: int,
) -> Path:
    batch_root = tmp_path / "batch"
    scout_root = batch_root / "runtime_touch_market_timing_scout"
    scout_root.mkdir(parents=True)
    (scout_root / "runtime_touch_market_timing_scout.json").write_text(
        json.dumps(
            {
                "can_execute_trades": False,
                "decision_policy": "offline_runtime_touch_market_timing_scout_only",
                "market_asset_ids_count": selected_assets,
                "counts": {
                    "eligible_windows": eligible_windows,
                },
            }
        ),
        encoding="utf-8",
    )
    (batch_root / "runtime_touch_route_decision.json").write_text(
        json.dumps(
            {
                "can_execute_trades": False,
                "decision_policy": "offline_runtime_touch_route_decision_only",
                "next_action": route_next_action,
                "input_status": {
                    "market_timing_scout_assets": selected_assets,
                    "market_timing_scout_status": "ready",
                },
            }
        ),
        encoding="utf-8",
    )
    return batch_root
