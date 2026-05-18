import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPORT_VERSION = "runtime_touch_discovery_loop_control_v1"


def evaluate_discovery_loop_batch(batch_root: Path, min_assets: int) -> dict[str, Any]:
    if min_assets < 2:
        raise ValueError("min_assets must be >= 2")
    scout = read_optional_json(
        batch_root
        / "runtime_touch_market_timing_scout"
        / "runtime_touch_market_timing_scout.json"
    )
    route = read_optional_json(batch_root / "runtime_touch_route_decision.json")
    scout_counts = object_value(scout.get("counts"))
    route_status = object_value(route.get("input_status"))
    selected_assets = int_value(
        scout.get("market_asset_ids_count")
        or route_status.get("market_timing_scout_assets")
    )
    eligible_windows = int_value(scout_counts.get("eligible_windows"))
    next_action = str(route.get("next_action") or "MISSING_ROUTE_DECISION")
    can_execute_trades = bool(route.get("can_execute_trades", False))
    should_stop = (
        not can_execute_trades
        and (
            next_action == "READY_FOR_AB_RETRY"
            or (
                next_action == "CHANGE_TIME_WINDOW"
                and selected_assets >= min_assets
                and eligible_windows > 0
            )
        )
    )
    return {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "runtime_touch_discovery_loop_early_stop_control",
        "batch_root": str(batch_root),
        "min_assets": min_assets,
        "route_next_action": next_action,
        "route_can_execute_trades": can_execute_trades,
        "selected_assets": selected_assets,
        "eligible_windows": eligible_windows,
        "should_stop": should_stop,
        "reason": early_stop_reason(
            should_stop,
            next_action,
            selected_assets,
            eligible_windows,
            min_assets,
            can_execute_trades,
        ),
    }


def early_stop_reason(
    should_stop: bool,
    next_action: str,
    selected_assets: int,
    eligible_windows: int,
    min_assets: int,
    can_execute_trades: bool,
) -> str:
    if can_execute_trades:
        return "invalid_research_contract"
    if should_stop and next_action == "READY_FOR_AB_RETRY":
        return "ready_for_ab_retry"
    if should_stop and next_action == "CHANGE_TIME_WINDOW":
        return "time_window_candidate_ready"
    if next_action == "MISSING_ROUTE_DECISION":
        return "missing_route_decision"
    if selected_assets < min_assets:
        return "insufficient_selected_assets"
    if eligible_windows <= 0:
        return "no_eligible_runtime_windows"
    return "continue_discovery"


def read_optional_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"expected JSON object: {path}")
    return data


def object_value(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def int_value(value: Any) -> int:
    if value is None or value == "":
        return 0
    return int(value)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-root", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--min-assets", type=int, default=2)
    args = parser.parse_args()
    report = evaluate_discovery_loop_batch(Path(args.batch_root), args.min_assets)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
