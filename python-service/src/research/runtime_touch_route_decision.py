import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPORT_VERSION = "runtime_touch_route_decision_v1"
READY_FOR_AB_RETRY = "READY_FOR_AB_RETRY"
CHANGE_TIME_WINDOW = "CHANGE_TIME_WINDOW"
EXPAND_MARKET_DISCOVERY = "EXPAND_MARKET_DISCOVERY"
BLOCK_LIVE = "BLOCK_LIVE"


@dataclass(frozen=True)
class RuntimeTouchRouteDecisionConfig:
    min_assets: int = 2

    def __post_init__(self) -> None:
        if self.min_assets <= 0:
            raise ValueError("min_assets must be positive")


def create_runtime_touch_route_decision(
    output_path: Path,
    selection_probe_summary_path: Path,
    market_timing_scout_path: Path,
    config: RuntimeTouchRouteDecisionConfig = RuntimeTouchRouteDecisionConfig(),
) -> dict[str, Any]:
    selection_probe_summary = read_report(selection_probe_summary_path)
    market_timing_scout = read_report(market_timing_scout_path)
    next_action, rationale = classify_next_action(
        selection_probe_summary,
        market_timing_scout,
        config,
    )
    payload: dict[str, Any] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "can_promote_live": False,
        "decision_policy": "offline_runtime_touch_route_decision_only",
        "min_assets": config.min_assets,
        "next_action": next_action,
        "live_gate": "BLOCK_LIVE",
        "go_no_go": "NO_GO",
        "rationale": rationale,
        "inputs": {
            "selection_probe_summary": str(selection_probe_summary_path),
            "market_timing_scout": str(market_timing_scout_path),
        },
        "input_status": {
            "candidate_expansion_status": selection_probe_summary.get(
                "candidate_expansion_status"
            ),
            "candidate_expansion_assets": selection_probe_summary.get(
                "candidate_expansion_assets"
            ),
            "opportunity_window_status": selection_probe_summary.get(
                "opportunity_window_status"
            ),
            "opportunity_window_assets": selection_probe_summary.get(
                "opportunity_window_assets"
            ),
            "market_timing_scout_status": market_timing_scout.get("status"),
            "market_timing_scout_assets": market_timing_scout.get(
                "market_asset_ids_count"
            ),
        },
        "config": asdict(config),
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload


def classify_next_action(
    selection_probe_summary: dict[str, Any],
    market_timing_scout: dict[str, Any],
    config: RuntimeTouchRouteDecisionConfig,
) -> tuple[str, list[str]]:
    contract_errors = validate_research_only_contract(
        selection_probe_summary,
        "selection_probe_summary",
    )
    contract_errors.extend(
        validate_research_only_contract(market_timing_scout, "market_timing_scout")
    )
    if contract_errors:
        return BLOCK_LIVE, contract_errors

    expansion_status = str(selection_probe_summary.get("candidate_expansion_status"))
    expansion_assets = int_value(selection_probe_summary.get("candidate_expansion_assets"))
    opportunity_status = str(selection_probe_summary.get("opportunity_window_status"))
    opportunity_assets = int_value(selection_probe_summary.get("opportunity_window_assets"))
    scout_status = str(market_timing_scout.get("status"))
    scout_assets = int_value(market_timing_scout.get("market_asset_ids_count"))

    if (
        expansion_status == "ready"
        and expansion_assets >= config.min_assets
        and opportunity_status == "ready"
        and opportunity_assets >= config.min_assets
    ):
        return READY_FOR_AB_RETRY, [
            "candidate_expansion_and_opportunity_windows_have_comparable_universe",
            "next_step_is_research_only_ab_retry_ladder_not_live",
        ]

    if scout_status == "ready" and scout_assets >= config.min_assets:
        return CHANGE_TIME_WINDOW, [
            "market_timing_scout_found_a_comparable_active_window",
            "repeat_selection_probe_in_a_matching_time_window_before_ab_retry",
        ]

    if expansion_status == "ready" and expansion_assets >= config.min_assets:
        return CHANGE_TIME_WINDOW, [
            "candidate_expansion_has_assets_but_opportunity_windows_are_insufficient",
            "change_capture_window_or_wait_for_runtime_signalable_windows",
        ]

    return EXPAND_MARKET_DISCOVERY, [
        "candidate_expansion_has_insufficient_assets",
        "expand_market_discovery_before_running_another_ab_retry",
    ]


def validate_research_only_contract(report: dict[str, Any], label: str) -> list[str]:
    errors: list[str] = []
    if report.get("can_execute_trades") is not False:
        errors.append(f"{label}_can_execute_trades_not_false")
    if report.get("can_promote_live") is True:
        errors.append(f"{label}_can_promote_live_true")
    if not report.get("decision_policy"):
        errors.append(f"{label}_missing_decision_policy")
    return errors


def read_report(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"report does not exist: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"report must be a JSON object: {path}")
    return data


def int_value(value: Any) -> int:
    if value is None:
        return 0
    return int(value)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--selection-probe-summary", required=True)
    parser.add_argument("--market-timing-scout", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--min-assets", type=int, default=2)
    args = parser.parse_args()
    report = create_runtime_touch_route_decision(
        Path(args.output),
        Path(args.selection_probe_summary),
        Path(args.market_timing_scout),
        RuntimeTouchRouteDecisionConfig(min_assets=args.min_assets),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
