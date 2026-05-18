import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd  # type: ignore[import-untyped]

from src.research.runtime_touch_ranking import normalize_records


REPORT_VERSION = "runtime_touch_discovery_batch_diagnostics_v1"


@dataclass(frozen=True)
class RuntimeTouchDiscoveryBatchDiagnosticsConfig:
    min_assets: int = 2

    def __post_init__(self) -> None:
        if self.min_assets <= 0:
            raise ValueError("min_assets must be positive")


def create_runtime_touch_discovery_batch_diagnostics(
    discovery_batches_path: Path,
    batch_results_root: Path,
    output_dir: Path,
    comparison_path: Path | None = None,
    config: RuntimeTouchDiscoveryBatchDiagnosticsConfig = (
        RuntimeTouchDiscoveryBatchDiagnosticsConfig()
    ),
) -> dict[str, Any]:
    discovery_batches = read_json(discovery_batches_path)
    comparison = read_optional_json(comparison_path) if comparison_path else {}
    rows = [
        batch_diagnostic_row(batch, batch_results_root, config)
        for batch in discovery_batches.get("batches", [])
        if isinstance(batch, dict)
    ]
    normalized_rows = normalize_records(rows)
    adjustment = recommended_selector_adjustment(normalized_rows, config)
    output_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(normalized_rows).to_parquet(
        output_dir / "runtime_touch_discovery_batch_diagnostics.parquet",
        index=False,
    )
    payload: dict[str, Any] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "can_promote_live": False,
        "decision_policy": "offline_runtime_touch_discovery_batch_diagnostics_only",
        "selection_source": "runtime_touch_discovery_batches",
        "config": asdict(config),
        "source_discovery_batches": str(discovery_batches_path),
        "batch_results_root": str(batch_results_root),
        "source_comparison": str(comparison_path) if comparison_path else None,
        "comparison_status": comparison.get("status"),
        "comparison_next_action": comparison.get("next_action"),
        "counts": {
            "batches": len(rows),
            "processed_batches": sum(1 for row in rows if row["is_processed"]),
            "not_processed_batches": sum(
                1 for row in rows if row["readiness_status"] == "NOT_PROCESSED"
            ),
            "ready_batches": sum(
                1 for row in rows if row["route_next_action"] == "READY_FOR_AB_RETRY"
            ),
            "change_time_window_batches": sum(
                1 for row in rows if row["route_next_action"] == "CHANGE_TIME_WINDOW"
            ),
            "expand_discovery_batches": sum(
                1
                for row in rows
                if row["route_next_action"] == "EXPAND_MARKET_DISCOVERY"
            ),
        },
        "recommended_selector_adjustment": adjustment,
        "batches": normalized_rows,
        "outputs": [
            "runtime_touch_discovery_batch_diagnostics.parquet",
            "runtime_touch_discovery_batch_diagnostics.json",
        ],
    }
    (output_dir / "runtime_touch_discovery_batch_diagnostics.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload


def batch_diagnostic_row(
    batch: dict[str, Any],
    batch_results_root: Path,
    config: RuntimeTouchDiscoveryBatchDiagnosticsConfig,
) -> dict[str, Any]:
    batch_id = str(batch.get("batch_id") or "")
    batch_root = batch_results_root / batch_id
    signalability = read_optional_json(
        batch_root
        / "runtime_touch_signalability_diagnostic"
        / "runtime_touch_signalability_diagnostic.json"
    )
    expansion = read_optional_json(
        batch_root
        / "runtime_touch_candidate_expansion"
        / "runtime_touch_candidate_expansion.json"
    )
    windows = read_optional_json(
        batch_root
        / "runtime_touch_opportunity_windows"
        / "runtime_touch_opportunity_windows.json"
    )
    scout = read_optional_json(
        batch_root
        / "runtime_touch_market_timing_scout"
        / "runtime_touch_market_timing_scout.json"
    )
    route = read_optional_json(batch_root / "runtime_touch_route_decision.json")
    selection_summary = read_optional_json(
        batch_root / "runtime_touch_selection_probe_summary.json"
    )
    is_processed = bool(route or selection_summary or signalability)
    scout_counts = object_value(scout.get("counts"))
    signalability_blockers = object_value(signalability.get("blocker_counts"))
    route_next_action = route_next_action_for_batch(is_processed, route)
    signalable_assets = int_value(signalability.get("signalable_assets_count"))
    candidate_assets = int_value(
        expansion.get("market_asset_ids_count")
        or selection_summary.get("candidate_expansion_assets")
    )
    opportunity_assets = int_value(
        windows.get("market_asset_ids_count")
        or selection_summary.get("opportunity_window_assets")
    )
    scout_assets = int_value(
        scout.get("market_asset_ids_count")
        or object_value(route.get("input_status")).get("market_timing_scout_assets")
    )
    eligible_windows = int_value(scout_counts.get("eligible_windows"))
    families_count = int_value(batch.get("families_count"))
    fillability_covered_assets = int_value(batch.get("fillability_covered_assets_count"))
    primary_blocker = classify_primary_blocker(
        route_next_action=route_next_action,
        signalable_assets=signalable_assets,
        candidate_assets=candidate_assets,
        opportunity_assets=opportunity_assets,
        scout_assets=scout_assets,
        eligible_windows=eligible_windows,
        families_count=families_count,
        fillability_covered_assets=fillability_covered_assets,
        signalability_blockers=signalability_blockers,
        config=config,
    )
    readiness_status = classify_readiness_status(route_next_action)
    blockers = build_blockers(
        primary_blocker=primary_blocker,
        signalable_assets=signalable_assets,
        candidate_assets=candidate_assets,
        opportunity_assets=opportunity_assets,
        scout_assets=scout_assets,
        eligible_windows=eligible_windows,
        config=config,
    )
    selector_adjustments = selector_adjustment_recommendations(primary_blocker)
    stage_status = {
        "signalability": {
            "status": str(signalability.get("status") or "missing"),
            "assets": signalable_assets,
            "blocker_counts": signalability_blockers,
        },
        "candidate_expansion": {
            "status": str(expansion.get("status") or "missing"),
            "selected_assets": candidate_assets,
            "ranked_assets": int_value(object_value(expansion.get("counts")).get("ranked_assets")),
        },
        "opportunity_windows": {
            "status": str(windows.get("status") or "missing"),
            "selected_assets": opportunity_assets,
            "ranked_assets": int_value(object_value(windows.get("counts")).get("ranked_assets")),
            "windows": int_value(object_value(windows.get("counts")).get("windows")),
        },
        "market_timing_scout": {
            "status": str(scout.get("status") or "missing"),
            "selected_assets": scout_assets,
            "eligible_windows": eligible_windows,
            "windows": int_value(scout_counts.get("windows")),
            "asset_windows": int_value(scout_counts.get("asset_windows")),
        },
        "route_decision": {
            "next_action": route_next_action,
            "rationale": route.get("rationale") or [],
        },
    }
    return {
        "batch_id": batch_id,
        "batch_index": int_value(batch.get("batch_index")),
        "is_processed": is_processed,
        "markets_count": int_value(batch.get("markets_count")),
        "asset_ids_count": int_value(batch.get("asset_ids_count")),
        "families_count": families_count,
        "market_families": batch.get("market_families") or [],
        "selection_modes": batch.get("selection_modes") or [],
        "fillability_covered_assets_count": fillability_covered_assets,
        "readiness_status": readiness_status,
        "ready_for_ab_retry": readiness_status == "READY_FOR_AB_RETRY",
        "blockers": blockers,
        "route_next_action": route_next_action,
        "signalability_status": str(signalability.get("status") or "missing"),
        "signalable_assets_count": signalable_assets,
        "candidate_expansion_status": str(expansion.get("status") or "missing"),
        "candidate_expansion_assets": candidate_assets,
        "opportunity_window_status": str(windows.get("status") or "missing"),
        "opportunity_window_assets": opportunity_assets,
        "market_timing_scout_status": str(scout.get("status") or "missing"),
        "market_timing_scout_assets": scout_assets,
        "eligible_windows": eligible_windows,
        "signalability_blocker_counts": signalability_blockers,
        "stage_status": stage_status,
        "primary_blocker": primary_blocker,
        "selector_adjustment": selector_adjustment_for_blocker(primary_blocker),
        "selector_adjustment_recommendations": selector_adjustments,
        "source_artifacts": source_artifacts(batch_root),
        "batch_root": str(batch_root),
    }


def classify_readiness_status(route_next_action: str) -> str:
    if route_next_action == "READY_FOR_AB_RETRY":
        return "READY_FOR_AB_RETRY"
    if route_next_action == "CHANGE_TIME_WINDOW":
        return "TIME_WINDOW_READY_ONLY"
    if route_next_action == "BLOCK_LIVE":
        return "BLOCKED_CONTRACT"
    if route_next_action == "NOT_PROCESSED":
        return "NOT_PROCESSED"
    if route_next_action == "MISSING_ROUTE_DECISION":
        return "MISSING_ARTIFACTS"
    return "DISCOVERY_EXPANSION_REQUIRED"


def route_next_action_for_batch(is_processed: bool, route: dict[str, Any]) -> str:
    if not is_processed:
        return "NOT_PROCESSED"
    return str(route.get("next_action") or "MISSING_ROUTE_DECISION")


def build_blockers(
    *,
    primary_blocker: str,
    signalable_assets: int,
    candidate_assets: int,
    opportunity_assets: int,
    scout_assets: int,
    eligible_windows: int,
    config: RuntimeTouchDiscoveryBatchDiagnosticsConfig,
) -> list[dict[str, Any]]:
    if primary_blocker == "ready_for_ab_retry":
        return []
    if primary_blocker == "timing_window_candidate":
        return [
            {
                "stage": "route_decision",
                "code": "time_window_ready_only",
                "actual": scout_assets,
                "required": config.min_assets,
            }
        ]
    stage = "discovery"
    actual = 0
    required = config.min_assets
    if primary_blocker.startswith("signalability_"):
        stage = "signalability"
        actual = signalable_assets
    elif primary_blocker == "insufficient_candidate_expansion_assets":
        stage = "candidate_expansion"
        actual = candidate_assets
    elif primary_blocker == "insufficient_opportunity_window_assets":
        stage = "opportunity_windows"
        actual = opportunity_assets
    elif primary_blocker == "insufficient_market_timing_windows":
        stage = "market_timing_scout"
        actual = max(scout_assets, eligible_windows)
    elif primary_blocker == "missing_batch_artifacts":
        stage = "route_decision"
        required = 1
    elif primary_blocker == "not_processed_by_loop_limit":
        stage = "batch_loop"
        required = 1
    return [
        {
            "stage": stage,
            "code": primary_blocker,
            "actual": actual,
            "required": required,
        }
    ]


def classify_primary_blocker(
    *,
    route_next_action: str,
    signalable_assets: int,
    candidate_assets: int,
    opportunity_assets: int,
    scout_assets: int,
    eligible_windows: int,
    families_count: int,
    fillability_covered_assets: int,
    signalability_blockers: dict[str, Any],
    config: RuntimeTouchDiscoveryBatchDiagnosticsConfig,
) -> str:
    if route_next_action == "READY_FOR_AB_RETRY":
        return "ready_for_ab_retry"
    if route_next_action == "CHANGE_TIME_WINDOW":
        return "timing_window_candidate"
    if route_next_action == "NOT_PROCESSED":
        return "not_processed_by_loop_limit"
    if route_next_action == "MISSING_ROUTE_DECISION":
        return "missing_batch_artifacts"
    if families_count < config.min_assets:
        return "insufficient_family_diversity"
    if fillability_covered_assets < config.min_assets:
        return "insufficient_fillability_coverage"
    if signalable_assets < config.min_assets:
        strongest = strongest_blocker(signalability_blockers)
        return f"signalability_{strongest}" if strongest else "insufficient_signalable_assets"
    if candidate_assets < config.min_assets:
        return "insufficient_candidate_expansion_assets"
    if opportunity_assets < config.min_assets:
        return "insufficient_opportunity_window_assets"
    if scout_assets < config.min_assets or eligible_windows <= 0:
        return "insufficient_market_timing_windows"
    return "not_ready_unknown"


def selector_adjustment_for_blocker(blocker: str) -> str:
    if blocker == "ready_for_ab_retry":
        return "run_retry_ladder"
    if blocker == "timing_window_candidate":
        return "repeat_same_selector_in_matching_time_window"
    if blocker == "insufficient_family_diversity":
        return "keep_diversification_enabled_and_increase_discovery_limit"
    if blocker == "insufficient_fillability_coverage":
        return "increase_exploration_rate_or_refresh_fillability_scores"
    if blocker.startswith("signalability_signalable"):
        return "increase_market_diversity_before_relaxing_signalability_thresholds"
    if blocker.startswith("signalability_avg_spread"):
        return "prefer_markets_with_higher_recent_spread_opportunities"
    if blocker.startswith("signalability_stale"):
        return "drop_stale_market_families_or_change_time_window"
    if blocker.startswith("signalability_"):
        return "expand_discovery_or_change_market_timing"
    if blocker == "insufficient_candidate_expansion_assets":
        return "increase_batch_size_or_discovery_limit_with_family_diversity"
    if blocker == "insufficient_opportunity_window_assets":
        return "change_time_window_or_prefer_assets_with_window_evidence"
    if blocker == "insufficient_market_timing_windows":
        return "repeat_during_more_active_market_window"
    if blocker == "missing_batch_artifacts":
        return "run_or_resume_batch_probe"
    if blocker == "not_processed_by_loop_limit":
        return "increase_max_batches_or_ignore_if_previous_batches_blocked"
    return "expand_market_discovery"


def selector_adjustment_recommendations(blocker: str) -> list[dict[str, str]]:
    adjustment = selector_adjustment_for_blocker(blocker)
    if adjustment == "run_retry_ladder":
        parameter = "recommended_next_run"
    elif "exploration_rate" in adjustment or "explore" in adjustment:
        parameter = "exploration_rate"
    elif "batch_size" in adjustment:
        parameter = "batch_size"
    elif "diversity" in adjustment:
        parameter = "max_markets_per_family_per_batch"
    elif "time_window" in adjustment:
        parameter = "capture_window"
    else:
        parameter = "discovery_limit"
    return [
        {
            "selector": "runtime_touch_discovery_batches",
            "parameter": parameter,
            "recommendation": adjustment,
            "reason": blocker,
        }
    ]


def source_artifacts(batch_root: Path) -> dict[str, str]:
    return {
        "selection_probe_summary": str(
            batch_root / "runtime_touch_selection_probe_summary.json"
        ),
        "signalability_diagnostic": str(
            batch_root
            / "runtime_touch_signalability_diagnostic"
            / "runtime_touch_signalability_diagnostic.json"
        ),
        "candidate_expansion": str(
            batch_root
            / "runtime_touch_candidate_expansion"
            / "runtime_touch_candidate_expansion.json"
        ),
        "opportunity_windows": str(
            batch_root
            / "runtime_touch_opportunity_windows"
            / "runtime_touch_opportunity_windows.json"
        ),
        "market_timing_scout": str(
            batch_root
            / "runtime_touch_market_timing_scout"
            / "runtime_touch_market_timing_scout.json"
        ),
        "route_decision": str(batch_root / "runtime_touch_route_decision.json"),
    }


def recommended_selector_adjustment(
    rows: list[dict[str, Any]],
    config: RuntimeTouchDiscoveryBatchDiagnosticsConfig,
) -> dict[str, Any]:
    processed = [row for row in rows if row.get("is_processed")]
    ready = [row for row in processed if row.get("primary_blocker") == "ready_for_ab_retry"]
    if ready:
        return {
            "action": "RUN_RETRY_LADDER",
            "reason": "at_least_one_batch_ready_for_ab_retry",
            "selector_config": {},
            "can_execute_trades": False,
        }
    blocker_counts: dict[str, int] = {}
    for row in processed:
        blocker = str(row.get("primary_blocker") or "unknown")
        blocker_counts[blocker] = blocker_counts.get(blocker, 0) + 1
    dominant = max(blocker_counts.items(), key=lambda item: item[1])[0] if blocker_counts else "none"
    max_families = max((int_value(row.get("families_count")) for row in processed), default=0)
    max_fillability = max(
        (int_value(row.get("fillability_covered_assets_count")) for row in processed),
        default=0,
    )
    if dominant == "insufficient_family_diversity" or max_families < config.min_assets:
        return {
            "action": "INCREASE_DISCOVERY_DIVERSITY",
            "reason": dominant,
            "selector_config": {
                "diversify_batches": True,
                "max_markets_per_family_per_batch": 1,
                "increase_discovery_limit": True,
            },
            "can_execute_trades": False,
        }
    if dominant == "insufficient_fillability_coverage" or max_fillability < config.min_assets:
        return {
            "action": "REFRESH_FILLABILITY_OR_EXPLORE_MORE",
            "reason": dominant,
            "selector_config": {
                "exploration_rate_min": 0.35,
                "refresh_market_fillability_score": True,
            },
            "can_execute_trades": False,
        }
    if dominant.startswith("signalability_"):
        return {
            "action": "EXPAND_DISCOVERY_WITH_SIGNALABILITY_AWARE_FILTER",
            "reason": dominant,
            "selector_config": {
                "diversify_batches": True,
                "increase_max_batches": True,
                "do_not_relax_min_assets": True,
            },
            "can_execute_trades": False,
        }
    return {
        "action": "RUN_MORE_DIVERSIFIED_BATCHES",
        "reason": dominant,
        "selector_config": {
            "diversify_batches": True,
            "max_batches_min": 2,
        },
        "can_execute_trades": False,
    }


def strongest_blocker(blockers: dict[str, Any]) -> str:
    if not blockers:
        return ""
    return max(blockers.items(), key=lambda item: int_value(item[1]))[0]


def read_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"expected JSON object: {path}")
    return data


def read_optional_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    return read_json(path)


def object_value(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def int_value(value: Any) -> int:
    if value is None or value == "":
        return 0
    return int(value)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--discovery-batches", required=True)
    parser.add_argument("--batch-results-root", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--comparison")
    parser.add_argument("--min-assets", type=int, default=2)
    args = parser.parse_args()
    report = create_runtime_touch_discovery_batch_diagnostics(
        Path(args.discovery_batches),
        Path(args.batch_results_root),
        Path(args.output_dir),
        comparison_path=Path(args.comparison) if args.comparison else None,
        config=RuntimeTouchDiscoveryBatchDiagnosticsConfig(min_assets=args.min_assets),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
