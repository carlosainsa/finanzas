import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.research.profile_observation_comparison import (
    list_of_dicts,
    numeric_or_none,
    read_json,
    typed_dict,
)


REPORT_VERSION = "runtime_touch_ab_root_cause_v1"


@dataclass(frozen=True)
class RuntimeTouchAbRootCauseThresholds:
    min_signals: int = 50
    min_reports: int = 1
    min_observed_fill_rate: float = 0.005
    max_error_rate: float = 0.05
    max_synthetic_observed_gap: float = 0.05
    max_adverse_selection: float = 0.0
    max_toxic_adverse_rate: float = 0.50


def create_runtime_touch_ab_root_cause(
    *,
    cycle_summary: dict[str, object] | None = None,
    profile_observation_comparison: dict[str, object] | None = None,
    touch_comparison: dict[str, object] | None = None,
    runtime_touch_ab_decision: dict[str, object] | None = None,
    thresholds: RuntimeTouchAbRootCauseThresholds = RuntimeTouchAbRootCauseThresholds(),
) -> dict[str, object]:
    cycle_summary = cycle_summary or {}
    profile_observation_comparison = profile_observation_comparison or {}
    touch_comparison = touch_comparison or {}
    runtime_touch_ab_decision = runtime_touch_ab_decision or {}
    observations = list_of_dicts(profile_observation_comparison.get("observations"))
    candidate = observations[-1] if observations else {}
    category, evidence, next_action = classify_root_cause(
        cycle_summary=cycle_summary,
        candidate=candidate,
        touch_comparison=touch_comparison,
        runtime_touch_ab_decision=runtime_touch_ab_decision,
        thresholds=thresholds,
    )
    return {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "can_promote_live": False,
        "decision_policy": "offline_runtime_touch_ab_root_cause_only",
        "source_report_versions": {
            "profile_observation_comparison": profile_observation_comparison.get(
                "report_version"
            ),
            "execution_probe_touch_comparison": touch_comparison.get("report_version"),
            "runtime_touch_ab_decision": runtime_touch_ab_decision.get(
                "report_version"
            ),
        },
        "thresholds": asdict(thresholds),
        "root_cause_category": category,
        "recommended_next_action": next_action,
        "evidence": evidence,
        "runtime_touch_ab_decision_recommendation": runtime_touch_ab_decision.get(
            "recommendation"
        ),
        "outputs": ["runtime_touch_ab_root_cause.json"],
    }


def classify_root_cause(
    *,
    cycle_summary: dict[str, object],
    candidate: dict[str, Any],
    touch_comparison: dict[str, object],
    runtime_touch_ab_decision: dict[str, object],
    thresholds: RuntimeTouchAbRootCauseThresholds,
) -> tuple[str, dict[str, object], str]:
    blocker = str(cycle_summary.get("blocker") or "")
    if blocker == "runtime_touch_signalability_gate":
        gate = typed_dict(cycle_summary.get("signalability_gate"))
        evidence = {
            "blocker": blocker,
            "gate_status": gate.get("status"),
            "signalable_assets_count": gate.get("signalable_assets_count"),
            "min_assets": gate.get("min_assets"),
            "blocker_counts": gate.get("blocker_counts"),
        }
        return (
            "NO_SIGNALABLE_ASSETS",
            evidence,
            "RERANK_OR_EXPAND_RUNTIME_TOUCH_UNIVERSE",
        )

    preflight = typed_dict(cycle_summary.get("failed_preflight"))
    blockers = [str(item) for item in list_values(preflight.get("blockers"))]
    if "missing_signals_stream_progress" in blockers:
        return (
            "SIGNALS_ZERO",
            {"preflight_blockers": blockers, "failed_profile": cycle_summary.get("failed_profile")},
            "RERANK_OR_CHANGE_MARKET_TIMING",
        )
    if "missing_execution_reports_stream_progress" in blockers:
        return (
            "REPORTS_ZERO",
            {"preflight_blockers": blockers, "failed_profile": cycle_summary.get("failed_profile")},
            "FIX_RISK_EXECUTOR_OR_REPORTING",
        )

    activity = typed_dict(candidate.get("activity"))
    execution_failure = typed_dict(candidate.get("execution_failure_diagnostics"))
    failure_summary = typed_dict(execution_failure.get("summary"))
    failure_counts = typed_dict(execution_failure.get("counts"))
    fills = typed_dict(candidate.get("fills"))
    risk = typed_dict(candidate.get("risk"))
    fill_toxicity = typed_dict(candidate.get("fill_toxicity"))
    touch_diagnosis = typed_dict(touch_comparison.get("diagnosis"))

    signals = numeric_or_none(activity.get("signals"))
    reports = numeric_or_none(failure_counts.get("reports"))
    error_rate = numeric_or_none(failure_summary.get("error_rate")) or 0.0
    failure_family = str(failure_summary.get("dominant_failure_family") or "")
    observed_fill_rate = (
        numeric_or_none(fills.get("observed_fill_rate"))
        or numeric_or_none(fills.get("dry_run_observed_fill_rate"))
        or numeric_or_none(fills.get("fill_rate"))
        or 0.0
    )
    synthetic_gap = (
        numeric_or_none(fills.get("fill_rate_gap"))
        or numeric_or_none(fills.get("adjusted_fill_rate_gap"))
        or 0.0
    )
    adverse_selection = numeric_or_none(risk.get("adverse_selection")) or 0.0
    toxic_adverse_rate = numeric_or_none(fill_toxicity.get("adverse_30s_rate"))
    rejected_segments = numeric_or_none(fill_toxicity.get("rejected_segments")) or 0.0

    evidence = {
        "profile": candidate.get("profile"),
        "signals": signals,
        "reports": reports,
        "error_rate": error_rate,
        "dominant_failure_family": failure_family,
        "observed_fill_rate": observed_fill_rate,
        "synthetic_observed_gap": synthetic_gap,
        "adverse_selection": adverse_selection,
        "toxic_adverse_rate": toxic_adverse_rate,
        "rejected_toxic_segments": rejected_segments,
        "touch_next_action": touch_diagnosis.get("next_action"),
        "decision_recommendation": runtime_touch_ab_decision.get("recommendation"),
    }

    if signals is None or signals <= 0:
        return "SIGNALS_ZERO", evidence, "RERANK_OR_CHANGE_MARKET_TIMING"
    if reports is not None and reports < thresholds.min_reports:
        return "REPORTS_ZERO", evidence, "FIX_RISK_EXECUTOR_OR_REPORTING"
    if error_rate > thresholds.max_error_rate or failure_family == "ERROR":
        return "ERRORS_RISK_EXECUTOR", evidence, "FIX_RISK_OR_EXECUTOR_ERRORS"
    if failure_family == "UNMATCHED" and observed_fill_rate <= 0:
        return "UNMATCHED_QUOTES", evidence, "CHANGE_MARKET_TIMING_OR_QUOTE_POLICY"
    if (
        toxic_adverse_rate is not None
        and toxic_adverse_rate > thresholds.max_toxic_adverse_rate
    ) or rejected_segments > 0:
        return "TOXIC_FILLS", evidence, "ADD_OR_REPAIR_TOXICITY_FILTERS"
    if observed_fill_rate < thresholds.min_observed_fill_rate:
        return "LOW_FILL_RATE", evidence, "REPEAT_OR_RETUNE_FILLABILITY_SELECTION"
    if synthetic_gap > thresholds.max_synthetic_observed_gap:
        return "SYNTHETIC_OPTIMISM", evidence, "REDUCE_SYNTHETIC_DEPENDENCE"
    if adverse_selection > thresholds.max_adverse_selection:
        return "ADVERSE_SELECTION", evidence, "FILTER_TOXIC_MARKET_TIMING"
    return "NO_ROOT_CAUSE_BLOCKER", evidence, "REPEAT_LONGER_OBSERVATION"


def list_values(value: object) -> list[object]:
    return value if isinstance(value, list) else []


def write_runtime_touch_ab_root_cause(
    output_path: Path,
    *,
    cycle_summary_path: Path | None = None,
    profile_observation_comparison_path: Path | None = None,
    touch_comparison_path: Path | None = None,
    runtime_touch_ab_decision_path: Path | None = None,
) -> dict[str, object]:
    report = create_runtime_touch_ab_root_cause(
        cycle_summary=read_json(cycle_summary_path) if cycle_summary_path else {},
        profile_observation_comparison=(
            read_json(profile_observation_comparison_path)
            if profile_observation_comparison_path
            else {}
        ),
        touch_comparison=read_json(touch_comparison_path) if touch_comparison_path else {},
        runtime_touch_ab_decision=(
            read_json(runtime_touch_ab_decision_path)
            if runtime_touch_ab_decision_path
            else {}
        ),
    )
    write_json_atomic(output_path, report)
    return report


def write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="runtime-touch-ab-root-cause")
    parser.add_argument("--cycle-summary", type=Path)
    parser.add_argument("--profile-observation-comparison", type=Path)
    parser.add_argument("--touch-comparison", type=Path)
    parser.add_argument("--runtime-touch-ab-decision", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = write_runtime_touch_ab_root_cause(
        args.output,
        cycle_summary_path=args.cycle_summary,
        profile_observation_comparison_path=args.profile_observation_comparison,
        touch_comparison_path=args.touch_comparison,
        runtime_touch_ab_decision_path=args.runtime_touch_ab_decision,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
