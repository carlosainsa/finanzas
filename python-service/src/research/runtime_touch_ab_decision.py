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


REPORT_VERSION = "runtime_touch_ab_decision_v1"


@dataclass(frozen=True)
class RuntimeTouchAbDecisionThresholds:
    min_signals: int = 50
    min_observed_fill_rate: float = 0.005
    max_error_rate: float = 0.05
    max_synthetic_observed_gap: float = 0.05
    max_adverse_selection: float = 0.0
    max_drawdown: float = 0.0


def create_runtime_touch_ab_decision(
    profile_observation_comparison: dict[str, object],
    touch_comparison: dict[str, object],
    thresholds: RuntimeTouchAbDecisionThresholds = RuntimeTouchAbDecisionThresholds(),
) -> dict[str, object]:
    observations = list_of_dicts(profile_observation_comparison.get("observations"))
    candidate = observations[-1] if observations else {}
    baseline = observations[-2] if len(observations) >= 2 else {}
    diagnosis = typed_dict(touch_comparison.get("diagnosis"))
    comparability_checks = build_comparability_checks(
        profile_observation_comparison,
        baseline,
        candidate,
    )
    checks = [*comparability_checks, *build_checks(candidate, thresholds)]
    recommendation, next_step, rationale = classify_next_action(
        baseline,
        candidate,
        diagnosis,
        thresholds,
        comparability_checks,
    )
    return {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "can_promote_live": False,
        "decision_policy": "offline_runtime_touch_ab_decision_only",
        "source_report_versions": {
            "profile_observation_comparison": profile_observation_comparison.get(
                "report_version"
            ),
            "execution_probe_touch_comparison": touch_comparison.get("report_version"),
        },
        "thresholds": asdict(thresholds),
        "baseline": summarize_observation(baseline),
        "candidate": summarize_observation(candidate),
        "touch_diagnosis": diagnosis,
        "comparability_checks": comparability_checks,
        "recommendation": recommendation,
        "next_step": next_step,
        "rationale": rationale,
        "checks": checks,
        "next_command_templates": command_templates(recommendation),
    }


def create_runtime_touch_ab_preflight_failure_decision(
    preflight: dict[str, object],
    *,
    failed_profile: str,
) -> dict[str, object]:
    raw_blockers = preflight.get("blockers")
    blockers = [
        str(item)
        for item in (raw_blockers if isinstance(raw_blockers, list) else [])
        if isinstance(item, str)
    ]
    recommendation, next_step = classify_preflight_failure(
        blockers, failed_profile, preflight
    )
    return {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "can_promote_live": False,
        "decision_policy": "offline_runtime_touch_ab_preflight_failure_only",
        "source_report_versions": {
            "real_dry_run_preflight": preflight.get("report_version"),
        },
        "thresholds": asdict(RuntimeTouchAbDecisionThresholds()),
        "baseline": {},
        "candidate": {
            "profile": failed_profile,
            "market_asset_ids_count": preflight.get("market_asset_ids_count"),
            "market_asset_ids_sha256": preflight.get("market_asset_ids_sha256"),
            "capture_seconds": preflight.get("capture_seconds"),
        },
        "touch_diagnosis": {},
        "comparability_checks": [
            check_equals(
                "preflight_can_execute_trades_false",
                preflight.get("can_execute_trades"),
                False,
            )
        ],
        "recommendation": recommendation,
        "next_step": next_step,
        "rationale": blockers or [str(preflight.get("classification") or "unknown")],
        "checks": [
            check_equals(
                "preflight_status_ok",
                preflight.get("status"),
                "ok",
            )
        ],
        "next_command_templates": command_templates(recommendation),
    }


def create_runtime_touch_ab_signalability_gate_failure_decision(
    gate: dict[str, object],
) -> dict[str, object]:
    signalable_assets = numeric_or_none(gate.get("signalable_assets_count"))
    min_assets = numeric_or_none(gate.get("min_assets"))
    blocker_counts = typed_dict(gate.get("blocker_counts"))
    dominant_blockers = sorted(
        (
            (str(blocker), numeric_or_none(count) or 0.0)
            for blocker, count in blocker_counts.items()
        ),
        key=lambda item: (-item[1], item[0]),
    )
    rationale = [
        f"{blocker}={int(count) if count.is_integer() else count}"
        for blocker, count in dominant_blockers[:3]
    ] or [str(gate.get("status") or "unknown")]
    return {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "can_promote_live": False,
        "decision_policy": "offline_runtime_touch_ab_signalability_gate_failure_only",
        "source_report_versions": {
            "runtime_touch_signalability_diagnostic": gate.get("report_version"),
        },
        "thresholds": asdict(RuntimeTouchAbDecisionThresholds()),
        "baseline": {},
        "candidate": {
            "signalable_assets_count": gate.get("signalable_assets_count"),
            "min_assets": gate.get("min_assets"),
            "assets_analyzed": gate.get("assets_analyzed"),
        },
        "touch_diagnosis": {},
        "comparability_checks": [
            check_equals(
                "signalability_gate_can_execute_trades_false",
                gate.get("can_execute_trades"),
                False,
            )
        ],
        "recommendation": "RERANK_RUNTIME_TOUCH_UNIVERSE",
        "next_step": (
            "Fresh runtime data did not produce enough signalable assets; rerank, "
            "widen, or change market/timing selection before retrying A/B."
        ),
        "rationale": rationale,
        "checks": [
            check_equals(
                "signalability_gate_status_ready",
                gate.get("status"),
                "ready",
            ),
            check_at_least(
                "minimum_signalable_assets",
                signalable_assets,
                min_assets or 0,
            ),
        ],
        "next_command_templates": command_templates("RERANK_RUNTIME_TOUCH_UNIVERSE"),
    }


def classify_preflight_failure(
    blockers: list[str],
    failed_profile: str,
    preflight: dict[str, object] | None = None,
) -> tuple[str, str]:
    decision_diagnostics = (
        preflight.get("predictor_decision_diagnostics")
        if isinstance(preflight, dict)
        else None
    )
    primary_rejection_reason = None
    if isinstance(decision_diagnostics, dict):
        reason = decision_diagnostics.get("primary_rejection_reason")
        primary_rejection_reason = str(reason) if isinstance(reason, str) else None
    if "missing_predictor_decisions_stream_progress" in blockers:
        return (
            "FIX_PREDICTOR_DECISION_TRACE",
            f"{failed_profile} consumed orderbook data without predictor decision traces; repair consumer tracing before retrying A/B.",
        )
    if "missing_signals_stream_progress" in blockers:
        if primary_rejection_reason == "low_depth":
            return (
                "RERANK_RUNTIME_TOUCH_UNIVERSE",
                f"{failed_profile} rejected all preflight decisions for low_depth; rerank with fresher depth evidence or widen the runtime-touch universe before retrying A/B.",
            )
        return (
            "RERANK_RUNTIME_TOUCH_UNIVERSE",
            f"{failed_profile} produced no signals during preflight; rerank or widen the runtime-touch universe before retrying A/B.",
        )
    if "missing_execution_reports_stream_progress" in blockers:
        return (
            "FIX_RISK_OR_EXECUTOR_ERRORS",
            f"{failed_profile} produced signals without execution reports; inspect executor/risk before retrying A/B.",
        )
    return (
        "REPEAT_AB_AFTER_PREFLIGHT_REPAIR",
        f"{failed_profile} failed preflight; repair the listed blockers before retrying A/B.",
    )


def build_checks(
    candidate: dict[str, Any],
    thresholds: RuntimeTouchAbDecisionThresholds,
) -> list[dict[str, object]]:
    summary = candidate_metrics(candidate)
    return [
        check_equals(
            "candidate_profile_is_execution_probe_v12",
            summary.get("profile"),
            "execution_probe_v12",
        ),
        check_at_least(
            "minimum_signal_sample",
            numeric_or_none(summary.get("signals")),
            float(thresholds.min_signals),
        ),
        check_at_most(
            "execution_error_rate",
            numeric_or_none(summary.get("error_rate")),
            thresholds.max_error_rate,
        ),
        check_at_least(
            "minimum_observed_fill_rate",
            numeric_or_none(summary.get("observed_fill_rate")),
            thresholds.min_observed_fill_rate,
        ),
        check_at_most(
            "synthetic_observed_gap",
            numeric_or_none(summary.get("fill_rate_gap")),
            thresholds.max_synthetic_observed_gap,
        ),
        check_at_most(
            "adverse_selection",
            numeric_or_none(summary.get("adverse_selection")),
            thresholds.max_adverse_selection,
            required=False,
        ),
        check_at_most(
            "drawdown",
            numeric_or_none(summary.get("drawdown")),
            thresholds.max_drawdown,
            required=False,
        ),
    ]


def build_comparability_checks(
    profile_observation_comparison: dict[str, object],
    baseline: dict[str, Any],
    candidate: dict[str, Any],
) -> list[dict[str, object]]:
    baseline_hash = baseline.get("market_asset_ids_sha256")
    candidate_hash = candidate.get("market_asset_ids_sha256")
    baseline_count = numeric_or_none(baseline.get("market_asset_ids_count"))
    candidate_count = numeric_or_none(candidate.get("market_asset_ids_count"))
    baseline_capture = numeric_or_none(baseline.get("capture_seconds"))
    candidate_capture = numeric_or_none(candidate.get("capture_seconds"))
    capture_delta = (
        abs(candidate_capture - baseline_capture)
        if baseline_capture is not None and candidate_capture is not None
        else None
    )
    return [
        check_equals(
            "comparison_can_execute_trades_false",
            profile_observation_comparison.get("can_execute_trades"),
            False,
        ),
        check_equals(
            "baseline_profile_is_execution_probe_v11",
            baseline.get("profile"),
            "execution_probe_v11",
        ),
        check_equals(
            "candidate_profile_is_execution_probe_v12",
            candidate.get("profile"),
            "execution_probe_v12",
        ),
        check_equals(
            "same_market_asset_universe_hash",
            candidate_hash,
            baseline_hash,
        ),
        check_equals(
            "same_market_asset_count",
            candidate_count,
            baseline_count,
        ),
        check_at_most(
            "capture_duration_delta_seconds",
            capture_delta,
            60.0,
        ),
    ]


def classify_next_action(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    touch_diagnosis: dict[str, Any],
    thresholds: RuntimeTouchAbDecisionThresholds,
    comparability_checks: list[dict[str, object]],
) -> tuple[str, str, list[str]]:
    failed_comparability = [
        item
        for item in comparability_checks
        if item["status"] in {"FAIL", "MISSING"}
    ]
    if failed_comparability:
        return (
            "REPEAT_AB_WITH_COMPARABLE_UNIVERSE",
            "Repeat the A/B only after v11 and v12 share the same universe, duration, and research-only contract.",
            [str(item["check_name"]) for item in failed_comparability],
        )
    if not candidate:
        return (
            "GENERATE_AB_OBSERVATION",
            "Run scripts/run_runtime_touch_ab_cycle.sh before making another profile decision.",
            ["missing_candidate"],
        )
    candidate_summary = candidate_metrics(candidate)
    baseline_summary = candidate_metrics(baseline)
    profile = str(candidate_summary.get("profile") or "")
    signals = numeric_or_none(candidate_summary.get("signals")) or 0.0
    observed_fill_rate = (
        numeric_or_none(candidate_summary.get("observed_fill_rate")) or 0.0
    )
    baseline_fill_rate = (
        numeric_or_none(baseline_summary.get("observed_fill_rate")) or 0.0
    )
    fill_rate_gap = numeric_or_none(candidate_summary.get("fill_rate_gap")) or 0.0
    adverse_selection = numeric_or_none(candidate_summary.get("adverse_selection"))
    drawdown = numeric_or_none(candidate_summary.get("drawdown"))
    error_rate = numeric_or_none(candidate_summary.get("error_rate")) or 0.0
    failure_family = str(candidate_summary.get("dominant_failure_family") or "")
    touch_next_action = str(touch_diagnosis.get("next_action") or "")
    no_fill_future_touch_rate = (
        numeric_or_none(candidate_summary.get("no_fill_future_touch_rate")) or 0.0
    )

    if profile != "execution_probe_v12":
        return (
            "REGENERATE_AB_WITH_V12_CANDIDATE",
            "The A/B candidate must be execution_probe_v12 so at-touch runtime evidence is comparable.",
            [f"candidate_profile={profile}"],
        )
    if signals < thresholds.min_signals:
        return (
            "EXPAND_RUNTIME_TOUCH_UNIVERSE",
            "Regenerate the runtime-touch universe with more active assets before interpreting v12.",
            [f"signals={signals} below min_signals={thresholds.min_signals}"],
        )
    if error_rate > thresholds.max_error_rate or failure_family == "ERROR":
        return (
            "FIX_RISK_OR_EXECUTOR_ERRORS",
            "Do not tune quote policy until risk/executor ERROR reports are below threshold.",
            [
                f"error_rate={error_rate}",
                f"max_error_rate={thresholds.max_error_rate}",
                f"dominant_failure_family={failure_family}",
            ],
        )
    if observed_fill_rate <= 0:
        if touch_next_action == "CHANGE_MARKET_OR_TIMING_FILTERS" or (
            failure_family == "UNMATCHED" and no_fill_future_touch_rate <= 0
        ):
            return (
                "CHANGE_MARKET_OR_TIMING_FILTERS",
                "At-touch v12 still did not receive reachable touches; change market/timing selection.",
                [
                    "no_observed_fills",
                    f"dominant_failure_family={failure_family}",
                    f"no_fill_future_touch_rate={no_fill_future_touch_rate}",
                ],
            )
        return (
            "REPEAT_V12_WITH_LARGER_SAMPLE",
            "Repeat v12 longer before changing policy because failures are not isolated yet.",
            ["no_observed_fills", f"touch_next_action={touch_next_action}"],
        )
    if fill_rate_gap > thresholds.max_synthetic_observed_gap:
        return (
            "REDUCE_SYNTHETIC_DEPENDENCE",
            "Observed fills exist but synthetic optimism is too high for promotion.",
            [
                f"fill_rate_gap={fill_rate_gap}",
                f"max_gap={thresholds.max_synthetic_observed_gap}",
            ],
        )
    if (
        adverse_selection is not None
        and adverse_selection > thresholds.max_adverse_selection
    ) or (drawdown is not None and drawdown > thresholds.max_drawdown):
        return (
            "ADD_TOXICITY_OR_TIMING_FILTERS",
            "At-touch v12 improves execution but fails risk; add toxicity/timing filters before repeat.",
            [f"adverse_selection={adverse_selection}", f"drawdown={drawdown}"],
        )
    if observed_fill_rate <= baseline_fill_rate:
        return (
            "REJECT_V12_OR_CHANGE_UNIVERSE",
            "v12 did not improve observed fill-rate over the baseline on the same universe.",
            [
                f"baseline_observed_fill_rate={baseline_fill_rate}",
                f"candidate_observed_fill_rate={observed_fill_rate}",
            ],
        )
    return (
        "REPEAT_V12_LONGER",
        "v12 improved fill evidence without synthetic optimism or risk regression; repeat 90-120 minutes.",
        [
            f"baseline_observed_fill_rate={baseline_fill_rate}",
            f"candidate_observed_fill_rate={observed_fill_rate}",
        ],
    )


def summarize_observation(observation: dict[str, Any]) -> dict[str, object]:
    if not observation:
        return {}
    return {
        "run_id": observation.get("run_id"),
        "report_root": observation.get("report_root"),
        "market_asset_ids_count": observation.get("market_asset_ids_count"),
        "market_asset_ids_sha256": observation.get("market_asset_ids_sha256"),
        "capture_seconds": observation.get("capture_seconds"),
        **candidate_metrics(observation),
    }


def candidate_metrics(observation: dict[str, Any]) -> dict[str, object]:
    activity = typed_dict(observation.get("activity"))
    fills = typed_dict(observation.get("fills"))
    risk = typed_dict(observation.get("risk"))
    quote_policy = typed_dict(observation.get("quote_policy"))
    failure = typed_dict(observation.get("execution_failure_diagnostics"))
    failure_summary = typed_dict(failure.get("summary"))
    failure_counts = typed_dict(failure.get("counts"))
    return {
        "profile": observation.get("profile"),
        "signals": activity.get("signals") or failure_counts.get("signals"),
        "filled_signals": activity.get("filled_signals") or failure_counts.get("filled"),
        "observed_fill_rate": fills.get("observed_fill_rate"),
        "synthetic_fill_rate": fills.get("synthetic_fill_rate"),
        "fill_rate_gap": fills.get("fill_rate_gap"),
        "adverse_selection": risk.get("adverse_selection"),
        "drawdown": risk.get("drawdown"),
        "error_rate": failure_summary.get("error_rate"),
        "unmatched_rate": failure_summary.get("unmatched_rate"),
        "dominant_failure_family": failure_summary.get("dominant_failure_family"),
        "no_fill_future_touch_rate": quote_policy.get("no_fill_future_touch_rate"),
        "avg_required_quote_move": quote_policy.get("avg_required_quote_move"),
        "failure_recommended_next_action": failure.get("recommended_next_action"),
    }


def command_templates(recommendation: str) -> list[str]:
    if recommendation == "REPEAT_V12_LONGER":
        return [
            "PREDICTOR_STRATEGY_PROFILE=execution_probe_v12 scripts/run_runtime_touch_observation.sh --universe-selection <UNIVERSE_SELECTION_JSON> --duration-seconds 5400"
        ]
    if recommendation in {
        "CHANGE_MARKET_OR_TIMING_FILTERS",
        "EXPAND_RUNTIME_TOUCH_UNIVERSE",
        "REPEAT_AB_WITH_COMPARABLE_UNIVERSE",
        "RERANK_RUNTIME_TOUCH_UNIVERSE",
    }:
        return [
            "scripts/run_runtime_touch_ab_cycle.sh --print-plan",
            "scripts/run_runtime_touch_ab_cycle.sh --duration-seconds 3600 --universe-limit 20 --min-assets 2",
        ]
    if recommendation == "ADD_TOXICITY_OR_TIMING_FILTERS":
        return [
            "Generate a research-only toxicity/timing filter candidate, then repeat scripts/run_runtime_touch_ab_cycle.sh."
        ]
    if recommendation == "FIX_RISK_OR_EXECUTOR_ERRORS":
        return [
            "Inspect execution_failure_diagnostics.json error_diagnostics before changing quote or market selection."
        ]
    if recommendation == "FIX_PREDICTOR_DECISION_TRACE":
        return [
            "Inspect predictor:decisions:stream and python-service consumer logs before changing market selection."
        ]
    return []


def check_equals(
    check_name: str,
    metric_value: object,
    expected: object,
) -> dict[str, object]:
    return {
        "check_name": check_name,
        "status": "PASS" if metric_value == expected else "FAIL",
        "metric_value": metric_value,
        "threshold": expected,
    }


def check_at_least(
    check_name: str,
    metric_value: float | None,
    threshold: float,
) -> dict[str, object]:
    if metric_value is None:
        return check_result(check_name, "MISSING", None, threshold)
    return check_result(
        check_name,
        "PASS" if metric_value >= threshold else "FAIL",
        metric_value,
        threshold,
    )


def check_at_most(
    check_name: str,
    metric_value: float | None,
    threshold: float,
    *,
    required: bool = True,
) -> dict[str, object]:
    if metric_value is None:
        return check_result(check_name, "MISSING" if required else "PASS", None, threshold)
    return check_result(
        check_name,
        "PASS" if metric_value <= threshold else "FAIL",
        metric_value,
        threshold,
    )


def check_result(
    check_name: str,
    status: str,
    metric_value: object,
    threshold: object,
) -> dict[str, object]:
    return {
        "check_name": check_name,
        "status": status,
        "metric_value": metric_value,
        "threshold": threshold,
    }


def write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="runtime-touch-ab-decision")
    parser.add_argument("--profile-observation-comparison", type=Path)
    parser.add_argument("--touch-comparison", type=Path)
    parser.add_argument("--preflight-failure", type=Path)
    parser.add_argument("--signalability-gate-failure", type=Path)
    parser.add_argument("--failed-profile")
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.signalability_gate_failure:
        report = create_runtime_touch_ab_signalability_gate_failure_decision(
            read_json(args.signalability_gate_failure),
        )
    elif args.preflight_failure:
        if not args.failed_profile:
            raise SystemExit("--failed-profile is required with --preflight-failure")
        report = create_runtime_touch_ab_preflight_failure_decision(
            read_json(args.preflight_failure),
            failed_profile=str(args.failed_profile),
        )
    else:
        if not args.profile_observation_comparison or not args.touch_comparison:
            raise SystemExit(
                "--profile-observation-comparison and --touch-comparison are required"
            )
        report = create_runtime_touch_ab_decision(
            read_json(args.profile_observation_comparison),
            read_json(args.touch_comparison),
        )
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        write_json_atomic(args.output, report)
    print(payload, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
