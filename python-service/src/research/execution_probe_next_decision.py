import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPORT_VERSION = "execution_probe_next_decision_v1"
SUPPORTED_CANDIDATE_PROFILES = {"execution_probe_v6", "execution_probe_v7"}


@dataclass(frozen=True)
class ExecutionProbeDecisionThresholds:
    min_signals: int = 50
    min_filled_signals: int = 3
    min_observed_fill_rate: float = 0.01
    max_synthetic_observed_gap: float = 0.05
    max_adverse_selection: float = 0.0
    max_drawdown: float = 0.0
    min_no_fill_future_touch_rate: float = 0.10
    min_market_timing_filter_fill_rate_lift: float = 0.005


def decide_execution_probe_next_step(
    comparison: dict[str, object],
    thresholds: ExecutionProbeDecisionThresholds = ExecutionProbeDecisionThresholds(),
) -> dict[str, object]:
    observations = list_of_dicts(comparison.get("observations"))
    candidate = observations[-1] if observations else {}
    baseline = observations[-2] if len(observations) >= 2 else {}
    checks = [
        check_equals(
            "source_can_execute_trades_false",
            comparison.get("can_execute_trades"),
            False,
        ),
        *build_checks(candidate, thresholds),
    ]
    failed = [item for item in checks if item["status"] == "FAIL"]
    missing = [item for item in checks if item["status"] == "MISSING"]
    market_timing_filter_decision = decide_market_timing_filter(
        baseline,
        candidate,
        thresholds,
    )
    recommendation, next_step, rationale = classify_next_step(
        candidate,
        failed=failed,
        missing=missing,
        thresholds=thresholds,
    )
    return {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "can_promote_live": False,
        "decision_policy": "offline_execution_probe_next_step_only",
        "source_report_version": comparison.get("report_version"),
        "baseline": observation_summary(baseline),
        "candidate": observation_summary(candidate),
        "recommendation": recommendation,
        "next_step": next_step,
        "rationale": rationale,
        "market_timing_filter_decision": market_timing_filter_decision,
        "checks": checks,
        "summary": {
            "passed": sum(1 for item in checks if item["status"] == "PASS"),
            "failed": len(failed),
            "missing": len(missing),
        },
        "next_command_templates": command_templates(
            recommendation,
            str(candidate.get("profile") or ""),
        ),
    }


def build_checks(
    candidate: dict[str, Any],
    thresholds: ExecutionProbeDecisionThresholds,
) -> list[dict[str, object]]:
    activity = typed_dict(candidate.get("activity"))
    fills = typed_dict(candidate.get("fills"))
    risk = typed_dict(candidate.get("risk"))
    quote_policy = typed_dict(candidate.get("quote_policy"))
    return [
        check_equals(
            "candidate_profile_is_supported_execution_probe",
            candidate.get("profile"),
            SUPPORTED_CANDIDATE_PROFILES,
        ),
        check_at_least(
            "minimum_signal_sample",
            numeric_or_none(activity.get("signals")),
            float(thresholds.min_signals),
        ),
        check_at_least(
            "minimum_filled_signal_sample",
            numeric_or_none(activity.get("filled_signals")),
            float(thresholds.min_filled_signals),
        ),
        check_at_least(
            "minimum_observed_fill_rate",
            numeric_or_none(fills.get("observed_fill_rate")),
            thresholds.min_observed_fill_rate,
        ),
        check_at_most(
            "synthetic_observed_gap",
            numeric_or_none(fills.get("fill_rate_gap")),
            thresholds.max_synthetic_observed_gap,
        ),
        check_at_most(
            "adverse_selection",
            numeric_or_none(risk.get("adverse_selection")),
            thresholds.max_adverse_selection,
        ),
        check_at_most(
            "drawdown",
            numeric_or_none(risk.get("drawdown")),
            thresholds.max_drawdown,
        ),
        check_at_least(
            "no_fill_future_touch_rate",
            numeric_or_none(quote_policy.get("no_fill_future_touch_rate")),
            thresholds.min_no_fill_future_touch_rate,
            required=False,
        ),
    ]


def classify_next_step(
    candidate: dict[str, Any],
    *,
    failed: list[dict[str, object]],
    missing: list[dict[str, object]],
    thresholds: ExecutionProbeDecisionThresholds,
) -> tuple[str, str, list[str]]:
    if not candidate:
        return (
            "WAIT_FOR_OBSERVATION",
            "Run execution_probe_v6 or execution_probe_v7 and generate profile_observation_comparison.json.",
            ["no_candidate_observation"],
        )
    profile = candidate.get("profile")
    if profile not in SUPPORTED_CANDIDATE_PROFILES:
        return (
            "WAIT_FOR_EXECUTION_PROBE_OBSERVATION",
            "Compare a completed execution_probe_v6 or execution_probe_v7 report before tuning.",
            [f"candidate_profile={profile}"],
        )

    activity = typed_dict(candidate.get("activity"))
    fills = typed_dict(candidate.get("fills"))
    risk = typed_dict(candidate.get("risk"))
    quote_policy = typed_dict(candidate.get("quote_policy"))
    signals = numeric_or_none(activity.get("signals")) or 0.0
    filled = numeric_or_none(activity.get("filled_signals")) or 0.0
    observed_fill_rate = numeric_or_none(fills.get("observed_fill_rate")) or 0.0
    fill_rate_gap = numeric_or_none(fills.get("fill_rate_gap")) or 0.0
    adverse_selection = numeric_or_none(risk.get("adverse_selection"))
    drawdown = numeric_or_none(risk.get("drawdown"))
    no_fill_future_touch_rate = numeric_or_none(
        quote_policy.get("no_fill_future_touch_rate")
    )

    if signals < thresholds.min_signals:
        return (
            "REPEAT_V6_WITH_LARGER_SAMPLE",
            f"Repeat {profile} with a longer duration or wider universe before changing policy.",
            [f"signals={signals} below min_signals={thresholds.min_signals}"],
        )
    if filled <= 0 or observed_fill_rate <= 0:
        if (
            no_fill_future_touch_rate is not None
            and no_fill_future_touch_rate < thresholds.min_no_fill_future_touch_rate
        ):
            return (
                "CHANGE_MARKET_OR_TIMING_FILTERS",
                f"Keep {profile} research-only and retune market/timing selection before quote aggression.",
                [
                    "no_observed_fills",
                    (
                        "no_fill_future_touch_rate="
                        f"{no_fill_future_touch_rate} below "
                        f"{thresholds.min_no_fill_future_touch_rate}"
                    ),
                ],
            )
        return (
            "RELAX_SIGNAL_FILTERS",
            f"Relax min_confidence or min_signal_interval_ms and repeat {profile}.",
            ["no_observed_fills", "sample_is_large_enough"],
        )
    if fill_rate_gap > thresholds.max_synthetic_observed_gap:
        if profile == "execution_probe_v7":
            return (
                "HOLD_RESEARCH",
                "Do not add another quote profile until synthetic-only evidence is guarded or excluded.",
                [
                    (
                        "fill_rate_gap="
                        f"{fill_rate_gap} above {thresholds.max_synthetic_observed_gap}"
                    ),
                    "execution_probe_v7_already_less_aggressive",
                ],
            )
        return (
            "CREATE_V7_LESS_AGGRESSIVE_QUOTE",
            "Create v7 with lower quote aggressiveness or stricter synthetic-fill guards.",
            [
                (
                    "fill_rate_gap="
                    f"{fill_rate_gap} above {thresholds.max_synthetic_observed_gap}"
                )
            ],
        )
    if adverse_selection is None or drawdown is None:
        return (
            "REPEAT_V6_WITH_RISK_METRICS",
            f"Repeat {profile} until adverse selection and drawdown are measurable.",
            ["risk_metrics_missing"],
        )
    if adverse_selection > thresholds.max_adverse_selection or drawdown > thresholds.max_drawdown:
        return (
            "ADD_MARKET_SIDE_RISK_FILTERS",
            f"Keep the {profile} quote policy but add market/side filters before repeating.",
            [
                f"adverse_selection={adverse_selection}",
                f"drawdown={drawdown}",
            ],
        )
    if missing:
        return (
            "REPEAT_V6_WITH_COMPLETE_ARTIFACTS",
            f"Repeat {profile} or regenerate diagnostics until all required artifacts are present.",
            [f"missing_checks={len(missing)}"],
        )
    if failed:
        return (
            "HOLD_RESEARCH",
            "Do not create a new profile until failed checks are resolved.",
            [f"failed_checks={len(failed)}"],
        )
    return (
        "REPEAT_EXECUTION_PROBE_LONGER",
        f"Repeat {profile} for 90 minutes before any promotion decision.",
        [f"{profile}_has_fills_without_synthetic_optimism_or_risk_regression"],
    )


def observation_summary(observation: dict[str, Any]) -> dict[str, object]:
    activity = typed_dict(observation.get("activity"))
    fills = typed_dict(observation.get("fills"))
    risk = typed_dict(observation.get("risk"))
    quote_policy = typed_dict(observation.get("quote_policy"))
    return {
        "run_id": observation.get("run_id"),
        "report_root": observation.get("report_root"),
        "profile": observation.get("profile"),
        "quote_placement": observation.get("quote_placement"),
        "market_timing_selection": observation.get("market_timing_selection"),
        "signals": activity.get("signals"),
        "filled_signals": activity.get("filled_signals"),
        "observed_fill_rate": fills.get("observed_fill_rate"),
        "synthetic_fill_rate": fills.get("synthetic_fill_rate"),
        "fill_rate_gap": fills.get("fill_rate_gap"),
        "adverse_selection": risk.get("adverse_selection"),
        "drawdown": risk.get("drawdown"),
        "no_fill_future_touch_rate": quote_policy.get("no_fill_future_touch_rate"),
        "avg_required_quote_move": quote_policy.get("avg_required_quote_move"),
    }


def decide_market_timing_filter(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    thresholds: ExecutionProbeDecisionThresholds,
) -> dict[str, object]:
    market_timing_selection = typed_dict(candidate.get("market_timing_selection"))
    candidate_filter = market_timing_selection.get("market_timing_filter")
    if candidate_filter != "future_touch":
        return {
            "decision": "NOT_EVALUATED",
            "reason": "candidate_did_not_use_market_timing_filter",
            "can_execute_trades": False,
        }

    baseline_fills = typed_dict(baseline.get("fills"))
    candidate_fills = typed_dict(candidate.get("fills"))
    baseline_risk = typed_dict(baseline.get("risk"))
    candidate_risk = typed_dict(candidate.get("risk"))
    candidate_activity = typed_dict(candidate.get("activity"))
    baseline_fill_rate = numeric_or_none(baseline_fills.get("observed_fill_rate"))
    candidate_fill_rate = numeric_or_none(candidate_fills.get("observed_fill_rate"))
    fill_rate_gap = numeric_or_none(candidate_fills.get("fill_rate_gap"))
    adverse_selection = numeric_or_none(candidate_risk.get("adverse_selection"))
    drawdown = numeric_or_none(candidate_risk.get("drawdown"))
    signals = numeric_or_none(candidate_activity.get("signals")) or 0.0
    fill_rate_lift = (
        candidate_fill_rate - baseline_fill_rate
        if baseline_fill_rate is not None and candidate_fill_rate is not None
        else None
    )
    checks = [
        check_at_least(
            "market_timing_minimum_signal_sample",
            signals,
            float(thresholds.min_signals),
        ),
        check_at_least(
            "market_timing_fill_rate_lift",
            fill_rate_lift,
            thresholds.min_market_timing_filter_fill_rate_lift,
        ),
        check_at_most(
            "market_timing_synthetic_observed_gap",
            fill_rate_gap,
            thresholds.max_synthetic_observed_gap,
        ),
        check_at_most(
            "market_timing_adverse_selection",
            adverse_selection,
            thresholds.max_adverse_selection,
            required=False,
        ),
        check_at_most(
            "market_timing_drawdown",
            drawdown,
            thresholds.max_drawdown,
            required=False,
        ),
    ]
    failed = [item for item in checks if item["status"] == "FAIL"]
    missing = [item for item in checks if item["status"] == "MISSING"]
    if missing:
        decision = "REPEAT_WITH_COMPLETE_FILTER_EVIDENCE"
        reason = f"missing_checks={len(missing)}"
    elif not failed:
        decision = "KEEP_MARKET_TIMING_FILTER"
        reason = "filter_improved_observed_fill_rate_without_synthetic_or_risk_regression"
    elif candidate_fill_rate is not None and candidate_fill_rate <= 0:
        decision = "RELAX_MARKET_TIMING_FILTER"
        reason = "filtered_universe_still_has_no_observed_fills"
    else:
        decision = "REJECT_MARKET_TIMING_FILTER"
        reason = f"failed_checks={len(failed)}"
    return {
        "decision": decision,
        "reason": reason,
        "can_execute_trades": False,
        "candidate_filter": candidate_filter,
        "selection": market_timing_selection,
        "next_cycle": market_timing_next_cycle(decision, market_timing_selection),
        "baseline_observed_fill_rate": baseline_fill_rate,
        "candidate_observed_fill_rate": candidate_fill_rate,
        "fill_rate_lift": fill_rate_lift,
        "baseline_adverse_selection": numeric_or_none(
            baseline_risk.get("adverse_selection")
        ),
        "candidate_adverse_selection": adverse_selection,
        "checks": checks,
    }


def market_timing_next_cycle(
    decision: str,
    market_timing_selection: dict[str, Any],
) -> dict[str, object]:
    min_future_touch_rate = numeric_or_none(
        market_timing_selection.get("min_future_touch_rate")
    )
    min_timing_signals = numeric_or_none(market_timing_selection.get("min_timing_signals"))
    min_avg_opportunity_spread = numeric_or_none(
        market_timing_selection.get("min_avg_opportunity_spread")
    )
    if decision == "RELAX_MARKET_TIMING_FILTER":
        min_future_touch_rate = (
            min_future_touch_rate / 2 if min_future_touch_rate is not None else 0.05
        )
        min_avg_opportunity_spread = (
            min_avg_opportunity_spread / 2
            if min_avg_opportunity_spread is not None
            else 0.005
        )
    return {
        "script": "scripts/run_execution_probe_v7_cycle.sh",
        "args": {
            "--market-timing-filter": "future_touch",
            "--min-future-touch-rate": format_number(
                min_future_touch_rate if min_future_touch_rate is not None else 0.10
            ),
            "--min-timing-signals": format_number(
                min_timing_signals if min_timing_signals is not None else 5
            ),
            "--min-avg-opportunity-spread": format_number(
                min_avg_opportunity_spread
                if min_avg_opportunity_spread is not None
                else 0.01
            ),
        },
    }


def format_number(value: float) -> str:
    return f"{value:.6f}".rstrip("0").rstrip(".")


def command_templates(recommendation: str, candidate_profile: str) -> list[str]:
    if recommendation == "REPEAT_EXECUTION_PROBE_LONGER":
        if candidate_profile == "execution_probe_v7":
            return [
                "scripts/run_execution_probe_v7_observation.sh --duration-seconds 5400"
            ]
        return [
            "scripts/run_execution_probe_v6_observation.sh --duration-seconds 5400"
        ]
    if recommendation in {
        "RELAX_SIGNAL_FILTERS",
        "REPEAT_V6_WITH_LARGER_SAMPLE",
        "REPEAT_V6_WITH_COMPLETE_ARTIFACTS",
        "REPEAT_V6_WITH_RISK_METRICS",
    }:
        return [
            "scripts/prepare_execution_probe_cycle.sh --universe-duckdb <RESEARCH_DUCKDB> --baseline-report-root <BASELINE_REPORT_ROOT> --duration-seconds 5400",
            "scripts/run_execution_probe_v6_observation.sh --universe-selection <UNIVERSE_SELECTION_JSON> --duration-seconds 5400",
        ]
    if recommendation == "CHANGE_MARKET_OR_TIMING_FILTERS":
        cycle_script = (
            "scripts/run_execution_probe_v7_cycle.sh"
            if candidate_profile == "execution_probe_v7"
            else "scripts/run_execution_probe_v6_cycle.sh"
        )
        return [
            (
                f"{cycle_script} --universe-duckdb <RESEARCH_DUCKDB> "
                "--baseline-report-root <BASELINE_REPORT_ROOT> "
                "--market-timing-filter future_touch "
                "--min-future-touch-rate 0.10 "
                "--min-timing-signals 5 "
                "--duration-seconds 5400"
            )
        ]
    if recommendation == "CREATE_V7_LESS_AGGRESSIVE_QUOTE":
        return [
            "Create execution_probe_v7 as dry_run-only with lower quote aggressiveness, then add a dedicated observation script."
        ]
    if recommendation == "ADD_MARKET_SIDE_RISK_FILTERS":
        return [
            "Generate a research-only market/side filter candidate and repeat execution_probe_v6 before changing quote policy."
        ]
    return []


def check_equals(
    check_name: str,
    metric_value: object,
    expected: object,
) -> dict[str, object]:
    if metric_value is None:
        return check_result(check_name, "MISSING", None, expected)
    if isinstance(expected, set):
        return check_result(
            check_name,
            "PASS" if metric_value in expected else "FAIL",
            metric_value,
            sorted(str(item) for item in expected),
        )
    return check_result(
        check_name,
        "PASS" if metric_value == expected else "FAIL",
        metric_value,
        expected,
    )


def check_at_least(
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


def read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def typed_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def list_of_dicts(value: object) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def numeric_or_none(value: object) -> float | None:
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return None


def main() -> int:
    parser = argparse.ArgumentParser(prog="execution-probe-next-decision")
    parser.add_argument("--comparison", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    report = decide_execution_probe_next_step(read_json(args.comparison))
    if args.output:
        write_json_atomic(args.output, report)
    if args.json or not args.output:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
