import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.research.compare_runs import compare_report_roots, compare_runs


DECISION_REPORT_VERSION = "research_promotion_decision_v1"


@dataclass(frozen=True)
class PromotionThresholds:
    min_realized_edge_delta: float = 0.0
    min_fill_rate_delta: float = -0.10
    max_drawdown_delta: float = 0.0
    max_simulator_delta_delta: float = 0.0
    max_stale_data_rate_delta: float = 0.01
    max_reconciliation_divergence_delta: float = 0.0
    max_newly_blocked_segments: int = 0
    min_shared_segments: int = 1


def decide_from_report(
    report: dict[str, object],
    thresholds: PromotionThresholds = PromotionThresholds(),
) -> dict[str, object]:
    comparison = typed_dict(report.get("comparison"))
    deltas = deltas_by_metric(comparison)
    checks = [
        check_candidate_absolute_gate(report),
        check_comparison_verdict(comparison),
        check_delta_at_least(
            deltas,
            "realized_edge",
            thresholds.min_realized_edge_delta,
            required=True,
        ),
        check_delta_at_least(
            deltas,
            "fill_rate",
            thresholds.min_fill_rate_delta,
            required=True,
        ),
        check_delta_at_most(
            deltas,
            "drawdown",
            thresholds.max_drawdown_delta,
            required=True,
        ),
        check_delta_at_most(
            deltas,
            "max_abs_simulator_fill_rate_delta",
            thresholds.max_simulator_delta_delta,
            required=True,
        ),
        check_delta_at_most(
            deltas,
            "stale_data_rate",
            thresholds.max_stale_data_rate_delta,
            required=True,
        ),
        check_delta_at_most(
            deltas,
            "reconciliation_divergence_rate",
            thresholds.max_reconciliation_divergence_delta,
            required=True,
        ),
        check_blocked_segments(comparison, thresholds.max_newly_blocked_segments),
        check_segment_comparability(comparison, thresholds.min_shared_segments),
    ]
    failed = [item for item in checks if item["status"] == "FAIL"]
    missing = [item for item in checks if item["status"] == "MISSING"]
    decision = "PROMOTE"
    if failed:
        decision = "REJECT"
    elif missing:
        decision = "NEED_MORE_DATA"
    return {
        "report_version": DECISION_REPORT_VERSION,
        "decision": decision,
        "baseline_run_id": comparison.get("baseline_run_id"),
        "candidate_run_id": comparison.get("candidate_run_id"),
        "checks": checks,
        "summary": {
            "passed": sum(1 for item in checks if item["status"] == "PASS"),
            "failed": len(failed),
            "missing": len(missing),
        },
    }


def deltas_by_metric(comparison: dict[str, Any]) -> dict[str, dict[str, Any]]:
    metric_deltas = comparison.get("metric_deltas")
    if not isinstance(metric_deltas, list):
        return {}
    return {
        str(item["metric"]): item
        for item in metric_deltas
        if isinstance(item, dict) and "metric" in item
    }


def check_delta_at_least(
    deltas: dict[str, dict[str, Any]],
    metric: str,
    threshold: float,
    required: bool,
) -> dict[str, object]:
    delta = metric_delta(deltas, metric)
    if delta is None:
        return check_result(metric, "MISSING" if required else "PASS", None, threshold)
    return check_result(metric, "PASS" if delta >= threshold else "FAIL", delta, threshold)


def check_delta_at_most(
    deltas: dict[str, dict[str, Any]],
    metric: str,
    threshold: float,
    required: bool,
) -> dict[str, object]:
    delta = metric_delta(deltas, metric)
    if delta is None:
        return check_result(metric, "MISSING" if required else "PASS", None, threshold)
    return check_result(metric, "PASS" if delta <= threshold else "FAIL", delta, threshold)


def metric_delta(deltas: dict[str, dict[str, Any]], metric: str) -> float | None:
    value = deltas.get(metric, {}).get("delta")
    return float(value) if isinstance(value, (int, float)) else None


def check_blocked_segments(
    comparison: dict[str, Any],
    max_newly_blocked_segments: int,
) -> dict[str, object]:
    changes = typed_dict(comparison.get("blocked_segment_changes"))
    value = numeric_or_none(changes.get("newly_blocked_count"))
    if value is None:
        return check_result(
            "newly_blocked_segments",
            "MISSING",
            None,
            float(max_newly_blocked_segments),
        )
    return check_result(
        "newly_blocked_segments",
        "PASS" if value <= max_newly_blocked_segments else "FAIL",
        value,
        float(max_newly_blocked_segments),
    )


def check_candidate_absolute_gate(report: dict[str, object]) -> dict[str, object]:
    candidate = typed_dict(report.get("candidate"))
    passed = candidate.get("passed")
    if not isinstance(passed, bool):
        return {
            "check_name": "candidate_absolute_gate_passed",
            "status": "MISSING",
            "metric_value": None,
            "threshold": 1.0,
            "message": "candidate passed flag missing",
        }
    return {
        "check_name": "candidate_absolute_gate_passed",
        "status": "PASS" if passed else "FAIL",
        "metric_value": 1.0 if passed else 0.0,
        "threshold": 1.0,
    }


def check_comparison_verdict(comparison: dict[str, Any]) -> dict[str, object]:
    verdict = comparison.get("verdict")
    if verdict == "candidate_improved":
        status = "PASS"
    elif verdict == "no_comparable":
        status = "MISSING"
    else:
        status = "FAIL"
    return {
        "check_name": "comparison_verdict",
        "status": status,
        "metric_value": str(verdict) if verdict is not None else None,
        "threshold": "candidate_improved",
    }


def check_segment_comparability(
    comparison: dict[str, Any],
    min_shared_segments: int,
) -> dict[str, object]:
    comparability = typed_dict(comparison.get("segment_comparability"))
    summary = typed_dict(comparison.get("segment_change_summary"))
    shared_segments = numeric_or_none(summary.get("shared_segments"))
    if comparability.get("status") != "comparable":
        return {
            "check_name": "segment_comparability",
            "status": "MISSING",
            "metric_value": shared_segments,
            "threshold": float(min_shared_segments),
            "message": str(comparability.get("reason") or "not_comparable"),
        }
    status = (
        "PASS"
        if shared_segments is not None and shared_segments >= min_shared_segments
        else "MISSING"
    )
    return check_result(
        "segment_comparability",
        status,
        shared_segments,
        float(min_shared_segments),
    )


def check_result(
    name: str,
    status: str,
    value: float | None,
    threshold: float,
) -> dict[str, object]:
    return {
        "check_name": name,
        "status": status,
        "metric_value": value,
        "threshold": threshold,
    }


def numeric_or_none(value: object) -> float | None:
    return float(value) if isinstance(value, (int, float)) else None


def typed_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="research-promotion-decision")
    parser.add_argument("--manifest-root", default="data_lake/research_runs")
    parser.add_argument("--baseline-run-id")
    parser.add_argument("--candidate-run-id")
    parser.add_argument("--baseline-report-root")
    parser.add_argument("--candidate-report-root")
    parser.add_argument("--output")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    if args.baseline_report_root or args.candidate_report_root:
        if not args.baseline_report_root or not args.candidate_report_root:
            raise SystemExit("--baseline-report-root and --candidate-report-root must be used together")
        comparison = compare_report_roots(
            Path(args.baseline_report_root),
            Path(args.candidate_report_root),
        )
    else:
        comparison = compare_runs(
            Path(args.manifest_root),
            baseline_run_id=args.baseline_run_id,
            candidate_run_id=args.candidate_run_id,
        )
    decision = decide_from_report(comparison)
    payload = json.dumps(decision, indent=2, sort_keys=True) + "\n"
    if args.output:
        Path(args.output).write_text(payload, encoding="utf-8")
    if args.json or not args.output:
        print(payload, end="")
    return 0 if decision["decision"] == "PROMOTE" else 2


if __name__ == "__main__":
    raise SystemExit(main())
