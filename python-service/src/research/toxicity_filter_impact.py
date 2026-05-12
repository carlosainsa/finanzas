import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.research.profile_observation_comparison import (
    numeric_or_none,
    profile_observation,
    read_json,
    typed_dict,
)


REPORT_VERSION = "toxicity_filter_impact_v1"


def create_toxicity_filter_impact_report(
    report_root: Path,
    *,
    baseline_report_root: Path | None = None,
    output_path: Path | None = None,
) -> dict[str, object]:
    candidate = profile_observation(report_root)
    baseline = profile_observation(baseline_report_root) if baseline_report_root else None
    evidence = read_json(report_root / "real_dry_run_evidence.json")
    universe = read_universe_selection(evidence)
    toxicity_filter = typed_dict(universe.get("toxicity_filter"))
    blocklist_path = first_string(
        toxicity_filter.get("blocked_segments_path"),
        evidence.get("blocked_segments_path"),
    )
    blocked_segments = load_blocked_segments(blocklist_path)
    rejection = rejection_impact(
        read_json(report_root / "signal_rejection_diagnostics.json"),
        str(evidence.get("predictor_strategy_profile") or candidate.get("profile") or ""),
    )
    candidate_metrics = observation_metrics(candidate)
    baseline_metrics = observation_metrics(baseline) if baseline is not None else {}
    deltas = metric_deltas(baseline_metrics, candidate_metrics)
    decision = impact_decision(
        toxicity_filter=toxicity_filter,
        candidate_metrics=candidate_metrics,
        baseline_metrics=baseline_metrics,
        rejection=rejection,
    )
    payload: dict[str, object] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_toxicity_filter_impact_only",
        "report_root": str(report_root),
        "baseline_report_root": str(baseline_report_root) if baseline_report_root else None,
        "filter": {
            "enabled": bool(toxicity_filter.get("enabled")),
            "mode": toxicity_filter.get("mode"),
            "blocked_segments_path": blocklist_path,
            "blocked_segments": len(blocked_segments),
            "quality_rows": toxicity_filter.get("quality_rows"),
            "filtered_count": toxicity_filter.get("filtered_count"),
        },
        "runtime_rejection_impact": rejection,
        "candidate_metrics": candidate_metrics,
        "baseline_metrics": baseline_metrics,
        "metric_deltas": deltas,
        "decision": decision,
        "outputs": ["toxicity_filter_impact.json"],
    }
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return payload


def read_universe_selection(evidence: dict[str, object]) -> dict[str, object]:
    path_value = evidence.get("execution_probe_universe_selection_path")
    if not isinstance(path_value, str) or not path_value:
        return {}
    return read_json(Path(path_value))


def load_blocked_segments(path_value: str | None) -> list[dict[str, Any]]:
    if not path_value:
        return []
    payload = read_json(Path(path_value))
    segments = payload.get("segments")
    return [item for item in segments if isinstance(item, dict)] if isinstance(segments, list) else []


def rejection_impact(report: dict[str, object], profile: str) -> dict[str, object]:
    summary = report.get("summary")
    rows = [item for item in summary if isinstance(item, dict)] if isinstance(summary, list) else []
    row = next((item for item in rows if item.get("profile") == profile), {})
    rejection_counts = typed_dict(row.get("rejection_counts"))
    snapshots = int_value(row.get("snapshots"))
    accepted = int_value(row.get("accepted"))
    blocked = int_value(rejection_counts.get("blocked_segment"))
    return {
        "profile": profile or None,
        "snapshots": snapshots,
        "accepted": accepted,
        "blocked_segment_rejections": blocked,
        "acceptance_rate": accepted / snapshots if snapshots > 0 else None,
        "blocked_segment_rate": blocked / snapshots if snapshots > 0 else None,
        "rejection_counts": rejection_counts,
    }


def observation_metrics(observation: dict[str, object] | None) -> dict[str, object]:
    if observation is None:
        return {}
    activity = typed_dict(observation.get("activity"))
    fills = typed_dict(observation.get("fills"))
    risk = typed_dict(observation.get("risk"))
    fill_toxicity = typed_dict(observation.get("fill_toxicity"))
    return {
        "signals": numeric_or_none(activity.get("signals")),
        "filled_signals": numeric_or_none(activity.get("filled_signals")),
        "observed_fill_rate": numeric_or_none(fills.get("observed_fill_rate")),
        "synthetic_fill_rate": numeric_or_none(fills.get("synthetic_fill_rate")),
        "fill_rate_gap": numeric_or_none(fills.get("fill_rate_gap")),
        "realized_edge": numeric_or_none(risk.get("realized_edge")),
        "adverse_selection": numeric_or_none(risk.get("adverse_selection")),
        "drawdown": numeric_or_none(risk.get("drawdown")),
        "fill_toxicity_adverse_30s_rate": numeric_or_none(
            fill_toxicity.get("adverse_30s_rate")
        ),
        "fill_toxicity_rejected_segments": numeric_or_none(
            fill_toxicity.get("rejected_segments")
        ),
    }


def metric_deltas(
    baseline_metrics: dict[str, object],
    candidate_metrics: dict[str, object],
) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for metric in sorted(set(baseline_metrics) | set(candidate_metrics)):
        baseline = numeric_or_none(baseline_metrics.get(metric))
        candidate = numeric_or_none(candidate_metrics.get(metric))
        output.append(
            {
                "metric": metric,
                "baseline": baseline,
                "candidate": candidate,
                "delta": (
                    candidate - baseline
                    if baseline is not None and candidate is not None
                    else None
                ),
            }
        )
    return output


def impact_decision(
    *,
    toxicity_filter: dict[str, Any],
    candidate_metrics: dict[str, object],
    baseline_metrics: dict[str, object],
    rejection: dict[str, object],
) -> dict[str, object]:
    if toxicity_filter.get("enabled") is not True:
        return {
            "recommendation": "NOT_APPLICABLE",
            "reason": "toxicity_filter_disabled",
            "next_step": "Run execution_probe_v9 with --toxicity-filter segment before evaluating impact.",
        }
    if not baseline_metrics:
        return {
            "recommendation": "NEED_BASELINE_COMPARISON",
            "reason": "missing_baseline_report_root",
            "next_step": "Compare this run against the expanded unfiltered v9 baseline before keeping or relaxing the filter.",
        }
    candidate_signals = numeric_or_none(candidate_metrics.get("signals")) or 0.0
    filtered_count = numeric_or_none(toxicity_filter.get("filtered_count")) or 0.0
    snapshots = numeric_or_none(rejection.get("snapshots")) or 0.0
    accepted = numeric_or_none(rejection.get("accepted")) or 0.0
    blocked_rejections = numeric_or_none(rejection.get("blocked_segment_rejections")) or 0.0
    blocked_rate = numeric_or_none(rejection.get("blocked_segment_rate")) or 0.0
    baseline_fill = numeric_or_none(baseline_metrics.get("observed_fill_rate"))
    candidate_fill = numeric_or_none(candidate_metrics.get("observed_fill_rate"))
    baseline_adverse = numeric_or_none(baseline_metrics.get("adverse_selection"))
    candidate_adverse = numeric_or_none(candidate_metrics.get("adverse_selection"))
    fill_delta = (
        candidate_fill - baseline_fill
        if candidate_fill is not None and baseline_fill is not None
        else None
    )
    adverse_delta = (
        candidate_adverse - baseline_adverse
        if candidate_adverse is not None and baseline_adverse is not None
        else None
    )
    if (
        filtered_count > 0
        and snapshots > 0
        and accepted > 0
        and candidate_signals > 0
        and blocked_rejections <= 0
    ):
        return {
            "recommendation": "REPAIR_FILTER_CONTRACT",
            "reason": "blocked_segments_never_matched_runtime",
            "next_step": "Fix the blocklist matching scope, regenerate the toxicity filter, and rerun a fixed-universe observation before extending runtime.",
        }
    if candidate_signals <= 0 or blocked_rate >= 0.95:
        return {
            "recommendation": "RELAX_FILTER",
            "reason": "toxicity_filter_killed_activity",
            "next_step": "Relax min_toxicity_filled_events or convert hard blocks into quality-score penalties.",
        }
    if adverse_delta is not None and adverse_delta <= -0.05 and (
        fill_delta is None or fill_delta >= -0.005
    ):
        return {
            "recommendation": "KEEP_FILTER",
            "reason": "toxicity_filter_reduced_adverse_selection_without_material_fill_loss",
            "next_step": "Repeat a longer 90-120 minute observation before promotion discussion.",
        }
    if adverse_delta is not None and adverse_delta >= 0 and (
        fill_delta is None or fill_delta <= 0
    ):
        return {
            "recommendation": "REJECT_FILTER",
            "reason": "toxicity_filter_did_not_reduce_adverse_selection",
            "next_step": "Move to feature/model redesign instead of repeating this filter unchanged.",
        }
    return {
        "recommendation": "NEED_MORE_DATA",
        "reason": "mixed_or_insufficient_filter_impact",
        "next_step": "Repeat the same filter on a longer fixed universe before changing quote aggression.",
    }


def first_string(*values: object) -> str | None:
    for value in values:
        if isinstance(value, str) and value:
            return value
    return None


def int_value(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="toxicity-filter-impact")
    parser.add_argument("--report-root", type=Path, required=True)
    parser.add_argument("--baseline-report-root", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    output = args.output or args.report_root / "toxicity_filter_impact.json"
    payload = create_toxicity_filter_impact_report(
        args.report_root,
        baseline_report_root=args.baseline_report_root,
        output_path=output,
    )
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
