import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd  # type: ignore[import-untyped]


REPORT_VERSION = "signal_activity_audit_v1"


def create_signal_activity_audit(
    baseline_report_root: Path,
    candidate_report_root: Path,
    output_path: Path | None = None,
) -> dict[str, object]:
    baseline = run_activity(baseline_report_root)
    candidate = run_activity(candidate_report_root)
    report: dict[str, object] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_diagnostics_only",
        "baseline": baseline,
        "candidate": candidate,
        "comparison": compare_activity(baseline, candidate),
    }
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    return report


def run_activity(report_root: Path) -> dict[str, object]:
    summary = read_json(report_root / "research_summary.json")
    manifest = read_json(report_root / "research_manifest.json")
    data_lake = typed_dict(summary.get("data_lake"))
    promotion = typed_dict(summary.get("pre_live_promotion"))
    promotion_metrics = typed_dict(promotion.get("metrics"))
    baseline = typed_dict(summary.get("baseline"))
    baseline_counts = typed_dict(baseline.get("counts"))
    baseline_filter_counts = baseline_filter_pass_counts(report_root)
    signal_rejection = read_json(report_root / "signal_rejection_diagnostics.json")
    return {
        "run_id": manifest.get("run_id") or report_root.name,
        "report_root": str(report_root),
        "signals": number(data_lake.get("signals")),
        "execution_reports": number(data_lake.get("execution_reports")),
        "orderbook_snapshots": number(data_lake.get("orderbook_snapshots")),
        "capture_duration_ms": number(promotion_metrics.get("capture_duration_ms")),
        "signals_per_snapshot": rate(
            number(data_lake.get("signals")), number(data_lake.get("orderbook_snapshots"))
        ),
        "signals_per_minute": per_minute(
            number(data_lake.get("signals")),
            number(promotion_metrics.get("capture_duration_ms")),
        ),
        "baseline_signals": number(baseline_counts.get("baseline_signals")),
        "baseline_filter_counts": baseline_filter_counts,
        "signal_rejection_profile_comparison": signal_rejection.get(
            "profile_comparison"
        ),
        "signal_rejection_summary": signal_rejection.get("summary"),
    }


def compare_activity(
    baseline: dict[str, object], candidate: dict[str, object]
) -> dict[str, object]:
    baseline_signals = number(baseline.get("signals"))
    candidate_signals = number(candidate.get("signals"))
    baseline_rate = number(baseline.get("signals_per_snapshot"))
    candidate_rate = number(candidate.get("signals_per_snapshot"))
    reason = classify_reason(baseline, candidate)
    return {
        "signals_delta": delta(baseline_signals, candidate_signals),
        "signals_per_snapshot_delta": delta(baseline_rate, candidate_rate),
        "candidate_less_active": (
            candidate_signals is not None
            and baseline_signals is not None
            and candidate_signals < baseline_signals
        ),
        "primary_reason": reason,
        "baseline_filter_deltas": filter_deltas(
            typed_dict(baseline.get("baseline_filter_counts")),
            typed_dict(candidate.get("baseline_filter_counts")),
        ),
    }


def classify_reason(
    baseline: dict[str, object], candidate: dict[str, object]
) -> str:
    rejection_comparison = typed_dict(
        candidate.get("signal_rejection_profile_comparison")
    )
    if rejection_comparison.get("candidate_less_active") is True:
        primary_gap = rejection_comparison.get("primary_gap_reason")
        if isinstance(primary_gap, str) and primary_gap:
            return f"profile_rejection_gap:{primary_gap}"
        return "profile_rejection_gap"

    baseline_filters = typed_dict(baseline.get("baseline_filter_counts"))
    candidate_filters = typed_dict(candidate.get("baseline_filter_counts"))
    spread_delta = delta(
        number(baseline_filters.get("passes_spread")),
        number(candidate_filters.get("passes_spread")),
    )
    if spread_delta is not None and spread_delta < 0:
        return "market_window_fewer_spread_candidates"

    baseline_rate = number(baseline.get("signals_per_snapshot"))
    candidate_rate = number(candidate.get("signals_per_snapshot"))
    if (
        baseline_rate is not None
        and candidate_rate is not None
        and candidate_rate < baseline_rate
    ):
        return "market_window_lower_candidate_density"
    return "no_profile_specific_activity_loss_detected"


def baseline_filter_pass_counts(report_root: Path) -> dict[str, int]:
    path = report_root / "baseline" / "baseline_filter_decisions.parquet"
    if not path.exists():
        return {}
    frame = pd.read_parquet(path)
    result: dict[str, int] = {"snapshots": len(frame.index)}
    for column in (
        "passes_spread",
        "passes_depth",
        "passes_imbalance",
        "passes_momentum",
        "passes_stale",
        "passes_adverse_selection",
    ):
        if column in frame.columns:
            result[column] = int(frame[column].fillna(False).sum())
    if all(
        column in frame.columns
        for column in (
            "passes_spread",
            "passes_depth",
            "passes_imbalance",
            "passes_momentum",
            "passes_stale",
            "passes_adverse_selection",
        )
    ):
        result["all_pass"] = int(
            (
                frame["passes_spread"].fillna(False)
                & frame["passes_depth"].fillna(False)
                & frame["passes_imbalance"].fillna(False)
                & frame["passes_momentum"].fillna(False)
                & frame["passes_stale"].fillna(False)
                & frame["passes_adverse_selection"].fillna(False)
            ).sum()
        )
    return result


def filter_deltas(
    baseline: dict[str, object], candidate: dict[str, object]
) -> list[dict[str, object]]:
    return [
        {
            "metric": key,
            "baseline": number(baseline.get(key)),
            "candidate": number(candidate.get(key)),
            "delta": delta(number(baseline.get(key)), number(candidate.get(key))),
        }
        for key in sorted(set(baseline) | set(candidate))
    ]


def read_json(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def typed_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and pd.notna(value):
        return float(value)
    return None


def delta(baseline: float | None, candidate: float | None) -> float | None:
    if baseline is None or candidate is None:
        return None
    return candidate - baseline


def rate(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return numerator / denominator


def per_minute(signals: float | None, capture_duration_ms: float | None) -> float | None:
    if signals is None or capture_duration_ms is None or capture_duration_ms <= 0:
        return None
    return signals / (capture_duration_ms / 60_000)


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="signal-activity-audit")
    parser.add_argument("--baseline-report-root", required=True)
    parser.add_argument("--candidate-report-root", required=True)
    parser.add_argument("--output")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    report = create_signal_activity_audit(
        Path(args.baseline_report_root),
        Path(args.candidate_report_root),
        output_path=Path(args.output) if args.output else None,
    )
    if args.json or not args.output:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
