import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd  # type: ignore[import-untyped]

from src.research.compare_runs import load_run_index, normalize_row


FEATURE_DECISION_REPORT_VERSION = "feature_research_decision_v1"


@dataclass(frozen=True)
class FeatureDecisionThresholds:
    min_shared_sentiment_buckets: int = 1
    min_shared_feature_candidates: int = 1
    min_realized_edge_lift_delta: float = 0.0
    min_fill_rate_lift_delta: float = 0.0
    max_adverse_edge_rate_lift_delta: float = 0.0
    max_drawdown_delta: float = 0.0
    max_new_blocklist_candidates: int = 0


def compare_feature_research_runs(
    manifest_root: Path,
    baseline_run_id: str | None = None,
    candidate_run_id: str | None = None,
    thresholds: FeatureDecisionThresholds = FeatureDecisionThresholds(),
) -> dict[str, object]:
    frame = load_run_index(manifest_root)
    baseline, candidate = select_runs(frame, baseline_run_id, candidate_run_id)
    return compare_feature_research_report_roots(
        Path(str(baseline["report_root"])),
        Path(str(candidate["report_root"])),
        thresholds=thresholds,
        baseline_run_id=str(baseline["run_id"]),
        candidate_run_id=str(candidate["run_id"]),
    )


def create_missing_baseline_report(candidate_report_root: Path) -> dict[str, object]:
    report: dict[str, object] = {
        "report_version": FEATURE_DECISION_REPORT_VERSION,
        "decision_policy": "offline_diagnostics_only",
        "can_apply_live": False,
        "status": "skipped",
        "reason": "no_prior_research_run",
        "baseline_run_id": None,
        "candidate_run_id": candidate_report_root.name,
        "sentiment_lift_comparison": {
            "status": "missing_baseline",
            "shared_buckets": 0,
            "missing_baseline": True,
            "missing_candidate": False,
        },
        "feature_blocklist_candidate_comparison": {
            "status": "missing_baseline",
            "shared_candidates": 0,
            "missing_baseline": True,
            "missing_candidate": False,
            "candidate_payload_can_apply_live": False,
        },
        "thresholds": FeatureDecisionThresholds().__dict__,
    }
    report.update(decide_feature_research(report))
    return report


def compare_feature_research_report_roots(
    baseline_report_root: Path,
    candidate_report_root: Path,
    thresholds: FeatureDecisionThresholds = FeatureDecisionThresholds(),
    baseline_run_id: str | None = None,
    candidate_run_id: str | None = None,
) -> dict[str, object]:
    report = {
        "report_version": FEATURE_DECISION_REPORT_VERSION,
        "decision_policy": "offline_diagnostics_only",
        "can_apply_live": False,
        "baseline_run_id": baseline_run_id or baseline_report_root.name,
        "candidate_run_id": candidate_run_id or candidate_report_root.name,
        "sentiment_lift_comparison": compare_sentiment_lift(
            baseline_report_root, candidate_report_root
        ),
        "feature_blocklist_candidate_comparison": compare_feature_blocklist_candidates(
            baseline_report_root, candidate_report_root
        ),
        "thresholds": thresholds.__dict__,
    }
    decision = decide_feature_research(report, thresholds)
    report.update(decision)
    return report


def decide_feature_research(
    report: dict[str, object],
    thresholds: FeatureDecisionThresholds = FeatureDecisionThresholds(),
) -> dict[str, object]:
    sentiment = typed_dict(report.get("sentiment_lift_comparison"))
    blocklist = typed_dict(report.get("feature_blocklist_candidate_comparison"))
    checks = [
        check_candidate_only(report),
        check_count_at_least(
            "shared_sentiment_buckets",
            sentiment.get("shared_buckets"),
            thresholds.min_shared_sentiment_buckets,
        ),
        check_count_at_least(
            "shared_feature_candidates",
            blocklist.get("shared_candidates"),
            thresholds.min_shared_feature_candidates,
        ),
        check_delta_at_least(
            "realized_edge_lift_delta",
            sentiment.get("avg_realized_edge_lift_delta"),
            thresholds.min_realized_edge_lift_delta,
        ),
        check_delta_at_least(
            "fill_rate_lift_delta",
            sentiment.get("avg_fill_rate_lift_delta"),
            thresholds.min_fill_rate_lift_delta,
        ),
        check_delta_at_most(
            "adverse_edge_rate_lift_delta",
            sentiment.get("avg_adverse_edge_rate_lift_delta"),
            thresholds.max_adverse_edge_rate_lift_delta,
        ),
        check_delta_at_most(
            "max_drawdown_delta",
            sentiment.get("max_drawdown_delta"),
            thresholds.max_drawdown_delta,
        ),
        check_count_at_most(
            "new_blocklist_candidates",
            blocklist.get("new_block_candidates"),
            thresholds.max_new_blocklist_candidates,
        ),
    ]
    failed = [item for item in checks if item["status"] == "FAIL"]
    missing = [item for item in checks if item["status"] == "MISSING"]
    if failed:
        decision = "REJECT_FEATURE"
    elif missing:
        decision = "KEEP_DIAGNOSTIC"
    else:
        decision = "PROMOTE_FEATURE"
    return {
        "decision": decision,
        "checks": checks,
        "summary": {
            "passed": sum(1 for item in checks if item["status"] == "PASS"),
            "failed": len(failed),
            "missing": len(missing),
        },
    }


def compare_sentiment_lift(
    baseline_report_root: Path, candidate_report_root: Path
) -> dict[str, object]:
    baseline = read_parquet_optional(
        baseline_report_root / "sentiment_lift" / "sentiment_lift_summary.parquet"
    )
    candidate = read_parquet_optional(
        candidate_report_root / "sentiment_lift" / "sentiment_lift_summary.parquet"
    )
    if baseline is None or candidate is None:
        return {
            "status": "missing_data",
            "shared_buckets": 0,
            "missing_baseline": baseline is None,
            "missing_candidate": candidate is None,
        }
    keys = ["sentiment_bucket", "signal_sentiment_alignment", "strategy", "side"]
    if missing_columns(baseline, keys) or missing_columns(candidate, keys):
        return {"status": "missing_keys", "shared_buckets": 0}
    merged = baseline.merge(candidate, on=keys, suffixes=("_baseline", "_candidate"))
    if merged.empty:
        return {
            "status": "no_comparable",
            "shared_buckets": 0,
            "baseline_buckets": len(baseline.index),
            "candidate_buckets": len(candidate.index),
        }
    deltas = metric_deltas(
        merged,
        (
            "realized_edge_lift",
            "fill_rate_lift",
            "adverse_edge_rate_lift",
            "max_drawdown",
        ),
    )
    return {
        "status": "comparable",
        "shared_buckets": len(merged.index),
        "baseline_buckets": len(baseline.index),
        "candidate_buckets": len(candidate.index),
        "new_buckets": max(0, len(candidate.index) - len(merged.index)),
        "removed_buckets": max(0, len(baseline.index) - len(merged.index)),
        "avg_realized_edge_lift_delta": deltas.get("realized_edge_lift"),
        "avg_fill_rate_lift_delta": deltas.get("fill_rate_lift"),
        "avg_adverse_edge_rate_lift_delta": deltas.get("adverse_edge_rate_lift"),
        "max_drawdown_delta": deltas.get("max_drawdown"),
        "bucket_deltas": bucket_delta_rows(merged, keys),
    }


def compare_feature_blocklist_candidates(
    baseline_report_root: Path, candidate_report_root: Path
) -> dict[str, object]:
    baseline = read_parquet_optional(
        baseline_report_root
        / "feature_blocklist_candidates"
        / "research_feature_blocklist_candidates.parquet"
    )
    candidate = read_parquet_optional(
        candidate_report_root
        / "feature_blocklist_candidates"
        / "research_feature_blocklist_candidates.parquet"
    )
    candidate_payload = read_json_optional(
        candidate_report_root
        / "feature_blocklist_candidates"
        / "blocked_segments_candidates.json"
    )
    if baseline is None or candidate is None:
        return {
            "status": "missing_data",
            "shared_candidates": 0,
            "missing_baseline": baseline is None,
            "missing_candidate": candidate is None,
            "candidate_payload_can_apply_live": typed_dict(candidate_payload).get(
                "can_apply_live"
            ),
        }
    keys = ["feature_family", "feature_name", "bucket", "strategy", "side"]
    if missing_columns(baseline, keys) or missing_columns(candidate, keys):
        return {"status": "missing_keys", "shared_candidates": 0}
    baseline_keys = key_set(baseline, keys, only_block_candidates=True)
    candidate_keys = key_set(candidate, keys, only_block_candidates=True)
    shared = baseline_keys & candidate_keys
    new = candidate_keys - baseline_keys
    removed = baseline_keys - candidate_keys
    return {
        "status": "comparable" if shared else "no_comparable",
        "shared_candidates": len(shared),
        "baseline_block_candidates": len(baseline_keys),
        "candidate_block_candidates": len(candidate_keys),
        "new_block_candidates": len(new),
        "removed_block_candidates": len(removed),
        "new_candidate_keys": sorted(new),
        "removed_candidate_keys": sorted(removed),
        "candidate_payload_can_apply_live": typed_dict(candidate_payload).get(
            "can_apply_live", False
        ),
    }


def select_runs(
    frame: pd.DataFrame, baseline_run_id: str | None, candidate_run_id: str | None
) -> tuple[dict[str, object], dict[str, object]]:
    if len(frame.index) < 2 and (baseline_run_id is None or candidate_run_id is None):
        raise ValueError("at least two runs are required when run IDs are not provided")
    baseline = select_run(frame, baseline_run_id, default_index=-2)
    candidate = select_run(frame, candidate_run_id, default_index=-1)
    if baseline["run_id"] == candidate["run_id"]:
        raise ValueError("baseline and candidate run IDs must differ")
    return normalize_row(baseline.to_dict()), normalize_row(candidate.to_dict())


def select_run(frame: pd.DataFrame, run_id: str | None, default_index: int) -> pd.Series:  # type: ignore[type-arg]
    if run_id is None:
        return frame.iloc[default_index]
    matches = frame[frame["run_id"] == run_id]
    if matches.empty:
        raise ValueError(f"run_id not found: {run_id}")
    return matches.iloc[-1]


def metric_deltas(merged: pd.DataFrame, metrics: tuple[str, ...]) -> dict[str, float | None]:
    deltas: dict[str, float | None] = {}
    for metric in metrics:
        baseline_col = f"{metric}_baseline"
        candidate_col = f"{metric}_candidate"
        if baseline_col not in merged.columns or candidate_col not in merged.columns:
            deltas[metric] = None
            continue
        deltas[metric] = float(
            (merged[candidate_col].astype(float) - merged[baseline_col].astype(float)).mean()
        )
    return deltas


def bucket_delta_rows(merged: pd.DataFrame, keys: list[str]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for _, row in merged.sort_values(keys, kind="stable").iterrows():
        item: dict[str, object] = {key: row[key] for key in keys}
        for metric in ("realized_edge_lift", "fill_rate_lift", "adverse_edge_rate_lift"):
            baseline = numeric_or_none(row.get(f"{metric}_baseline"))
            candidate = numeric_or_none(row.get(f"{metric}_candidate"))
            item[f"{metric}_delta"] = (
                candidate - baseline
                if baseline is not None and candidate is not None
                else None
            )
        rows.append(item)
    return rows


def key_set(frame: pd.DataFrame, keys: list[str], only_block_candidates: bool) -> set[str]:
    source = frame
    if only_block_candidates and "should_block_candidate" in source.columns:
        source = source[source["should_block_candidate"] == True]  # noqa: E712
    return {
        "|".join(str(row[key]) for key in keys)
        for _, row in source.iterrows()
    }


def missing_columns(frame: pd.DataFrame, columns: list[str]) -> bool:
    return any(column not in frame.columns for column in columns)


def check_candidate_only(report: dict[str, object]) -> dict[str, object]:
    blocklist = typed_dict(report.get("feature_blocklist_candidate_comparison"))
    can_apply_live = blocklist.get("candidate_payload_can_apply_live")
    if can_apply_live is True:
        return check_result("candidate_only", "FAIL", 1.0, 0.0)
    return check_result("candidate_only", "PASS", 0.0, 0.0)


def check_count_at_least(name: str, value: object, threshold: int) -> dict[str, object]:
    number = numeric_or_none(value)
    if number is None:
        return check_result(name, "MISSING", None, float(threshold))
    return check_result(name, "PASS" if number >= threshold else "MISSING", number, float(threshold))


def check_count_at_most(name: str, value: object, threshold: int) -> dict[str, object]:
    number = numeric_or_none(value)
    if number is None:
        return check_result(name, "MISSING", None, float(threshold))
    return check_result(name, "PASS" if number <= threshold else "FAIL", number, float(threshold))


def check_delta_at_least(name: str, value: object, threshold: float) -> dict[str, object]:
    number = numeric_or_none(value)
    if number is None:
        return check_result(name, "MISSING", None, threshold)
    return check_result(name, "PASS" if number >= threshold else "FAIL", number, threshold)


def check_delta_at_most(name: str, value: object, threshold: float) -> dict[str, object]:
    number = numeric_or_none(value)
    if number is None:
        return check_result(name, "MISSING", None, threshold)
    return check_result(name, "PASS" if number <= threshold else "FAIL", number, threshold)


def check_result(
    name: str, status: str, value: float | None, threshold: float
) -> dict[str, object]:
    return {
        "check_name": name,
        "status": status,
        "metric_value": value,
        "threshold": threshold,
    }


def read_parquet_optional(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    return pd.read_parquet(path)


def read_json_optional(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    value = json.loads(path.read_text(encoding="utf-8"))
    return value if isinstance(value, dict) else None


def typed_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def numeric_or_none(value: object) -> float | None:
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return None


def main() -> int:
    parser = argparse.ArgumentParser(prog="feature-research-decision")
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
        report = compare_feature_research_report_roots(
            Path(args.baseline_report_root),
            Path(args.candidate_report_root),
        )
    else:
        report = compare_feature_research_runs(
            Path(args.manifest_root),
            baseline_run_id=args.baseline_run_id,
            candidate_run_id=args.candidate_run_id,
        )
    if args.output:
        Path(args.output).write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    if args.json or not args.output:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
