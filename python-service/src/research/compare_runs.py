import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd  # type: ignore[import-untyped]


DEFAULT_METRICS = (
    "signals",
    "filled_signals",
    "execution_reports",
    "realized_edge",
    "fill_rate",
    "dry_run_observed_fill_rate",
    "slippage",
    "drawdown",
    "max_abs_simulator_fill_rate_delta",
    "blocked_segments",
    "runtime_blocked_segments",
    "stale_data_rate",
    "reconciliation_divergence_rate",
    "test_brier_score",
    "advisory_failed",
)
LOWER_IS_BETTER = {
    "slippage",
    "drawdown",
    "max_abs_simulator_fill_rate_delta",
    "blocked_segments",
    "stale_data_rate",
    "reconciliation_divergence_rate",
    "test_brier_score",
    "advisory_failed",
}
NEUTRAL_METRICS = {"runtime_blocked_segments"}
MIN_SHARED_SEGMENT_RATIO = 0.80
MIN_SHARED_SIGNAL_COVERAGE_RATIO = 0.50
MIN_SHARED_FILL_COVERAGE_RATIO = 0.50


@dataclass(frozen=True)
class RunComparison:
    baseline_run_id: str
    candidate_run_id: str
    metric_deltas: list[dict[str, object]]
    verdict: str


def load_run_index(manifest_root: Path) -> pd.DataFrame:
    index_path = manifest_root / "research_runs.parquet"
    if not index_path.exists():
        raise FileNotFoundError(f"research run index not found: {index_path}")
    frame = pd.read_parquet(index_path)
    if "run_id" not in frame.columns:
        raise ValueError(f"research run index missing run_id column: {index_path}")
    return frame.sort_values(["created_at", "run_id"], kind="stable").reset_index(drop=True)


def summarize_runs(manifest_root: Path, limit: int = 10) -> list[dict[str, object]]:
    frame = load_run_index(manifest_root)
    rows = frame.tail(limit).to_dict(orient="records")
    return [normalize_row(row) for row in rows]


def compare_runs(
    manifest_root: Path,
    baseline_run_id: str | None = None,
    candidate_run_id: str | None = None,
    metrics: tuple[str, ...] = DEFAULT_METRICS,
) -> dict[str, object]:
    frame = load_run_index(manifest_root)
    baseline, candidate = select_runs(frame, baseline_run_id, candidate_run_id)
    deltas = [
        metric_delta(metric, baseline.get(metric), candidate.get(metric))
        for metric in metrics
        if metric in frame.columns
    ]
    comparison = RunComparison(
        baseline_run_id=str(baseline["run_id"]),
        candidate_run_id=str(candidate["run_id"]),
        metric_deltas=deltas,
        verdict=comparison_verdict_for(
            deltas, report_root_from_row(baseline), report_root_from_row(candidate)
        ),
    )
    return {
        "baseline": normalize_row(baseline),
        "candidate": normalize_row(candidate),
        "comparison": {
            "baseline_run_id": comparison.baseline_run_id,
            "candidate_run_id": comparison.candidate_run_id,
            "metric_deltas": comparison.metric_deltas,
            "verdict": comparison.verdict,
            "segment_changes": segment_changes(
                report_root_from_row(baseline), report_root_from_row(candidate)
            ),
            "segment_change_summary": segment_change_summary(
                report_root_from_row(baseline), report_root_from_row(candidate)
            ),
            "segment_comparability": segment_comparability(
                report_root_from_row(baseline), report_root_from_row(candidate)
            ),
            "coverage_assessment": coverage_assessment(
                report_root_from_row(baseline), report_root_from_row(candidate)
            ),
            "restricted_blocklist_assessment": restricted_blocklist_assessment(
                deltas,
                report_root_from_row(baseline),
                report_root_from_row(candidate),
            ),
            "blocked_segment_changes": blocked_segment_changes(
                report_root_from_row(baseline), report_root_from_row(candidate)
            ),
        },
    }


def compare_report_roots(
    baseline_report_root: Path,
    candidate_report_root: Path,
    metrics: tuple[str, ...] = DEFAULT_METRICS,
) -> dict[str, object]:
    baseline_manifest = load_report_manifest(baseline_report_root)
    candidate_manifest = load_report_manifest(candidate_report_root)
    baseline = normalize_row(flatten_manifest_like(baseline_manifest))
    candidate = normalize_row(flatten_manifest_like(candidate_manifest))
    deltas = [
        metric_delta(metric, baseline.get(metric), candidate.get(metric))
        for metric in metrics
        if metric in baseline or metric in candidate
    ]
    return {
        "baseline": baseline,
        "candidate": candidate,
        "comparison": {
            "baseline_run_id": str(baseline.get("run_id")),
            "candidate_run_id": str(candidate.get("run_id")),
            "metric_deltas": deltas,
            "verdict": comparison_verdict_for(
                deltas, baseline_report_root, candidate_report_root
            ),
            "segment_changes": segment_changes(baseline_report_root, candidate_report_root),
            "segment_change_summary": segment_change_summary(
                baseline_report_root, candidate_report_root
            ),
            "segment_comparability": segment_comparability(
                baseline_report_root, candidate_report_root
            ),
            "coverage_assessment": coverage_assessment(
                baseline_report_root, candidate_report_root
            ),
            "restricted_blocklist_assessment": restricted_blocklist_assessment(
                deltas,
                baseline_report_root,
                candidate_report_root,
            ),
            "blocked_segment_changes": blocked_segment_changes(
                baseline_report_root, candidate_report_root
            ),
        },
    }


def select_runs(
    frame: pd.DataFrame, baseline_run_id: str | None, candidate_run_id: str | None
) -> tuple[dict[str, Any], dict[str, Any]]:
    if len(frame.index) < 2 and (baseline_run_id is None or candidate_run_id is None):
        raise ValueError("at least two runs are required when run IDs are not provided")
    if baseline_run_id is None:
        baseline = frame.iloc[-2].to_dict()
    else:
        baseline = run_by_id(frame, baseline_run_id)
    if candidate_run_id is None:
        candidate = frame.iloc[-1].to_dict()
    else:
        candidate = run_by_id(frame, candidate_run_id)
    if baseline["run_id"] == candidate["run_id"]:
        raise ValueError("baseline and candidate run IDs must differ")
    return normalize_row(baseline), normalize_row(candidate)


def run_by_id(frame: pd.DataFrame, run_id: str) -> dict[str, Any]:
    matches = frame[frame["run_id"] == run_id]
    if matches.empty:
        raise ValueError(f"run_id not found: {run_id}")
    return matches.iloc[-1].to_dict()


def load_report_manifest(report_root: Path) -> dict[str, object]:
    manifest_path = report_root / "research_manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"research manifest not found: {manifest_path}")
    value = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"research manifest must be an object: {manifest_path}")
    return value


def flatten_manifest_like(manifest: dict[str, object]) -> dict[str, object]:
    metrics = typed_dict(manifest.get("metrics"))
    counts = typed_dict(manifest.get("counts"))
    row = {
        "run_id": manifest.get("run_id"),
        "source": manifest.get("source"),
        "created_at": manifest.get("created_at"),
        "report_root": manifest.get("report_root"),
        "passed": manifest.get("passed"),
    }
    row.update(metrics)
    row.update(counts)
    return row


def report_root_from_row(row: dict[str, object]) -> Path | None:
    value = row.get("report_root")
    return Path(value) if isinstance(value, str) and value else None


def segment_changes(
    baseline_report_root: Path | None,
    candidate_report_root: Path | None,
    limit: int = 20,
) -> list[dict[str, object]]:
    merged = joined_segment_frame(baseline_report_root, candidate_report_root)
    if merged.empty:
        return []
    merged["classification"] = merged.apply(classify_segment_change, axis=1)
    merged["score"] = merged.apply(segment_change_score, axis=1)
    rows = merged.sort_values("score", ascending=False).head(limit).to_dict(orient="records")
    return [normalize_row(row) for row in rows]


def segment_change_summary(
    baseline_report_root: Path | None,
    candidate_report_root: Path | None,
) -> dict[str, object]:
    if baseline_report_root is None or candidate_report_root is None:
        return {
            "baseline_segments": 0,
            "candidate_segments": 0,
            "shared_segments": 0,
            "new_segments": 0,
            "removed_segments": 0,
            "improved_segments": 0,
            "worsened_segments": 0,
        }
    baseline = load_segments(baseline_report_root)
    candidate = load_segments(candidate_report_root)
    keys = segment_keys()
    if baseline.empty and candidate.empty:
        return {
            "baseline_segments": 0,
            "candidate_segments": 0,
            "shared_segments": 0,
            "new_segments": 0,
            "removed_segments": 0,
            "improved_segments": 0,
            "worsened_segments": 0,
        }
    baseline_keys = key_set(baseline, keys)
    candidate_keys = key_set(candidate, keys)
    merged = joined_segment_frame(baseline_report_root, candidate_report_root)
    improved = 0
    worsened = 0
    if not merged.empty:
        classifications = [
            classify_segment_change(row)
            for _, row in merged.iterrows()
        ]
        improved = sum(1 for item in classifications if item == "improved")
        worsened = sum(1 for item in classifications if item == "worsened")
    return {
        "baseline_segments": len(baseline_keys),
        "candidate_segments": len(candidate_keys),
        "shared_segments": len(baseline_keys & candidate_keys),
        "new_segments": len(candidate_keys - baseline_keys),
        "removed_segments": len(baseline_keys - candidate_keys),
        "improved_segments": improved,
        "worsened_segments": worsened,
    }


def segment_comparability(
    baseline_report_root: Path | None,
    candidate_report_root: Path | None,
) -> dict[str, object]:
    if baseline_report_root is None or candidate_report_root is None:
        return {
            "status": "no_comparable",
            "reason": "missing_report_root",
            "baseline_segments_available": False,
            "candidate_segments_available": False,
            "missing_in_baseline": 0,
            "missing_in_candidate": 0,
        }
    baseline_path = segment_path(baseline_report_root)
    candidate_path = segment_path(candidate_report_root)
    baseline_exists = baseline_path.exists()
    candidate_exists = candidate_path.exists()
    baseline = load_segments(baseline_report_root)
    candidate = load_segments(candidate_report_root)
    keys = segment_keys()
    baseline_keys = key_set(baseline, keys)
    candidate_keys = key_set(candidate, keys)
    expected_removed_keys = expected_restricted_blocked_segments(candidate_report_root)
    expected_removed = (baseline_keys - candidate_keys) & expected_removed_keys
    unexpected_removed = (baseline_keys - candidate_keys) - expected_removed_keys
    unexpected_new = candidate_keys - baseline_keys
    shared_keys = baseline_keys & candidate_keys
    expected_unblocked_baseline_count = len(baseline_keys - expected_removed_keys)
    shared_segment_ratio = ratio(len(shared_keys), expected_unblocked_baseline_count)
    signal_coverage = shared_metric_coverage_ratio(baseline, candidate, keys, "signals")
    fill_coverage = shared_metric_coverage_ratio(baseline, candidate, keys, "filled_signals")
    if not baseline_exists and not candidate_exists:
        reason = "missing_both_segment_exports"
    elif not baseline_exists:
        reason = "missing_baseline_segment_export"
    elif not candidate_exists:
        reason = "missing_candidate_segment_export"
    elif missing_segment_columns(baseline):
        reason = "missing_baseline_segment_keys"
    elif missing_segment_columns(candidate):
        reason = "missing_candidate_segment_keys"
    elif baseline_keys == candidate_keys:
        reason = None
    elif unexpected_removed:
        reason = "unexpected_candidate_segment_loss"
    elif unexpected_new:
        reason = "unexpected_candidate_new_segments"
    elif shared_segment_ratio is None or shared_segment_ratio < MIN_SHARED_SEGMENT_RATIO:
        reason = "insufficient_shared_segment_coverage"
    elif signal_coverage is None or signal_coverage < MIN_SHARED_SIGNAL_COVERAGE_RATIO:
        reason = "insufficient_shared_signal_coverage"
    elif fill_coverage is None or fill_coverage < MIN_SHARED_FILL_COVERAGE_RATIO:
        reason = "insufficient_shared_fill_coverage"
    else:
        reason = None
    status = "comparable" if reason is None else "no_comparable"
    return {
        "status": status,
        "reason": reason,
        "policy_version": "segment_comparability_v2",
        "minimums": {
            "min_shared_segment_ratio": MIN_SHARED_SEGMENT_RATIO,
            "min_shared_signal_coverage_ratio": MIN_SHARED_SIGNAL_COVERAGE_RATIO,
            "min_shared_fill_coverage_ratio": MIN_SHARED_FILL_COVERAGE_RATIO,
        },
        "baseline_segments_available": baseline_exists,
        "candidate_segments_available": candidate_exists,
        "baseline_segment_path": str(baseline_path),
        "candidate_segment_path": str(candidate_path),
        "baseline_segments": len(baseline_keys),
        "candidate_segments": len(candidate_keys),
        "shared_segments": len(shared_keys),
        "shared_segment_ratio": shared_segment_ratio,
        "shared_signal_coverage_ratio": signal_coverage,
        "shared_fill_coverage_ratio": fill_coverage,
        "expected_removed_segments": len(expected_removed),
        "expected_removed_segments_present": len(expected_removed_keys & baseline_keys),
        "unexpected_removed_segments": len(unexpected_removed),
        "unexpected_new_segments": len(unexpected_new),
        "missing_in_baseline": len(unexpected_new),
        "missing_in_candidate": len(unexpected_removed),
        "expected_removed": [
            segment_key_to_dict(item) for item in sorted(expected_removed)
        ],
        "unexpected_removed": [
            segment_key_to_dict(item) for item in sorted(unexpected_removed)
        ],
        "unexpected_new": [
            segment_key_to_dict(item) for item in sorted(unexpected_new)
        ],
    }


def coverage_assessment(
    baseline_report_root: Path | None,
    candidate_report_root: Path | None,
) -> dict[str, object]:
    comparability = segment_comparability(baseline_report_root, candidate_report_root)
    status = "acceptable" if comparability.get("status") == "comparable" else "blocked"
    return {
        "status": status,
        "policy_version": comparability.get("policy_version"),
        "reason": comparability.get("reason"),
        "minimums": comparability.get("minimums"),
        "shared_segment_ratio": comparability.get("shared_segment_ratio"),
        "shared_signal_coverage_ratio": comparability.get(
            "shared_signal_coverage_ratio"
        ),
        "shared_fill_coverage_ratio": comparability.get("shared_fill_coverage_ratio"),
        "unexpected_removed_segments": comparability.get("unexpected_removed_segments"),
        "unexpected_new_segments": comparability.get("unexpected_new_segments"),
        "expected_removed_segments": comparability.get("expected_removed_segments"),
    }


def restricted_blocklist_assessment(
    deltas: list[dict[str, object]],
    baseline_report_root: Path | None,
    candidate_report_root: Path | None,
) -> dict[str, object]:
    if not restricted_blocklist_enabled(candidate_report_root):
        return {
            "status": "not_applicable",
            "reason": "candidate_run_not_restricted",
            "can_promote_blocklist": False,
        }
    coverage = coverage_assessment(baseline_report_root, candidate_report_root)
    if coverage.get("status") != "acceptable":
        return {
            "status": "need_more_data",
            "reason": "coverage_not_acceptable",
            "can_promote_blocklist": False,
            "coverage_assessment": coverage,
        }
    regressions = protected_metric_regressions(deltas)
    if regressions:
        return {
            "status": "rejected",
            "reason": "protected_metric_regression",
            "can_promote_blocklist": False,
            "regressions": regressions,
            "coverage_assessment": coverage,
        }
    return {
        "status": "accepted_for_observation",
        "reason": "comparable_without_protected_metric_regression",
        "can_promote_blocklist": False,
        "next_step": "repeat_restricted_run_before_promotion",
        "coverage_assessment": coverage,
    }


def restricted_blocklist_enabled(candidate_report_root: Path | None) -> bool:
    if candidate_report_root is None:
        return False
    evidence = read_json(candidate_report_root / "real_dry_run_evidence.json")
    return evidence.get("blocked_segments_enabled") is True


def protected_metric_regressions(
    deltas: list[dict[str, object]],
) -> list[dict[str, object]]:
    protected = {
        "realized_edge",
        "fill_rate",
        "max_abs_simulator_fill_rate_delta",
        "reconciliation_divergence_rate",
    }
    return [
        item
        for item in deltas
        if item.get("metric") in protected and item.get("improved") is False
    ]


def joined_segment_frame(
    baseline_report_root: Path | None,
    candidate_report_root: Path | None,
) -> pd.DataFrame:
    if baseline_report_root is None or candidate_report_root is None:
        return pd.DataFrame()
    baseline = load_segments(baseline_report_root)
    candidate = load_segments(candidate_report_root)
    if baseline.empty or candidate.empty:
        return pd.DataFrame()
    keys = segment_keys()
    missing = [key for key in keys if key not in baseline.columns or key not in candidate.columns]
    if missing:
        return pd.DataFrame()
    merged = baseline.merge(candidate, on=keys, suffixes=("_baseline", "_candidate"))
    if merged.empty:
        return pd.DataFrame()
    for metric in (
        "signals",
        "filled_signals",
        "realized_edge",
        "pnl",
        "max_drawdown",
        "fill_rate",
        "dry_run_observed_fill_rate",
        "simulator_fill_rate_delta",
        "abs_simulator_fill_rate_delta",
    ):
        baseline_metric = f"{metric}_baseline"
        candidate_metric = f"{metric}_candidate"
        if baseline_metric in merged.columns and candidate_metric in merged.columns:
            merged[f"{metric}_delta"] = (
                pd.to_numeric(merged[candidate_metric], errors="coerce")
                - pd.to_numeric(merged[baseline_metric], errors="coerce")
            )
    return merged


def classify_segment_change(row: pd.Series) -> str:
    score = 0
    for metric in ("realized_edge_delta", "pnl_delta", "fill_rate_delta"):
        value = numeric_or_none(row.get(metric))
        if value is not None and value != 0:
            score += 1 if value > 0 else -1
    for metric in (
        "max_drawdown_delta",
        "abs_simulator_fill_rate_delta_delta",
        "simulator_fill_rate_delta_delta",
    ):
        value = numeric_or_none(row.get(metric))
        if value is not None and value != 0:
            score += 1 if value < 0 else -1
    if score > 0:
        return "improved"
    if score < 0:
        return "worsened"
    return "mixed"


def segment_change_score(row: pd.Series) -> float:
    score = 0.0
    for metric in (
        "realized_edge_delta",
        "pnl_delta",
        "max_drawdown_delta",
        "fill_rate_delta",
        "dry_run_observed_fill_rate_delta",
        "abs_simulator_fill_rate_delta_delta",
    ):
        value = numeric_or_none(row.get(metric))
        if value is not None:
            score += abs(value)
    return score


def load_segments(report_root: Path) -> pd.DataFrame:
    path = segment_path(report_root)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def missing_segment_columns(frame: pd.DataFrame) -> list[str]:
    return [key for key in segment_keys() if key not in frame.columns]


def segment_path(report_root: Path) -> Path:
    return report_root / "pre_live_promotion" / "pre_live_promotion_segments.parquet"


def blocked_segment_changes(
    baseline_report_root: Path | None,
    candidate_report_root: Path | None,
) -> dict[str, object]:
    baseline = load_blocked_segments(baseline_report_root)
    candidate = load_blocked_segments(candidate_report_root)
    newly_blocked = sorted(candidate - baseline)
    unblocked = sorted(baseline - candidate)
    still_blocked = sorted(baseline & candidate)
    return {
        "baseline_count": len(baseline),
        "candidate_count": len(candidate),
        "newly_blocked_count": len(newly_blocked),
        "unblocked_count": len(unblocked),
        "still_blocked_count": len(still_blocked),
        "newly_blocked": [segment_key_to_dict(item) for item in newly_blocked],
        "unblocked": [segment_key_to_dict(item) for item in unblocked],
        "still_blocked": [segment_key_to_dict(item) for item in still_blocked],
    }


def expected_restricted_blocked_segments(
    candidate_report_root: Path | None,
) -> set[tuple[str, str, str, str, str]]:
    if candidate_report_root is None:
        return set()
    evidence = read_json(candidate_report_root / "real_dry_run_evidence.json")
    if evidence.get("blocked_segments_enabled") is not True:
        return set()
    path_value = evidence.get("blocked_segments_path")
    if not isinstance(path_value, str) or not path_value:
        return set()
    blocklist_path = Path(path_value)
    if not blocklist_path.is_absolute():
        repo_relative = Path.cwd() / blocklist_path
        report_relative = candidate_report_root / blocklist_path
        blocklist_path = repo_relative if repo_relative.exists() else report_relative
    return load_blocked_segments_from_json(blocklist_path)


def load_blocked_segments(report_root: Path | None) -> set[tuple[str, str, str, str, str]]:
    if report_root is None:
        return set()
    parquet_path = report_root / "pre_live_promotion" / "pre_live_blocked_segments.parquet"
    if parquet_path.exists():
        return key_set(pd.read_parquet(parquet_path), segment_keys())
    json_path = report_root / "pre_live_promotion" / "blocked_segments.json"
    return load_blocked_segments_from_json(json_path)


def load_blocked_segments_from_json(path: Path) -> set[tuple[str, str, str, str, str]]:
    if not path.exists():
        return set()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    if not isinstance(payload, dict):
        return set()
    segments = payload.get("segments")
    if not isinstance(segments, list):
        return set()
    rows = [item for item in segments if isinstance(item, dict)]
    return key_set(pd.DataFrame(rows), segment_keys())


def read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def shared_metric_coverage_ratio(
    baseline: pd.DataFrame,
    candidate: pd.DataFrame,
    keys: list[str],
    metric: str,
) -> float | None:
    if baseline.empty or candidate.empty:
        return None
    if any(key not in baseline.columns or key not in candidate.columns for key in keys):
        return None
    if metric not in baseline.columns or metric not in candidate.columns:
        return None
    merged = baseline[keys + [metric]].merge(
        candidate[keys + [metric]],
        on=keys,
        suffixes=("_baseline", "_candidate"),
    )
    if merged.empty:
        return 0.0
    baseline_total = pd.to_numeric(merged[f"{metric}_baseline"], errors="coerce").fillna(0).sum()
    candidate_total = pd.to_numeric(merged[f"{metric}_candidate"], errors="coerce").fillna(0).sum()
    return ratio(float(candidate_total), float(baseline_total))


def ratio(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator


def segment_keys() -> list[str]:
    return ["market_id", "asset_id", "side", "strategy", "model_version"]


def key_set(frame: pd.DataFrame, keys: list[str]) -> set[tuple[str, str, str, str, str]]:
    if frame.empty or any(key not in frame.columns for key in keys):
        return set()
    values: set[tuple[str, str, str, str, str]] = set()
    for row in frame[keys].fillna("").to_dict(orient="records"):
        values.add(tuple(str(row[key]) for key in keys))  # type: ignore[arg-type]
    return values


def segment_key_to_dict(key: tuple[str, str, str, str, str]) -> dict[str, str]:
    return dict(zip(segment_keys(), key, strict=True))


def metric_delta(metric: str, baseline: object, candidate: object) -> dict[str, object]:
    baseline_value = numeric_or_none(baseline)
    candidate_value = numeric_or_none(candidate)
    delta = (
        candidate_value - baseline_value
        if baseline_value is not None and candidate_value is not None
        else None
    )
    return {
        "metric": metric,
        "baseline": baseline_value,
        "candidate": candidate_value,
        "delta": delta,
        "direction": metric_direction(metric),
        "improved": metric_improved(metric, delta),
    }


def metric_direction(metric: str) -> str:
    if metric in NEUTRAL_METRICS:
        return "neutral"
    if metric in LOWER_IS_BETTER:
        return "lower_is_better"
    return "higher_is_better"


def metric_improved(metric: str, delta: float | None) -> bool | None:
    if metric in NEUTRAL_METRICS or delta is None or delta == 0:
        return None
    if metric in LOWER_IS_BETTER:
        return delta < 0
    return delta > 0


def comparison_verdict(deltas: list[dict[str, object]]) -> str:
    scored = [item for item in deltas if item["improved"] is not None]
    if not scored:
        return "insufficient_data"
    improved = sum(1 for item in scored if item["improved"] is True)
    worsened = sum(1 for item in scored if item["improved"] is False)
    if improved > worsened:
        return "candidate_improved"
    if worsened > improved:
        return "candidate_regressed"
    return "mixed"


def comparison_verdict_for(
    deltas: list[dict[str, object]],
    baseline_report_root: Path | None,
    candidate_report_root: Path | None,
) -> str:
    comparability = segment_comparability(baseline_report_root, candidate_report_root)
    if comparability.get("status") != "comparable":
        return "no_comparable"
    return comparison_verdict(deltas)


def normalize_row(row: dict[str, Any]) -> dict[str, object]:
    return {key: normalize_value(value) for key, value in row.items()}


def typed_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def normalize_value(value: object) -> object:
    if isinstance(value, (list, tuple)):
        return [normalize_value(item) for item in value]
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        return value.item()  # type: ignore[no-any-return]
    return value


def numeric_or_none(value: object) -> float | None:
    value = normalize_value(value)
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return None


def format_summary_table(rows: list[dict[str, object]]) -> str:
    columns = ("run_id", "passed", "realized_edge", "fill_rate", "drawdown", "advisory_failed")
    return format_table(rows, columns)


def format_comparison_table(report: dict[str, object]) -> str:
    comparison = report["comparison"]
    assert isinstance(comparison, dict)
    rows = comparison["metric_deltas"]
    assert isinstance(rows, list)
    return format_table(
        [row for row in rows if isinstance(row, dict)],
        ("metric", "baseline", "candidate", "delta", "improved"),
    )


def format_segment_changes_table(report: dict[str, object]) -> str:
    comparison = report["comparison"]
    assert isinstance(comparison, dict)
    rows = comparison.get("segment_changes", [])
    assert isinstance(rows, list)
    return format_table(
        [row for row in rows if isinstance(row, dict)],
        (
            "market_id",
            "asset_id",
            "classification",
            "realized_edge_delta",
            "pnl_delta",
            "max_drawdown_delta",
            "fill_rate_delta",
        ),
    )


def format_table(rows: list[dict[str, object]], columns: tuple[str, ...]) -> str:
    widths = {
        column: max([len(column), *[len(format_cell(row.get(column))) for row in rows]])
        for column in columns
    }
    header = "  ".join(column.ljust(widths[column]) for column in columns)
    divider = "  ".join("-" * widths[column] for column in columns)
    body = [
        "  ".join(format_cell(row.get(column)).ljust(widths[column]) for column in columns)
        for row in rows
    ]
    return "\n".join([header, divider, *body])


def format_cell(value: object) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.6g}"
    return str(value)


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="research-compare-runs")
    parser.add_argument("--manifest-root", default="data_lake/research_runs")
    parser.add_argument("--baseline-run-id")
    parser.add_argument("--candidate-run-id")
    parser.add_argument("--baseline-report-root")
    parser.add_argument("--candidate-report-root")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--json", action="store_true")
    parser.add_argument(
        "--summary",
        action="store_true",
        help="show recent runs instead of comparing the latest two runs",
    )
    args = parser.parse_args()

    manifest_root = Path(args.manifest_root)
    if args.summary:
        rows = summarize_runs(manifest_root, limit=args.limit)
        if args.json:
            print(json.dumps({"runs": rows}, indent=2, sort_keys=True))
        else:
            print(format_summary_table(rows))
        return 0

    if args.baseline_report_root or args.candidate_report_root:
        if not args.baseline_report_root or not args.candidate_report_root:
            raise SystemExit("--baseline-report-root and --candidate-report-root must be used together")
        report = compare_report_roots(
            Path(args.baseline_report_root),
            Path(args.candidate_report_root),
        )
    else:
        report = compare_runs(
            manifest_root,
            baseline_run_id=args.baseline_run_id,
            candidate_run_id=args.candidate_run_id,
        )
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        comparison = report["comparison"]
        assert isinstance(comparison, dict)
        print(
            f"{comparison['baseline_run_id']} -> {comparison['candidate_run_id']}: "
            f"{comparison['verdict']}"
        )
        print(format_comparison_table(report))
        segment_table = format_segment_changes_table(report)
        if segment_table.strip():
            print()
            print(segment_table)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
