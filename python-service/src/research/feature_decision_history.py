import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd  # type: ignore[import-untyped]

from src.research.compare_runs import load_run_index, normalize_row
from src.research.feature_research_decision import read_json_optional, read_parquet_optional


FEATURE_DECISION_HISTORY_REPORT_VERSION = "feature_decision_history_v1"
FEATURE_BUCKET_KEYS = (
    "feature_family",
    "feature_name",
    "bucket",
    "strategy",
    "side",
)


@dataclass(frozen=True)
class FeatureDecisionHistoryConfig:
    min_runs: int = 2
    stable_min_coverage_rate: float = 0.8
    stable_max_block_transition_rate: float = 0.2
    stable_max_metric_cv: float = 0.5


def build_feature_decision_history(
    manifest_root: Path,
    output_dir: Path,
    run_ids: list[str] | None = None,
    latest: int | None = None,
    config: FeatureDecisionHistoryConfig = FeatureDecisionHistoryConfig(),
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    run_frame = select_run_frame(load_run_index(manifest_root), run_ids, latest)
    run_rows = build_run_rows(run_frame)
    bucket_history = build_bucket_history(run_rows)
    bucket_stability = build_bucket_stability(bucket_history, len(run_rows), config)

    run_history_frame = pd.DataFrame(run_rows)
    run_history_frame.to_parquet(output_dir / "feature_decision_runs.parquet", index=False)
    bucket_history.to_parquet(
        output_dir / "feature_decision_bucket_history.parquet", index=False
    )
    bucket_stability.to_parquet(
        output_dir / "feature_decision_bucket_stability.parquet", index=False
    )

    report: dict[str, object] = {
        "report_version": FEATURE_DECISION_HISTORY_REPORT_VERSION,
        "decision_policy": "offline_diagnostics_only",
        "can_apply_live": False,
        "total_runs": len(run_rows),
        "decision_counts": decision_counts(run_rows),
        "missing_bucket_exports": count_missing_bucket_exports(run_rows),
        "stable_buckets": count_stability(bucket_stability, "stable"),
        "unstable_buckets": count_stability(bucket_stability, "unstable"),
        "insufficient_data_buckets": count_stability(bucket_stability, "insufficient_data"),
        "top_unstable_buckets": top_unstable_buckets(bucket_stability),
        "artifacts": [
            "feature_decision_runs.parquet",
            "feature_decision_bucket_history.parquet",
            "feature_decision_bucket_stability.parquet",
        ],
    }
    (output_dir / "feature_decision_history.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return report


def select_run_frame(
    frame: pd.DataFrame, run_ids: list[str] | None, latest: int | None
) -> pd.DataFrame:
    selected = frame.copy()
    if run_ids:
        selected = selected[selected["run_id"].isin(run_ids)]
        order = {run_id: index for index, run_id in enumerate(run_ids)}
        selected = selected.assign(
            requested_order=selected["run_id"].map(order).fillna(len(order))
        ).sort_values("requested_order", kind="stable")
        selected = selected.drop(columns=["requested_order"])
    elif latest is not None:
        selected = selected.tail(latest)
    return selected.reset_index(drop=True)


def build_run_rows(run_frame: pd.DataFrame) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for _, item in run_frame.iterrows():
        row = normalize_row(item.to_dict())
        report_root_value = row.get("report_root")
        report_root = Path(report_root_value) if isinstance(report_root_value, str) else None
        decision_report = (
            read_json_optional(report_root / "feature_research_decision.json")
            if report_root
            else None
        )
        bucket_path = (
            report_root
            / "feature_blocklist_candidates"
            / "research_feature_blocklist_candidates.parquet"
            if report_root
            else None
        )
        rows.append(
            {
                "run_id": row.get("run_id"),
                "created_at": row.get("created_at"),
                "report_root": row.get("report_root"),
                "passed": row.get("passed"),
                "pre_live_promotion_passed": row.get("pre_live_promotion_passed"),
                "feature_research_decision": row.get("feature_research_decision")
                or typed_dict(decision_report).get("decision"),
                "feature_research_status": row.get("feature_research_status")
                or typed_dict(decision_report).get("status"),
                "feature_decision_report_version": row.get(
                    "feature_decision_report_version"
                )
                or typed_dict(decision_report).get("report_version"),
                "realized_edge": row.get("realized_edge"),
                "fill_rate": row.get("fill_rate"),
                "drawdown": row.get("drawdown"),
                "bucket_export_exists": bool(bucket_path and bucket_path.exists()),
            }
        )
    return rows


def build_bucket_history(run_rows: list[dict[str, object]]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for row in run_rows:
        report_root_value = row.get("report_root")
        if not isinstance(report_root_value, str):
            continue
        bucket_frame = read_parquet_optional(
            Path(report_root_value)
            / "feature_blocklist_candidates"
            / "research_feature_blocklist_candidates.parquet"
        )
        if bucket_frame is None or bucket_frame.empty:
            continue
        required = set(FEATURE_BUCKET_KEYS)
        if not required.issubset(bucket_frame.columns):
            continue
        enriched = bucket_frame.copy()
        enriched["run_id"] = row.get("run_id")
        enriched["created_at"] = row.get("created_at")
        enriched["run_feature_decision"] = row.get("feature_research_decision")
        enriched["feature_bucket_id"] = enriched.apply(bucket_id_from_row, axis=1)
        frames.append(enriched)
    if not frames:
        return empty_bucket_history()
    columns = [
        "run_id",
        "created_at",
        *FEATURE_BUCKET_KEYS,
        "feature_bucket_id",
        "signals",
        "filled_signals",
        "avg_fill_rate",
        "avg_realized_edge_after_slippage",
        "adverse_edge_rate",
        "max_drawdown",
        "fill_rate_lift",
        "realized_edge_lift",
        "adverse_edge_rate_lift",
        "should_block_candidate",
        "candidate_reason",
        "run_feature_decision",
    ]
    combined = pd.concat(frames, ignore_index=True, sort=False)
    for column in columns:
        if column not in combined.columns:
            combined[column] = None
    return combined[columns]


def build_bucket_stability(
    bucket_history: pd.DataFrame,
    total_runs: int,
    config: FeatureDecisionHistoryConfig,
) -> pd.DataFrame:
    if bucket_history.empty:
        return empty_bucket_stability()
    rows: list[dict[str, object]] = []
    for keys, group in bucket_history.groupby(list(FEATURE_BUCKET_KEYS), dropna=False):
        key_values = keys if isinstance(keys, tuple) else (keys,)
        base = dict(zip(FEATURE_BUCKET_KEYS, key_values, strict=True))
        observed_runs = int(group["run_id"].nunique())
        coverage_rate = observed_runs / total_runs if total_runs else 0.0
        block_values = group["should_block_candidate"].fillna(False).astype(bool).tolist()
        transition_count = sum(
            1 for previous, current in zip(block_values, block_values[1:]) if previous != current
        )
        transition_rate = transition_count / max(1, len(block_values) - 1)
        metrics = metric_summary(group)
        stability_class = classify_stability(
            observed_runs, coverage_rate, transition_rate, metrics, config
        )
        decision_counts_for_bucket = group["run_feature_decision"].value_counts().to_dict()
        row: dict[str, object] = {
            **base,
            "feature_bucket_id": bucket_id_from_values(base),
            "observed_runs": observed_runs,
            "coverage_rate": coverage_rate,
            "promote_runs": int(decision_counts_for_bucket.get("PROMOTE_FEATURE", 0)),
            "keep_diagnostic_runs": int(
                decision_counts_for_bucket.get("KEEP_DIAGNOSTIC", 0)
            ),
            "reject_runs": int(decision_counts_for_bucket.get("REJECT_FEATURE", 0)),
            "block_candidate_runs": int(sum(block_values)),
            "block_candidate_rate": float(sum(block_values) / len(block_values)),
            "block_transition_count": transition_count,
            "block_transition_rate": transition_rate,
            "dominant_candidate_reason": dominant_value(group, "candidate_reason"),
            "stability_class": stability_class,
        }
        row.update(metrics)
        rows.append(row)
    return pd.DataFrame(rows).sort_values(
        ["stability_class", "feature_bucket_id"], kind="stable"
    )


def metric_summary(group: pd.DataFrame) -> dict[str, float | None]:
    output: dict[str, float | None] = {}
    for metric in (
        "avg_realized_edge_after_slippage",
        "avg_fill_rate",
        "adverse_edge_rate",
        "max_drawdown",
    ):
        series = pd.to_numeric(group.get(metric), errors="coerce").dropna()
        if series.empty:
            output[f"{metric}_mean"] = None
            output[f"{metric}_std"] = None
            output[f"{metric}_cv"] = None
            continue
        mean = float(series.mean())
        std = float(series.std(ddof=0))
        output[f"{metric}_mean"] = mean
        output[f"{metric}_std"] = std
        output[f"{metric}_cv"] = abs(std / mean) if mean else None
    return output


def classify_stability(
    observed_runs: int,
    coverage_rate: float,
    transition_rate: float,
    metrics: dict[str, float | None],
    config: FeatureDecisionHistoryConfig,
) -> str:
    if observed_runs < config.min_runs:
        return "insufficient_data"
    cvs = [
        value
        for key, value in metrics.items()
        if key.endswith("_cv") and value is not None
    ]
    max_cv = max(cvs) if cvs else 0.0
    if (
        coverage_rate >= config.stable_min_coverage_rate
        and transition_rate <= config.stable_max_block_transition_rate
        and max_cv <= config.stable_max_metric_cv
    ):
        return "stable"
    return "unstable"


def decision_counts(run_rows: list[dict[str, object]]) -> dict[str, int]:
    counts = {"PROMOTE_FEATURE": 0, "KEEP_DIAGNOSTIC": 0, "REJECT_FEATURE": 0}
    for row in run_rows:
        decision = row.get("feature_research_decision")
        if isinstance(decision, str):
            counts[decision] = counts.get(decision, 0) + 1
    return counts


def count_missing_bucket_exports(run_rows: list[dict[str, object]]) -> int:
    return sum(1 for row in run_rows if not row.get("bucket_export_exists"))


def count_stability(frame: pd.DataFrame, stability_class: str) -> int:
    if frame.empty or "stability_class" not in frame.columns:
        return 0
    return int((frame["stability_class"] == stability_class).sum())


def top_unstable_buckets(frame: pd.DataFrame, limit: int = 10) -> list[dict[str, object]]:
    if frame.empty:
        return []
    unstable = frame[frame["stability_class"] == "unstable"]
    if unstable.empty:
        return []
    rows = unstable.sort_values(
        ["block_transition_rate", "coverage_rate"],
        ascending=[False, False],
        kind="stable",
    ).head(limit)
    return [
        normalize_row(row)
        for row in rows[
            [
                "feature_bucket_id",
                "observed_runs",
                "coverage_rate",
                "block_transition_rate",
                "dominant_candidate_reason",
            ]
        ].to_dict(orient="records")
    ]


def dominant_value(group: pd.DataFrame, column: str) -> object:
    if column not in group.columns:
        return None
    values = group[column].dropna()
    if values.empty:
        return None
    return values.value_counts().index[0]


def bucket_id_from_row(row: pd.Series) -> str:  # type: ignore[type-arg]
    return bucket_id_from_values({key: row[key] for key in FEATURE_BUCKET_KEYS})


def bucket_id_from_values(values: dict[str, object]) -> str:
    return "|".join(str(values.get(key, "")) for key in FEATURE_BUCKET_KEYS)


def empty_bucket_history() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "run_id",
            "created_at",
            *FEATURE_BUCKET_KEYS,
            "feature_bucket_id",
            "signals",
            "filled_signals",
            "avg_fill_rate",
            "avg_realized_edge_after_slippage",
            "adverse_edge_rate",
            "max_drawdown",
            "fill_rate_lift",
            "realized_edge_lift",
            "adverse_edge_rate_lift",
            "should_block_candidate",
            "candidate_reason",
            "run_feature_decision",
        ]
    )


def empty_bucket_stability() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            *FEATURE_BUCKET_KEYS,
            "feature_bucket_id",
            "observed_runs",
            "coverage_rate",
            "promote_runs",
            "keep_diagnostic_runs",
            "reject_runs",
            "block_candidate_runs",
            "block_candidate_rate",
            "block_transition_count",
            "block_transition_rate",
            "dominant_candidate_reason",
            "stability_class",
        ]
    )


def typed_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def main() -> int:
    parser = argparse.ArgumentParser(prog="feature-decision-history")
    parser.add_argument("--manifest-root", default="data_lake/research_runs")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--run-id", action="append", dest="run_ids")
    parser.add_argument("--latest", type=int)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    report = build_feature_decision_history(
        Path(args.manifest_root),
        Path(args.output_dir),
        run_ids=args.run_ids,
        latest=args.latest,
    )
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
