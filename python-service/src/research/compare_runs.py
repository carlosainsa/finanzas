import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd  # type: ignore[import-untyped]


DEFAULT_METRICS = (
    "realized_edge",
    "fill_rate",
    "slippage",
    "drawdown",
    "stale_data_rate",
    "reconciliation_divergence_rate",
    "test_brier_score",
    "advisory_failed",
)
LOWER_IS_BETTER = {
    "slippage",
    "drawdown",
    "stale_data_rate",
    "reconciliation_divergence_rate",
    "test_brier_score",
    "advisory_failed",
}


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
        verdict=comparison_verdict(deltas),
    )
    return {
        "baseline": normalize_row(baseline),
        "candidate": normalize_row(candidate),
        "comparison": {
            "baseline_run_id": comparison.baseline_run_id,
            "candidate_run_id": comparison.candidate_run_id,
            "metric_deltas": comparison.metric_deltas,
            "verdict": comparison.verdict,
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
        "direction": "lower_is_better" if metric in LOWER_IS_BETTER else "higher_is_better",
        "improved": metric_improved(metric, delta),
    }


def metric_improved(metric: str, delta: float | None) -> bool | None:
    if delta is None or delta == 0:
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


def normalize_row(row: dict[str, Any]) -> dict[str, object]:
    return {key: normalize_value(value) for key, value in row.items()}


def normalize_value(value: object) -> object:
    if pd.isna(value):
        return None
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


def format_table(rows: list[dict[str, object]], columns: tuple[str, ...]) -> str:
    widths = {
        column: max(len(column), *(len(format_cell(row.get(column))) for row in rows))
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
