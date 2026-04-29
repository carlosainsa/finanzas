import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd  # type: ignore[import-untyped]

from src.research.compare_runs import metric_delta


REPORT_VERSION = "strategy_family_comparison_v1"
DEFAULT_METRICS = (
    "signals",
    "fill_rate",
    "realized_edge",
    "drawdown",
    "adverse_selection",
    "stale_data_rate",
    "reconciliation_divergence_rate",
)
RUN_SCOPED_METRICS = {
    "stale_data_rate",
    "reconciliation_divergence_rate",
}
MIN_FILL_RATE = 0.10
MIN_REALIZED_EDGE = 0.0


@dataclass(frozen=True)
class FamilyMetrics:
    strategy_family: str
    model_version: str
    metrics: dict[str, object]
    metric_scope: dict[str, str]


def create_strategy_family_comparison(
    report_roots: list[Path],
    output_path: Path | None = None,
) -> dict[str, object]:
    runs = [load_run_report(root) for root in report_roots]
    families = family_rows(runs)
    decision = decision_for(families)
    report: dict[str, object] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision": decision["decision"],
        "decision_reason": decision["reason"],
        "input_report_roots": [str(root) for root in report_roots],
        "runs": runs,
        "families": families,
        "metric_notes": {
            "family_scope": (
                "signals, fill_rate, realized_edge and drawdown are aggregated from "
                "pre_live_promotion_segments.parquet by strategy/model_version."
            ),
            "strategy_scope": (
                "adverse_selection is joined from game_theory/adverse_selection_by_strategy.parquet "
                "when available; that artifact is strategy-scoped, not model-version scoped."
            ),
            "run_scope": (
                "stale_data_rate and reconciliation_divergence_rate are copied from the run-level "
                "pre_live_promotion.json because no family-level view exists yet."
            ),
        },
    }
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    return report


def load_run_report(report_root: Path) -> dict[str, object]:
    promotion = read_json(report_root / "pre_live_promotion.json")
    go_no_go = read_json(report_root / "go_no_go.json")
    manifest = read_json(report_root / "research_manifest.json")
    evidence = read_json(report_root / "real_dry_run_evidence.json")
    promotion_metrics = typed_dict(promotion.get("metrics"))
    run_id = str(manifest.get("run_id") or report_root.name)
    return {
        "run_id": run_id,
        "report_root": str(report_root),
        "predictor_strategy_profile": predictor_strategy_profile(evidence),
        "go_no_go": go_no_go_summary(go_no_go),
        "run_metrics": {
            "stale_data_rate": finite_number(promotion_metrics.get("stale_data_rate")),
            "reconciliation_divergence_rate": finite_number(
                promotion_metrics.get("reconciliation_divergence_rate")
            ),
        },
        "families": [
            family_to_dict(family)
            for family in load_family_metrics(report_root, evidence, promotion_metrics)
        ],
    }


def load_family_metrics(
    report_root: Path,
    evidence: dict[str, object],
    promotion_metrics: dict[str, object],
) -> list[FamilyMetrics]:
    segments_path = report_root / "pre_live_promotion" / "pre_live_promotion_segments.parquet"
    if not segments_path.exists():
        family = str(predictor_strategy_profile(evidence))
        return [
            FamilyMetrics(
                strategy_family=family,
                model_version=family,
                metrics={
                    "signals": None,
                    "fill_rate": None,
                    "realized_edge": None,
                    "drawdown": None,
                    "adverse_selection": None,
                    "stale_data_rate": finite_number(
                        promotion_metrics.get("stale_data_rate")
                    ),
                    "reconciliation_divergence_rate": finite_number(
                        promotion_metrics.get("reconciliation_divergence_rate")
                    ),
                    "missing_segments": True,
                },
                metric_scope={
                    "signals": "missing",
                    "fill_rate": "missing",
                    "realized_edge": "missing",
                    "drawdown": "missing",
                    "adverse_selection": "missing",
                    "stale_data_rate": "run",
                    "reconciliation_divergence_rate": "run",
                },
            )
        ]

    frame = pd.read_parquet(segments_path)
    if frame.empty:
        return []
    for column in ("filled_signals", "signals", "pnl", "max_drawdown", "filled_notional"):
        if column not in frame.columns:
            frame[column] = 0
    if "realized_edge" not in frame.columns:
        frame["realized_edge"] = None
    if "model_version" not in frame.columns:
        frame["model_version"] = "unknown"
    if "strategy" not in frame.columns:
        frame["strategy"] = "unknown"

    adverse_by_strategy = load_adverse_selection(report_root)
    result: list[FamilyMetrics] = []
    grouped = frame.groupby(["strategy", "model_version"], dropna=False)
    for (strategy_value, model_version_value), group in grouped:
        strategy = str(strategy_value or "unknown")
        model_version = str(model_version_value or "unknown")
        signals = float(group["signals"].fillna(0).sum())
        filled_signals = float(group["filled_signals"].fillna(0).sum())
        weighted_edge = weighted_average(group, "realized_edge", "filled_signals")
        if weighted_edge is None:
            weighted_edge = weighted_average(group, "realized_edge", "signals")
        metrics: dict[str, object] = {
            "signals": signals,
            "fill_rate": filled_signals / signals if signals > 0 else None,
            "realized_edge": weighted_edge,
            "drawdown": finite_number(group["max_drawdown"].fillna(0).max()),
            "adverse_selection": adverse_by_strategy.get(strategy),
            "stale_data_rate": finite_number(promotion_metrics.get("stale_data_rate")),
            "reconciliation_divergence_rate": finite_number(
                promotion_metrics.get("reconciliation_divergence_rate")
            ),
        }
        result.append(
            FamilyMetrics(
                strategy_family=strategy_family_for(strategy, model_version, evidence),
                model_version=model_version,
                metrics=metrics,
                metric_scope={
                    "signals": "family",
                    "fill_rate": "family",
                    "realized_edge": "family",
                    "drawdown": "family",
                    "adverse_selection": (
                        "strategy" if adverse_by_strategy.get(strategy) is not None else "missing"
                    ),
                    "stale_data_rate": "run",
                    "reconciliation_divergence_rate": "run",
                },
            )
        )
    return sorted(result, key=lambda item: (item.strategy_family, item.model_version))


def family_rows(runs: list[dict[str, object]]) -> list[dict[str, object]]:
    latest_by_key: dict[tuple[str, str], dict[str, object]] = {}
    history_by_key: dict[tuple[str, str], list[dict[str, object]]] = {}
    for run in runs:
        for family in typed_list(run.get("families")):
            family_dict = typed_dict(family)
            key = (
                str(family_dict.get("strategy_family") or "unknown"),
                str(family_dict.get("model_version") or "unknown"),
            )
            entry = {
                "run_id": run.get("run_id"),
                "metrics": family_dict.get("metrics", {}),
                "metric_scope": family_dict.get("metric_scope", {}),
                "go_no_go": run.get("go_no_go", {}),
            }
            history_by_key.setdefault(key, []).append(entry)
            latest_by_key[key] = entry

    rows: list[dict[str, object]] = []
    baseline_latest = latest_non_conservative(history_by_key)
    for key, latest in sorted(latest_by_key.items()):
        metrics = typed_dict(latest.get("metrics"))
        baseline_metrics = baseline_latest if is_conservative(key[0]) else {}
        rows.append(
            {
                "strategy_family": key[0],
                "model_version": key[1],
                "latest_run_id": latest.get("run_id"),
                "observations": len(history_by_key.get(key, [])),
                "metrics": metrics,
                "metric_scope": latest.get("metric_scope", {}),
                "baseline_metrics": baseline_metrics,
                "deltas_vs_baseline": metric_deltas(baseline_metrics, metrics),
                "go_no_go": latest.get("go_no_go", {}),
            }
        )
    return rows


def decision_for(families: list[dict[str, object]]) -> dict[str, str]:
    conservative = [
        family
        for family in families
        if is_conservative(str(family.get("strategy_family") or ""))
    ]
    if not conservative:
        return {
            "decision": "RUN_CONSERVATIVE_OBSERVATION",
            "reason": "No conservative_v1 family was found in the compared runs.",
        }
    best = conservative[-1]
    metrics = typed_dict(best.get("metrics"))
    deltas = typed_list(best.get("deltas_vs_baseline"))
    improved_adverse_or_drawdown = any(
        typed_dict(delta).get("metric") in {"adverse_selection", "drawdown"}
        and typed_dict(delta).get("improved") is True
        for delta in deltas
    )
    fill_rate = finite_number(metrics.get("fill_rate"))
    realized_edge = finite_number(metrics.get("realized_edge"))
    if (
        improved_adverse_or_drawdown
        and fill_rate is not None
        and fill_rate >= MIN_FILL_RATE
        and realized_edge is not None
        and realized_edge > MIN_REALIZED_EDGE
    ):
        return {
            "decision": "REPEAT_CONSERVATIVE_OBSERVATION",
            "reason": (
                "conservative_v1 improved adverse selection or drawdown while keeping "
                "fill rate and realized edge above observation thresholds."
            ),
        }
    return {
        "decision": "REDESIGN_STRATEGY_AGAIN",
        "reason": (
            "conservative_v1 did not show enough evidence of lower adverse/drawdown "
            "without sacrificing fill rate or edge."
        ),
    }


def latest_non_conservative(
    history_by_key: dict[tuple[str, str], list[dict[str, object]]],
) -> dict[str, object]:
    for key, rows in reversed(list(history_by_key.items())):
        if not is_conservative(key[0]) and rows:
            metrics = typed_dict(rows[-1].get("metrics"))
            if metrics:
                return metrics
    return {}


def metric_deltas(
    baseline_metrics: dict[str, object], candidate_metrics: dict[str, object]
) -> list[dict[str, object]]:
    return [
        metric_delta(metric, baseline_metrics.get(metric), candidate_metrics.get(metric))
        for metric in DEFAULT_METRICS
        if metric in baseline_metrics or metric in candidate_metrics
    ]


def load_adverse_selection(report_root: Path) -> dict[str, float | None]:
    path = report_root / "game_theory" / "adverse_selection_by_strategy.parquet"
    if not path.exists():
        return {}
    frame = pd.read_parquet(path)
    if frame.empty or "strategy" not in frame.columns:
        return {}
    metric_column = next(
        (
            column
            for column in ("adverse_30s_rate", "adverse_selection", "adverse_selection_rate")
            if column in frame.columns
        ),
        None,
    )
    if metric_column is None:
        return {}
    result: dict[str, float | None] = {}
    for strategy, group in frame.groupby("strategy", dropna=False):
        result[str(strategy or "unknown")] = finite_number(group[metric_column].mean())
    return result


def strategy_family_for(
    strategy: str,
    model_version: str,
    evidence: dict[str, object],
) -> str:
    profile = predictor_strategy_profile(evidence)
    if profile != "unknown":
        return profile
    value = f"{strategy} {model_version}".lower()
    if "conservative" in value:
        return "conservative_v1"
    if "near_touch" in value:
        return "near_touch"
    if "passive_spread_capture" in value:
        return "baseline"
    return strategy or model_version or "unknown"


def predictor_strategy_profile(evidence: dict[str, object]) -> str:
    for key in ("predictor_strategy_profile", "predictor_profile"):
        value = evidence.get(key)
        if isinstance(value, str) and value:
            return value
    return "unknown"


def go_no_go_summary(go_no_go: dict[str, object]) -> dict[str, object]:
    blockers = [
        typed_dict(item).get("check_name")
        for item in typed_list(go_no_go.get("blockers"))
        if typed_dict(item).get("check_name")
    ]
    return {
        "decision": go_no_go.get("decision"),
        "passed": go_no_go.get("passed"),
        "profile": go_no_go.get("profile"),
        "blockers": blockers,
    }


def family_to_dict(family: FamilyMetrics) -> dict[str, object]:
    return {
        "strategy_family": family.strategy_family,
        "model_version": family.model_version,
        "metrics": family.metrics,
        "metric_scope": family.metric_scope,
    }


def weighted_average(frame: pd.DataFrame, value_column: str, weight_column: str) -> float | None:
    values = pd.to_numeric(frame[value_column], errors="coerce")
    weights = pd.to_numeric(frame[weight_column], errors="coerce").fillna(0)
    valid = values.notna() & (weights > 0)
    if not bool(valid.any()):
        return None
    weight_sum = float(weights[valid].sum())
    if weight_sum <= 0:
        return None
    return finite_number(float((values[valid] * weights[valid]).sum()) / weight_sum)


def is_conservative(strategy_family: str) -> bool:
    return "conservative" in strategy_family.lower()


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


def typed_list(value: object) -> list[object]:
    return value if isinstance(value, list) else []


def finite_number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        if pd.notna(number):
            return number
    return None


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="strategy-family-comparison")
    parser.add_argument(
        "--report-root",
        action="append",
        required=True,
        help="research report root to include; repeat to compare multiple runs",
    )
    parser.add_argument("--output")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    report = create_strategy_family_comparison(
        [Path(value) for value in args.report_root],
        output_path=Path(args.output) if args.output else None,
    )
    if args.json or not args.output:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
