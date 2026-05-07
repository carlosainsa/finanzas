import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPORT_VERSION = "profile_observation_comparison_v1"


def create_profile_observation_comparison(
    report_roots: list[Path],
) -> dict[str, object]:
    observations = [profile_observation(root) for root in report_roots]
    return {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_profile_observation_comparison_only",
        "counts": {
            "report_roots": len(report_roots),
            "observations": len(observations),
        },
        "observations": observations,
        "pairwise_deltas": pairwise_deltas(observations),
        "artifact_paths": [str(root) for root in report_roots],
    }


def profile_observation(report_root: Path) -> dict[str, object]:
    evidence = read_json(report_root / "real_dry_run_evidence.json")
    promotion = read_json(report_root / "pre_live_promotion.json")
    quote = read_json(report_root / "quote_execution_diagnostics.json")
    rejection = read_json(report_root / "signal_rejection_diagnostics.json")
    go_no_go = read_json(report_root / "go_no_go.json")
    metrics = typed_dict(promotion.get("metrics"))
    quote_summary = typed_dict(quote.get("summary"))
    universe_selection_path = str(
        evidence.get("execution_probe_universe_selection_path") or ""
    )
    universe_selection = (
        read_json(Path(universe_selection_path)) if universe_selection_path else {}
    )
    universe_config = typed_dict(universe_selection.get("config"))
    return {
        "run_id": report_root.name,
        "report_root": str(report_root),
        "profile": evidence.get("predictor_strategy_profile"),
        "quote_placement": evidence.get("predictor_quote_placement"),
        "capture_seconds": evidence.get("capture_seconds"),
        "market_asset_ids_count": evidence.get("market_asset_ids_count"),
        "market_asset_ids_sha256": evidence.get("market_asset_ids_sha256"),
        "market_timing_selection": {
            "source_path": universe_selection_path or None,
            "status": universe_selection.get("status"),
            "profile": universe_selection.get("profile"),
            "market_timing_filter": universe_config.get("market_timing_filter"),
            "min_future_touch_rate": universe_config.get("min_future_touch_rate"),
            "min_timing_signals": universe_config.get("min_timing_signals"),
            "min_avg_opportunity_spread": universe_config.get(
                "min_avg_opportunity_spread"
            ),
            "max_avg_opportunity_spread": universe_config.get(
                "max_avg_opportunity_spread"
            ),
            "market_asset_ids_count": universe_selection.get("market_asset_ids_count"),
            "market_asset_ids_sha256": universe_selection.get(
                "market_asset_ids_sha256"
            ),
            "selection_reason": universe_selection.get("selection_reason"),
        },
        "stream_lengths": evidence.get("stream_lengths"),
        "report_status_counts": evidence.get("recent_report_status_counts"),
        "activity": {
            "orderbook_snapshots": metrics.get("orderbook_snapshots"),
            "signals": metrics.get("signals"),
            "filled_signals": metrics.get("filled_signals"),
            "signals_without_observed_report": quote_summary.get(
                "signals_without_observed_report"
            ),
        },
        "fills": {
            "fill_rate": metrics.get("fill_rate"),
            "dry_run_observed_fill_rate": metrics.get("dry_run_observed_fill_rate"),
            "observed_fill_rate": quote_summary.get("observed_fill_rate"),
            "synthetic_fill_rate": quote_summary.get("synthetic_fill_rate"),
            "adjusted_synthetic_fill_rate": quote_summary.get(
                "adjusted_synthetic_fill_rate"
            ),
            "synthetic_only_signals": quote_summary.get("synthetic_only_signals"),
            "dry_run_unfilled_but_synthetic_available": quote_summary.get(
                "dry_run_unfilled_but_synthetic_available"
            ),
            "fill_rate_gap": fill_rate_gap(quote_summary),
            "adjusted_fill_rate_gap": quote_summary.get("adjusted_fill_rate_gap"),
        },
        "quote_policy": {
            "avg_no_fill_distance_to_touch": quote_summary.get(
                "avg_no_fill_distance_to_touch"
            ),
            "avg_no_fill_distance_to_mid": quote_summary.get(
                "avg_no_fill_distance_to_mid"
            ),
            "avg_no_fill_spread": quote_summary.get("avg_no_fill_spread"),
            "no_fill_future_touch_rate": quote_summary.get("no_fill_future_touch_rate"),
            "avg_required_quote_move": quote_summary.get("avg_required_quote_move"),
        },
        "risk": {
            "realized_edge": metrics.get("realized_edge"),
            "adverse_selection": metrics.get("adverse_selection"),
            "drawdown": metrics.get("drawdown"),
            "stale_data_rate": metrics.get("stale_data_rate"),
            "test_brier_score": metrics.get("test_brier_score"),
            "reconciliation_divergence_rate": metrics.get(
                "reconciliation_divergence_rate"
            ),
        },
        "blockers": [item.get("check_name") for item in list_of_dicts(go_no_go.get("blockers"))],
        "signal_rejection": {
            "summary": rejection.get("summary"),
            "profile_comparison": rejection.get("profile_comparison"),
        },
        "synthetic_vs_observed_gap": quote.get("synthetic_vs_observed_gap"),
    }


def pairwise_deltas(observations: list[dict[str, object]]) -> list[dict[str, object]]:
    deltas: list[dict[str, object]] = []
    for previous, current in zip(observations, observations[1:], strict=False):
        deltas.append(
            {
                "baseline_run_id": previous.get("run_id"),
                "candidate_run_id": current.get("run_id"),
                "baseline_profile": previous.get("profile"),
                "candidate_profile": current.get("profile"),
                "activity_deltas": metric_deltas(
                    typed_dict(previous.get("activity")),
                    typed_dict(current.get("activity")),
                    ("signals", "filled_signals", "signals_without_observed_report"),
                ),
                "fill_deltas": metric_deltas(
                    typed_dict(previous.get("fills")),
                    typed_dict(current.get("fills")),
                    (
                        "fill_rate",
                        "dry_run_observed_fill_rate",
                        "observed_fill_rate",
                        "synthetic_fill_rate",
                        "adjusted_synthetic_fill_rate",
                        "fill_rate_gap",
                        "adjusted_fill_rate_gap",
                    ),
                ),
                "risk_deltas": metric_deltas(
                    typed_dict(previous.get("risk")),
                    typed_dict(current.get("risk")),
                    (
                        "realized_edge",
                        "adverse_selection",
                        "drawdown",
                        "stale_data_rate",
                        "test_brier_score",
                    ),
                ),
                "quote_policy_deltas": metric_deltas(
                    typed_dict(previous.get("quote_policy")),
                    typed_dict(current.get("quote_policy")),
                    (
                        "avg_no_fill_distance_to_touch",
                        "avg_no_fill_distance_to_mid",
                        "avg_no_fill_spread",
                        "no_fill_future_touch_rate",
                        "avg_required_quote_move",
                    ),
                ),
            }
        )
    return deltas


def metric_deltas(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    metrics: tuple[str, ...],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for metric in metrics:
        baseline_value = numeric_or_none(baseline.get(metric))
        candidate_value = numeric_or_none(candidate.get(metric))
        rows.append(
            {
                "metric": metric,
                "baseline": baseline_value,
                "candidate": candidate_value,
                "delta": (
                    candidate_value - baseline_value
                    if baseline_value is not None and candidate_value is not None
                    else None
                ),
            }
        )
    return rows


def fill_rate_gap(summary: dict[str, Any]) -> float | None:
    synthetic = numeric_or_none(summary.get("synthetic_fill_rate"))
    observed = numeric_or_none(summary.get("observed_fill_rate"))
    if synthetic is None or observed is None:
        return None
    return synthetic - observed


def read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


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
    parser = argparse.ArgumentParser(prog="profile-observation-comparison")
    parser.add_argument("--report-root", action="append", required=True)
    parser.add_argument("--output")
    args = parser.parse_args()
    report = create_profile_observation_comparison(
        [Path(value) for value in args.report_root]
    )
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        Path(args.output).write_text(payload, encoding="utf-8")
    print(payload, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
