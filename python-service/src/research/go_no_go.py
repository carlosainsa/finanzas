import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from src.research.pre_live_promotion import (
    PromotionConfig,
    create_promotion_report,
)


GO_NO_GO_REPORT_VERSION = "go_no_go_v1"
THRESHOLD_SET_VERSION = "go_no_go_thresholds_v1"
DEFAULT_PROFILE = "dev"
PROFILE_CONFIGS: dict[str, PromotionConfig] = {
    "dev": PromotionConfig(),
    "paper": PromotionConfig(
        min_capture_duration_ms=60_000,
        min_signals=25,
        min_realized_edge=0.0,
        min_fill_rate=0.05,
        max_abs_slippage=0.05,
        max_adverse_selection_rate=0.50,
        max_drawdown=0.25,
        max_stale_data_rate=0.10,
        max_reconciliation_divergence_rate=0.02,
        max_brier_score=0.30,
    ),
    "pre_live": PromotionConfig(
        min_capture_duration_ms=3_600_000,
        min_signals=250,
        min_realized_edge=0.005,
        min_fill_rate=0.10,
        min_dry_run_observed_fill_rate=0.05,
        max_abs_simulator_fill_rate_delta=0.35,
        max_abs_slippage=0.03,
        max_adverse_selection_rate=0.40,
        max_drawdown=0.10,
        max_stale_data_rate=0.03,
        max_reconciliation_divergence_rate=0.005,
        max_brier_score=0.25,
    ),
    "live_candidate": PromotionConfig(
        min_capture_duration_ms=21_600_000,
        min_signals=1_000,
        min_realized_edge=0.01,
        min_fill_rate=0.15,
        min_dry_run_observed_fill_rate=0.10,
        max_abs_simulator_fill_rate_delta=0.20,
        max_abs_slippage=0.02,
        max_adverse_selection_rate=0.35,
        max_drawdown=0.05,
        max_stale_data_rate=0.01,
        max_reconciliation_divergence_rate=0.001,
        max_brier_score=0.22,
    ),
}


def create_go_no_go_report(
    db_path: Path,
    config: PromotionConfig | None = None,
    profile: str = DEFAULT_PROFILE,
) -> dict[str, object]:
    resolved_config = config or config_for_profile(profile)
    promotion = create_promotion_report(db_path, resolved_config)
    checks = normalize_checks(promotion.get("checks"))
    blockers = [check for check in checks if not bool(check.get("passed"))]
    metrics = typed_dict(promotion.get("metrics"))
    decision = "GO" if not blockers and bool(promotion.get("passed")) else "NO_GO"
    return {
        "report_version": GO_NO_GO_REPORT_VERSION,
        "source_report_version": promotion.get("report_version"),
        "profile": profile,
        "threshold_set_version": THRESHOLD_SET_VERSION,
        "decision": decision,
        "passed": decision == "GO",
        "can_execute_trades": False,
        "reason": "all_quantitative_gates_passed"
        if decision == "GO"
        else "quantitative_gate_failure",
        "config": asdict(resolved_config),
        "metrics": selected_metrics(metrics),
        "checks": checks,
        "blockers": blockers,
        "availability": promotion.get("availability", {}),
        "source": str(db_path),
    }


def export_go_no_go_report(
    db_path: Path,
    output_dir: Path,
    config: PromotionConfig | None = None,
    profile: str = DEFAULT_PROFILE,
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    report = create_go_no_go_report(db_path, config=config, profile=profile)
    (output_dir / "go_no_go.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return report


def selected_metrics(metrics: dict[str, Any]) -> dict[str, object]:
    keys = (
        "signals",
        "filled_signals",
        "realized_edge",
        "fill_rate",
        "slippage",
        "avg_slippage",
        "adverse_selection",
        "adverse_selection_rate",
        "drawdown",
        "max_drawdown",
        "stale_data_rate",
        "reconciliation_divergence_rate",
        "test_brier_score",
        "test_log_loss",
        "dry_run_observed_fill_rate",
        "max_abs_simulator_fill_rate_delta",
    )
    return {key: metrics.get(key) for key in keys if key in metrics}


def normalize_checks(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    checks: list[dict[str, object]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        checks.append(
            {
                "check_name": item.get("check_name"),
                "metric_value": item.get("metric_value"),
                "threshold": item.get("threshold"),
                "passed": bool(item.get("passed")),
            }
        )
    return checks


def typed_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def config_for_profile(profile: str) -> PromotionConfig:
    try:
        return PROFILE_CONFIGS[profile]
    except KeyError as exc:
        valid = ", ".join(sorted(PROFILE_CONFIGS))
        raise ValueError(f"unknown go/no-go profile {profile!r}; expected one of: {valid}") from exc


def main() -> int:
    parser = argparse.ArgumentParser(description="Export pre-live go/no-go gates")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--profile", choices=sorted(PROFILE_CONFIGS), default=DEFAULT_PROFILE)
    parser.add_argument("--min-capture-duration-ms", type=int)
    parser.add_argument("--min-signals", type=int)
    parser.add_argument("--min-realized-edge", type=float)
    parser.add_argument("--min-fill-rate", type=float)
    parser.add_argument("--min-dry-run-observed-fill-rate", type=float)
    parser.add_argument("--max-abs-simulator-fill-rate-delta", type=float)
    parser.add_argument("--max-abs-slippage", type=float)
    parser.add_argument("--max-adverse-selection-rate", type=float)
    parser.add_argument("--max-drawdown", type=float)
    parser.add_argument("--max-stale-data-rate", type=float)
    parser.add_argument("--max-reconciliation-divergence-rate", type=float)
    parser.add_argument("--max-brier-score", type=float)
    parser.add_argument("--stale-gap-ms", type=int)
    args = parser.parse_args()

    config = promotion_config_from_args(args)
    report = export_go_no_go_report(
        Path(args.duckdb),
        Path(args.output_dir),
        config=config,
        profile=args.profile,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def promotion_config_from_args(args: argparse.Namespace) -> PromotionConfig:
    defaults = config_for_profile(str(args.profile))
    return PromotionConfig(
        min_capture_duration_ms=args.min_capture_duration_ms
        if args.min_capture_duration_ms is not None
        else defaults.min_capture_duration_ms,
        min_signals=args.min_signals
        if args.min_signals is not None
        else defaults.min_signals,
        min_realized_edge=args.min_realized_edge
        if args.min_realized_edge is not None
        else defaults.min_realized_edge,
        min_fill_rate=args.min_fill_rate
        if args.min_fill_rate is not None
        else defaults.min_fill_rate,
        min_dry_run_observed_fill_rate=args.min_dry_run_observed_fill_rate
        if args.min_dry_run_observed_fill_rate is not None
        else defaults.min_dry_run_observed_fill_rate,
        max_abs_simulator_fill_rate_delta=args.max_abs_simulator_fill_rate_delta
        if args.max_abs_simulator_fill_rate_delta is not None
        else defaults.max_abs_simulator_fill_rate_delta,
        max_abs_slippage=args.max_abs_slippage
        if args.max_abs_slippage is not None
        else defaults.max_abs_slippage,
        max_adverse_selection_rate=args.max_adverse_selection_rate
        if args.max_adverse_selection_rate is not None
        else defaults.max_adverse_selection_rate,
        max_drawdown=args.max_drawdown
        if args.max_drawdown is not None
        else defaults.max_drawdown,
        max_stale_data_rate=args.max_stale_data_rate
        if args.max_stale_data_rate is not None
        else defaults.max_stale_data_rate,
        max_reconciliation_divergence_rate=args.max_reconciliation_divergence_rate
        if args.max_reconciliation_divergence_rate is not None
        else defaults.max_reconciliation_divergence_rate,
        max_brier_score=args.max_brier_score
        if args.max_brier_score is not None
        else defaults.max_brier_score,
        stale_gap_ms=args.stale_gap_ms
        if args.stale_gap_ms is not None
        else defaults.stale_gap_ms,
    )


if __name__ == "__main__":
    raise SystemExit(main())
