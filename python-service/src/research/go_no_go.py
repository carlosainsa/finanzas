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


def create_go_no_go_report(
    db_path: Path, config: PromotionConfig = PromotionConfig()
) -> dict[str, object]:
    promotion = create_promotion_report(db_path, config)
    checks = normalize_checks(promotion.get("checks"))
    blockers = [check for check in checks if not bool(check.get("passed"))]
    metrics = typed_dict(promotion.get("metrics"))
    decision = "GO" if not blockers and bool(promotion.get("passed")) else "NO_GO"
    return {
        "report_version": GO_NO_GO_REPORT_VERSION,
        "source_report_version": promotion.get("report_version"),
        "decision": decision,
        "passed": decision == "GO",
        "can_execute_trades": False,
        "reason": "all_quantitative_gates_passed"
        if decision == "GO"
        else "quantitative_gate_failure",
        "config": asdict(config),
        "metrics": selected_metrics(metrics),
        "checks": checks,
        "blockers": blockers,
        "availability": promotion.get("availability", {}),
        "source": str(db_path),
    }


def export_go_no_go_report(
    db_path: Path, output_dir: Path, config: PromotionConfig = PromotionConfig()
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    report = create_go_no_go_report(db_path, config)
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Export pre-live go/no-go gates")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--min-signals", type=int)
    parser.add_argument("--min-realized-edge", type=float)
    parser.add_argument("--min-fill-rate", type=float)
    parser.add_argument("--max-abs-slippage", type=float)
    parser.add_argument("--max-adverse-selection-rate", type=float)
    parser.add_argument("--max-drawdown", type=float)
    parser.add_argument("--max-brier-score", type=float)
    args = parser.parse_args()

    config = promotion_config_from_args(args)
    report = export_go_no_go_report(Path(args.duckdb), Path(args.output_dir), config)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def promotion_config_from_args(args: argparse.Namespace) -> PromotionConfig:
    defaults = PromotionConfig()
    return PromotionConfig(
        min_signals=args.min_signals
        if args.min_signals is not None
        else defaults.min_signals,
        min_realized_edge=args.min_realized_edge
        if args.min_realized_edge is not None
        else defaults.min_realized_edge,
        min_fill_rate=args.min_fill_rate
        if args.min_fill_rate is not None
        else defaults.min_fill_rate,
        max_abs_slippage=args.max_abs_slippage
        if args.max_abs_slippage is not None
        else defaults.max_abs_slippage,
        max_adverse_selection_rate=args.max_adverse_selection_rate
        if args.max_adverse_selection_rate is not None
        else defaults.max_adverse_selection_rate,
        max_drawdown=args.max_drawdown
        if args.max_drawdown is not None
        else defaults.max_drawdown,
        max_brier_score=args.max_brier_score
        if args.max_brier_score is not None
        else defaults.max_brier_score,
    )


if __name__ == "__main__":
    raise SystemExit(main())
