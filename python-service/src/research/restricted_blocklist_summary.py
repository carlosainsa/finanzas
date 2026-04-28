import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.research.restricted_blocklist_ranking import (
    metric_delta_index,
    numeric_or_none,
    observation_row,
    read_json,
    typed_dict,
    write_json,
)


REPORT_VERSION = "restricted_blocklist_observation_summary_v2"
CORE_METRICS = (
    "realized_edge",
    "fill_rate",
    "drawdown",
    "adverse_selection",
    "max_abs_simulator_fill_rate_delta",
    "stale_data_rate",
    "reconciliation_divergence_rate",
)


def build_restricted_blocklist_observation_summary(
    *,
    plan: dict[str, object],
    candidate_report_root: Path,
    output_dir: Path,
    decision_status: int,
) -> dict[str, object]:
    comparison_report = read_json(output_dir / "comparison.json")
    promotion = read_json(output_dir / "research_promotion_decision.json")
    decision = read_json(output_dir / "restricted_blocklist_decision.json")
    diagnostics = read_json(output_dir / "restricted_blocklist_diagnostics.json")
    comparison = typed_dict(comparison_report.get("comparison"))
    risk = risk_summary(comparison, diagnostics)
    payload: dict[str, object] = {
        **plan,
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "candidate_report_root": str(candidate_report_root),
        "output_dir": str(output_dir),
        "comparison_path": str(output_dir / "comparison.json"),
        "restricted_blocklist_diagnostics_path": str(
            output_dir / "restricted_blocklist_diagnostics.json"
        ),
        "migrated_risk_blocklist_variants_path": str(
            output_dir / "migrated_risk_blocklist_variants.json"
        ),
        "research_promotion_decision_path": str(
            output_dir / "research_promotion_decision.json"
        ),
        "restricted_blocklist_decision_path": str(
            output_dir / "restricted_blocklist_decision.json"
        ),
        "decision": promotion.get("decision"),
        "decision_exit_code": decision_status,
        "final_decision": {
            "research_promotion_decision": promotion.get("decision"),
            "restricted_decision": decision.get("restricted_decision"),
            "restricted_decision_reason": decision.get("reason"),
            "restricted_next_step": decision.get("next_step"),
        },
        "metric_comparison": metric_comparison(comparison),
        "risk_comparison": risk,
        "stable_review_fields": {
            "baseline_report_root": plan.get("baseline_report_root"),
            "candidate_report_root": str(candidate_report_root),
            "blocklist_kind": plan.get("blocklist_kind"),
            "market_asset_ids_sha256": plan.get("market_asset_ids_sha256"),
            "comparison_verdict": comparison.get("verdict"),
            "segment_comparability_status": risk.get("segment_comparability_status"),
            "risk_migration_status": risk.get("risk_migration_status"),
            "unexpected_blocked_segments": risk.get("unexpected_blocked_segments"),
            "migrated_risk_segments": risk.get("migrated_risk_segments"),
            "migrated_risk_signal_ratio": risk.get("migrated_risk_signal_ratio"),
            "research_promotion_decision": promotion.get("decision"),
            "restricted_decision": decision.get("restricted_decision"),
        },
        "can_execute_trades": False,
    }
    if decision:
        row = observation_row(output_dir)
        payload["ranking_snapshot"] = {
            "score": row.get("score"),
            "recommendation": row.get("recommendation"),
            "blockers": row.get("blockers"),
            "status": row.get("status"),
        }
    return payload


def metric_comparison(comparison: dict[str, Any]) -> dict[str, object]:
    deltas = comparison.get("metric_deltas")
    rows = deltas if isinstance(deltas, list) else []
    by_metric: dict[str, dict[str, object]] = {}
    for item in rows:
        if not isinstance(item, dict):
            continue
        metric = item.get("metric")
        if isinstance(metric, str) and metric in CORE_METRICS:
            by_metric[metric] = {
                "baseline": numeric_or_none(item.get("baseline")),
                "candidate": numeric_or_none(item.get("candidate")),
                "delta": numeric_or_none(item.get("delta")),
                "direction": item.get("direction"),
                "improved": item.get("improved"),
            }
    for metric in CORE_METRICS:
        by_metric.setdefault(
            metric,
            {
                "baseline": None,
                "candidate": None,
                "delta": None,
                "direction": None,
                "improved": None,
            },
        )
    return {
        "metrics": by_metric,
        "deltas": metric_delta_index(comparison),
    }


def risk_summary(
    comparison: dict[str, Any],
    diagnostics: dict[str, object],
) -> dict[str, object]:
    blocked_changes = typed_dict(comparison.get("blocked_segment_changes"))
    comparability = typed_dict(comparison.get("segment_comparability"))
    fixed_universe = typed_dict(comparability.get("fixed_market_universe"))
    efficacy = typed_dict(diagnostics.get("efficacy"))
    risk_migration = typed_dict(efficacy.get("risk_migration"))
    net_effect = typed_dict(efficacy.get("net_effect"))
    return {
        "comparison_verdict": comparison.get("verdict"),
        "segment_comparability_status": comparability.get("status"),
        "fixed_market_universe_status": fixed_universe.get("status"),
        "unexpected_blocked_segments": numeric_or_none(
            blocked_changes.get("unexpected_newly_blocked_count")
        ),
        "expected_newly_blocked_segments": numeric_or_none(
            blocked_changes.get("expected_newly_blocked_count")
        ),
        "candidate_blocked_segments": numeric_or_none(blocked_changes.get("candidate_count")),
        "baseline_blocked_segments": numeric_or_none(blocked_changes.get("baseline_count")),
        "risk_migration_status": efficacy.get("status"),
        "migrated_risk_segments": numeric_or_none(
            risk_migration.get("unexpected_blocked_segments")
        ),
        "migrated_risk_signal_ratio": numeric_or_none(
            net_effect.get("unexpected_to_expected_signal_ratio")
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(prog="restricted-blocklist-summary")
    parser.add_argument("--plan-json", required=True)
    parser.add_argument("--candidate-report-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--decision-status", type=int, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    plan = json.loads(args.plan_json)
    if not isinstance(plan, dict):
        raise SystemExit("--plan-json must decode to an object")
    payload = build_restricted_blocklist_observation_summary(
        plan=plan,
        candidate_report_root=args.candidate_report_root,
        output_dir=args.output_dir,
        decision_status=args.decision_status,
    )
    output = args.output or args.output_dir / "restricted_blocklist_observation_summary.json"
    write_json(output, payload)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
