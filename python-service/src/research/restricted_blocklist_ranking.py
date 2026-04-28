import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPORT_VERSION = "restricted_blocklist_ranking_v1"


def rank_restricted_blocklist_observations(
    observation_roots: list[Path],
) -> dict[str, object]:
    observations = [observation_row(path) for path in observation_roots]
    ranked = sorted(
        observations,
        key=lambda item: (
            numeric_or_default(item.get("score"), -1_000_000.0),
            str(item.get("blocklist_kind") or ""),
            str(item.get("observation_root") or ""),
        ),
        reverse=True,
    )
    top = ranked[0] if ranked else None
    return {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "observation_roots": [str(path) for path in observation_roots],
        "observations": ranked,
        "top_candidate": top,
        "summary": {
            "observations": len(ranked),
            "complete_observations": sum(
                1 for item in ranked if item.get("status") == "complete"
            ),
            "insufficient_evidence_observations": sum(
                1 for item in ranked if item.get("status") == "insufficient_evidence"
            ),
            "repeat_observation_candidates": sum(
                1
                for item in ranked
                if item.get("restricted_decision") == "REPEAT_OBSERVATION"
            ),
            "blocked_observations": sum(
                1 for item in ranked if typed_list(item.get("blockers"))
            ),
        },
        "can_execute_trades": False,
    }


def observation_row(observation_root: Path) -> dict[str, object]:
    summary = read_json(observation_root / "restricted_blocklist_observation_summary.json")
    failure = read_json(observation_root / "restricted_blocklist_observation_failure.json")
    comparison_report = read_json(observation_root / "comparison.json")
    promotion = read_json(observation_root / "research_promotion_decision.json")
    decision = read_json(observation_root / "restricted_blocklist_decision.json")
    diagnostics = read_json(observation_root / "restricted_blocklist_diagnostics.json")
    missing = missing_artifacts(
        observation_root,
        (
            "restricted_blocklist_observation_summary.json",
            "comparison.json",
            "research_promotion_decision.json",
            "restricted_blocklist_decision.json",
            "restricted_blocklist_diagnostics.json",
        ),
    )
    failure_status = failure.get("status")
    failure_diagnostics = typed_dict(failure.get("diagnostics"))
    failure_plan = typed_dict(failure.get("plan"))
    has_failure = failure_status == "insufficient_evidence"
    comparison = typed_dict(comparison_report.get("comparison"))
    blocked_changes = typed_dict(comparison.get("blocked_segment_changes"))
    comparability = typed_dict(comparison.get("segment_comparability"))
    fixed_universe = typed_dict(comparability.get("fixed_market_universe"))
    metric_deltas = metric_delta_index(comparison)
    efficacy = typed_dict(diagnostics.get("efficacy"))
    risk_migration = typed_dict(efficacy.get("risk_migration"))
    net_effect = typed_dict(efficacy.get("net_effect"))
    checks = check_index(promotion)
    blockers = ranking_blockers(
        missing_artifact_names=missing,
        failure=failure,
        decision=decision,
        promotion=promotion,
        comparison=comparison,
        diagnostics=diagnostics,
        fixed_universe=fixed_universe,
        blocked_changes=blocked_changes,
        checks=checks,
    )
    row: dict[str, object] = {
        "observation_root": str(observation_root),
        "status": "insufficient_evidence"
        if has_failure
        else "missing_artifacts"
        if missing
        else "complete",
        "missing_artifacts": missing,
        "blocklist_kind": summary.get("blocklist_kind")
        or failure_plan.get("blocklist_kind"),
        "blocklist_path": summary.get("blocklist_path")
        or failure_plan.get("blocklist_path"),
        "duration_seconds": summary.get("duration_seconds")
        or failure_plan.get("duration_seconds"),
        "market_asset_ids_sha256": summary.get("market_asset_ids_sha256")
        or failure_plan.get("market_asset_ids_sha256"),
        "research_promotion_decision": promotion.get("decision"),
        "restricted_decision": decision.get("restricted_decision"),
        "restricted_decision_reason": decision.get("reason"),
        "comparison_verdict": comparison.get("verdict"),
        "fixed_market_universe_status": fixed_universe.get("status"),
        "segment_comparability_status": comparability.get("status"),
        "risk_migration_status": efficacy.get("status"),
        "unexpected_blocked_segments": numeric_or_none(
            blocked_changes.get("unexpected_newly_blocked_count")
        ),
        "migrated_risk_segments": numeric_or_none(
            risk_migration.get("unexpected_blocked_segments")
        ),
        "migrated_risk_signal_ratio": numeric_or_none(
            net_effect.get("unexpected_to_expected_signal_ratio")
        ),
        "realized_edge_delta": metric_deltas.get("realized_edge"),
        "fill_rate_delta": metric_deltas.get("fill_rate"),
        "drawdown_delta": metric_deltas.get("drawdown"),
        "max_abs_simulator_fill_rate_delta_delta": metric_deltas.get(
            "max_abs_simulator_fill_rate_delta"
        ),
        "stale_data_rate_delta": metric_deltas.get("stale_data_rate"),
        "reconciliation_divergence_rate_delta": metric_deltas.get(
            "reconciliation_divergence_rate"
        ),
        "failed_checks": failed_check_names(checks),
        "failure_status": failure_status,
        "failure_reason": failure.get("reason"),
        "failure_stage": failure.get("stage"),
        "failure_exit_code": failure.get("exit_code"),
        "dry_run_exit_code": failure.get("dry_run_exit_code"),
        "failure_classification": failure_diagnostics.get("classification"),
        "failure_diagnosis_hints": failure_diagnostics.get("diagnosis_hints")
        if isinstance(failure_diagnostics.get("diagnosis_hints"), list)
        else [],
        "pipeline_report_root_exists": failure_diagnostics.get(
            "candidate_report_root_exists"
        ),
        "pipeline_data_lake_root_exists": failure_diagnostics.get(
            "data_lake_root_exists"
        ),
        "blockers": blockers,
        "can_execute_trades": False,
    }
    row["score"] = ranking_score(row)
    row["recommendation"] = ranking_recommendation(row)
    return row


def ranking_score(row: dict[str, object]) -> float:
    if row.get("status") == "insufficient_evidence":
        return -500_000.0
    if row.get("status") != "complete":
        return -1_000_000.0
    score = 0.0
    restricted_decision = row.get("restricted_decision")
    if restricted_decision == "REPEAT_OBSERVATION":
        score += 1_000.0
    elif restricted_decision == "NEED_MORE_DATA":
        score += 100.0
    elif restricted_decision == "REJECT":
        score -= 100.0
    if row.get("fixed_market_universe_status") == "match":
        score += 50.0
    score += numeric_or_default(row.get("realized_edge_delta"), 0.0) * 100.0
    score += numeric_or_default(row.get("fill_rate_delta"), 0.0) * 50.0
    score -= numeric_or_default(row.get("drawdown_delta"), 0.0) * 50.0
    score -= numeric_or_default(row.get("unexpected_blocked_segments"), 0.0) * 100.0
    score -= numeric_or_default(row.get("migrated_risk_signal_ratio"), 0.0) * 10.0
    score -= len(typed_list(row.get("blockers"))) * 25.0
    return score


def ranking_recommendation(row: dict[str, object]) -> str:
    blockers = typed_list(row.get("blockers"))
    if row.get("status") == "insufficient_evidence":
        if "missing_signals_stream_progress" in blockers or row.get(
            "failure_classification"
        ) == "no_dry_run_execution_reports":
            return "relax_variant_candidate"
        return "repair_pipeline_before_repeat"
    if row.get("status") != "complete":
        return "repair_missing_artifacts"
    if row.get("restricted_decision") == "REPEAT_OBSERVATION" and not blockers:
        return "repeat_observation"
    if row.get("risk_migration_status") == "risk_migration_detected":
        return "test_migrated_risk_variant"
    if row.get("restricted_decision") == "NEED_MORE_DATA":
        return "repeat_with_more_data"
    return "reject_or_redesign"


def ranking_blockers(
    *,
    missing_artifact_names: list[str],
    failure: dict[str, object],
    decision: dict[str, object],
    promotion: dict[str, object],
    comparison: dict[str, Any],
    diagnostics: dict[str, object],
    fixed_universe: dict[str, Any],
    blocked_changes: dict[str, Any],
    checks: dict[str, dict[str, object]],
) -> list[str]:
    blockers: list[str] = []
    failure_status = failure.get("status")
    if failure_status == "insufficient_evidence":
        blockers.append("insufficient_evidence")
        reason = failure.get("reason")
        if isinstance(reason, str) and reason:
            blockers.append(reason)
        diagnostics = typed_dict(failure.get("diagnostics"))
        classification = diagnostics.get("classification")
        if isinstance(classification, str) and classification:
            blockers.append(classification)
        preflight = typed_dict(diagnostics.get("real_dry_run_preflight"))
        blockers.extend(
            item for item in typed_list(preflight.get("blockers")) if isinstance(item, str)
        )
    elif missing_artifact_names:
        blockers.append("missing_artifacts")
    if fixed_universe.get("status") not in (None, "match", "not_applicable"):
        blockers.append("fixed_market_universe_not_matched")
    if comparison.get("verdict") == "no_comparable":
        blockers.append("not_comparable")
    if promotion.get("decision") == "REJECT":
        blockers.append("promotion_rejected")
    if decision.get("restricted_decision") == "REJECT":
        blockers.append("restricted_decision_rejected")
    if typed_dict(diagnostics.get("efficacy")).get("status") == "risk_migration_detected":
        blockers.append("risk_migration_detected")
    if numeric_or_default(
        blocked_changes.get("unexpected_newly_blocked_count"), 0.0
    ) > 0:
        blockers.append("unexpected_blocked_segments")
    if checks.get("candidate_absolute_gate_passed", {}).get("status") == "FAIL":
        blockers.append("candidate_absolute_gate_failed")
    if checks.get("migrated_risk", {}).get("status") == "FAIL":
        blockers.append("migrated_risk_gate_failed")
    return sorted(set(blockers))


def metric_delta_index(comparison: dict[str, Any]) -> dict[str, float]:
    output: dict[str, float] = {}
    metric_deltas = comparison.get("metric_deltas")
    if not isinstance(metric_deltas, list):
        return output
    for item in metric_deltas:
        if not isinstance(item, dict):
            continue
        metric = item.get("metric")
        delta = numeric_or_none(item.get("delta"))
        if isinstance(metric, str) and delta is not None:
            output[metric] = delta
    return output


def check_index(report: dict[str, object]) -> dict[str, dict[str, object]]:
    checks = report.get("checks")
    if not isinstance(checks, list):
        return {}
    output: dict[str, dict[str, object]] = {}
    for item in checks:
        if not isinstance(item, dict):
            continue
        name = item.get("check_name")
        if isinstance(name, str):
            output[name] = item
    return output


def failed_check_names(checks: dict[str, dict[str, object]]) -> list[str]:
    return sorted(
        name for name, item in checks.items() if item.get("status") == "FAIL"
    )


def missing_artifacts(root: Path, names: tuple[str, ...]) -> list[str]:
    return [name for name in names if not (root / name).exists()]


def read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def typed_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def typed_list(value: object) -> list[Any]:
    return value if isinstance(value, list) else []


def numeric_or_none(value: object) -> float | None:
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return None


def numeric_or_default(value: object, default: float) -> float:
    parsed = numeric_or_none(value)
    return parsed if parsed is not None else default


def main() -> int:
    parser = argparse.ArgumentParser(prog="restricted-blocklist-ranking")
    parser.add_argument(
        "--observation-root",
        type=Path,
        action="append",
        required=True,
        help="restricted observation report root; repeat for multiple variants",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    report = rank_restricted_blocklist_observations(args.observation_root)
    if args.output:
        write_json(args.output, report)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
