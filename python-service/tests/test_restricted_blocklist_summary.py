import json
from pathlib import Path
from typing import Any, cast

from src.research.restricted_blocklist_summary import (
    build_restricted_blocklist_observation_summary,
)


def test_restricted_blocklist_summary_surfaces_stable_review_fields(
    tmp_path: Path,
) -> None:
    root = seed_restricted_summary_observation(tmp_path)
    plan: dict[str, object] = {
        "baseline_report_root": "/reports/baseline",
        "blocklist_kind": "restricted_input_plus_top_migrated_risk",
        "blocklist_path": "/blocklists/migrated.json",
        "duration_seconds": 900,
        "market_asset_ids_sha256": "hash",
        "can_execute_trades": False,
    }

    summary = build_restricted_blocklist_observation_summary(
        plan=plan,
        candidate_report_root=root,
        output_dir=root,
        decision_status=2,
    )

    assert summary["report_version"] == "restricted_blocklist_observation_summary_v2"
    assert summary["decision"] == "REJECT"
    assert summary["can_execute_trades"] is False
    final = cast(dict[str, Any], summary["final_decision"])
    assert final["research_promotion_decision"] == "REJECT"
    assert final["restricted_decision"] == "REJECT"
    metrics = cast(dict[str, Any], summary["metric_comparison"])
    metric_rows = cast(dict[str, Any], metrics["metrics"])
    assert metric_rows["realized_edge"]["baseline"] == 0.04
    assert metric_rows["realized_edge"]["candidate"] == 0.06
    assert metric_rows["realized_edge"]["delta"] == 0.02
    assert metric_rows["fill_rate"]["delta"] == 0.05
    assert metric_rows["drawdown"]["delta"] == -0.4
    assert metric_rows["adverse_selection"]["delta"] == -0.1
    risk = cast(dict[str, Any], summary["risk_comparison"])
    assert risk["comparison_verdict"] == "candidate_improved"
    assert risk["fixed_market_universe_status"] == "match"
    assert risk["unexpected_blocked_segments"] == 1.0
    assert risk["migrated_risk_segments"] == 1.0
    assert risk["migrated_risk_signal_ratio"] == 3.0
    stable = cast(dict[str, Any], summary["stable_review_fields"])
    assert stable["blocklist_kind"] == "restricted_input_plus_top_migrated_risk"
    assert stable["research_promotion_decision"] == "REJECT"
    assert stable["restricted_decision"] == "REJECT"
    ranking = cast(dict[str, Any], summary["ranking_snapshot"])
    assert ranking["status"] == "complete"
    assert "risk_migration_detected" in cast(list[str], ranking["blockers"])


def seed_restricted_summary_observation(tmp_path: Path) -> Path:
    root = tmp_path / "restricted"
    root.mkdir()
    write_json(
        root / "restricted_blocklist_observation_summary.json",
        {
            "baseline_report_root": "/reports/baseline",
            "candidate_report_root": str(root),
            "blocklist_kind": "restricted_input_plus_top_migrated_risk",
            "blocklist_path": "/blocklists/migrated.json",
            "duration_seconds": 900,
            "market_asset_ids_sha256": "hash",
            "decision": "REJECT",
            "can_execute_trades": False,
        },
    )
    write_json(
        root / "comparison.json",
        {
            "comparison": {
                "verdict": "candidate_improved",
                "metric_deltas": [
                    metric("realized_edge", 0.04, 0.06, 0.02, "higher_is_better", True),
                    metric("fill_rate", 0.20, 0.25, 0.05, "higher_is_better", True),
                    metric("drawdown", 0.80, 0.40, -0.40, "lower_is_better", True),
                    metric(
                        "adverse_selection",
                        0.50,
                        0.40,
                        -0.10,
                        "lower_is_better",
                        True,
                    ),
                ],
                "blocked_segment_changes": {
                    "baseline_count": 0,
                    "candidate_count": 2,
                    "expected_newly_blocked_count": 1,
                    "unexpected_newly_blocked_count": 1,
                },
                "segment_comparability": {
                    "status": "comparable",
                    "fixed_market_universe": {"status": "match"},
                },
            }
        },
    )
    write_json(
        root / "research_promotion_decision.json",
        {
            "decision": "REJECT",
            "checks": [
                {"check_name": "candidate_absolute_gate_passed", "status": "PASS"},
                {"check_name": "migrated_risk", "status": "FAIL"},
            ],
        },
    )
    write_json(
        root / "restricted_blocklist_decision.json",
        {
            "restricted_decision": "REJECT",
            "reason": "research_promotion_decision_rejected",
            "next_step": "Review regressions.",
        },
    )
    write_json(
        root / "restricted_blocklist_diagnostics.json",
        {
            "efficacy": {
                "status": "risk_migration_detected",
                "risk_migration": {"unexpected_blocked_segments": 1},
                "net_effect": {"unexpected_to_expected_signal_ratio": 3.0},
            }
        },
    )
    return root


def metric(
    name: str,
    baseline: float,
    candidate: float,
    delta: float,
    direction: str,
    improved: bool,
) -> dict[str, object]:
    return {
        "metric": name,
        "baseline": baseline,
        "candidate": candidate,
        "delta": delta,
        "direction": direction,
        "improved": improved,
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(cast(dict[str, object], payload)) + "\n", encoding="utf-8")
