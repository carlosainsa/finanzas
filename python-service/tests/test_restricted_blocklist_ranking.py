import json
from pathlib import Path
from typing import Any, cast

from src.research.restricted_blocklist_ranking import (
    rank_restricted_blocklist_observations,
)


def test_rank_restricted_blocklist_observations_prefers_repeat_candidate(
    tmp_path: Path,
) -> None:
    rejected = seed_observation(
        tmp_path / "rejected",
        blocklist_kind="defensive_top_1",
        promotion_decision="REJECT",
        restricted_decision="REJECT",
        realized_edge_delta=0.05,
        drawdown_delta=-0.2,
        unexpected_blocked=2,
        risk_migration_status="risk_migration_detected",
        migrated_signal_ratio=13.4,
    )
    repeat = seed_observation(
        tmp_path / "repeat",
        blocklist_kind="migrated_risk_only",
        promotion_decision="PROMOTE",
        restricted_decision="REPEAT_OBSERVATION",
        realized_edge_delta=0.04,
        drawdown_delta=-0.1,
        unexpected_blocked=0,
        risk_migration_status="restricted_input_isolated",
        migrated_signal_ratio=0.0,
    )

    report = rank_restricted_blocklist_observations([rejected, repeat])

    observations = cast(list[dict[str, Any]], report["observations"])
    assert observations[0]["blocklist_kind"] == "migrated_risk_only"
    assert observations[0]["recommendation"] == "repeat_observation"
    assert observations[0]["blockers"] == []
    assert observations[1]["blocklist_kind"] == "defensive_top_1"
    assert "risk_migration_detected" in cast(list[str], observations[1]["blockers"])
    assert report["can_execute_trades"] is False


def test_rank_restricted_blocklist_observations_reports_missing_artifacts(
    tmp_path: Path,
) -> None:
    root = tmp_path / "broken"
    root.mkdir()
    write_json(
        root / "restricted_blocklist_observation_summary.json",
        {"blocklist_kind": "broken"},
    )

    report = rank_restricted_blocklist_observations([root])

    row = cast(list[dict[str, Any]], report["observations"])[0]
    assert row["status"] == "missing_artifacts"
    assert row["recommendation"] == "repair_missing_artifacts"
    assert "missing_artifacts" in cast(list[str], row["blockers"])


def test_rank_restricted_blocklist_observations_surfaces_insufficient_evidence(
    tmp_path: Path,
) -> None:
    root = tmp_path / "insufficient"
    root.mkdir()
    write_json(
        root / "restricted_blocklist_observation_failure.json",
        {
            "status": "insufficient_evidence",
            "reason": "real dry-run preflight failed; inspect report",
            "stage": "pre_live_dry_run",
            "exit_code": 75,
            "dry_run_exit_code": 75,
            "can_execute_trades": False,
            "plan": {
                "blocklist_kind": "restricted_input_plus_top_migrated_risk",
                "blocklist_path": "/tmp/blocked_segments.json",
                "duration_seconds": 900,
                "market_asset_ids_sha256": "hash",
            },
            "diagnostics": {
                "classification": "preflight_no_stream_progress",
                "candidate_report_root_exists": True,
                "data_lake_root_exists": True,
                "diagnosis_hints": ["inspect_real_dry_run_preflight_report"],
                "real_dry_run_preflight": {
                    "blockers": [
                        "missing_execution_reports_stream_progress",
                        "missing_dry_run_execution_report",
                    ]
                },
            },
        },
    )

    report = rank_restricted_blocklist_observations([root])

    row = cast(list[dict[str, Any]], report["observations"])[0]
    summary = cast(dict[str, Any], report["summary"])
    blockers = cast(list[str], row["blockers"])
    assert row["status"] == "insufficient_evidence"
    assert row["blocklist_kind"] == "restricted_input_plus_top_migrated_risk"
    assert row["score"] == -500_000.0
    assert row["recommendation"] == "repair_pipeline_before_repeat"
    assert row["failure_classification"] == "preflight_no_stream_progress"
    assert row["pipeline_report_root_exists"] is True
    assert "insufficient_evidence" in blockers
    assert "missing_execution_reports_stream_progress" in blockers
    assert summary["insufficient_evidence_observations"] == 1
    assert report["can_execute_trades"] is False


def test_rank_restricted_blocklist_observations_prefers_complete_over_insufficient(
    tmp_path: Path,
) -> None:
    complete = seed_observation(
        tmp_path / "complete",
        blocklist_kind="migrated_risk_only",
        promotion_decision="PROMOTE",
        restricted_decision="REPEAT_OBSERVATION",
        realized_edge_delta=0.04,
        drawdown_delta=-0.1,
        unexpected_blocked=0,
        risk_migration_status="restricted_input_isolated",
        migrated_signal_ratio=0.0,
    )
    insufficient = tmp_path / "insufficient"
    insufficient.mkdir()
    write_json(
        insufficient / "restricted_blocklist_observation_failure.json",
        {
            "status": "insufficient_evidence",
            "reason": "no dry-run execution report found",
            "plan": {"blocklist_kind": "restricted_input_plus_top_migrated_risk"},
            "diagnostics": {"classification": "no_dry_run_execution_reports"},
        },
    )

    report = rank_restricted_blocklist_observations([insufficient, complete])

    observations = cast(list[dict[str, Any]], report["observations"])
    assert observations[0]["status"] == "complete"
    assert observations[1]["status"] == "insufficient_evidence"


def seed_observation(
    root: Path,
    *,
    blocklist_kind: str,
    promotion_decision: str,
    restricted_decision: str,
    realized_edge_delta: float,
    drawdown_delta: float,
    unexpected_blocked: int,
    risk_migration_status: str,
    migrated_signal_ratio: float,
) -> Path:
    root.mkdir(parents=True)
    write_json(
        root / "restricted_blocklist_observation_summary.json",
        {
            "blocklist_kind": blocklist_kind,
            "blocklist_path": f"/blocklists/{blocklist_kind}.json",
            "duration_seconds": 900,
            "market_asset_ids_sha256": "hash",
        },
    )
    write_json(
        root / "comparison.json",
        {
            "comparison": {
                "verdict": "candidate_improved",
                "metric_deltas": [
                    {
                        "metric": "realized_edge",
                        "delta": realized_edge_delta,
                    },
                    {
                        "metric": "fill_rate",
                        "delta": 0.01,
                    },
                    {
                        "metric": "drawdown",
                        "delta": drawdown_delta,
                    },
                    {
                        "metric": "adverse_selection",
                        "delta": -0.02,
                    },
                    {
                        "metric": "max_abs_simulator_fill_rate_delta",
                        "delta": -0.01,
                    },
                ],
                "blocked_segment_changes": {
                    "unexpected_newly_blocked_count": unexpected_blocked,
                },
                "segment_comparability": {
                    "status": "comparable",
                    "fixed_market_universe": {"status": "match"},
                },
            }
        },
    )
    checks = [
        {"check_name": "candidate_absolute_gate_passed", "status": "PASS"},
        {
            "check_name": "migrated_risk",
            "status": "FAIL"
            if risk_migration_status == "risk_migration_detected"
            else "PASS",
        },
    ]
    write_json(
        root / "research_promotion_decision.json",
        {"decision": promotion_decision, "checks": checks},
    )
    write_json(
        root / "restricted_blocklist_decision.json",
        {"restricted_decision": restricted_decision, "reason": "fixture"},
    )
    write_json(
        root / "restricted_blocklist_diagnostics.json",
        {
            "efficacy": {
                "status": risk_migration_status,
                "risk_migration": {
                    "unexpected_blocked_segments": unexpected_blocked,
                },
                "net_effect": {
                    "unexpected_to_expected_signal_ratio": migrated_signal_ratio,
                },
            }
        },
    )
    return root


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
