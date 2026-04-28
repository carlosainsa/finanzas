import json
from pathlib import Path
from typing import Any, cast

from src.research.restricted_blocklist_history import (
    build_restricted_blocklist_history,
)
from test_restricted_blocklist_ranking import seed_observation


def test_restricted_blocklist_history_groups_statuses_and_stability(
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
            "reason": "real dry-run preflight failed",
            "plan": {
                "blocklist_kind": "restricted_input_plus_top_migrated_risk",
            },
            "diagnostics": {
                "classification": "preflight_no_stream_progress",
                "real_dry_run_preflight": {
                    "blockers": ["missing_execution_reports_stream_progress"]
                },
            },
        },
    )

    report = build_restricted_blocklist_history([complete, insufficient])

    summary = cast(dict[str, Any], report["summary"])
    counts = cast(dict[str, Any], report["counts"])
    by_status = cast(dict[str, int], counts["by_status"])
    by_failure = cast(dict[str, int], counts["by_failure_classification"])
    by_kind = cast(dict[str, int], counts["by_blocklist_kind"])
    stability = cast(list[dict[str, Any]], report["blocklist_kind_stability"])
    families = cast(list[dict[str, Any]], report["variant_family_summary"])
    observations = cast(list[dict[str, Any]], report["observations"])
    assert summary["observations"] == 2
    assert summary["complete_observations"] == 1
    assert summary["insufficient_evidence_observations"] == 1
    assert by_status["complete"] == 1
    assert by_status["insufficient_evidence"] == 1
    assert by_failure["preflight_no_stream_progress"] == 1
    assert by_kind["migrated_risk_only"] == 1
    assert by_kind["restricted_input_plus_top_migrated_risk"] == 1
    assert report["can_execute_trades"] is False
    assert observations[0]["can_execute_trades"] is False
    assert {
        item["blocklist_kind"]: item["latest_status"] for item in stability
    } == {
        "migrated_risk_only": "complete",
        "restricted_input_plus_top_migrated_risk": "insufficient_evidence",
    }
    migrated_family = next(
        item for item in families if item["variant_family"] == "migrated_risk_only"
    )
    assert migrated_family["complete_observations"] == 1
    assert migrated_family["risk_migration_detected_rate"] == 0.0
    assert migrated_family["unexpected_blocked_segments_rate"] == 0.0
    assert migrated_family["realized_edge_delta_avg"] == 0.04
    assert migrated_family["fill_rate_delta_avg"] == 0.01
    assert migrated_family["drawdown_delta_avg"] == -0.1
    assert migrated_family["adverse_selection_delta_avg"] == -0.02
    assert migrated_family["stable_recommendation"] == "REPEAT"


def test_restricted_blocklist_history_marks_repeated_recommendation_stable(
    tmp_path: Path,
) -> None:
    first = seed_observation(
        tmp_path / "first",
        blocklist_kind="migrated_risk_only",
        promotion_decision="PROMOTE",
        restricted_decision="REPEAT_OBSERVATION",
        realized_edge_delta=0.04,
        drawdown_delta=-0.1,
        unexpected_blocked=0,
        risk_migration_status="restricted_input_isolated",
        migrated_signal_ratio=0.0,
    )
    second = seed_observation(
        tmp_path / "second",
        blocklist_kind="migrated_risk_only",
        promotion_decision="PROMOTE",
        restricted_decision="REPEAT_OBSERVATION",
        realized_edge_delta=0.05,
        drawdown_delta=-0.2,
        unexpected_blocked=0,
        risk_migration_status="restricted_input_isolated",
        migrated_signal_ratio=0.0,
    )

    report = build_restricted_blocklist_history([first, second])

    summary = cast(dict[str, Any], report["summary"])
    stability = cast(list[dict[str, Any]], report["blocklist_kind_stability"])
    assert summary["stable_blocklist_kinds"] == 1
    assert stability[0]["stable_recommendation"] is True
    assert stability[0]["stability_status"] == "stable_repeat_candidate"
    assert stability[0]["observations"] == 2


def test_restricted_blocklist_history_family_summary_flags_migrated_risk(
    tmp_path: Path,
) -> None:
    first = seed_observation(
        tmp_path / "first",
        blocklist_kind="restricted_input_plus_top_migrated_risk",
        promotion_decision="REJECT",
        restricted_decision="REJECT",
        realized_edge_delta=0.04,
        drawdown_delta=-0.1,
        unexpected_blocked=1,
        risk_migration_status="risk_migration_detected",
        migrated_signal_ratio=1.5,
    )
    second = seed_observation(
        tmp_path / "second",
        blocklist_kind="restricted_input_plus_top_migrated_risk",
        promotion_decision="REJECT",
        restricted_decision="REJECT",
        realized_edge_delta=0.02,
        drawdown_delta=-0.05,
        unexpected_blocked=0,
        risk_migration_status="restricted_input_isolated",
        migrated_signal_ratio=0.0,
    )

    report = build_restricted_blocklist_history([first, second])

    families = cast(list[dict[str, Any]], report["variant_family_summary"])
    family = families[0]
    assert family["variant_family"] == "restricted_input_plus_top_migrated_risk"
    assert family["complete_observations"] == 2
    assert family["risk_migration_detected_count"] == 1
    assert family["risk_migration_detected_rate"] == 0.5
    assert family["unexpected_blocked_segments_count"] == 1
    assert family["unexpected_blocked_segments_rate"] == 0.5
    assert family["unexpected_blocked_segments_total"] == 1.0
    assert family["unexpected_blocked_segments_avg"] == 0.5
    assert family["realized_edge_delta_avg"] == 0.03
    assert family["drawdown_delta_avg"] == -0.07500000000000001
    assert family["adverse_selection_delta_avg"] == -0.02
    assert family["stable_recommendation"] == "TRY_ALL_MIGRATED"


def test_restricted_blocklist_history_family_summary_redesigns_failed_all_migrated(
    tmp_path: Path,
) -> None:
    root = seed_observation(
        tmp_path / "all-migrated",
        blocklist_kind="restricted_input_plus_all_migrated_risk",
        promotion_decision="REJECT",
        restricted_decision="REJECT",
        realized_edge_delta=0.08,
        drawdown_delta=-0.5,
        unexpected_blocked=1,
        risk_migration_status="risk_migration_detected",
        migrated_signal_ratio=2.0,
    )

    report = build_restricted_blocklist_history([root])

    families = cast(list[dict[str, Any]], report["variant_family_summary"])
    assert families[0]["variant_family"] == "restricted_input_plus_all_migrated_risk"
    assert families[0]["stable_recommendation"] == "REDESIGN_STRATEGY"


def test_restricted_blocklist_history_marks_repeated_preflight_failure_stable(
    tmp_path: Path,
) -> None:
    first = seed_failure(
        tmp_path / "failure-1",
        blocklist_kind="restricted_input_plus_top_migrated_risk",
        classification="preflight_no_stream_progress",
    )
    second = seed_failure(
        tmp_path / "failure-2",
        blocklist_kind="restricted_input_plus_top_migrated_risk",
        classification="preflight_no_stream_progress",
    )

    report = build_restricted_blocklist_history([first, second])

    stability = cast(list[dict[str, Any]], report["blocklist_kind_stability"])
    assert stability[0]["stable_failure_classification"] is True
    assert stability[0]["stability_status"] == "stable_insufficient_evidence"
    assert stability[0]["latest_failure_classification"] == "preflight_no_stream_progress"


def seed_failure(root: Path, *, blocklist_kind: str, classification: str) -> Path:
    root.mkdir(parents=True)
    write_json(
        root / "restricted_blocklist_observation_failure.json",
        {
            "status": "insufficient_evidence",
            "reason": "real dry-run preflight failed",
            "plan": {"blocklist_kind": blocklist_kind},
            "diagnostics": {"classification": classification},
        },
    )
    return root


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
