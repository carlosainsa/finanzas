import json
from pathlib import Path

from src.research.pre_live_readiness import (
    format_readiness_summary,
    load_readiness_report,
    main,
    summarize_readiness_report,
)


def test_summarize_ready_report_recommends_comparable_run() -> None:
    summary = summarize_readiness_report(
        {
            "status": "ready",
            "run_id": "run-1",
            "report_root": "/tmp/run-1",
            "can_execute_trades": False,
            "go_no_go": {"decision": "GO", "profile": "pre_live"},
            "audit": {"status": "ok"},
            "blockers": [],
            "artifacts": {
                "go_no_go": {"path": "/tmp/run-1/go_no_go.json"},
                "pre_live_promotion": {"path": "/tmp/run-1/pre_live_promotion.json"},
            },
        }
    )

    assert summary["status"] == "ready"
    assert summary["recommendation"] == "advance_to_second_comparable_pre_live_run"
    assert summary["go_no_go_decision"] == "GO"
    assert summary["go_no_go_profile"] == "pre_live"
    assert summary["audit_status"] == "ok"
    assert summary["blocker_count"] == 0


def test_summarize_blocked_report_includes_blockers_and_artifacts() -> None:
    summary = summarize_readiness_report(
        {
            "status": "blocked",
            "run_id": "run-2",
            "go_no_go": {"decision": "NO_GO", "profile": "pre_live"},
            "audit": {"status": "error"},
            "blockers": [
                {"check_name": "go_no_go_passed", "passed": False},
                {"check_name": "postgres_audit_available", "passed": False},
            ],
            "artifacts": {"real_dry_run_evidence": {"path": "/tmp/evidence.json"}},
        }
    )

    assert summary["recommendation"] == "investigate_blockers_before_repeat"
    assert summary["blocker_count"] == 2
    assert summary["blockers"] == [
        "go_no_go_passed",
        "postgres_audit_available",
    ]
    assert summary["artifact_paths"] == {
        "real_dry_run_evidence": "/tmp/evidence.json"
    }


def test_format_readiness_summary_is_operator_friendly() -> None:
    output = format_readiness_summary(
        {
            "status": "blocked",
            "recommendation": "investigate_blockers_before_repeat",
            "run_id": "run-2",
            "go_no_go_profile": "pre_live",
            "go_no_go_decision": "NO_GO",
            "audit_status": "error",
            "blocker_count": 1,
            "blockers": ["go_no_go_passed"],
            "artifact_paths": {"go_no_go": "/tmp/go_no_go.json"},
        }
    )

    assert "pre_live_readiness_summary" in output
    assert "recommendation=investigate_blockers_before_repeat" in output
    assert "blocker_names=go_no_go_passed" in output
    assert "artifact.go_no_go=/tmp/go_no_go.json" in output
    assert "can_execute_trades=false" in output


def test_main_reads_input_and_writes_summary_json(tmp_path: Path) -> None:
    input_path = tmp_path / "pre_live_readiness.json"
    output_path = tmp_path / "summary.json"
    input_path.write_text(
        json.dumps(
            {
                "status": "blocked",
                "run_id": "run-3",
                "go_no_go": {"decision": "NO_GO", "profile": "pre_live"},
                "audit": {"status": "ok"},
                "blockers": [{"check_name": "go_no_go_passed"}],
                "artifacts": {},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    exit_code = main(
        [
            "--input",
            str(input_path),
            "--format",
            "summary-json",
            "--output",
            str(output_path),
        ]
    )

    assert exit_code == 2
    summary = json.loads(output_path.read_text(encoding="utf-8"))
    assert summary["status"] == "blocked"
    assert summary["blockers"] == ["go_no_go_passed"]


def test_load_readiness_report_rejects_non_object(tmp_path: Path) -> None:
    input_path = tmp_path / "pre_live_readiness.json"
    input_path.write_text("[]\n", encoding="utf-8")

    try:
        load_readiness_report(input_path)
    except ValueError as exc:
        assert "must be a JSON object" in str(exc)
    else:
        raise AssertionError("expected ValueError")
