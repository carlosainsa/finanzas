import json
from pathlib import Path
from typing import Any, cast

from src.research.pre_live_candidate_report import (
    REPORT_VERSION,
    create_pre_live_candidate_report,
    main,
)


def test_candidate_report_combines_existing_artifacts(tmp_path: Path) -> None:
    report_root = seed_candidate_root(tmp_path / "run-1", ready=True)

    report = create_pre_live_candidate_report(report_root)

    assert report["report_version"] == REPORT_VERSION
    assert report["status"] == "ready"
    assert report["recommendation"] == "advance_to_second_comparable_pre_live_run"
    assert report["can_execute_trades"] is False
    candidate = cast(dict[str, Any], report["candidate"])
    assert cast(dict[str, Any], candidate["go_no_go"])["decision"] == "GO"
    assert cast(dict[str, Any], candidate["readiness"])["status"] == "ready"
    assert cast(dict[str, Any], candidate["market_selection"])[
        "selected_market_asset_ids"
    ] == ["asset-1"]
    assert cast(dict[str, Any], candidate["execution_quality"])["top_asset_ids"] == [
        "asset-1"
    ]
    assert (report_root / "pre_live_candidate_report.json").exists()


def test_candidate_report_uses_readiness_as_authority(tmp_path: Path) -> None:
    report_root = seed_candidate_root(tmp_path / "run-2", ready=False)

    report = create_pre_live_candidate_report(report_root)

    assert report["status"] == "blocked"
    blockers = cast(list[dict[str, Any]], report["blockers"])
    assert "postgres_audit_available" in {item["check_name"] for item in blockers}


def test_candidate_report_blocks_missing_artifacts_without_crashing(tmp_path: Path) -> None:
    report_root = tmp_path / "missing-run"
    report_root.mkdir()
    write_json(
        report_root / "go_no_go.json",
        {"decision": "NO_GO", "passed": False, "profile": "pre_live"},
    )

    report = create_pre_live_candidate_report(report_root)

    assert report["status"] == "blocked"
    blockers = cast(list[dict[str, Any]], report["blockers"])
    missing = {item["artifact"] for item in blockers if item["check_name"] == "artifact_available"}
    assert "research_summary.json" in missing
    assert "execution_quality.json" in missing


def test_candidate_report_cli_writes_output(tmp_path: Path) -> None:
    report_root = seed_candidate_root(tmp_path / "run-3", ready=True)
    output_path = tmp_path / "candidate.json"

    exit_code = main(["--report-root", str(report_root), "--output", str(output_path)])

    assert exit_code == 0
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["status"] == "ready"


def seed_candidate_root(report_root: Path, ready: bool) -> Path:
    report_root.mkdir(parents=True)
    write_json(
        report_root / "research_summary.json",
        {
            "passed": ready,
            "pre_live_promotion_passed": ready,
            "go_no_go_passed": ready,
            "agent_advisory_acceptable": True,
        },
    )
    write_json(
        report_root / "go_no_go.json",
        {
            "report_version": "go_no_go_v1",
            "decision": "GO" if ready else "NO_GO",
            "profile": "pre_live",
            "passed": ready,
            "threshold_set_version": "go_no_go_thresholds_v1",
            "metrics": {"fill_rate": 0.2},
        },
    )
    write_json(
        report_root / "pre_live_promotion.json",
        {"passed": ready, "metrics": {"fill_rate": 0.2}, "checks": []},
    )
    write_json(
        report_root / "market_opportunity_selector.json",
        {
            "selected_market_asset_ids": ["asset-1"],
            "counts": {"selected_markets": 1},
            "decision_policy": "offline_market_selection_only",
        },
    )
    write_json(
        report_root / "execution_quality.json",
        {
            "top_asset_ids": ["asset-1"],
            "counts": {"execution_quality_ranking": 1},
            "decision_policy": "offline_execution_quality_only",
        },
    )
    write_json(
        report_root / "real_dry_run_evidence.json",
        {
            "status": "ok",
            "run_id": "run-1",
            "execution_mode": "dry_run",
            "capture_seconds": 3600,
            "stream_lengths": {"signals": 10},
        },
    )
    write_json(
        report_root / "pre_live_readiness.json",
        {
            "status": "ready" if ready else "blocked",
            "blockers": []
            if ready
            else [{"check_name": "postgres_audit_available", "passed": False}],
        },
    )
    write_json(
        report_root / "research_manifest.json",
        {"run_id": "run-1", "created_at": "2026-04-30T00:00:00+00:00"},
    )
    return report_root


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
