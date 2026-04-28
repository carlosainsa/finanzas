import json
from pathlib import Path
from typing import cast

from src.api.research_service import (
    get_research_run,
    latest_go_no_go,
    latest_nim_budget,
    latest_pre_live_readiness,
    latest_restricted_blocklist_history,
    latest_restricted_blocklist_ranking,
    list_research_runs,
)


def test_latest_nim_budget_returns_missing_when_index_absent(tmp_path: Path) -> None:
    result = latest_nim_budget(tmp_path / "research_runs")

    assert result["status"] == "missing"
    assert result["can_execute_trades"] is False
    assert result["budget_violations"] == []


def test_latest_nim_budget_reads_latest_manifest_row(tmp_path: Path) -> None:
    manifest_root = tmp_path / "research_runs"
    manifest_root.mkdir()
    index = manifest_root / "research_runs.jsonl"
    index.write_text(
        "\n".join(
            [
                json.dumps({"run_id": "run-1", "counts": {}}),
                json.dumps(
                    {
                        "run_id": "run-2",
                        "created_at": "2026-04-27T00:00:00+00:00",
                        "report_root": "/tmp/run-2",
                        "versions": {"nim_advisory_model": "deepseek-ai/deepseek-v3.2"},
                        "counts": {
                            "nim_advisory_enabled": True,
                            "nim_advisory_annotations": 1,
                            "nim_advisory_failures": 0,
                            "nim_advisory_prompt_tokens": 200,
                            "nim_advisory_completion_tokens": 66,
                            "nim_advisory_total_tokens": 266,
                            "nim_advisory_latency_ms_avg": 9625.576,
                            "nim_advisory_estimated_cost": 0.0,
                            "nim_advisory_budget_status": "OK",
                            "nim_advisory_budget_violations": "[]",
                        },
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = latest_nim_budget(manifest_root)

    assert result["status"] == "ok"
    assert result["run_id"] == "run-2"
    assert result["nim_model"] == "deepseek-ai/deepseek-v3.2"
    assert result["total_tokens"] == 266
    assert result["budget_status"] == "OK"
    assert result["budget_violations"] == []
    assert result["can_execute_trades"] is False


def test_latest_go_no_go_reads_latest_manifest_report(tmp_path: Path) -> None:
    manifest_root = tmp_path / "research_runs"
    report_root = tmp_path / "run-2"
    manifest_root.mkdir()
    report_root.mkdir()
    (report_root / "go_no_go.json").write_text(
        json.dumps(
            {
                "decision": "NO_GO",
                "passed": False,
                "profile": "pre_live",
                "threshold_set_version": "go_no_go_thresholds_v1",
                "reason": "quantitative_gate_failure",
                "blockers": [{"check_name": "positive_realized_edge", "passed": False}],
                "metrics": {"realized_edge": -0.01, "fill_rate": 0.2},
                "checks": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (manifest_root / "research_runs.jsonl").write_text(
        json.dumps(
            {
                "run_id": "run-2",
                "created_at": "2026-04-27T00:00:00+00:00",
                "report_root": str(report_root),
                "go_no_go_passed": False,
                "metrics": {"go_no_go_decision": "NO_GO"},
                "counts": {"nim_advisory_budget_status": "OK"},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = latest_go_no_go(manifest_root)

    assert result["status"] == "ok"
    assert result["run_id"] == "run-2"
    assert result["decision"] == "NO_GO"
    assert result["profile"] == "pre_live"
    assert result["threshold_set_version"] == "go_no_go_thresholds_v1"
    assert result["passed"] is False
    assert result["can_execute_trades"] is False
    assert result["nim_budget_status"] == "OK"
    assert cast(dict[str, object], result["metrics"])["realized_edge"] == -0.01


def test_latest_restricted_blocklist_ranking_reads_latest_report(
    tmp_path: Path,
) -> None:
    manifest_root = tmp_path / "research_runs"
    report_root = tmp_path / "run-2"
    manifest_root.mkdir()
    report_root.mkdir()
    (report_root / "restricted_blocklist_ranking.json").write_text(
        json.dumps(
            {
                "report_version": "restricted_blocklist_ranking_v1",
                "summary": {
                    "observations": 2,
                    "blocked_observations": 2,
                    "insufficient_evidence_observations": 1,
                    "repeat_observation_candidates": 0,
                },
                "top_candidate": {
                    "blocklist_kind": "migrated_risk_only",
                    "recommendation": "test_migrated_risk_variant",
                },
                "observations": [
                    {
                        "blocklist_kind": "migrated_risk_only",
                        "recommendation": "test_migrated_risk_variant",
                    },
                    {
                        "status": "insufficient_evidence",
                        "failure_reason": "real dry-run preflight failed",
                        "recommendation": "repair_pipeline_before_repeat",
                    }
                ],
                "can_execute_trades": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (manifest_root / "research_runs.jsonl").write_text(
        json.dumps(
            {
                "run_id": "run-2",
                "created_at": "2026-04-27T00:00:00+00:00",
                "report_root": str(report_root),
                "counts": {},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = latest_restricted_blocklist_ranking(manifest_root)

    assert result["status"] == "ok"
    assert result["run_id"] == "run-2"
    assert result["can_execute_trades"] is False
    assert cast(dict[str, object], result["top_candidate"])["blocklist_kind"] == (
        "migrated_risk_only"
    )
    summary = cast(dict[str, object], result["summary"])
    observations = cast(list[dict[str, object]], result["observations"])
    assert summary["observations"] == 2
    assert summary["insufficient_evidence_observations"] == 1
    assert observations[1]["status"] == "insufficient_evidence"


def test_latest_restricted_blocklist_history_reads_latest_report(
    tmp_path: Path,
) -> None:
    manifest_root = tmp_path / "research_runs"
    report_root = tmp_path / "run-2"
    manifest_root.mkdir()
    report_root.mkdir()
    (report_root / "restricted_blocklist_observation_history.json").write_text(
        json.dumps(
            {
                "report_version": "restricted_blocklist_observation_history_v1",
                "summary": {
                    "observations": 2,
                    "complete_observations": 1,
                    "insufficient_evidence_observations": 1,
                    "missing_artifacts_observations": 0,
                    "blocklist_kinds": 2,
                    "stable_blocklist_kinds": 0,
                    "unstable_blocklist_kinds": 2,
                    "blocked_observations": 2,
                },
                "counts": {
                    "by_status": {"complete": 1, "insufficient_evidence": 1},
                    "by_recommendation": {"repair_pipeline_before_repeat": 1},
                    "by_failure_classification": {
                        "preflight_no_stream_progress": 1
                    },
                    "by_blocklist_kind": {
                        "restricted_input_plus_top_migrated_risk": 1
                    },
                },
                "blocklist_kind_stability": [
                    {
                        "blocklist_kind": "restricted_input_plus_top_migrated_risk",
                        "latest_status": "insufficient_evidence",
                    }
                ],
                "can_execute_trades": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (manifest_root / "research_runs.jsonl").write_text(
        json.dumps(
            {
                "run_id": "run-2",
                "created_at": "2026-04-27T00:00:00+00:00",
                "report_root": str(report_root),
                "counts": {},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = latest_restricted_blocklist_history(manifest_root)

    assert result["status"] == "ok"
    assert result["run_id"] == "run-2"
    assert result["can_execute_trades"] is False
    summary = cast(dict[str, object], result["summary"])
    counts = cast(dict[str, object], result["counts"])
    stability = cast(list[dict[str, object]], result["blocklist_kind_stability"])
    assert summary["insufficient_evidence_observations"] == 1
    assert cast(dict[str, object], counts["by_failure_classification"])[
        "preflight_no_stream_progress"
    ] == 1
    assert stability[0]["latest_status"] == "insufficient_evidence"


def test_latest_pre_live_readiness_blocks_missing_dry_run_evidence(tmp_path: Path) -> None:
    manifest_root = tmp_path / "research_runs"
    report_root = tmp_path / "run-2"
    manifest_root.mkdir()
    report_root.mkdir()
    (report_root / "go_no_go.json").write_text(
        json.dumps({"decision": "GO", "passed": True, "profile": "pre_live"}) + "\n",
        encoding="utf-8",
    )
    (report_root / "research_summary.json").write_text("{}\n", encoding="utf-8")
    (manifest_root / "research_runs.jsonl").write_text(
        json.dumps(
            {
                "run_id": "run-2",
                "created_at": "2026-04-27T00:00:00+00:00",
                "report_root": str(report_root),
                "go_no_go_passed": True,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = latest_pre_live_readiness(
        manifest_root,
        audit_summary={"status": "ok", "source": "postgres"},
    )

    assert result["status"] == "blocked"
    assert result["can_execute_trades"] is False
    blockers_value = result["blockers"]
    assert isinstance(blockers_value, list)
    blockers = [
        item.get("check_name")
        for item in blockers_value
        if isinstance(item, dict)
    ]
    assert "real_dry_run_evidence_available" in blockers


def test_latest_pre_live_readiness_ready_with_artifacts_and_audit(tmp_path: Path) -> None:
    manifest_root = tmp_path / "research_runs"
    report_root = tmp_path / "run-ready"
    manifest_root.mkdir()
    report_root.mkdir()
    (report_root / "go_no_go.json").write_text(
        json.dumps({"decision": "GO", "passed": True, "profile": "pre_live"}) + "\n",
        encoding="utf-8",
    )
    (report_root / "research_summary.json").write_text(
        json.dumps({"status": "ok"}) + "\n",
        encoding="utf-8",
    )
    (report_root / "real_dry_run_evidence.json").write_text(
        json.dumps({"execution_mode": "dry_run"}) + "\n",
        encoding="utf-8",
    )
    (report_root / "pre_live_promotion.json").write_text(
        json.dumps({"passed": True}) + "\n",
        encoding="utf-8",
    )
    (manifest_root / "research_runs.jsonl").write_text(
        json.dumps(
            {
                "run_id": "run-ready",
                "created_at": "2026-04-27T00:00:00+00:00",
                "report_root": str(report_root),
                "go_no_go_passed": True,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = latest_pre_live_readiness(
        manifest_root,
        audit_summary={"status": "ok", "source": "postgres"},
    )

    assert result["status"] == "ready"
    assert result["run_id"] == "run-ready"
    assert cast(dict[str, object], result["go_no_go"])["profile"] == "pre_live"


def test_list_research_runs_returns_latest_first(tmp_path: Path) -> None:
    manifest_root = seed_research_index(tmp_path)

    result = list_research_runs(manifest_root, limit=1)

    assert cast(str, result["source"]).endswith("research_runs.jsonl")
    assert result["runs"] == [
        {
            "run_id": "run-2",
            "created_at": "2026-04-27T00:00:00+00:00",
            "source": "unit-test",
            "report_root": "/tmp/run-2",
            "passed": True,
            "pre_live_gate_passed": True,
            "calibration_passed": True,
            "pre_live_promotion_passed": True,
            "go_no_go_passed": True,
            "feature_research_decision": "KEEP_DIAGNOSTIC",
            "go_no_go_decision": "GO",
            "go_no_go_profile": "dev",
            "realized_edge": 0.04,
            "fill_rate": 0.5,
            "nim_budget_status": "OK",
            "nim_total_tokens": 266,
            "nim_estimated_cost": 0.0,
            "nim_model": "deepseek-ai/deepseek-v3.2",
            "can_execute_trades": False,
        }
    ]


def test_get_research_run_reads_run_manifest_by_id(tmp_path: Path) -> None:
    manifest_root = seed_research_index(tmp_path)
    runs_dir = manifest_root / "runs"
    runs_dir.mkdir()
    (runs_dir / "run-2.json").write_text(
        json.dumps({"run_id": "run-2", "passed": True}) + "\n",
        encoding="utf-8",
    )

    result = get_research_run("run-2", manifest_root)

    assert result["status"] == "ok"
    assert result["run"] == {"run_id": "run-2", "passed": True}
    assert result["can_execute_trades"] is False


def test_get_research_run_rejects_path_traversal(tmp_path: Path) -> None:
    manifest_root = seed_research_index(tmp_path)

    result = get_research_run("../secret", manifest_root)

    assert result["status"] == "invalid_run_id"
    assert result["run"] is None
    assert result["can_execute_trades"] is False


def seed_research_index(tmp_path: Path) -> Path:
    manifest_root = tmp_path / "research_runs"
    manifest_root.mkdir()
    (manifest_root / "research_runs.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "run_id": "run-1",
                        "created_at": "2026-04-26T00:00:00+00:00",
                        "counts": {},
                    }
                ),
                json.dumps(
                    {
                        "run_id": "run-2",
                        "created_at": "2026-04-27T00:00:00+00:00",
                        "source": "unit-test",
                        "report_root": "/tmp/run-2",
                        "passed": True,
                        "pre_live_gate_passed": True,
                        "calibration_passed": True,
                        "pre_live_promotion_passed": True,
                        "go_no_go_passed": True,
                        "feature_research_decision": "KEEP_DIAGNOSTIC",
                        "metrics": {
                            "realized_edge": 0.04,
                            "fill_rate": 0.5,
                            "go_no_go_decision": "GO",
                            "go_no_go_profile": "dev",
                        },
                        "versions": {"nim_advisory_model": "deepseek-ai/deepseek-v3.2"},
                        "counts": {
                            "nim_advisory_total_tokens": 266,
                            "nim_advisory_estimated_cost": 0.0,
                            "nim_advisory_budget_status": "OK",
                        },
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return manifest_root
