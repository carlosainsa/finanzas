import json
from pathlib import Path
from typing import Any, cast

import pytest

from src.research.compare_runs import (
    compare_runs,
    format_comparison_table,
    format_summary_table,
    summarize_runs,
)
from src.research.run_manifest import create_run_manifest
from test_run_manifest import seed_report_root


def test_compare_runs_detects_candidate_improvement(tmp_path: Path) -> None:
    manifest_root = seed_manifest_index(tmp_path)

    report = compare_runs(
        manifest_root,
        baseline_run_id="run-1",
        candidate_run_id="run-2",
    )

    comparison = cast(dict[str, Any], report["comparison"])
    deltas = {
        item["metric"]: item
        for item in cast(list[dict[str, Any]], comparison["metric_deltas"])
    }
    assert comparison["verdict"] == "candidate_improved"
    assert deltas["realized_edge"]["delta"] == pytest.approx(0.03)
    assert deltas["drawdown"]["improved"] is True
    assert deltas["advisory_failed"]["improved"] is False


def test_compare_runs_defaults_to_latest_two_runs(tmp_path: Path) -> None:
    manifest_root = seed_manifest_index(tmp_path)

    report = compare_runs(manifest_root)

    comparison = cast(dict[str, Any], report["comparison"])
    assert comparison["baseline_run_id"] == "run-1"
    assert comparison["candidate_run_id"] == "run-2"


def test_summarize_runs_returns_recent_rows(tmp_path: Path) -> None:
    manifest_root = seed_manifest_index(tmp_path)

    rows = summarize_runs(manifest_root, limit=1)

    assert len(rows) == 1
    assert rows[0]["run_id"] == "run-2"
    assert rows[0]["realized_edge"] == pytest.approx(0.07)


def test_compare_runs_formats_tables(tmp_path: Path) -> None:
    manifest_root = seed_manifest_index(tmp_path)
    report = compare_runs(manifest_root)

    comparison_table = format_comparison_table(report)
    summary_table = format_summary_table(summarize_runs(manifest_root))

    assert "realized_edge" in comparison_table
    assert "candidate_improved" not in comparison_table
    assert "run_id" in summary_table
    assert "run-2" in summary_table


def test_compare_runs_requires_two_runs(tmp_path: Path) -> None:
    manifest_root = tmp_path / "research_runs"
    create_run_manifest(
        seed_report_root(tmp_path / "reports" / "run-1"),
        manifest_root,
        run_id="run-1",
    )

    with pytest.raises(ValueError, match="at least two runs"):
        compare_runs(manifest_root)


def seed_manifest_index(tmp_path: Path) -> Path:
    manifest_root = tmp_path / "research_runs"
    run_1 = seed_report_root(tmp_path / "reports" / "run-1")
    run_2 = seed_report_root(tmp_path / "reports" / "run-2")
    override_metric(run_1, "realized_edge", 0.04)
    override_metric(run_1, "drawdown", 0.03)
    override_advisory(run_1, failed=0)
    override_metric(run_2, "realized_edge", 0.07)
    override_metric(run_2, "drawdown", 0.01)
    override_advisory(run_2, failed=1)
    create_run_manifest(run_1, manifest_root, run_id="run-1")
    create_run_manifest(run_2, manifest_root, run_id="run-2")
    return manifest_root


def override_metric(report_root: Path, metric: str, value: float) -> None:
    path = report_root / "pre_live_promotion.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["metrics"][metric] = value
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def override_advisory(report_root: Path, failed: int) -> None:
    path = report_root / "agent_advisory.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["summary"]["failed"] = failed
    payload["summary"]["advisory_acceptable"] = failed == 0
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
