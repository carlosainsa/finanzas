from pathlib import Path
from typing import Any, cast

from src.research.compare_runs import compare_report_roots
from src.research.research_promotion_decision import decide_from_report
from src.research.run_manifest import create_run_manifest
from test_compare_runs import (
    blocked_segment,
    override_metric,
    segment_row,
    write_blocked_segments,
    write_candidate_blocklist,
    write_report_manifest,
    write_real_dry_run_evidence,
    write_segments,
)
from test_run_manifest import seed_report_root


def test_promotion_decision_promotes_comparable_improved_candidate(
    tmp_path: Path,
) -> None:
    baseline, candidate = seed_comparable_reports(tmp_path)
    override_metric(candidate, "realized_edge", 0.07)
    override_metric(candidate, "drawdown", 0.0)
    override_metric(candidate, "max_abs_simulator_fill_rate_delta", 0.05)
    rewrite_manifests(tmp_path, baseline, candidate)

    decision = decide_from_report(compare_report_roots(baseline, candidate))

    assert decision["decision"] == "PROMOTE"
    summary = cast(dict[str, Any], decision["summary"])
    assert summary["failed"] == 0
    assert summary["missing"] == 0


def test_promotion_decision_rejects_regressed_candidate(tmp_path: Path) -> None:
    baseline, candidate = seed_comparable_reports(tmp_path)
    override_metric(candidate, "realized_edge", -0.01)
    rewrite_manifests(tmp_path, baseline, candidate)

    decision = decide_from_report(compare_report_roots(baseline, candidate))

    assert decision["decision"] == "REJECT"
    checks = checks_by_name(decision)
    assert checks["comparison_verdict"]["status"] == "FAIL"
    assert checks["realized_edge"]["status"] == "FAIL"


def test_promotion_decision_needs_more_data_when_segments_not_comparable(
    tmp_path: Path,
) -> None:
    baseline = seed_report_root(tmp_path / "reports" / "run-1")
    candidate = seed_report_root(tmp_path / "reports" / "run-2")
    write_segments(
        baseline,
        [segment_row("market-1", "asset-1", realized_edge=0.01)],
    )
    write_segments(
        candidate,
        [segment_row("market-2", "asset-2", realized_edge=0.04)],
    )
    rewrite_manifests(tmp_path, baseline, candidate)

    decision = decide_from_report(compare_report_roots(baseline, candidate))

    assert decision["decision"] == "NEED_MORE_DATA"
    checks = checks_by_name(decision)
    assert checks["comparison_verdict"]["status"] == "MISSING"
    assert checks["segment_comparability"]["status"] == "MISSING"


def test_promotion_decision_rejects_failed_candidate_absolute_gate(
    tmp_path: Path,
) -> None:
    baseline, candidate = seed_comparable_reports(tmp_path)
    override_summary_passed(candidate, passed=False)
    rewrite_manifests(tmp_path, baseline, candidate)

    decision = decide_from_report(compare_report_roots(baseline, candidate))

    assert decision["decision"] == "REJECT"
    checks = checks_by_name(decision)
    assert checks["candidate_absolute_gate_passed"]["status"] == "FAIL"


def test_promotion_decision_allows_expected_restricted_blocked_segment(
    tmp_path: Path,
) -> None:
    baseline = seed_report_root(tmp_path / "reports" / "run-1")
    candidate = seed_report_root(tmp_path / "reports" / "run-2")
    write_segments(
        baseline,
        [
            segment_row("market-1", "asset-1", realized_edge=0.01, max_drawdown=0.03),
            segment_row("market-2", "asset-2", realized_edge=-0.02, max_drawdown=0.05),
        ],
    )
    write_segments(
        candidate,
        [segment_row("market-1", "asset-1", realized_edge=0.04, max_drawdown=0.0)],
    )
    blocklist_path = tmp_path / "blocked_segments_candidate.json"
    write_candidate_blocklist(
        blocklist_path,
        [blocked_segment("market-2", "asset-2")],
    )
    write_real_dry_run_evidence(candidate, blocklist_path)
    write_blocked_segments(candidate, [blocked_segment("market-2", "asset-2")])
    override_metric(candidate, "realized_edge", 0.07)
    override_metric(candidate, "drawdown", 0.0)
    override_metric(candidate, "max_abs_simulator_fill_rate_delta", 0.0)
    rewrite_manifests(tmp_path, baseline, candidate)

    decision = decide_from_report(compare_report_roots(baseline, candidate))

    checks = checks_by_name(decision)
    assert checks["newly_blocked_segments"]["status"] == "PASS"
    assert checks["newly_blocked_segments"]["metric_value"] == 0.0
    assert checks["newly_blocked_segments"]["threshold"] == 0.0


def test_promotion_decision_rejects_unexpected_restricted_blocked_segment(
    tmp_path: Path,
) -> None:
    baseline = seed_report_root(tmp_path / "reports" / "run-1")
    candidate = seed_report_root(tmp_path / "reports" / "run-2")
    write_segments(
        baseline,
        [
            segment_row("market-1", "asset-1", realized_edge=0.01, max_drawdown=0.03),
            segment_row("market-2", "asset-2", realized_edge=-0.02, max_drawdown=0.05),
        ],
    )
    write_segments(
        candidate,
        [segment_row("market-1", "asset-1", realized_edge=0.04, max_drawdown=0.0)],
    )
    blocklist_path = tmp_path / "blocked_segments_candidate.json"
    write_candidate_blocklist(
        blocklist_path,
        [blocked_segment("market-2", "asset-2")],
    )
    write_real_dry_run_evidence(candidate, blocklist_path)
    write_blocked_segments(candidate, [blocked_segment("market-other", "asset-other")])
    override_metric(candidate, "realized_edge", 0.07)
    override_metric(candidate, "drawdown", 0.0)
    override_metric(candidate, "max_abs_simulator_fill_rate_delta", 0.0)
    rewrite_manifests(tmp_path, baseline, candidate)

    decision = decide_from_report(compare_report_roots(baseline, candidate))

    checks = checks_by_name(decision)
    assert decision["decision"] == "REJECT"
    assert checks["newly_blocked_segments"]["status"] == "FAIL"
    assert checks["newly_blocked_segments"]["metric_value"] == 1.0


def seed_comparable_reports(tmp_path: Path) -> tuple[Path, Path]:
    baseline = seed_report_root(tmp_path / "reports" / "run-1")
    candidate = seed_report_root(tmp_path / "reports" / "run-2")
    write_segments(
        baseline,
        [segment_row("market-1", "asset-1", realized_edge=0.01, max_drawdown=0.03)],
    )
    write_segments(
        candidate,
        [segment_row("market-1", "asset-1", realized_edge=0.04, max_drawdown=0.0)],
    )
    return baseline, candidate


def rewrite_manifests(tmp_path: Path, baseline: Path, candidate: Path) -> None:
    manifest_root = tmp_path / "research_runs"
    write_report_manifest(
        baseline,
        create_run_manifest(baseline, manifest_root, run_id="run-1"),
    )
    write_report_manifest(
        candidate,
        create_run_manifest(candidate, manifest_root, run_id="run-2"),
    )


def override_summary_passed(report_root: Path, passed: bool) -> None:
    import json

    path = report_root / "research_summary.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["passed"] = passed
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def checks_by_name(decision: dict[str, object]) -> dict[str, dict[str, object]]:
    checks = decision["checks"]
    assert isinstance(checks, list)
    return {
        str(check["check_name"]): check
        for check in checks
        if isinstance(check, dict)
    }
