import json
from pathlib import Path
from typing import Any, cast

import pytest
import pandas as pd  # type: ignore[import-untyped]

from src.research.compare_runs import (
    compare_report_roots,
    compare_runs,
    format_comparison_table,
    format_segment_changes_table,
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
    assert "segment_change_summary" in comparison
    assert "blocked_segment_changes" in comparison
    assert deltas["realized_edge"]["delta"] == pytest.approx(0.03)
    assert deltas["filled_signals"]["delta"] == pytest.approx(0.0)
    assert deltas["runtime_blocked_segments"]["direction"] == "neutral"
    assert deltas["runtime_blocked_segments"]["improved"] is None
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
    segment_table = format_segment_changes_table(report)
    summary_table = format_summary_table(summarize_runs(manifest_root))

    assert "realized_edge" in comparison_table
    assert "candidate_improved" not in comparison_table
    assert "classification" in segment_table
    assert "run_id" in summary_table
    assert "run-2" in summary_table


def test_compare_report_roots_reports_segment_and_blocklist_changes(
    tmp_path: Path,
) -> None:
    baseline = seed_report_root(tmp_path / "reports" / "run-1")
    candidate = seed_report_root(tmp_path / "reports" / "run-2")
    write_segments(
        baseline,
        [
            segment_row("market-1", "asset-1", realized_edge=0.01, max_drawdown=0.05),
            segment_row("market-removed", "asset-removed", realized_edge=0.02),
        ],
    )
    write_segments(
        candidate,
        [
            segment_row("market-1", "asset-1", realized_edge=0.04, max_drawdown=0.02),
            segment_row("market-new", "asset-new", realized_edge=0.03),
        ],
    )
    write_blocked_segments(
        baseline,
        [
            blocked_segment("market-1", "asset-1"),
            blocked_segment("market-unblocked", "asset-unblocked"),
        ],
    )
    write_blocked_segments(
        candidate,
        [
            blocked_segment("market-1", "asset-1"),
            blocked_segment("market-newly-blocked", "asset-newly-blocked"),
        ],
    )
    write_report_manifest(
        baseline,
        create_run_manifest(baseline, tmp_path / "research_runs", run_id="run-1"),
    )
    write_report_manifest(
        candidate,
        create_run_manifest(candidate, tmp_path / "research_runs", run_id="run-2"),
    )

    report = compare_report_roots(baseline, candidate)

    comparison = cast(dict[str, Any], report["comparison"])
    summary = cast(dict[str, Any], comparison["segment_change_summary"])
    comparability = cast(dict[str, Any], comparison["segment_comparability"])
    blocked = cast(dict[str, Any], comparison["blocked_segment_changes"])
    segment_table = format_segment_changes_table(report)
    assert comparison["verdict"] == "no_comparable"
    assert comparability["status"] == "no_comparable"
    assert comparability["reason"] == "unexpected_candidate_segment_loss"
    assert summary["improved_segments"] == 1
    assert summary["new_segments"] == 1
    assert summary["removed_segments"] == 1
    assert blocked["newly_blocked_count"] == 1
    assert blocked["unblocked_count"] == 1
    assert "improved" in segment_table


def test_compare_report_roots_allows_expected_restricted_segment_loss(
    tmp_path: Path,
) -> None:
    baseline = seed_report_root(tmp_path / "reports" / "run-1")
    candidate = seed_report_root(tmp_path / "reports" / "run-2")
    write_segments(
        baseline,
        [
            segment_row("market-1", "asset-1", realized_edge=0.01),
            segment_row("market-2", "asset-2", realized_edge=-0.02),
        ],
    )
    write_segments(
        candidate,
        [
            segment_row("market-1", "asset-1", realized_edge=0.04),
        ],
    )
    blocklist_path = tmp_path / "blocked_segments_candidate.json"
    write_candidate_blocklist(
        blocklist_path,
        [blocked_segment("market-2", "asset-2")],
    )
    write_real_dry_run_evidence(candidate, blocklist_path)
    override_metric(candidate, "realized_edge", 0.07)
    write_report_manifest(
        baseline,
        create_run_manifest(baseline, tmp_path / "research_runs", run_id="run-1"),
    )
    write_report_manifest(
        candidate,
        create_run_manifest(candidate, tmp_path / "research_runs", run_id="run-2"),
    )

    report = compare_report_roots(baseline, candidate)

    comparison = cast(dict[str, Any], report["comparison"])
    comparability = cast(dict[str, Any], comparison["segment_comparability"])
    coverage = cast(dict[str, Any], comparison["coverage_assessment"])
    blocklist_assessment = cast(
        dict[str, Any], comparison["restricted_blocklist_assessment"]
    )
    assert comparison["verdict"] == "candidate_improved"
    assert comparability["status"] == "comparable"
    assert coverage["status"] == "acceptable"
    assert blocklist_assessment["status"] == "accepted_for_observation"
    assert blocklist_assessment["can_promote_blocklist"] is False
    assert comparability["reason"] is None
    assert comparability["expected_removed_segments"] == 1
    assert comparability["unexpected_removed_segments"] == 0
    assert comparability["shared_segment_ratio"] == pytest.approx(1.0)


def test_compare_report_roots_rejects_restricted_run_with_low_shared_coverage(
    tmp_path: Path,
) -> None:
    baseline = seed_report_root(tmp_path / "reports" / "run-1")
    candidate = seed_report_root(tmp_path / "reports" / "run-2")
    baseline_rows = [
        segment_row(f"market-{index}", f"asset-{index}", realized_edge=0.01)
        for index in range(10)
    ]
    candidate_rows = [baseline_rows[0]]
    write_segments(baseline, baseline_rows)
    write_segments(candidate, candidate_rows)
    blocklist_path = tmp_path / "blocked_segments_candidate.json"
    write_candidate_blocklist(
        blocklist_path,
        [
            blocked_segment(f"market-{index}", f"asset-{index}")
            for index in range(1, 8)
        ],
    )
    write_real_dry_run_evidence(candidate, blocklist_path)
    write_report_manifest(
        baseline,
        create_run_manifest(baseline, tmp_path / "research_runs", run_id="run-1"),
    )
    write_report_manifest(
        candidate,
        create_run_manifest(candidate, tmp_path / "research_runs", run_id="run-2"),
    )

    report = compare_report_roots(baseline, candidate)

    comparison = cast(dict[str, Any], report["comparison"])
    comparability = cast(dict[str, Any], comparison["segment_comparability"])
    coverage = cast(dict[str, Any], comparison["coverage_assessment"])
    blocklist_assessment = cast(
        dict[str, Any], comparison["restricted_blocklist_assessment"]
    )
    assert comparison["verdict"] == "no_comparable"
    assert coverage["status"] == "blocked"
    assert blocklist_assessment["status"] == "need_more_data"
    assert comparability["reason"] == "unexpected_candidate_segment_loss"
    assert comparability["unexpected_removed_segments"] == 2


def test_compare_report_roots_rejects_restricted_blocklist_on_protected_regression(
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
        [segment_row("market-1", "asset-1", realized_edge=0.04)],
    )
    blocklist_path = tmp_path / "blocked_segments_candidate.json"
    write_candidate_blocklist(blocklist_path, [])
    write_real_dry_run_evidence(candidate, blocklist_path)
    write_simulator_quality(
        baseline,
        [
            simulator_quality_row(
                "market-1",
                "asset-1",
                dry_run_observed_fill_rate=1.0,
                synthetic_fill_rate=0.9,
            )
        ],
    )
    write_simulator_quality(
        candidate,
        [
            simulator_quality_row(
                "market-1",
                "asset-1",
                dry_run_observed_fill_rate=0.0,
                synthetic_fill_rate=1.0,
            )
        ],
    )
    write_unfilled_reason_summary(
        candidate,
        [
            {
                "market_id": "market-1",
                "side": "BUY",
                "unfilled_reason": "observed_error",
                "market_evidence_reason": "synthetic_fill_available",
                "signals": 3,
            }
        ],
    )
    override_metric(baseline, "max_abs_simulator_fill_rate_delta", 0.1)
    override_metric(candidate, "max_abs_simulator_fill_rate_delta", 1.0)
    write_report_manifest(
        baseline,
        create_run_manifest(baseline, tmp_path / "research_runs", run_id="run-1"),
    )
    write_report_manifest(
        candidate,
        create_run_manifest(candidate, tmp_path / "research_runs", run_id="run-2"),
    )

    report = compare_report_roots(baseline, candidate)

    comparison = cast(dict[str, Any], report["comparison"])
    blocklist_assessment = cast(
        dict[str, Any], comparison["restricted_blocklist_assessment"]
    )
    regressions = cast(list[dict[str, Any]], blocklist_assessment["regressions"])
    assert blocklist_assessment["status"] == "rejected"
    assert blocklist_assessment["reason"] == "protected_metric_regression"
    assert {item["metric"] for item in regressions} == {
        "max_abs_simulator_fill_rate_delta"
    }
    diagnostics = cast(
        dict[str, Any], blocklist_assessment["simulator_regression_diagnostics"]
    )
    diagnostic_segments = cast(list[dict[str, Any]], diagnostics["segments"])
    assert diagnostics["status"] == "ok"
    assert diagnostic_segments[0]["diagnosis"] == (
        "synthetic_fills_without_observed_dry_run_fills"
    )
    assert diagnostic_segments[0]["candidate_unfilled_reasons"][0][
        "unfilled_reason"
    ] == "observed_error"


def test_compare_report_roots_keeps_verdict_when_segments_match(tmp_path: Path) -> None:
    baseline = seed_report_root(tmp_path / "reports" / "run-1")
    candidate = seed_report_root(tmp_path / "reports" / "run-2")
    write_segments(
        baseline,
        [segment_row("market-1", "asset-1", realized_edge=0.01, max_drawdown=0.05)],
    )
    write_segments(
        candidate,
        [segment_row("market-1", "asset-1", realized_edge=0.04, max_drawdown=0.02)],
    )
    override_metric(candidate, "realized_edge", 0.07)
    write_report_manifest(
        baseline,
        create_run_manifest(baseline, tmp_path / "research_runs", run_id="run-1"),
    )
    write_report_manifest(
        candidate,
        create_run_manifest(candidate, tmp_path / "research_runs", run_id="run-2"),
    )

    report = compare_report_roots(baseline, candidate)

    comparison = cast(dict[str, Any], report["comparison"])
    comparability = cast(dict[str, Any], comparison["segment_comparability"])
    assert comparison["verdict"] == "candidate_improved"
    assert comparability["status"] == "comparable"
    assert comparability["reason"] is None


def test_compare_report_roots_marks_missing_candidate_segments(
    tmp_path: Path,
) -> None:
    baseline = seed_report_root(tmp_path / "reports" / "run-1")
    candidate = seed_report_root(tmp_path / "reports" / "run-2")
    write_segments(baseline, [segment_row("market-1", "asset-1", realized_edge=0.01)])
    write_report_manifest(
        baseline,
        create_run_manifest(baseline, tmp_path / "research_runs", run_id="run-1"),
    )
    write_report_manifest(
        candidate,
        create_run_manifest(candidate, tmp_path / "research_runs", run_id="run-2"),
    )

    report = compare_report_roots(baseline, candidate)

    comparison = cast(dict[str, Any], report["comparison"])
    comparability = cast(dict[str, Any], comparison["segment_comparability"])
    assert comparison["verdict"] == "no_comparable"
    assert comparability["reason"] == "missing_candidate_segment_export"


def test_compare_report_roots_marks_missing_segment_keys(tmp_path: Path) -> None:
    baseline = seed_report_root(tmp_path / "reports" / "run-1")
    candidate = seed_report_root(tmp_path / "reports" / "run-2")
    write_segments(baseline, [segment_row("market-1", "asset-1", realized_edge=0.01)])
    bad_candidate = segment_row("market-1", "asset-1", realized_edge=0.04)
    del bad_candidate["model_version"]
    write_segments(candidate, [bad_candidate])
    write_report_manifest(
        baseline,
        create_run_manifest(baseline, tmp_path / "research_runs", run_id="run-1"),
    )
    write_report_manifest(
        candidate,
        create_run_manifest(candidate, tmp_path / "research_runs", run_id="run-2"),
    )

    report = compare_report_roots(baseline, candidate)

    comparison = cast(dict[str, Any], report["comparison"])
    comparability = cast(dict[str, Any], comparison["segment_comparability"])
    assert comparison["verdict"] == "no_comparable"
    assert comparability["reason"] == "missing_candidate_segment_keys"


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
    override_metric(run_1, "filled_signals", 4)
    override_metric(run_1, "drawdown", 0.03)
    override_advisory(run_1, failed=0)
    override_metric(run_2, "realized_edge", 0.07)
    override_metric(run_2, "filled_signals", 4)
    override_metric(run_2, "drawdown", 0.01)
    override_advisory(run_2, failed=1)
    write_segments(
        run_1,
        [segment_row("market-1", "asset-1", realized_edge=0.04, max_drawdown=0.03)],
    )
    write_segments(
        run_2,
        [segment_row("market-1", "asset-1", realized_edge=0.07, max_drawdown=0.01)],
    )
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


def write_segments(report_root: Path, rows: list[dict[str, object]]) -> None:
    output_dir = report_root / "pre_live_promotion"
    output_dir.mkdir(exist_ok=True)
    pd.DataFrame(rows).to_parquet(
        output_dir / "pre_live_promotion_segments.parquet", index=False
    )


def write_blocked_segments(report_root: Path, rows: list[dict[str, object]]) -> None:
    output_dir = report_root / "pre_live_promotion"
    output_dir.mkdir(exist_ok=True)
    payload = {
        "version": "blocked_segments_v1",
        "source_report_version": "pre_live_promotion_v1",
        "segments": rows,
    }
    (output_dir / "blocked_segments.json").write_text(
        json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8"
    )


def write_candidate_blocklist(path: Path, rows: list[dict[str, object]]) -> None:
    payload = {
        "version": "blocked_segments_v1",
        "source_report_version": "pre_live_blocker_diagnostics_v1",
        "segments": rows,
    }
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def write_real_dry_run_evidence(report_root: Path, blocklist_path: Path) -> None:
    payload = {
        "blocked_segments_enabled": True,
        "blocked_segments_path": str(blocklist_path),
    }
    (report_root / "real_dry_run_evidence.json").write_text(
        json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8"
    )


def write_report_manifest(report_root: Path, manifest: dict[str, object]) -> None:
    (report_root / "research_manifest.json").write_text(
        json.dumps(manifest, sort_keys=True) + "\n", encoding="utf-8"
    )


def write_simulator_quality(report_root: Path, rows: list[dict[str, object]]) -> None:
    output_dir = report_root / "backtest"
    output_dir.mkdir(exist_ok=True)
    pd.DataFrame(rows).to_parquet(
        output_dir / "dry_run_simulator_quality.parquet", index=False
    )


def write_unfilled_reason_summary(
    report_root: Path,
    rows: list[dict[str, object]],
) -> None:
    output_dir = report_root / "backtest"
    output_dir.mkdir(exist_ok=True)
    normalized = [
        {
            "strategy": "near_touch",
            "model_version": "predictor_v1",
            "data_version": "data_v1",
            "feature_version": "features_v1",
            **row,
        }
        for row in rows
    ]
    pd.DataFrame(normalized).to_parquet(
        output_dir / "unfilled_reason_summary.parquet", index=False
    )


def segment_row(
    market_id: str,
    asset_id: str,
    realized_edge: float,
    max_drawdown: float = 0.0,
) -> dict[str, object]:
    return {
        "market_id": market_id,
        "asset_id": asset_id,
        "side": "BUY",
        "strategy": "near_touch",
        "model_version": "predictor_v1",
        "signals": 4,
        "filled_signals": 2,
        "realized_edge": realized_edge,
        "pnl": realized_edge * 10,
        "max_drawdown": max_drawdown,
        "fill_rate": 0.5,
        "dry_run_observed_fill_rate": 0.5,
        "abs_simulator_fill_rate_delta": 0.1,
    }


def simulator_quality_row(
    market_id: str,
    asset_id: str,
    *,
    dry_run_observed_fill_rate: float,
    synthetic_fill_rate: float,
) -> dict[str, object]:
    return {
        "market_id": market_id,
        "asset_id": asset_id,
        "side": "BUY",
        "strategy": "near_touch",
        "model_version": "predictor_v1",
        "data_version": "data_v1",
        "feature_version": "features_v1",
        "signals": 4,
        "dry_run_reports": 4,
        "dry_run_filled_signals": 4 * dry_run_observed_fill_rate,
        "dry_run_observed_fill_rate": dry_run_observed_fill_rate,
        "synthetic_fill_rate": synthetic_fill_rate,
        "fill_rate_delta_vs_synthetic": dry_run_observed_fill_rate - synthetic_fill_rate,
        "dry_run_avg_slippage": 0.0,
        "synthetic_avg_slippage": 0.0,
    }


def blocked_segment(market_id: str, asset_id: str) -> dict[str, object]:
    return {
        "market_id": market_id,
        "asset_id": asset_id,
        "side": "BUY",
        "strategy": "near_touch",
        "model_version": "predictor_v1",
        "reason": "negative_edge",
    }
