import json
from pathlib import Path
from typing import Any, cast

import pandas as pd  # type: ignore[import-untyped]

from src.research.feature_research_decision import (
    compare_feature_research_report_roots,
    compare_feature_research_runs,
)
from src.research.run_manifest import create_run_manifest
from test_compare_runs import write_report_manifest
from test_run_manifest import seed_report_root


def test_feature_research_decision_promotes_comparable_improvement(tmp_path: Path) -> None:
    baseline, candidate = seed_feature_reports(tmp_path)
    write_sentiment_lift(candidate, realized_edge_lift=0.04, fill_rate_lift=0.02)
    rewrite_manifests(tmp_path, baseline, candidate)

    report = compare_feature_research_report_roots(baseline, candidate)

    assert report["decision"] == "PROMOTE_FEATURE"
    assert report["can_apply_live"] is False
    summary = cast(dict[str, Any], report["summary"])
    assert summary["failed"] == 0
    checks = checks_by_name(report)
    assert checks["candidate_only"]["status"] == "PASS"


def test_feature_research_decision_keeps_diagnostic_without_shared_buckets(
    tmp_path: Path,
) -> None:
    baseline, candidate = seed_feature_reports(tmp_path)
    write_sentiment_lift(candidate, bucket="opposed:strong_negative")
    rewrite_manifests(tmp_path, baseline, candidate)

    report = compare_feature_research_report_roots(baseline, candidate)

    assert report["decision"] == "KEEP_DIAGNOSTIC"
    checks = checks_by_name(report)
    assert checks["shared_sentiment_buckets"]["status"] == "MISSING"


def test_feature_research_decision_rejects_regressed_lift(tmp_path: Path) -> None:
    baseline, candidate = seed_feature_reports(tmp_path)
    write_sentiment_lift(candidate, realized_edge_lift=-0.03, adverse_edge_rate_lift=0.20)
    rewrite_manifests(tmp_path, baseline, candidate)

    report = compare_feature_research_report_roots(baseline, candidate)

    assert report["decision"] == "REJECT_FEATURE"
    checks = checks_by_name(report)
    assert checks["realized_edge_lift_delta"]["status"] == "FAIL"
    assert checks["adverse_edge_rate_lift_delta"]["status"] == "FAIL"


def test_feature_research_decision_rejects_live_applicable_payload(
    tmp_path: Path,
) -> None:
    baseline, candidate = seed_feature_reports(tmp_path)
    write_blocklist_payload(candidate, can_apply_live=True)
    rewrite_manifests(tmp_path, baseline, candidate)

    report = compare_feature_research_report_roots(baseline, candidate)

    assert report["decision"] == "REJECT_FEATURE"
    assert checks_by_name(report)["candidate_only"]["status"] == "FAIL"


def test_feature_research_decision_selects_runs_from_manifest(tmp_path: Path) -> None:
    baseline, candidate = seed_feature_reports(tmp_path)
    manifest_root = rewrite_manifests(tmp_path, baseline, candidate)

    report = compare_feature_research_runs(
        manifest_root,
        baseline_run_id="run-1",
        candidate_run_id="run-2",
    )

    assert report["baseline_run_id"] == "run-1"
    assert report["candidate_run_id"] == "run-2"
    assert report["decision"] == "PROMOTE_FEATURE"


def seed_feature_reports(tmp_path: Path) -> tuple[Path, Path]:
    baseline = seed_report_root(tmp_path / "reports" / "run-1")
    candidate = seed_report_root(tmp_path / "reports" / "run-2")
    write_sentiment_lift(baseline)
    write_sentiment_lift(candidate)
    write_feature_candidates(baseline)
    write_feature_candidates(candidate)
    write_blocklist_payload(baseline)
    write_blocklist_payload(candidate)
    return baseline, candidate


def write_sentiment_lift(
    report_root: Path,
    realized_edge_lift: float = 0.02,
    fill_rate_lift: float = 0.01,
    adverse_edge_rate_lift: float = 0.0,
    max_drawdown: float = 0.0,
    bucket: str = "aligned:strong_positive",
) -> None:
    alignment, sentiment_bucket = bucket.split(":", 1)
    output_dir = report_root / "sentiment_lift"
    output_dir.mkdir(exist_ok=True)
    pd.DataFrame(
        [
            {
                "sentiment_bucket": sentiment_bucket,
                "signal_sentiment_alignment": alignment,
                "strategy": "sentiment-test",
                "side": "BUY",
                "signals": 10,
                "filled_signals": 8,
                "avg_fill_rate": 0.8,
                "avg_realized_edge_after_slippage": 0.03,
                "realized_edge_pnl": 0.3,
                "adverse_edge_rate": 0.1,
                "avg_slippage": 0.0,
                "avg_sentiment_disagreement": 0.0,
                "pnl_per_signal": 0.03,
                "max_drawdown": max_drawdown,
                "baseline_fill_rate": 0.7,
                "baseline_realized_edge_after_slippage": 0.01,
                "baseline_adverse_edge_rate": 0.1,
                "fill_rate_lift": fill_rate_lift,
                "realized_edge_lift": realized_edge_lift,
                "adverse_edge_rate_lift": adverse_edge_rate_lift,
            }
        ]
    ).to_parquet(output_dir / "sentiment_lift_summary.parquet", index=False)


def write_feature_candidates(report_root: Path, extra_candidate: bool = False) -> None:
    output_dir = report_root / "feature_blocklist_candidates"
    output_dir.mkdir(exist_ok=True)
    rows = [
        {
            "candidate_id": "candidate-1",
            "feature_family": "sentiment",
            "feature_name": "sentiment_alignment",
            "bucket": "aligned:strong_positive",
            "strategy": "sentiment-test",
            "side": "BUY",
            "signals": 10,
            "filled_signals": 8,
            "avg_fill_rate": 0.8,
            "avg_realized_edge_after_slippage": 0.03,
            "adverse_edge_rate": 0.1,
            "max_drawdown": 0.0,
            "candidate_reason": "diagnostic_only",
            "should_block_candidate": True,
            "can_apply_live": False,
        }
    ]
    if extra_candidate:
        rows.append({**rows[0], "candidate_id": "candidate-2", "bucket": "opposed:strong_negative"})
    pd.DataFrame(rows).to_parquet(
        output_dir / "research_feature_blocklist_candidates.parquet", index=False
    )


def write_blocklist_payload(report_root: Path, can_apply_live: bool = False) -> None:
    output_dir = report_root / "feature_blocklist_candidates"
    output_dir.mkdir(exist_ok=True)
    payload = {
        "version": "feature_blocklist_candidates_v1",
        "candidate_only": True,
        "can_apply_live": can_apply_live,
        "segments": [],
    }
    (output_dir / "blocked_segments_candidates.json").write_text(
        json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8"
    )


def rewrite_manifests(tmp_path: Path, baseline: Path, candidate: Path) -> Path:
    manifest_root = tmp_path / "research_runs"
    write_report_manifest(
        baseline,
        create_run_manifest(baseline, manifest_root, run_id="run-1"),
    )
    write_report_manifest(
        candidate,
        create_run_manifest(candidate, manifest_root, run_id="run-2"),
    )
    return manifest_root


def checks_by_name(report: dict[str, object]) -> dict[str, dict[str, object]]:
    checks = report["checks"]
    assert isinstance(checks, list)
    return {
        str(check["check_name"]): check
        for check in checks
        if isinstance(check, dict)
    }
