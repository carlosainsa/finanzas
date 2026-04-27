import json
from pathlib import Path
from typing import Any, cast

from src.research.run_manifest import (
    MANIFEST_SCHEMA_VERSION,
    create_run_manifest,
    flatten_manifest,
    sha256_file,
)


def test_run_manifest_persists_versioned_summary_and_index(tmp_path: Path) -> None:
    report_root = seed_report_root(tmp_path / "reports" / "run-1")
    manifest_root = tmp_path / "research_runs"

    manifest = create_run_manifest(
        report_root=report_root,
        manifest_root=manifest_root,
        run_id="run-1",
        source="unit-test",
    )

    metrics = cast(dict[str, Any], manifest["metrics"])
    counts = cast(dict[str, Any], manifest["counts"])
    versions = cast(dict[str, Any], manifest["versions"])
    assert manifest["schema_version"] == MANIFEST_SCHEMA_VERSION
    assert manifest["run_id"] == "run-1"
    assert manifest["source"] == "unit-test"
    assert manifest["passed"] is True
    assert metrics["realized_edge"] == 0.04
    assert metrics["filled_signals"] == 4
    assert metrics["fill_rate"] == 1.0
    assert metrics["capture_duration_ms"] == 3000
    assert metrics["dry_run_observed_fill_rate"] == 0.75
    assert counts["orderbook_snapshots"] == 4
    assert counts["signals"] == 4
    assert counts["market_regime_summary"] == 1
    assert counts["market_tail_risk"] == 1
    assert counts["whale_pressure"] == 1
    assert counts["market_regime_trade_context"] == 1
    assert counts["market_regime_bucket_performance"] == 4
    assert counts["sentiment_feature_candidates"] == 2
    assert counts["sentiment_lift_trade_context"] == 2
    assert counts["sentiment_lift_summary"] == 2
    assert counts["research_feature_blocklist_candidates"] == 3
    assert counts["blocked_segment_candidates"] == 1
    assert counts["blocked_segments"] == 1
    assert counts["runtime_blocked_segments"] == 1
    assert manifest["feature_research_decision"] == "PROMOTE_FEATURE"
    assert versions["promotion_report"] == "pre_live_promotion_v1"
    assert versions["feature_decision_report"] == "feature_research_decision_v1"
    assert (manifest_root / "runs" / "run-1.json").exists()
    assert (manifest_root / "research_runs.jsonl").exists()
    assert (manifest_root / "research_runs.parquet").exists()


def test_run_manifest_records_artifact_hashes(tmp_path: Path) -> None:
    report_root = seed_report_root(tmp_path / "reports" / "run-1")
    manifest = create_run_manifest(
        report_root=report_root,
        manifest_root=tmp_path / "research_runs",
        run_id="run-1",
    )

    artifacts = cast(list[dict[str, Any]], manifest["artifacts"])
    summary_artifact = [
        item for item in artifacts if item["relative_path"] == "research_summary.json"
    ][0]
    feature_decision_artifact = [
        item for item in artifacts if item["relative_path"] == "feature_research_decision.json"
    ][0]
    assert summary_artifact["kind"] == "json"
    assert summary_artifact["bytes"] > 0
    assert summary_artifact["sha256"] == sha256_file(report_root / "research_summary.json")
    assert feature_decision_artifact["kind"] == "json"
    assert feature_decision_artifact["bytes"] > 0


def test_run_manifest_index_contains_multiple_runs(tmp_path: Path) -> None:
    manifest_root = tmp_path / "research_runs"
    create_run_manifest(
        seed_report_root(tmp_path / "reports" / "run-1"),
        manifest_root,
        run_id="run-1",
    )
    create_run_manifest(
        seed_report_root(tmp_path / "reports" / "run-2"),
        manifest_root,
        run_id="run-2",
    )

    lines = (manifest_root / "research_runs.jsonl").read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["run_id"] for line in lines] == ["run-1", "run-2"]


def test_flatten_manifest_keeps_comparison_fields(tmp_path: Path) -> None:
    manifest = create_run_manifest(
        seed_report_root(tmp_path / "reports" / "run-1"),
        tmp_path / "research_runs",
        run_id="run-1",
    )

    flat = flatten_manifest(manifest)

    assert flat["run_id"] == "run-1"
    assert flat["realized_edge"] == 0.04
    assert flat["filled_signals"] == 4
    assert flat["capture_duration_ms"] == 3000
    assert flat["dry_run_observed_fill_rate"] == 0.75
    assert flat["max_abs_simulator_fill_rate_delta"] == 0.10
    assert flat["baseline_model_version"] == "deterministic_microstructure_baseline_v1"
    assert flat["baseline_data_version"] == "research_orderbook_snapshots_v1"
    assert flat["promotion_report_version"] == "pre_live_promotion_v1"
    assert flat["pre_live_gate_passed"] is True
    assert flat["calibration_passed"] is True
    assert flat["backtest_trades"] == 4
    assert flat["observed_vs_synthetic_fills"] == 4
    assert flat["observed_vs_synthetic_fill_summary"] == 1
    assert flat["unfilled_signal_reasons"] == 2
    assert flat["unfilled_reason_summary"] == 1
    assert flat["dry_run_simulator_quality"] == 1
    assert flat["pre_live_gate_signals"] == 4
    assert flat["market_regime_summary"] == 1
    assert flat["market_tail_risk"] == 1
    assert flat["whale_pressure"] == 1
    assert flat["market_regime_trade_context"] == 1
    assert flat["market_regime_bucket_performance"] == 4
    assert flat["sentiment_feature_candidates"] == 2
    assert flat["sentiment_lift_trade_context"] == 2
    assert flat["sentiment_lift_summary"] == 2
    assert flat["research_feature_blocklist_candidates"] == 3
    assert flat["blocked_segment_candidates"] == 1
    assert flat["blocked_segments"] == 1
    assert flat["runtime_blocked_segments"] == 1
    assert flat["feature_research_decision"] == "PROMOTE_FEATURE"
    assert flat["feature_decision_report_version"] == "feature_research_decision_v1"
    assert flat["synthetic_execution_reports"] == 3
    assert flat["synthetic_fill_model_version"] == "conservative_orderbook_fill_v1"
    assert isinstance(flat["artifact_count"], int)
    assert isinstance(flat["artifact_bytes_total"], int)
    assert flat["artifact_count"] >= 6
    assert flat["artifact_bytes_total"] > 0


def seed_report_root(report_root: Path) -> Path:
    report_root.mkdir(parents=True)
    write_json(
        report_root / "research_summary.json",
        {
            "passed": True,
            "pre_live_gate_passed": True,
            "calibration_passed": True,
            "pre_live_promotion_passed": True,
            "agent_advisory_acceptable": True,
            "data_lake": {
                "orderbook_snapshots": 4,
                "signals": 4,
                "execution_reports": 4,
            },
            "backtest_exports": {
                "backtest_trades": 4,
                "backtest_summary": 1,
                "observed_vs_synthetic_fills": 4,
                "observed_vs_synthetic_fill_summary": 1,
                "unfilled_signal_reasons": 2,
                "unfilled_reason_summary": 1,
                "dry_run_simulator_quality": 1,
            },
        },
    )
    write_json(
        report_root / "pre_live_promotion.json",
        {
            "report_version": "pre_live_promotion_v1",
            "passed": True,
            "metrics": {
                "realized_edge": 0.04,
                "filled_signals": 4,
                "fill_rate": 1.0,
                "slippage": 0.01,
                "capture_duration_ms": 3000,
                "dry_run_observed_fill_rate": 0.75,
                "simulator_fill_rate_delta": -0.05,
                "max_abs_simulator_fill_rate_delta": 0.10,
                "dry_run_avg_slippage": 0.0,
                "avg_ms_to_dry_run_fill": 20.0,
                "adverse_selection": None,
                "drawdown": 0.0,
                "stale_data_rate": 0.0,
                "reconciliation_divergence_rate": 0.0,
                "test_brier_score": 0.04,
            },
        },
    )
    write_json(
        report_root / "agent_advisory.json",
        {
            "report_version": "agent_advisory_offline_v1",
            "model_version": "offline_agent_advisory_v1",
            "data_version": "pre_live_promotion_metrics_v1",
            "feature_version": "advisory_evaluator_suite_v1",
            "summary": {"failed": 0, "warned": 0, "advisory_acceptable": True},
        },
    )
    write_json(
        report_root / "baseline.json",
        {
            "model_version": "deterministic_microstructure_baseline_v1",
            "data_version": "research_orderbook_snapshots_v1",
            "feature_version": "microstructure_features_v1",
            "counts": {"baseline_signals": 2},
        },
    )
    write_json(report_root / "calibration.json", {"metrics": []})
    write_json(
        report_root / "market_regime.json",
        {
            "report_version": "market_regime_diagnostics_v1",
            "decision_policy": "offline_diagnostics_only",
            "can_execute_trades": False,
            "counts": {
                "market_regime_summary": 1,
                "market_tail_risk": 1,
                "whale_pressure": 1,
                "market_regime_trade_context": 1,
                "market_regime_bucket_performance": 4,
            },
        },
    )
    write_json(
        report_root / "sentiment_features.json",
        {
            "report_version": "sentiment_feature_builder_v1",
            "decision_policy": "offline_feature_builder_only",
            "can_execute_trades": False,
            "counts": {"sentiment_feature_candidates": 2},
        },
    )
    write_json(
        report_root / "sentiment_lift.json",
        {
            "report_version": "sentiment_lift_evaluation_v1",
            "decision_policy": "offline_diagnostics_only",
            "can_execute_trades": False,
            "counts": {
                "sentiment_lift_trade_context": 2,
                "sentiment_lift_bucket_performance": 2,
                "sentiment_lift_drawdown": 2,
                "sentiment_lift_summary": 2,
            },
        },
    )
    write_json(
        report_root / "feature_blocklist_candidates.json",
        {
            "report_version": "feature_blocklist_candidates_v1",
            "decision_policy": "offline_diagnostics_only",
            "can_execute_trades": False,
            "can_apply_live": False,
            "counts": {
                "research_feature_bucket_performance": 3,
                "research_feature_blocklist_candidates": 3,
                "blocked_segment_candidates": 1,
            },
        },
    )
    write_json(
        report_root / "feature_research_decision.json",
        {
            "report_version": "feature_research_decision_v1",
            "decision_policy": "offline_diagnostics_only",
            "can_apply_live": False,
            "decision": "PROMOTE_FEATURE",
            "summary": {"passed": 8, "failed": 0, "missing": 0},
        },
    )
    write_json(
        report_root / "synthetic_fills.json",
        {
            "model_version": "conservative_orderbook_fill_v1",
            "data_version": "orderbook_snapshots_v1",
            "feature_version": "limit_touch_after_signal_v1",
            "counts": {"synthetic_execution_reports": 3},
        },
    )
    promotion_dir = report_root / "pre_live_promotion"
    promotion_dir.mkdir()
    write_json(
        promotion_dir / "blocked_segments.json",
        {
            "version": "blocked_segments_v1",
            "source_report_version": "pre_live_promotion_v1",
            "segments": [
                {
                    "market_id": "market-1",
                    "asset_id": "asset-1",
                    "side": "BUY",
                    "strategy": "near_touch",
                    "model_version": "predictor_v1",
                    "reason": "negative_edge",
                }
            ],
        },
    )
    write_json(
        report_root / "real_dry_run_evidence.json",
        {
            "status": "ok",
            "blocked_segments_enabled": True,
            "blocked_segments_path": str(promotion_dir / "blocked_segments.json"),
        },
    )
    write_json(report_root / "backtest.json", {"pre_live_gate": {"signals": 4}})
    return report_root


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
