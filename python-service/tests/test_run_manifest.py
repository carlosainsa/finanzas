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
    assert counts["nim_advisory_enabled"] is True
    assert counts["nim_advisory_annotations"] == 2
    assert counts["nim_advisory_cost_summary"] == 1
    assert counts["nim_advisory_failures"] == 0
    assert counts["nim_advisory_total_tokens"] == 26
    assert counts["nim_advisory_budget_status"] == "OK"
    assert counts["research_feature_blocklist_candidates"] == 3
    assert counts["blocked_segment_candidates"] == 1
    assert counts["restricted_blocklist_ranked_observations"] == 2
    assert counts["restricted_blocklist_complete_observations"] == 2
    assert counts["restricted_blocklist_blocked_observations"] == 2
    assert counts["restricted_blocklist_next_variant_status"] == "generated"
    assert counts["restricted_blocklist_next_variant_name"] == (
        "restricted_input_plus_top_migrated_risk"
    )
    assert counts["restricted_blocklist_history_observations"] == 3
    assert counts["restricted_blocklist_history_complete_observations"] == 2
    assert counts["restricted_blocklist_history_insufficient_evidence_observations"] == 1
    assert counts["restricted_blocklist_history_blocklist_kinds"] == 2
    assert counts["restricted_blocklist_history_stable_blocklist_kinds"] == 1
    assert counts["restricted_blocklist_history_unstable_blocklist_kinds"] == 1
    assert counts["restricted_blocklist_history_status_counts"] == (
        '{"complete": 2, "insufficient_evidence": 1}'
    )
    assert counts["restricted_blocklist_history_failure_classification_counts"] == (
        '{"preflight_no_stream_progress": 1}'
    )
    assert counts["restricted_blocklist_failure_status"] == "insufficient_evidence"
    assert counts["restricted_blocklist_failure_classification"] == (
        "postprocess_resource_exhaustion"
    )
    assert counts["restricted_blocklist_failure_exit_code"] == 137
    assert counts["blocked_segments"] == 1
    assert counts["runtime_blocked_segments"] == 1
    assert counts["market_opportunity_ranked_markets"] == 2
    assert counts["market_opportunity_selected_markets"] == 1
    assert counts["execution_quality_signals"] == 4
    assert counts["execution_quality_assets"] == 2
    assert counts["execution_quality_ranked_assets"] == 1
    assert counts["candidate_market_ranked_assets"] == 2
    assert counts["candidate_market_selected_assets"] == 1
    assert counts["candidate_market_promoted_assets"] == 1
    assert counts["candidate_market_needs_execution_evidence"] == 1
    assert counts["pre_live_candidate_status"] == "blocked"
    assert counts["pre_live_candidate_blockers"] == 1
    assert manifest["feature_research_decision"] == "PROMOTE_FEATURE"
    assert versions["promotion_report"] == "pre_live_promotion_v1"
    assert versions["feature_decision_report"] == "feature_research_decision_v1"
    assert versions["restricted_blocklist_ranking_report"] == (
        "restricted_blocklist_ranking_v1"
    )
    assert versions["restricted_blocklist_next_variant_report"] == (
        "restricted_blocklist_next_variant_v1"
    )
    assert versions["restricted_blocklist_history_report"] == (
        "restricted_blocklist_observation_history_v1"
    )
    assert versions["restricted_blocklist_failure_report"] == (
        "restricted_blocklist_observation_failure_v1"
    )
    assert versions["nim_advisory_report"] == "nim_advisory_offline_v1"
    assert versions["market_opportunity_selector_report"] == (
        "market_opportunity_selector_v1"
    )
    assert versions["execution_quality_report"] == "execution_quality_v1"
    assert versions["candidate_market_ranking_report"] == "candidate_market_ranking_v1"
    assert versions["pre_live_candidate_report"] == "pre_live_candidate_report_v1"
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
    history_artifact = [
        item
        for item in artifacts
        if item["relative_path"] == "restricted_blocklist_observation_history.json"
    ][0]
    nim_advisory_artifact = [
        item for item in artifacts if item["relative_path"] == "nim_advisory.json"
    ][0]
    candidate_artifact = [
        item
        for item in artifacts
        if item["relative_path"] == "pre_live_candidate_report.json"
    ][0]
    assert summary_artifact["kind"] == "json"
    assert summary_artifact["bytes"] > 0
    assert summary_artifact["sha256"] == sha256_file(report_root / "research_summary.json")
    assert feature_decision_artifact["kind"] == "json"
    assert feature_decision_artifact["bytes"] > 0
    assert history_artifact["kind"] == "json"
    assert history_artifact["bytes"] > 0
    assert nim_advisory_artifact["kind"] == "json"
    assert nim_advisory_artifact["bytes"] > 0
    assert candidate_artifact["kind"] == "json"
    assert candidate_artifact["bytes"] > 0


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
    assert flat["nim_advisory_enabled"] is True
    assert flat["nim_advisory_status"] == "ok"
    assert flat["nim_advisory_annotations"] == 2
    assert flat["nim_advisory_cost_summary"] == 1
    assert flat["nim_advisory_failures"] == 0
    assert flat["nim_advisory_prompt_tokens"] == 20
    assert flat["nim_advisory_completion_tokens"] == 6
    assert flat["nim_advisory_total_tokens"] == 26
    assert flat["nim_advisory_latency_ms_avg"] == 12.5
    assert flat["nim_advisory_estimated_cost"] == 0.0
    assert flat["nim_advisory_budget_status"] == "OK"
    assert flat["nim_advisory_budget_violations"] == "[]"
    assert flat["nim_advisory_report_version"] == "nim_advisory_offline_v1"
    assert flat["nim_advisory_model_version"] == "nvidia_nim_research_client_v1"
    assert flat["nim_advisory_feature_version"] == "nim_advisory_annotations_v1"
    assert flat["nim_advisory_prompt_version"] == "nim_evidence_advisory_prompt_v1"
    assert flat["market_opportunity_selector_report_version"] == (
        "market_opportunity_selector_v1"
    )
    assert flat["execution_quality_report_version"] == "execution_quality_v1"
    assert flat["candidate_market_ranking_report_version"] == (
        "candidate_market_ranking_v1"
    )
    assert flat["pre_live_candidate_report_version"] == "pre_live_candidate_report_v1"
    assert flat["research_feature_blocklist_candidates"] == 3
    assert flat["blocked_segment_candidates"] == 1
    assert flat["restricted_blocklist_ranked_observations"] == 2
    assert flat["restricted_blocklist_complete_observations"] == 2
    assert flat["restricted_blocklist_blocked_observations"] == 2
    assert flat["restricted_blocklist_ranking_top_score"] == -258.7
    assert flat["restricted_blocklist_ranking_top_recommendation"] == (
        "test_migrated_risk_variant"
    )
    assert flat["restricted_blocklist_ranking_top_blocklist_kind"] == (
        "migrated_risk_only"
    )
    assert flat["restricted_blocklist_next_variant_status"] == "generated"
    assert flat["restricted_blocklist_next_variant_name"] == (
        "restricted_input_plus_top_migrated_risk"
    )
    assert flat["restricted_blocklist_history_observations"] == 3
    assert flat["restricted_blocklist_history_complete_observations"] == 2
    assert flat["restricted_blocklist_history_insufficient_evidence_observations"] == 1
    assert flat["restricted_blocklist_history_blocklist_kinds"] == 2
    assert flat["restricted_blocklist_history_stable_blocklist_kinds"] == 1
    assert flat["restricted_blocklist_history_unstable_blocklist_kinds"] == 1
    assert flat["restricted_blocklist_history_status_counts"] == (
        '{"complete": 2, "insufficient_evidence": 1}'
    )
    assert flat["restricted_blocklist_history_failure_classification_counts"] == (
        '{"preflight_no_stream_progress": 1}'
    )
    assert flat["restricted_blocklist_failure_status"] == "insufficient_evidence"
    assert flat["restricted_blocklist_failure_classification"] == (
        "postprocess_resource_exhaustion"
    )
    assert flat["restricted_blocklist_failure_exit_code"] == 137
    assert flat["restricted_blocklist_ranking_report_version"] == (
        "restricted_blocklist_ranking_v1"
    )
    assert flat["restricted_blocklist_next_variant_report_version"] == (
        "restricted_blocklist_next_variant_v1"
    )
    assert flat["restricted_blocklist_history_report_version"] == (
        "restricted_blocklist_observation_history_v1"
    )
    assert flat["restricted_blocklist_failure_report_version"] == (
        "restricted_blocklist_observation_failure_v1"
    )
    assert flat["blocked_segments"] == 1
    assert flat["runtime_blocked_segments"] == 1
    assert flat["market_opportunity_ranked_markets"] == 2
    assert flat["market_opportunity_selected_markets"] == 1
    assert flat["execution_quality_signals"] == 4
    assert flat["execution_quality_assets"] == 2
    assert flat["execution_quality_ranked_assets"] == 1
    assert flat["candidate_market_ranked_assets"] == 2
    assert flat["candidate_market_selected_assets"] == 1
    assert flat["candidate_market_promoted_assets"] == 1
    assert flat["candidate_market_needs_execution_evidence"] == 1
    assert flat["pre_live_candidate_status"] == "blocked"
    assert flat["pre_live_candidate_blockers"] == 1
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
        report_root / "market_opportunity_selector.json",
        {
            "report_version": "market_opportunity_selector_v1",
            "decision_policy": "offline_market_selection_only",
            "can_execute_trades": False,
            "counts": {"ranked_markets": 2, "selected_markets": 1},
            "selected_market_asset_ids": ["asset-1"],
        },
    )
    write_json(
        report_root / "execution_quality.json",
        {
            "report_version": "execution_quality_v1",
            "decision_policy": "offline_execution_quality_only",
            "can_execute_trades": False,
            "counts": {
                "execution_quality_signals": 4,
                "execution_quality_by_asset": 2,
                "execution_quality_ranking": 1,
            },
            "top_asset_ids": ["asset-1"],
        },
    )
    write_json(
        report_root / "candidate_market_ranking.json",
        {
            "report_version": "candidate_market_ranking_v1",
            "decision_policy": "offline_combined_market_ranking_only",
            "can_execute_trades": False,
            "counts": {
                "candidate_market_ranking": 2,
                "selected_candidate_markets": 1,
                "recommendations": {
                    "PROMOTE_TO_OBSERVATION": 1,
                    "NEEDS_EXECUTION_EVIDENCE": 1,
                },
            },
            "selected_market_asset_ids": ["asset-1"],
        },
    )
    write_json(
        report_root / "pre_live_candidate_report.json",
        {
            "report_version": "pre_live_candidate_report_v1",
            "status": "blocked",
            "can_execute_trades": False,
            "blockers": [{"check_name": "go_no_go_passed", "passed": False}],
        },
    )
    write_json(
        report_root / "nim_advisory.json",
        {
            "report_version": "nim_advisory_offline_v1",
            "model_version": "nvidia_nim_research_client_v1",
            "data_version": "external_evidence_v1",
            "feature_version": "nim_advisory_annotations_v1",
            "prompt_version": "nim_evidence_advisory_prompt_v1",
            "decision_policy": "offline_advisory_only",
            "can_execute_trades": False,
            "enabled": True,
            "status": "ok",
            "summary": {
                "annotations": 2,
                "failures": 0,
                "prompt_tokens": 20,
                "completion_tokens": 6,
                "total_tokens": 26,
                "latency_ms_avg": 12.5,
                "estimated_cost": 0.0,
                "budget_status": "OK",
                "budget_violations": [],
                "advisory_acceptable": True,
                "can_execute_trades": False,
            },
            "counts": {"nim_advisory_annotations": 2, "nim_advisory_cost_summary": 1},
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
        report_root / "restricted_blocklist_ranking.json",
        {
            "report_version": "restricted_blocklist_ranking_v1",
            "summary": {
                "observations": 2,
                "complete_observations": 2,
                "repeat_observation_candidates": 0,
                "blocked_observations": 2,
            },
            "top_candidate": {
                "blocklist_kind": "migrated_risk_only",
                "recommendation": "test_migrated_risk_variant",
                "score": -258.7,
            },
            "observations": [],
            "can_execute_trades": False,
        },
    )
    write_json(
        report_root / "restricted_blocklist_next_variant.json",
        {
            "report_version": "restricted_blocklist_next_variant_v1",
            "status": "generated",
            "can_apply_live": False,
            "can_execute_trades": False,
            "variant": {
                "name": "restricted_input_plus_top_migrated_risk",
                "blocked_segments": 2,
            },
        },
    )
    write_json(
        report_root / "restricted_blocklist_observation_history.json",
        {
            "report_version": "restricted_blocklist_observation_history_v1",
            "summary": {
                "observations": 3,
                "complete_observations": 2,
                "insufficient_evidence_observations": 1,
                "missing_artifacts_observations": 0,
                "blocklist_kinds": 2,
                "stable_blocklist_kinds": 1,
                "unstable_blocklist_kinds": 1,
                "blocked_observations": 2,
            },
            "counts": {
                "by_status": {"complete": 2, "insufficient_evidence": 1},
                "by_recommendation": {
                    "repair_pipeline_before_repeat": 1,
                    "test_migrated_risk_variant": 2,
                },
                "by_failure_classification": {
                    "preflight_no_stream_progress": 1
                },
                "by_blocklist_kind": {
                    "migrated_risk_only": 2,
                    "restricted_input_plus_top_migrated_risk": 1,
                },
            },
            "blocklist_kind_stability": [],
            "can_execute_trades": False,
        },
    )
    write_json(
        report_root / "restricted_blocklist_observation_failure.json",
        {
            "report_version": "restricted_blocklist_observation_failure_v1",
            "status": "insufficient_evidence",
            "exit_code": 137,
            "can_execute_trades": False,
            "diagnostics": {
                "classification": "postprocess_resource_exhaustion",
            },
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
