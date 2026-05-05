import json
import os
import hashlib
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd  # type: ignore[import-untyped]


MANIFEST_SCHEMA_VERSION = "research_run_manifest_v1"


def create_run_manifest(
    report_root: Path,
    manifest_root: Path,
    run_id: str | None = None,
    source: str = "research_loop",
) -> dict[str, object]:
    resolved_run_id = run_id or report_root.name
    summary = read_json(report_root / "research_summary.json")
    promotion = read_json(report_root / "pre_live_promotion.json")
    go_no_go = read_json(report_root / "go_no_go.json")
    advisory = read_json(report_root / "agent_advisory.json")
    baseline = read_json(report_root / "baseline.json")
    synthetic_fills = read_json(report_root / "synthetic_fills.json")
    calibration = read_json(report_root / "calibration.json")
    backtest = read_json(report_root / "backtest.json")
    market_regime = read_json(report_root / "market_regime.json")
    market_opportunity_selector = read_json(report_root / "market_opportunity_selector.json")
    execution_quality = read_json(report_root / "execution_quality.json")
    quote_execution_diagnostics = read_json(report_root / "quote_execution_diagnostics.json")
    candidate_market_ranking = read_json(report_root / "candidate_market_ranking.json")
    pre_live_candidate = read_json(report_root / "pre_live_candidate_report.json")
    sentiment_features = read_json(report_root / "sentiment_features.json")
    sentiment_lift = read_json(report_root / "sentiment_lift.json")
    nim_advisory = read_json(report_root / "nim_advisory.json")
    feature_blocklist_candidates = read_json(
        report_root / "feature_blocklist_candidates.json"
    )
    feature_research_decision = read_json(report_root / "feature_research_decision.json")
    restricted_blocklist_ranking = read_json(
        report_root / "restricted_blocklist_ranking.json"
    )
    restricted_blocklist_next_variant = read_json(
        report_root / "restricted_blocklist_next_variant.json"
    )
    restricted_blocklist_history = read_json(
        report_root / "restricted_blocklist_observation_history.json"
    )
    restricted_blocklist_failure = read_json(
        report_root / "restricted_blocklist_observation_failure.json"
    )
    blocked_segments = read_json(report_root / "pre_live_promotion" / "blocked_segments.json")
    real_dry_run_evidence = read_json(report_root / "real_dry_run_evidence.json")

    manifest: dict[str, object] = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "run_id": resolved_run_id,
        "source": source,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "git_commit": git_commit(report_root),
        "report_root": str(report_root),
        "passed": bool(summary.get("passed", False)),
        "pre_live_gate_passed": bool(summary.get("pre_live_gate_passed", False)),
        "calibration_passed": bool(summary.get("calibration_passed", False)),
        "pre_live_promotion_passed": bool(summary.get("pre_live_promotion_passed", False)),
        "go_no_go_passed": bool(summary.get("go_no_go_passed", False)),
        "agent_advisory_acceptable": bool(summary.get("agent_advisory_acceptable", False)),
        "feature_research_decision": feature_research_decision.get("decision"),
        "feature_research_status": feature_research_decision.get("status"),
        "versions": {
            "promotion_report": promotion.get("report_version"),
            "go_no_go_report": go_no_go.get("report_version"),
            "go_no_go_threshold_set": go_no_go.get("threshold_set_version"),
            "go_no_go_profile": go_no_go.get("profile"),
            "advisory_report": advisory.get("report_version"),
            "advisory_model": advisory.get("model_version"),
            "advisory_data": advisory.get("data_version"),
            "advisory_feature": advisory.get("feature_version"),
            "baseline_model": baseline.get("model_version"),
            "baseline_data": baseline.get("data_version"),
            "baseline_feature": baseline.get("feature_version"),
            "synthetic_fill_model": synthetic_fills.get("model_version"),
            "synthetic_fill_data": synthetic_fills.get("data_version"),
            "synthetic_fill_feature": synthetic_fills.get("feature_version"),
            "feature_decision_report": feature_research_decision.get("report_version"),
            "restricted_blocklist_ranking_report": restricted_blocklist_ranking.get(
                "report_version"
            ),
            "restricted_blocklist_next_variant_report": (
                restricted_blocklist_next_variant.get("report_version")
            ),
            "restricted_blocklist_history_report": restricted_blocklist_history.get(
                "report_version"
            ),
            "restricted_blocklist_failure_report": restricted_blocklist_failure.get(
                "report_version"
            ),
            "nim_advisory_report": nim_advisory.get("report_version"),
            "nim_advisory_model": nim_advisory.get("model_version"),
            "nim_advisory_feature": nim_advisory.get("feature_version"),
            "nim_advisory_prompt": nim_advisory.get("prompt_version"),
            "market_opportunity_selector_report": market_opportunity_selector.get(
                "report_version"
            ),
            "execution_quality_report": execution_quality.get("report_version"),
            "quote_execution_diagnostics_report": quote_execution_diagnostics.get(
                "report_version"
            ),
            "candidate_market_ranking_report": candidate_market_ranking.get(
                "report_version"
            ),
            "pre_live_candidate_report": pre_live_candidate.get("report_version"),
        },
        "metrics": manifest_metrics(
            promotion,
            advisory,
            calibration,
            backtest,
            go_no_go,
            restricted_blocklist_ranking,
        ),
        "counts": manifest_counts(
            summary,
            baseline,
            synthetic_fills,
            backtest,
            market_regime,
            sentiment_features,
            sentiment_lift,
            nim_advisory,
            feature_blocklist_candidates,
            restricted_blocklist_ranking,
            restricted_blocklist_next_variant,
            restricted_blocklist_history,
            restricted_blocklist_failure,
            blocked_segments,
            real_dry_run_evidence,
            market_opportunity_selector,
            execution_quality,
            quote_execution_diagnostics,
            candidate_market_ranking,
            pre_live_candidate,
        ),
        "artifacts": artifact_metadata(report_root),
    }
    write_manifest(manifest_root, manifest)
    return manifest


def write_manifest(manifest_root: Path, manifest: dict[str, object]) -> None:
    run_id = str(manifest["run_id"])
    runs_dir = manifest_root / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    (runs_dir / f"{run_id}.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    rebuild_manifest_index(manifest_root)


def rebuild_manifest_index(manifest_root: Path) -> None:
    runs_dir = manifest_root / "runs"
    manifests = [
        read_json(path)
        for path in sorted(runs_dir.glob("*.json"))
        if read_json(path).get("schema_version") == MANIFEST_SCHEMA_VERSION
    ]
    manifest_root.mkdir(parents=True, exist_ok=True)
    jsonl_path = manifest_root / "research_runs.jsonl"
    jsonl_path.write_text(
        "".join(json.dumps(item, sort_keys=True) + "\n" for item in manifests),
        encoding="utf-8",
    )
    if manifests:
        pd.DataFrame([flatten_manifest(item) for item in manifests]).to_parquet(
            manifest_root / "research_runs.parquet", index=False
        )


def manifest_metrics(
    promotion: dict[str, object],
    advisory: dict[str, object],
    calibration: dict[str, object],
    backtest: dict[str, object],
    go_no_go: dict[str, object],
    restricted_blocklist_ranking: dict[str, object],
) -> dict[str, object]:
    promotion_metrics = typed_dict(promotion.get("metrics"))
    go_no_go_metrics = typed_dict(go_no_go.get("metrics"))
    advisory_summary = typed_dict(advisory.get("summary"))
    pre_live_gate = typed_dict(backtest.get("pre_live_gate"))
    top_restricted_candidate = typed_dict(
        restricted_blocklist_ranking.get("top_candidate")
    )
    return {
        "realized_edge": promotion_metrics.get("realized_edge"),
        "filled_signals": promotion_metrics.get("filled_signals"),
        "fill_rate": promotion_metrics.get("fill_rate"),
        "slippage": promotion_metrics.get("slippage"),
        "capture_duration_ms": promotion_metrics.get("capture_duration_ms"),
        "dry_run_observed_fill_rate": promotion_metrics.get(
            "dry_run_observed_fill_rate"
        ),
        "simulator_fill_rate_delta": promotion_metrics.get(
            "simulator_fill_rate_delta"
        ),
        "max_abs_simulator_fill_rate_delta": promotion_metrics.get(
            "max_abs_simulator_fill_rate_delta"
        ),
        "dry_run_avg_slippage": promotion_metrics.get("dry_run_avg_slippage"),
        "avg_ms_to_dry_run_fill": promotion_metrics.get("avg_ms_to_dry_run_fill"),
        "adverse_selection": promotion_metrics.get("adverse_selection"),
        "drawdown": promotion_metrics.get("drawdown"),
        "stale_data_rate": promotion_metrics.get("stale_data_rate"),
        "reconciliation_divergence_rate": promotion_metrics.get(
            "reconciliation_divergence_rate"
        ),
        "test_brier_score": promotion_metrics.get("test_brier_score"),
        "test_log_loss": promotion_metrics.get("test_log_loss"),
        "go_no_go_decision": go_no_go.get("decision"),
        "go_no_go_profile": go_no_go.get("profile"),
        "go_no_go_threshold_set_version": go_no_go.get("threshold_set_version"),
        "go_no_go_blockers": stable_json_list(
            [
                typed_dict(item).get("check_name")
                for item in typed_list(go_no_go.get("blockers"))
                if typed_dict(item).get("check_name")
            ]
        ),
        "go_no_go_realized_edge": go_no_go_metrics.get("realized_edge"),
        "go_no_go_fill_rate": go_no_go_metrics.get("fill_rate"),
        "advisory_failed": advisory_summary.get("failed"),
        "advisory_warned": advisory_summary.get("warned"),
        "legacy_pre_live_fill_rate": pre_live_gate.get("fill_rate"),
        "calibration_metrics": calibration.get("metrics", []),
        "restricted_blocklist_ranking_top_score": top_restricted_candidate.get("score"),
        "restricted_blocklist_ranking_top_recommendation": (
            top_restricted_candidate.get("recommendation")
        ),
        "restricted_blocklist_ranking_top_blocklist_kind": (
            top_restricted_candidate.get("blocklist_kind")
        ),
    }


def manifest_counts(
    summary: dict[str, object],
    baseline: dict[str, object],
    synthetic_fills: dict[str, object],
    backtest: dict[str, object],
    market_regime: dict[str, object],
    sentiment_features: dict[str, object],
    sentiment_lift: dict[str, object],
    nim_advisory: dict[str, object],
    feature_blocklist_candidates: dict[str, object],
    restricted_blocklist_ranking: dict[str, object],
    restricted_blocklist_next_variant: dict[str, object],
    restricted_blocklist_history: dict[str, object],
    restricted_blocklist_failure: dict[str, object],
    blocked_segments: dict[str, object] | None = None,
    real_dry_run_evidence: dict[str, object] | None = None,
    market_opportunity_selector: dict[str, object] | None = None,
    execution_quality: dict[str, object] | None = None,
    quote_execution_diagnostics: dict[str, object] | None = None,
    candidate_market_ranking: dict[str, object] | None = None,
    pre_live_candidate: dict[str, object] | None = None,
) -> dict[str, object]:
    data_lake = typed_dict(summary.get("data_lake"))
    baseline_counts = typed_dict(baseline.get("counts"))
    synthetic_counts = typed_dict(synthetic_fills.get("counts"))
    market_regime_counts = typed_dict(market_regime.get("counts"))
    sentiment_counts = typed_dict(sentiment_features.get("counts"))
    sentiment_lift_counts = typed_dict(sentiment_lift.get("counts"))
    nim_counts = typed_dict(nim_advisory.get("counts"))
    nim_summary = typed_dict(nim_advisory.get("summary"))
    feature_blocklist_counts = typed_dict(feature_blocklist_candidates.get("counts"))
    ranking_summary = typed_dict(restricted_blocklist_ranking.get("summary"))
    history_summary = typed_dict(restricted_blocklist_history.get("summary"))
    history_counts = typed_dict(restricted_blocklist_history.get("counts"))
    history_by_status = typed_dict(history_counts.get("by_status"))
    history_by_failure = typed_dict(history_counts.get("by_failure_classification"))
    history_by_kind = typed_dict(history_counts.get("by_blocklist_kind"))
    failure_diagnostics = typed_dict(restricted_blocklist_failure.get("diagnostics"))
    next_variant = typed_dict(restricted_blocklist_next_variant.get("variant"))
    backtest_exports = typed_dict(summary.get("backtest_exports"))
    market_opportunity_counts = typed_dict(
        typed_dict(market_opportunity_selector).get("counts")
    )
    execution_quality_counts = typed_dict(typed_dict(execution_quality).get("counts"))
    quote_execution_counts = typed_dict(
        typed_dict(quote_execution_diagnostics).get("counts")
    )
    quote_execution_summary = typed_dict(
        typed_dict(quote_execution_diagnostics).get("summary")
    )
    candidate_market_counts = typed_dict(
        typed_dict(candidate_market_ranking).get("counts")
    )
    candidate_market_recommendations = typed_dict(
        candidate_market_counts.get("recommendations")
    )
    return {
        "orderbook_snapshots": data_lake.get("orderbook_snapshots"),
        "signals": data_lake.get("signals"),
        "execution_reports": data_lake.get("execution_reports"),
        "baseline_signals": baseline_counts.get("baseline_signals"),
        "synthetic_execution_reports": synthetic_counts.get("synthetic_execution_reports"),
        "backtest_trades": backtest_exports.get("backtest_trades"),
        "backtest_summary": backtest_exports.get("backtest_summary"),
        "observed_vs_synthetic_fills": backtest_exports.get(
            "observed_vs_synthetic_fills"
        ),
        "observed_vs_synthetic_fill_summary": backtest_exports.get(
            "observed_vs_synthetic_fill_summary"
        ),
        "unfilled_signal_reasons": backtest_exports.get("unfilled_signal_reasons"),
        "unfilled_reason_summary": backtest_exports.get("unfilled_reason_summary"),
        "dry_run_simulator_quality": backtest_exports.get("dry_run_simulator_quality"),
        "pre_live_gate_signals": typed_dict(backtest.get("pre_live_gate")).get("signals"),
        "market_regime_summary": market_regime_counts.get("market_regime_summary"),
        "market_tail_risk": market_regime_counts.get("market_tail_risk"),
        "whale_pressure": market_regime_counts.get("whale_pressure"),
        "market_regime_trade_context": market_regime_counts.get(
            "market_regime_trade_context"
        ),
        "market_regime_bucket_performance": market_regime_counts.get(
            "market_regime_bucket_performance"
        ),
        "sentiment_feature_candidates": sentiment_counts.get(
            "sentiment_feature_candidates"
        ),
        "sentiment_lift_trade_context": sentiment_lift_counts.get(
            "sentiment_lift_trade_context"
        ),
        "sentiment_lift_summary": sentiment_lift_counts.get(
            "sentiment_lift_summary"
        ),
        "nim_advisory_enabled": nim_advisory.get("enabled"),
        "nim_advisory_status": nim_advisory.get("status"),
        "nim_advisory_annotations": nim_counts.get("nim_advisory_annotations"),
        "nim_advisory_cost_summary": nim_counts.get("nim_advisory_cost_summary"),
        "nim_advisory_failures": nim_summary.get("failures"),
        "nim_advisory_prompt_tokens": nim_summary.get("prompt_tokens"),
        "nim_advisory_completion_tokens": nim_summary.get("completion_tokens"),
        "nim_advisory_total_tokens": nim_summary.get("total_tokens"),
        "nim_advisory_latency_ms_avg": nim_summary.get("latency_ms_avg"),
        "nim_advisory_estimated_cost": nim_summary.get("estimated_cost"),
        "nim_advisory_budget_status": nim_summary.get("budget_status"),
        "nim_advisory_budget_violations": stable_json_list(
            nim_summary.get("budget_violations")
        ),
        "research_feature_bucket_performance": feature_blocklist_counts.get(
            "research_feature_bucket_performance"
        ),
        "research_feature_blocklist_candidates": feature_blocklist_counts.get(
            "research_feature_blocklist_candidates"
        ),
        "blocked_segment_candidates": feature_blocklist_counts.get(
            "blocked_segment_candidates"
        ),
        "restricted_blocklist_ranked_observations": ranking_summary.get(
            "observations"
        ),
        "restricted_blocklist_complete_observations": ranking_summary.get(
            "complete_observations"
        ),
        "restricted_blocklist_repeat_candidates": ranking_summary.get(
            "repeat_observation_candidates"
        ),
        "restricted_blocklist_blocked_observations": ranking_summary.get(
            "blocked_observations"
        ),
        "restricted_blocklist_next_variant_status": (
            restricted_blocklist_next_variant.get("status")
        ),
        "restricted_blocklist_next_variant_name": next_variant.get("name"),
        "restricted_blocklist_next_variant_segments": next_variant.get(
            "blocked_segments"
        ),
        "restricted_blocklist_history_observations": history_summary.get(
            "observations"
        ),
        "restricted_blocklist_history_complete_observations": history_summary.get(
            "complete_observations"
        ),
        "restricted_blocklist_history_insufficient_evidence_observations": (
            history_summary.get("insufficient_evidence_observations")
        ),
        "restricted_blocklist_history_missing_artifacts_observations": (
            history_summary.get("missing_artifacts_observations")
        ),
        "restricted_blocklist_history_blocklist_kinds": history_summary.get(
            "blocklist_kinds"
        ),
        "restricted_blocklist_history_stable_blocklist_kinds": history_summary.get(
            "stable_blocklist_kinds"
        ),
        "restricted_blocklist_history_unstable_blocklist_kinds": history_summary.get(
            "unstable_blocklist_kinds"
        ),
        "restricted_blocklist_history_status_counts": stable_json_object(
            history_by_status
        ),
        "restricted_blocklist_history_failure_classification_counts": (
            stable_json_object(history_by_failure)
        ),
        "restricted_blocklist_history_kind_counts": stable_json_object(history_by_kind),
        "restricted_blocklist_failure_status": restricted_blocklist_failure.get(
            "status"
        ),
        "restricted_blocklist_failure_classification": failure_diagnostics.get(
            "classification"
        ),
        "restricted_blocklist_failure_exit_code": restricted_blocklist_failure.get(
            "exit_code"
        ),
        "blocked_segments": count_blocked_segments(blocked_segments),
        "runtime_blocked_segments": count_runtime_blocked_segments(real_dry_run_evidence),
        "market_opportunity_ranked_markets": market_opportunity_counts.get(
            "ranked_markets"
        ),
        "market_opportunity_selected_markets": market_opportunity_counts.get(
            "selected_markets"
        ),
        "execution_quality_signals": execution_quality_counts.get(
            "execution_quality_signals"
        ),
        "execution_quality_assets": execution_quality_counts.get(
            "execution_quality_by_asset"
        ),
        "execution_quality_ranked_assets": execution_quality_counts.get(
            "execution_quality_ranking"
        ),
        "quote_execution_signals": quote_execution_summary.get("signals"),
        "quote_execution_synthetic_only_signals": quote_execution_summary.get(
            "synthetic_only_signals"
        ),
        "quote_execution_dry_run_lifecycles": quote_execution_summary.get(
            "dry_run_signal_lifecycles"
        ),
        "quote_execution_dry_run_filled_signals": quote_execution_summary.get(
            "dry_run_filled_signals"
        ),
        "quote_execution_outcomes": quote_execution_counts.get(
            "quote_execution_outcomes"
        ),
        "candidate_market_ranked_assets": candidate_market_counts.get(
            "candidate_market_ranking"
        ),
        "candidate_market_selected_assets": candidate_market_counts.get(
            "selected_candidate_markets"
        ),
        "candidate_market_promoted_assets": candidate_market_recommendations.get(
            "PROMOTE_TO_OBSERVATION"
        ),
        "candidate_market_needs_execution_evidence": (
            candidate_market_recommendations.get("NEEDS_EXECUTION_EVIDENCE")
        ),
        "pre_live_candidate_status": typed_dict(pre_live_candidate).get("status"),
        "pre_live_candidate_blockers": len(
            typed_list(typed_dict(pre_live_candidate).get("blockers"))
        ),
    }


def artifact_metadata(report_root: Path) -> list[dict[str, object]]:
    names = (
        "research_summary.json",
        "data_lake_export.json",
        "baseline.json",
        "backtest.json",
        "game_theory.json",
        "market_opportunity_selector.json",
        "execution_quality.json",
        "quote_execution_diagnostics.json",
        "candidate_market_ranking.json",
        "pre_live_candidate_report.json",
        "market_regime.json",
        "sentiment_features.json",
        "sentiment_lift.json",
        "feature_blocklist_candidates.json",
        "feature_research_decision.json",
        "restricted_blocklist_ranking.json",
        "restricted_blocklist_observation_history.json",
        "restricted_blocklist_next_variant.json",
        "nim_advisory.json",
        "restricted_blocklist_observation_failure.json",
        "calibration.json",
        "pre_live_promotion.json",
        "go_no_go.json",
        "agent_advisory.json",
        "synthetic_fills.json",
        "real_dry_run_evidence.json",
        "pre_live_promotion/blocked_segments.json",
    )
    artifacts: list[dict[str, object]] = []
    for name in names:
        path = report_root / name
        if path.exists():
            artifacts.append(file_artifact(path, report_root))
    for path in sorted(report_root.glob("**/*.parquet")):
        artifacts.append(file_artifact(path, report_root))
    return artifacts


def file_artifact(path: Path, report_root: Path) -> dict[str, object]:
    return {
        "path": str(path),
        "relative_path": path.relative_to(report_root).as_posix(),
        "kind": path.suffix.removeprefix(".") or "file",
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def flatten_manifest(manifest: dict[str, object]) -> dict[str, object]:
    metrics = typed_dict(manifest.get("metrics"))
    counts = typed_dict(manifest.get("counts"))
    versions = typed_dict(manifest.get("versions"))
    return {
        "schema_version": manifest.get("schema_version"),
        "run_id": manifest.get("run_id"),
        "source": manifest.get("source"),
        "created_at": manifest.get("created_at"),
        "git_commit": manifest.get("git_commit"),
        "report_root": manifest.get("report_root"),
        "passed": manifest.get("passed"),
        "pre_live_gate_passed": manifest.get("pre_live_gate_passed"),
        "calibration_passed": manifest.get("calibration_passed"),
        "pre_live_promotion_passed": manifest.get("pre_live_promotion_passed"),
        "go_no_go_passed": manifest.get("go_no_go_passed"),
        "agent_advisory_acceptable": manifest.get("agent_advisory_acceptable"),
        "feature_research_decision": manifest.get("feature_research_decision"),
        "feature_research_status": manifest.get("feature_research_status"),
        "realized_edge": metrics.get("realized_edge"),
        "filled_signals": metrics.get("filled_signals"),
        "fill_rate": metrics.get("fill_rate"),
        "slippage": metrics.get("slippage"),
        "capture_duration_ms": metrics.get("capture_duration_ms"),
        "dry_run_observed_fill_rate": metrics.get("dry_run_observed_fill_rate"),
        "simulator_fill_rate_delta": metrics.get("simulator_fill_rate_delta"),
        "max_abs_simulator_fill_rate_delta": metrics.get(
            "max_abs_simulator_fill_rate_delta"
        ),
        "dry_run_avg_slippage": metrics.get("dry_run_avg_slippage"),
        "avg_ms_to_dry_run_fill": metrics.get("avg_ms_to_dry_run_fill"),
        "adverse_selection": metrics.get("adverse_selection"),
        "drawdown": metrics.get("drawdown"),
        "stale_data_rate": metrics.get("stale_data_rate"),
        "reconciliation_divergence_rate": metrics.get("reconciliation_divergence_rate"),
        "test_brier_score": metrics.get("test_brier_score"),
        "test_log_loss": metrics.get("test_log_loss"),
        "go_no_go_decision": metrics.get("go_no_go_decision"),
        "go_no_go_blockers": metrics.get("go_no_go_blockers"),
        "go_no_go_realized_edge": metrics.get("go_no_go_realized_edge"),
        "go_no_go_fill_rate": metrics.get("go_no_go_fill_rate"),
        "restricted_blocklist_ranking_top_score": metrics.get(
            "restricted_blocklist_ranking_top_score"
        ),
        "restricted_blocklist_ranking_top_recommendation": metrics.get(
            "restricted_blocklist_ranking_top_recommendation"
        ),
        "restricted_blocklist_ranking_top_blocklist_kind": metrics.get(
            "restricted_blocklist_ranking_top_blocklist_kind"
        ),
        "legacy_pre_live_fill_rate": metrics.get("legacy_pre_live_fill_rate"),
        "advisory_failed": metrics.get("advisory_failed"),
        "advisory_warned": metrics.get("advisory_warned"),
        "orderbook_snapshots": counts.get("orderbook_snapshots"),
        "signals": counts.get("signals"),
        "execution_reports": counts.get("execution_reports"),
        "baseline_signals": counts.get("baseline_signals"),
        "synthetic_execution_reports": counts.get("synthetic_execution_reports"),
        "backtest_trades": counts.get("backtest_trades"),
        "backtest_summary": counts.get("backtest_summary"),
        "observed_vs_synthetic_fills": counts.get("observed_vs_synthetic_fills"),
        "observed_vs_synthetic_fill_summary": counts.get(
            "observed_vs_synthetic_fill_summary"
        ),
        "unfilled_signal_reasons": counts.get("unfilled_signal_reasons"),
        "unfilled_reason_summary": counts.get("unfilled_reason_summary"),
        "dry_run_simulator_quality": counts.get("dry_run_simulator_quality"),
        "pre_live_gate_signals": counts.get("pre_live_gate_signals"),
        "market_regime_summary": counts.get("market_regime_summary"),
        "market_tail_risk": counts.get("market_tail_risk"),
        "whale_pressure": counts.get("whale_pressure"),
        "market_regime_trade_context": counts.get("market_regime_trade_context"),
        "market_regime_bucket_performance": counts.get(
            "market_regime_bucket_performance"
        ),
        "sentiment_feature_candidates": counts.get("sentiment_feature_candidates"),
        "sentiment_lift_trade_context": counts.get("sentiment_lift_trade_context"),
        "sentiment_lift_summary": counts.get("sentiment_lift_summary"),
        "nim_advisory_enabled": counts.get("nim_advisory_enabled"),
        "nim_advisory_status": counts.get("nim_advisory_status"),
        "nim_advisory_annotations": counts.get("nim_advisory_annotations"),
        "nim_advisory_cost_summary": counts.get("nim_advisory_cost_summary"),
        "nim_advisory_failures": counts.get("nim_advisory_failures"),
        "nim_advisory_prompt_tokens": counts.get("nim_advisory_prompt_tokens"),
        "nim_advisory_completion_tokens": counts.get("nim_advisory_completion_tokens"),
        "nim_advisory_total_tokens": counts.get("nim_advisory_total_tokens"),
        "nim_advisory_latency_ms_avg": counts.get("nim_advisory_latency_ms_avg"),
        "nim_advisory_estimated_cost": counts.get("nim_advisory_estimated_cost"),
        "nim_advisory_budget_status": counts.get("nim_advisory_budget_status"),
        "nim_advisory_budget_violations": counts.get("nim_advisory_budget_violations"),
        "research_feature_bucket_performance": counts.get(
            "research_feature_bucket_performance"
        ),
        "research_feature_blocklist_candidates": counts.get(
            "research_feature_blocklist_candidates"
        ),
        "blocked_segment_candidates": counts.get("blocked_segment_candidates"),
        "restricted_blocklist_ranked_observations": counts.get(
            "restricted_blocklist_ranked_observations"
        ),
        "restricted_blocklist_complete_observations": counts.get(
            "restricted_blocklist_complete_observations"
        ),
        "restricted_blocklist_repeat_candidates": counts.get(
            "restricted_blocklist_repeat_candidates"
        ),
        "restricted_blocklist_blocked_observations": counts.get(
            "restricted_blocklist_blocked_observations"
        ),
        "restricted_blocklist_next_variant_status": counts.get(
            "restricted_blocklist_next_variant_status"
        ),
        "restricted_blocklist_next_variant_name": counts.get(
            "restricted_blocklist_next_variant_name"
        ),
        "restricted_blocklist_next_variant_segments": counts.get(
            "restricted_blocklist_next_variant_segments"
        ),
        "restricted_blocklist_history_observations": counts.get(
            "restricted_blocklist_history_observations"
        ),
        "restricted_blocklist_history_complete_observations": counts.get(
            "restricted_blocklist_history_complete_observations"
        ),
        "restricted_blocklist_history_insufficient_evidence_observations": counts.get(
            "restricted_blocklist_history_insufficient_evidence_observations"
        ),
        "restricted_blocklist_history_missing_artifacts_observations": counts.get(
            "restricted_blocklist_history_missing_artifacts_observations"
        ),
        "restricted_blocklist_history_blocklist_kinds": counts.get(
            "restricted_blocklist_history_blocklist_kinds"
        ),
        "restricted_blocklist_history_stable_blocklist_kinds": counts.get(
            "restricted_blocklist_history_stable_blocklist_kinds"
        ),
        "restricted_blocklist_history_unstable_blocklist_kinds": counts.get(
            "restricted_blocklist_history_unstable_blocklist_kinds"
        ),
        "restricted_blocklist_history_status_counts": counts.get(
            "restricted_blocklist_history_status_counts"
        ),
        "restricted_blocklist_history_failure_classification_counts": counts.get(
            "restricted_blocklist_history_failure_classification_counts"
        ),
        "restricted_blocklist_history_kind_counts": counts.get(
            "restricted_blocklist_history_kind_counts"
        ),
        "restricted_blocklist_failure_status": counts.get(
            "restricted_blocklist_failure_status"
        ),
        "restricted_blocklist_failure_classification": counts.get(
            "restricted_blocklist_failure_classification"
        ),
        "restricted_blocklist_failure_exit_code": counts.get(
            "restricted_blocklist_failure_exit_code"
        ),
        "blocked_segments": counts.get("blocked_segments"),
        "runtime_blocked_segments": counts.get("runtime_blocked_segments"),
        "market_opportunity_ranked_markets": counts.get(
            "market_opportunity_ranked_markets"
        ),
        "market_opportunity_selected_markets": counts.get(
            "market_opportunity_selected_markets"
        ),
        "execution_quality_signals": counts.get("execution_quality_signals"),
        "execution_quality_assets": counts.get("execution_quality_assets"),
        "execution_quality_ranked_assets": counts.get(
            "execution_quality_ranked_assets"
        ),
        "quote_execution_signals": counts.get("quote_execution_signals"),
        "quote_execution_synthetic_only_signals": counts.get(
            "quote_execution_synthetic_only_signals"
        ),
        "quote_execution_dry_run_lifecycles": counts.get(
            "quote_execution_dry_run_lifecycles"
        ),
        "quote_execution_dry_run_filled_signals": counts.get(
            "quote_execution_dry_run_filled_signals"
        ),
        "quote_execution_outcomes": counts.get("quote_execution_outcomes"),
        "candidate_market_ranked_assets": counts.get("candidate_market_ranked_assets"),
        "candidate_market_selected_assets": counts.get(
            "candidate_market_selected_assets"
        ),
        "candidate_market_promoted_assets": counts.get(
            "candidate_market_promoted_assets"
        ),
        "candidate_market_needs_execution_evidence": counts.get(
            "candidate_market_needs_execution_evidence"
        ),
        "pre_live_candidate_status": counts.get("pre_live_candidate_status"),
        "pre_live_candidate_blockers": counts.get("pre_live_candidate_blockers"),
        "promotion_report_version": versions.get("promotion_report"),
        "go_no_go_report_version": versions.get("go_no_go_report"),
        "go_no_go_threshold_set_version": versions.get("go_no_go_threshold_set"),
        "go_no_go_profile": versions.get("go_no_go_profile"),
        "advisory_report_version": versions.get("advisory_report"),
        "advisory_model_version": versions.get("advisory_model"),
        "advisory_data_version": versions.get("advisory_data"),
        "advisory_feature_version": versions.get("advisory_feature"),
        "baseline_model_version": versions.get("baseline_model"),
        "baseline_data_version": versions.get("baseline_data"),
        "baseline_feature_version": versions.get("baseline_feature"),
        "synthetic_fill_model_version": versions.get("synthetic_fill_model"),
        "synthetic_fill_data_version": versions.get("synthetic_fill_data"),
        "synthetic_fill_feature_version": versions.get("synthetic_fill_feature"),
        "feature_decision_report_version": versions.get("feature_decision_report"),
        "restricted_blocklist_ranking_report_version": versions.get(
            "restricted_blocklist_ranking_report"
        ),
        "restricted_blocklist_next_variant_report_version": versions.get(
            "restricted_blocklist_next_variant_report"
        ),
        "restricted_blocklist_history_report_version": versions.get(
            "restricted_blocklist_history_report"
        ),
        "restricted_blocklist_failure_report_version": versions.get(
            "restricted_blocklist_failure_report"
        ),
        "nim_advisory_report_version": versions.get("nim_advisory_report"),
        "nim_advisory_model_version": versions.get("nim_advisory_model"),
        "nim_advisory_feature_version": versions.get("nim_advisory_feature"),
        "nim_advisory_prompt_version": versions.get("nim_advisory_prompt"),
        "market_opportunity_selector_report_version": versions.get(
            "market_opportunity_selector_report"
        ),
        "execution_quality_report_version": versions.get("execution_quality_report"),
        "quote_execution_diagnostics_report_version": versions.get(
            "quote_execution_diagnostics_report"
        ),
        "candidate_market_ranking_report_version": versions.get(
            "candidate_market_ranking_report"
        ),
        "pre_live_candidate_report_version": versions.get("pre_live_candidate_report"),
        "artifact_count": artifact_count(manifest),
        "artifact_bytes_total": artifact_bytes_total(manifest),
    }


def artifact_count(manifest: dict[str, object]) -> int:
    artifacts = manifest.get("artifacts")
    return len(artifacts) if isinstance(artifacts, list) else 0


def artifact_bytes_total(manifest: dict[str, object]) -> int:
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list):
        return 0
    total = 0
    for artifact in artifacts:
        if isinstance(artifact, dict) and isinstance(artifact.get("bytes"), (int, float)):
            total += int(artifact["bytes"])
    return total


def count_blocked_segments(blocked_segments: dict[str, object] | None) -> int:
    if not blocked_segments:
        return 0
    segments = blocked_segments.get("segments")
    return len(segments) if isinstance(segments, list) else 0


def count_runtime_blocked_segments(evidence: dict[str, object] | None) -> int:
    if not evidence:
        return 0
    path_value = evidence.get("blocked_segments_path")
    if not isinstance(path_value, str) or not path_value:
        return 0
    return count_blocked_segments(read_json(Path(path_value)))


def read_json(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def typed_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def typed_list(value: object) -> list[object]:
    return value if isinstance(value, list) else []


def stable_json_list(value: object) -> str:
    if isinstance(value, list):
        return json.dumps(value, sort_keys=True)
    return "[]"


def stable_json_object(value: object) -> str:
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True)
    return "{}"


def git_commit(path: Path) -> str | None:
    env_commit = os.getenv("GIT_COMMIT")
    if env_commit:
        return env_commit
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=path,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip() or None


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="research-run-manifest")
    parser.add_argument("--report-root", required=True)
    parser.add_argument("--manifest-root", required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--source", default="research_loop")
    args = parser.parse_args()

    manifest = create_run_manifest(
        report_root=Path(args.report_root),
        manifest_root=Path(args.manifest_root),
        run_id=args.run_id,
        source=args.source,
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
