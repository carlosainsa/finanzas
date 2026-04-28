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
    sentiment_features = read_json(report_root / "sentiment_features.json")
    sentiment_lift = read_json(report_root / "sentiment_lift.json")
    nim_advisory = read_json(report_root / "nim_advisory.json")
    feature_blocklist_candidates = read_json(
        report_root / "feature_blocklist_candidates.json"
    )
    feature_research_decision = read_json(report_root / "feature_research_decision.json")
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
            "nim_advisory_report": nim_advisory.get("report_version"),
            "nim_advisory_model": nim_advisory.get("model_version"),
            "nim_advisory_feature": nim_advisory.get("feature_version"),
            "nim_advisory_prompt": nim_advisory.get("prompt_version"),
        },
        "metrics": manifest_metrics(promotion, advisory, calibration, backtest, go_no_go),
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
            blocked_segments,
            real_dry_run_evidence,
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
) -> dict[str, object]:
    promotion_metrics = typed_dict(promotion.get("metrics"))
    go_no_go_metrics = typed_dict(go_no_go.get("metrics"))
    advisory_summary = typed_dict(advisory.get("summary"))
    pre_live_gate = typed_dict(backtest.get("pre_live_gate"))
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
    blocked_segments: dict[str, object] | None = None,
    real_dry_run_evidence: dict[str, object] | None = None,
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
    backtest_exports = typed_dict(summary.get("backtest_exports"))
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
        "blocked_segments": count_blocked_segments(blocked_segments),
        "runtime_blocked_segments": count_runtime_blocked_segments(real_dry_run_evidence),
    }


def artifact_metadata(report_root: Path) -> list[dict[str, object]]:
    names = (
        "research_summary.json",
        "data_lake_export.json",
        "baseline.json",
        "backtest.json",
        "game_theory.json",
        "market_regime.json",
        "sentiment_features.json",
        "sentiment_lift.json",
        "feature_blocklist_candidates.json",
        "feature_research_decision.json",
        "nim_advisory.json",
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
        "blocked_segments": counts.get("blocked_segments"),
        "runtime_blocked_segments": counts.get("runtime_blocked_segments"),
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
        "nim_advisory_report_version": versions.get("nim_advisory_report"),
        "nim_advisory_model_version": versions.get("nim_advisory_model"),
        "nim_advisory_feature_version": versions.get("nim_advisory_feature"),
        "nim_advisory_prompt_version": versions.get("nim_advisory_prompt"),
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
