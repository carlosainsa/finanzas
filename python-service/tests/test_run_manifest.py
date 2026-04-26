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
    assert metrics["fill_rate"] == 1.0
    assert counts["orderbook_snapshots"] == 4
    assert counts["signals"] == 4
    assert versions["promotion_report"] == "pre_live_promotion_v1"
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
    assert summary_artifact["kind"] == "json"
    assert summary_artifact["bytes"] > 0
    assert summary_artifact["sha256"] == sha256_file(report_root / "research_summary.json")


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
    assert flat["baseline_model_version"] == "deterministic_microstructure_baseline_v1"
    assert flat["baseline_data_version"] == "research_orderbook_snapshots_v1"
    assert flat["promotion_report_version"] == "pre_live_promotion_v1"
    assert flat["pre_live_gate_passed"] is True
    assert flat["calibration_passed"] is True
    assert flat["backtest_trades"] == 4
    assert flat["pre_live_gate_signals"] == 4
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
                "fill_rate": 1.0,
                "slippage": 0.01,
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
        report_root / "synthetic_fills.json",
        {
            "model_version": "conservative_orderbook_fill_v1",
            "data_version": "orderbook_snapshots_v1",
            "feature_version": "limit_touch_after_signal_v1",
            "counts": {"synthetic_execution_reports": 3},
        },
    )
    write_json(report_root / "backtest.json", {"pre_live_gate": {"signals": 4}})
    return report_root


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
