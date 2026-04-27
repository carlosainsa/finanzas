from pathlib import Path
from typing import Any, cast

import pandas as pd  # type: ignore[import-untyped]

from src.research.feature_decision_history import build_feature_decision_history
from src.research.run_manifest import create_run_manifest
from test_compare_runs import write_report_manifest
from test_feature_research_decision import (
    write_blocklist_payload,
    write_feature_candidates,
    write_sentiment_lift,
)
from test_run_manifest import seed_report_root, write_json


def test_feature_decision_history_exports_run_and_bucket_stability(
    tmp_path: Path,
) -> None:
    manifest_root = seed_history_runs(tmp_path)
    output_dir = tmp_path / "history"

    report = build_feature_decision_history(manifest_root, output_dir)

    assert report["report_version"] == "feature_decision_history_v1"
    assert report["can_apply_live"] is False
    assert report["total_runs"] == 3
    assert cast(dict[str, int], report["decision_counts"]) == {
        "PROMOTE_FEATURE": 1,
        "KEEP_DIAGNOSTIC": 1,
        "REJECT_FEATURE": 1,
    }
    assert (output_dir / "feature_decision_history.json").exists()
    assert (output_dir / "feature_decision_runs.parquet").exists()
    assert (output_dir / "feature_decision_bucket_history.parquet").exists()
    assert (output_dir / "feature_decision_bucket_stability.parquet").exists()

    stability = pd.read_parquet(output_dir / "feature_decision_bucket_stability.parquet")
    stable = stability[stability["bucket"] == "aligned:strong_positive"].iloc[0]
    unstable = stability[stability["bucket"] == "opposed:strong_negative"].iloc[0]
    assert stable["stability_class"] == "stable"
    assert unstable["stability_class"] == "unstable"


def test_feature_decision_history_handles_missing_bucket_exports(
    tmp_path: Path,
) -> None:
    manifest_root = tmp_path / "research_runs"
    run = seed_report_root(tmp_path / "reports" / "run-1")
    write_decision(run, "KEEP_DIAGNOSTIC")
    write_report_manifest(run, create_run_manifest(run, manifest_root, run_id="run-1"))

    report = build_feature_decision_history(manifest_root, tmp_path / "history")

    assert report["missing_bucket_exports"] == 1
    assert report["insufficient_data_buckets"] == 0


def seed_history_runs(tmp_path: Path) -> Path:
    manifest_root = tmp_path / "research_runs"
    run_1 = seed_run(tmp_path, "run-1", "PROMOTE_FEATURE", block=False)
    run_2 = seed_run(tmp_path, "run-2", "KEEP_DIAGNOSTIC", block=False)
    run_3 = seed_run(tmp_path, "run-3", "REJECT_FEATURE", block=False)
    write_unstable_candidate(run_1, should_block=False, edge=0.04)
    write_unstable_candidate(run_2, should_block=True, edge=-0.04)
    write_unstable_candidate(run_3, should_block=False, edge=0.06)
    for run_id, run in (("run-1", run_1), ("run-2", run_2), ("run-3", run_3)):
        write_report_manifest(run, create_run_manifest(run, manifest_root, run_id=run_id))
    return manifest_root


def seed_run(
    tmp_path: Path, run_id: str, decision: str, block: bool
) -> Path:
    run = seed_report_root(tmp_path / "reports" / run_id)
    write_sentiment_lift(run)
    write_feature_candidates(run)
    override_block_candidate(run, "aligned:strong_positive", block, 0.03)
    write_blocklist_payload(run)
    write_decision(run, decision)
    return run


def write_decision(report_root: Path, decision: str) -> None:
    write_json(
        report_root / "feature_research_decision.json",
        {
            "report_version": "feature_research_decision_v1",
            "decision_policy": "offline_diagnostics_only",
            "can_apply_live": False,
            "decision": decision,
            "summary": {"passed": 1, "failed": 0, "missing": 0},
        },
    )


def override_block_candidate(
    report_root: Path, bucket: str, should_block: bool, edge: float
) -> None:
    path = (
        report_root
        / "feature_blocklist_candidates"
        / "research_feature_blocklist_candidates.parquet"
    )
    frame = pd.read_parquet(path)
    frame.loc[frame["bucket"] == bucket, "should_block_candidate"] = should_block
    frame.loc[frame["bucket"] == bucket, "avg_realized_edge_after_slippage"] = edge
    frame.to_parquet(path, index=False)


def write_unstable_candidate(
    report_root: Path, should_block: bool, edge: float
) -> None:
    path = (
        report_root
        / "feature_blocklist_candidates"
        / "research_feature_blocklist_candidates.parquet"
    )
    frame = pd.read_parquet(path)
    extra: dict[str, Any] = frame.iloc[0].to_dict()
    extra.update(
        {
            "candidate_id": f"unstable-{report_root.name}",
            "bucket": "opposed:strong_negative",
            "should_block_candidate": should_block,
            "candidate_reason": "negative_edge" if should_block else "diagnostic_only",
            "avg_realized_edge_after_slippage": edge,
            "adverse_edge_rate": 0.8 if should_block else 0.1,
        }
    )
    pd.concat([frame, pd.DataFrame([extra])], ignore_index=True).to_parquet(
        path, index=False
    )
