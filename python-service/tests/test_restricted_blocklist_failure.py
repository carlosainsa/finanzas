import json
from pathlib import Path
from typing import Any, cast

from src.research.restricted_blocklist_failure import (
    build_restricted_blocklist_failure,
    write_restricted_blocklist_failure,
)


def test_restricted_blocklist_failure_classifies_missing_dry_run_reports(
    tmp_path: Path,
) -> None:
    report_root = tmp_path / "data_lake" / "reports" / "run-1"
    output_dir = tmp_path / "failure"
    plan = {
        "blocklist_kind": "restricted_input_plus_top_migrated_risk",
        "blocklist_path": "/tmp/blocked_segments.json",
        "market_asset_ids_count": 20,
        "market_asset_ids_sha256": "hash",
        "duration_seconds": 900,
        "can_execute_trades": False,
    }

    payload = write_restricted_blocklist_failure(
        plan=plan,
        output_dir=output_dir,
        candidate_report_root=report_root,
        exit_code=1,
        reason="no dry-run execution report found",
        stage="pre_live_dry_run",
        output_tail="tail line",
    )

    persisted = json.loads(
        (output_dir / "restricted_blocklist_observation_failure.json").read_text(
            encoding="utf-8"
        )
    )
    diagnostics = cast(dict[str, Any], payload["diagnostics"])
    assert persisted == payload
    assert payload["status"] == "insufficient_evidence"
    assert payload["can_execute_trades"] is False
    assert payload["dry_run_exit_code"] == 1
    assert payload["exit_code_policy"] == "preserved"
    assert payload["output_tail"] == "tail line"
    assert diagnostics["classification"] == "no_dry_run_execution_reports"
    assert "check_signals_stream_for_eligible_signals" in cast(
        list[str], diagnostics["diagnosis_hints"]
    )
    assert diagnostics["candidate_report_root_exists"] is False
    assert cast(dict[str, Any], payload["plan"])["blocklist_kind"] == (
        "restricted_input_plus_top_migrated_risk"
    )


def test_restricted_blocklist_failure_summarizes_pipeline_partitions(
    tmp_path: Path,
) -> None:
    data_lake = tmp_path / "data_lake"
    report_root = data_lake / "reports" / "run-1"
    report_root.mkdir(parents=True)
    (data_lake / "signals").mkdir()
    (data_lake / "execution_reports").mkdir()
    (data_lake / "signals" / "part.parquet").write_text("x", encoding="utf-8")
    (report_root / "real_dry_run_evidence.json").write_text(
        json.dumps({"stream_lengths": {"signals": 3, "reports": 0}}) + "\n",
        encoding="utf-8",
    )

    payload = build_restricted_blocklist_failure(
        plan={"can_execute_trades": False},
        output_dir=tmp_path / "failure",
        candidate_report_root=report_root,
        exit_code=1,
        reason="missing real dry-run stream data: reports=0",
        stage="pre_live_dry_run",
    )

    diagnostics = cast(dict[str, Any], payload["diagnostics"])
    partitions = cast(dict[str, Any], diagnostics["data_lake_partitions"])
    signals = cast(dict[str, Any], partitions["signals"])
    evidence = cast(dict[str, Any], diagnostics["real_dry_run_evidence"])
    assert diagnostics["classification"] == "missing_stream_data"
    assert diagnostics["candidate_report_root_exists"] is True
    assert signals["parquet_files"] == 1
    assert cast(dict[str, Any], evidence["stream_lengths"])["reports"] == 0
