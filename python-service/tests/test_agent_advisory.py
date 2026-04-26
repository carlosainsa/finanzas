from pathlib import Path
from typing import Any, cast

from src.research.agent_advisory import (
    ADVISORY_FEATURE_VERSION,
    ADVISORY_MODEL_VERSION,
    ADVISORY_REPORT_VERSION,
    AdvisoryConfig,
    create_agent_advisory_report,
    export_agent_advisory_report,
)
from test_pre_live_promotion import seed_promotion_db


def test_agent_advisory_is_offline_and_does_not_authorize_trading(tmp_path: Path) -> None:
    db_path = seed_promotion_db(tmp_path)

    report = create_agent_advisory_report(
        db_path, AdvisoryConfig(max_stale_data_rate=1.0)
    )

    assert report["report_version"] == ADVISORY_REPORT_VERSION
    assert report["model_version"] == ADVISORY_MODEL_VERSION
    assert report["feature_version"] == ADVISORY_FEATURE_VERSION
    assert report["decision_policy"] == "offline_advisory_only"
    summary = cast(dict[str, Any], report["summary"])
    evaluations = cast(list[dict[str, Any]], report["evaluations"])
    assert summary["can_execute_trades"] is False
    assert summary["advisory_acceptable"] is True
    assert {item["evaluator_id"] for item in evaluations} == {
        "adverse_selection_reviewer",
        "calibration_reviewer",
        "data_quality_reviewer",
        "edge_reviewer",
        "execution_quality_reviewer",
        "reconciliation_reviewer",
    }


def test_agent_advisory_fails_when_reviewer_threshold_fails(tmp_path: Path) -> None:
    db_path = seed_promotion_db(tmp_path)

    report = create_agent_advisory_report(
        db_path, AdvisoryConfig(min_realized_edge=0.50, max_stale_data_rate=1.0)
    )
    evaluations = cast(list[dict[str, Any]], report["evaluations"])
    summary = cast(dict[str, Any], report["summary"])

    edge_review = [
        item for item in evaluations if item["evaluator_id"] == "edge_reviewer"
    ][0]
    assert edge_review["status"] == "FAIL"
    assert summary["advisory_acceptable"] is False
    assert summary["can_execute_trades"] is False


def test_export_agent_advisory_report_writes_auditable_artifacts(tmp_path: Path) -> None:
    db_path = seed_promotion_db(tmp_path)
    output_dir = tmp_path / "advisory"

    report = export_agent_advisory_report(
        db_path, output_dir, AdvisoryConfig(max_stale_data_rate=1.0)
    )
    summary = cast(dict[str, Any], report["summary"])

    assert summary["evaluations"] == 6
    assert (output_dir / "agent_advisory.json").exists()
    assert (output_dir / "agent_advisory_evaluations.parquet").exists()
    assert (output_dir / "agent_advisory_summary.parquet").exists()
