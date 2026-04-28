import json
from pathlib import Path
from typing import Any, cast

from src.research.restricted_blocklist_diagnostics import (
    write_migrated_risk_variants,
    write_restricted_blocklist_diagnostics,
)
from src.research.run_manifest import create_run_manifest
from test_compare_runs import (
    blocked_segment,
    segment_row,
    write_blocked_segments,
    write_candidate_blocklist,
    write_real_dry_run_evidence,
    write_report_manifest,
    write_segments,
)
from test_run_manifest import seed_report_root


def test_write_restricted_blocklist_diagnostics_exports_reviewable_report(
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
        [segment_row("market-1", "asset-1", realized_edge=0.04)],
    )
    blocklist_path = tmp_path / "blocked_segments_candidate.json"
    write_candidate_blocklist(
        blocklist_path,
        [blocked_segment("market-2", "asset-2")],
    )
    write_real_dry_run_evidence(candidate, blocklist_path)
    write_blocked_segments(candidate, [blocked_segment("market-other", "asset-other")])
    write_report_manifest(
        baseline,
        create_run_manifest(baseline, tmp_path / "research_runs", run_id="run-1"),
    )
    write_report_manifest(
        candidate,
        create_run_manifest(candidate, tmp_path / "research_runs", run_id="run-2"),
    )

    output = tmp_path / "restricted_blocklist_diagnostics.json"
    payload = write_restricted_blocklist_diagnostics(baseline, candidate, output)

    persisted = json.loads(output.read_text(encoding="utf-8"))
    assert persisted == payload
    assert payload["report_version"] == "restricted_blocklist_diagnostics_v1"
    assert payload["status"] == "needs_review"
    assert payload["can_execute_trades"] is False
    summary = cast(dict[str, Any], payload["summary"])
    assert summary["expected_restricted_input_segments"] == 1
    assert summary["unexpected_newly_blocked_segments"] == 1
    assert summary["expected_not_blocked_segments"] == 1
    assert summary["restricted_input_blocklist_segments"] == 1
    assert summary["candidate_generated_blocklist_segments"] == 1
    assert summary["candidate_generated_unexpected_segments"] == 1
    unexpected = cast(dict[str, Any], payload["unexpected"])
    missing_expected = cast(dict[str, Any], payload["missing_expected"])
    restricted_input = cast(dict[str, Any], payload["restricted_input_blocklist"])
    candidate_generated = cast(dict[str, Any], payload["candidate_generated_blocklist"])
    effectiveness = cast(dict[str, Any], payload["effectiveness"])
    efficacy = cast(dict[str, Any], payload["efficacy"])
    source_paths = cast(dict[str, Any], payload["source_paths"])
    assert unexpected["count"] == 1
    assert missing_expected["count"] == 1
    assert restricted_input["count"] == 1
    assert candidate_generated["count"] == 1
    assert source_paths["restricted_input_blocklist_path"] == str(blocklist_path)
    assert "candidate_generated_blocklist_json_path" in source_paths
    assert cast(dict[str, Any], unexpected["metrics"])["sample_count"] == 1
    assert effectiveness["report_version"] == "restricted_blocklist_effectiveness_v1"
    assert efficacy["report_version"] == "restricted_blocklist_effectiveness_v1"
    assert effectiveness["status"] == "risk_migration_detected"
    baseline_contribution = cast(
        dict[str, Any], effectiveness["baseline_restricted_input_contribution"]
    )
    assert baseline_contribution["signals"] == 4
    assert baseline_contribution["pnl"] == -0.2
    net_effect = cast(dict[str, Any], effectiveness["net_effect"])
    assert net_effect["verdict"] == "mixed"
    assert (
        "candidate_pre_live_promotion_generated_unexpected_blocks"
        in cast(list[str], payload["diagnosis"])
    )


def test_write_migrated_risk_variants_exports_research_only_blocklists(
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
        [segment_row("market-1", "asset-1", realized_edge=0.04)],
    )
    blocklist_path = tmp_path / "blocked_segments_candidate.json"
    write_candidate_blocklist(
        blocklist_path,
        [blocked_segment("market-2", "asset-2")],
        fixed_market_universe={
            "version": "fixed_market_universe_v1",
            "market_asset_ids_csv": "asset-1,asset-2",
            "market_asset_ids_sha256": "sha",
            "market_asset_ids_count": 2,
        },
    )
    write_real_dry_run_evidence(candidate, blocklist_path)
    write_blocked_segments(candidate, [blocked_segment("market-other", "asset-other")])
    write_report_manifest(
        baseline,
        create_run_manifest(baseline, tmp_path / "research_runs", run_id="run-1"),
    )
    write_report_manifest(
        candidate,
        create_run_manifest(candidate, tmp_path / "research_runs", run_id="run-2"),
    )
    diagnostics = write_restricted_blocklist_diagnostics(
        baseline,
        candidate,
        tmp_path / "restricted_blocklist_diagnostics.json",
    )

    summary = write_migrated_risk_variants(diagnostics, tmp_path)

    variants = cast(list[dict[str, Any]], summary["variants"])
    by_name = {str(item["name"]): item for item in variants}
    assert set(by_name) == {
        "restricted_input_plus_top_migrated_risk",
        "restricted_input_plus_all_migrated_risk",
        "migrated_risk_only",
    }
    assert by_name["restricted_input_plus_top_migrated_risk"]["blocked_segments"] == 2
    assert by_name["migrated_risk_only"]["blocked_segments"] == 1
    variant_path = Path(str(by_name["restricted_input_plus_top_migrated_risk"]["path"]))
    payload = json.loads(variant_path.read_text(encoding="utf-8"))
    assert payload["version"] == "blocked_segments_v1"
    assert payload["can_apply_live"] is False
    assert payload["can_execute_trades"] is False
    contract = cast(dict[str, Any], payload["evaluation_contract"])
    assert contract["expected_removed_segments_count"] == 2
    fixed = cast(dict[str, Any], contract["fixed_market_universe"])
    assert fixed["market_asset_ids_csv"] == "asset-1,asset-2"
    assert "PREDICTOR_BLOCKED_SEGMENTS_PATH" in str(
        by_name["restricted_input_plus_top_migrated_risk"]["next_command"]
    )
