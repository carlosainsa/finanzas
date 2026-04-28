import json
from pathlib import Path
from typing import Any, cast

from src.research.restricted_blocklist_diagnostics import (
    write_restricted_blocklist_diagnostics,
)
from src.research.restricted_blocklist_next_variant import (
    build_next_migrated_risk_variant,
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


def test_next_variant_selects_top_migrated_risk_candidate(tmp_path: Path) -> None:
    observation_root = seed_restricted_observation(tmp_path)
    ranking_path = tmp_path / "restricted_blocklist_ranking.json"
    write_json(
        ranking_path,
        {
            "report_version": "restricted_blocklist_ranking_v1",
            "observations": [
                {
                    "status": "complete",
                    "observation_root": str(observation_root),
                    "blocklist_kind": "migrated_risk_only",
                    "recommendation": "test_migrated_risk_variant",
                    "risk_migration_status": "risk_migration_detected",
                    "restricted_decision": "REJECT",
                }
            ],
            "can_execute_trades": False,
        },
    )

    report = build_next_migrated_risk_variant(ranking_path, tmp_path / "next")

    assert report["status"] == "generated"
    assert report["target_variant"] == "restricted_input_plus_top_migrated_risk"
    assert report["can_apply_live"] is False
    assert report["can_execute_trades"] is False
    variant = cast(dict[str, Any], report["variant"])
    assert variant["name"] == "restricted_input_plus_top_migrated_risk"
    assert "run_pre_live_dry_run.sh" in str(report["next_command"])
    assert "EXECUTION_MODE=live" not in str(report["next_command"])
    payload = cast(dict[str, Any], report["variant_payload"])
    contract = cast(dict[str, Any], payload["evaluation_contract"])
    assert payload["can_apply_live"] is False
    assert payload["can_execute_trades"] is False
    assert contract["can_promote_live"] is False
    assert contract["expected_removed_segments_count"] == 2


def test_next_variant_reports_missing_candidate(tmp_path: Path) -> None:
    ranking_path = tmp_path / "restricted_blocklist_ranking.json"
    write_json(
        ranking_path,
        {
            "report_version": "restricted_blocklist_ranking_v1",
            "observations": [],
            "can_execute_trades": False,
        },
    )

    report = build_next_migrated_risk_variant(ranking_path, tmp_path / "next")

    assert report["status"] == "missing_candidate"
    assert report["can_execute_trades"] is False
    assert (tmp_path / "next" / "restricted_blocklist_next_variant.json").exists()


def seed_restricted_observation(tmp_path: Path) -> Path:
    baseline = seed_report_root(tmp_path / "reports" / "run-1")
    candidate = seed_report_root(tmp_path / "reports" / "run-2")
    write_segments(
        baseline,
        [
            segment_row("market-1", "asset-1", realized_edge=0.01),
            segment_row("market-2", "asset-2", realized_edge=-0.02),
        ],
    )
    write_segments(candidate, [segment_row("market-1", "asset-1", realized_edge=0.04)])
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
    manifest_root = tmp_path / "research_runs"
    write_report_manifest(
        baseline,
        create_run_manifest(baseline, manifest_root, run_id="run-1"),
    )
    write_report_manifest(
        candidate,
        create_run_manifest(candidate, manifest_root, run_id="run-2"),
    )
    observation_root = tmp_path / "observation"
    observation_root.mkdir()
    write_restricted_blocklist_diagnostics(
        baseline,
        candidate,
        observation_root / "restricted_blocklist_diagnostics.json",
    )
    return observation_root


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
