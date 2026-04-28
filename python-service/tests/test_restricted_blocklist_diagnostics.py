import json
from pathlib import Path
from typing import Any, cast

from src.research.restricted_blocklist_diagnostics import (
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
    unexpected = cast(dict[str, Any], payload["unexpected"])
    missing_expected = cast(dict[str, Any], payload["missing_expected"])
    assert unexpected["count"] == 1
    assert missing_expected["count"] == 1
    assert cast(dict[str, Any], unexpected["metrics"])["sample_count"] == 1
    assert (
        "candidate_pre_live_promotion_generated_unexpected_blocks"
        in cast(list[str], payload["diagnosis"])
    )
