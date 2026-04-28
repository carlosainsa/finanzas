import json
from pathlib import Path
from typing import Any, cast

from src.research.restricted_blocklist_decision import (
    finalize_restricted_blocklist_decision,
)


def test_restricted_decision_rejects_research_rejection(tmp_path: Path) -> None:
    root = seed_observation(tmp_path, promotion_decision="REJECT")

    payload = finalize_restricted_blocklist_decision(root)

    assert payload["restricted_decision"] == "REJECT"
    assert payload["reason"] == "research_promotion_decision_rejected"
    assert payload["can_execute_trades"] is False
    inputs = cast(dict[str, Any], payload["inputs"])
    assert inputs["restricted_blocklist_diagnostics_path"] == str(
        root / "restricted_blocklist_diagnostics.json"
    )


def test_restricted_decision_needs_more_data_when_not_comparable(
    tmp_path: Path,
) -> None:
    root = seed_observation(
        tmp_path,
        promotion_decision="NEED_MORE_DATA",
        verdict="no_comparable",
        comparability_reason="fixed_market_universe_mismatch",
    )

    payload = finalize_restricted_blocklist_decision(root)

    assert payload["restricted_decision"] == "NEED_MORE_DATA"
    assert payload["reason"] == "fixed_market_universe_mismatch"


def test_restricted_decision_repeats_accepted_observation(tmp_path: Path) -> None:
    root = seed_observation(
        tmp_path,
        promotion_decision="PROMOTE",
        assessment_status="accepted_for_observation",
    )

    payload = finalize_restricted_blocklist_decision(root)

    assert payload["restricted_decision"] == "REPEAT_OBSERVATION"
    assert payload["reason"] == "accepted_for_observation_requires_repeat"
    assert payload["can_execute_trades"] is False


def test_restricted_decision_is_idempotent_except_timestamp(tmp_path: Path) -> None:
    root = seed_observation(tmp_path, promotion_decision="REJECT")

    first = finalize_restricted_blocklist_decision(root)
    second = finalize_restricted_blocklist_decision(root)

    first_without_time = dict(first)
    second_without_time = dict(second)
    first_without_time.pop("generated_at")
    second_without_time.pop("generated_at")
    assert second_without_time == first_without_time
    output = json.loads((root / "restricted_blocklist_decision.json").read_text())
    assert output["restricted_decision"] == "REJECT"


def seed_observation(
    tmp_path: Path,
    *,
    promotion_decision: str,
    verdict: str = "candidate_improved",
    assessment_status: str = "accepted_for_observation",
    comparability_reason: str | None = None,
) -> Path:
    root = tmp_path / "restricted"
    root.mkdir()
    write_json(
        root / "restricted_blocklist_observation_summary.json",
        {
            "baseline_report_root": "/reports/baseline",
            "candidate_report_root": str(root),
            "blocklist_kind": "top_1",
            "blocklist_path": "/blocklists/top_1.json",
        },
    )
    write_json(
        root / "research_promotion_decision.json",
        {"decision": promotion_decision},
    )
    write_json(
        root / "comparison.json",
        {
            "comparison": {
                "verdict": verdict,
                "segment_comparability": {
                    "status": "no_comparable"
                    if verdict == "no_comparable"
                    else "comparable",
                    "reason": comparability_reason,
                },
                "restricted_blocklist_assessment": {
                    "status": assessment_status,
                },
            }
        },
    )
    return root


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(cast(dict[str, object], payload)) + "\n", encoding="utf-8")
