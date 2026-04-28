import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPORT_VERSION = "restricted_blocklist_decision_v1"


def finalize_restricted_blocklist_decision(
    observation_root: Path,
    *,
    output: Path | None = None,
) -> dict[str, object]:
    comparison = read_json(observation_root / "comparison.json")
    promotion = read_json(observation_root / "research_promotion_decision.json")
    summary = read_json(observation_root / "restricted_blocklist_observation_summary.json")
    decision, reason, next_step = restricted_decision(comparison, promotion)
    payload: dict[str, object] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "observation_root": str(observation_root),
        "baseline_report_root": summary.get("baseline_report_root"),
        "candidate_report_root": summary.get("candidate_report_root"),
        "blocklist_kind": summary.get("blocklist_kind"),
        "blocklist_path": summary.get("blocklist_path"),
        "research_promotion_decision": promotion.get("decision"),
        "restricted_decision": decision,
        "reason": reason,
        "next_step": next_step,
        "can_execute_trades": False,
        "inputs": {
            "comparison_path": str(observation_root / "comparison.json"),
            "restricted_blocklist_diagnostics_path": str(
                observation_root / "restricted_blocklist_diagnostics.json"
            ),
            "research_promotion_decision_path": str(
                observation_root / "research_promotion_decision.json"
            ),
            "observation_summary_path": str(
                observation_root / "restricted_blocklist_observation_summary.json"
            ),
        },
    }
    output_path = output or observation_root / "restricted_blocklist_decision.json"
    write_json(output_path, payload)
    return payload


def restricted_decision(
    comparison_report: dict[str, object],
    promotion_report: dict[str, object],
) -> tuple[str, str, str]:
    comparison = typed_dict(comparison_report.get("comparison"))
    assessment = typed_dict(comparison.get("restricted_blocklist_assessment"))
    assessment_status = assessment.get("status")
    promotion_decision = promotion_report.get("decision")
    verdict = comparison.get("verdict")
    comparability = typed_dict(comparison.get("segment_comparability"))

    if promotion_decision == "REJECT":
        return (
            "REJECT",
            "research_promotion_decision_rejected",
            "Review regressions and generate a narrower or defensive candidate.",
        )
    if assessment_status == "rejected":
        return (
            "REJECT",
            "restricted_blocklist_assessment_rejected",
            "Inspect protected metric regressions before another restricted run.",
        )
    if verdict == "no_comparable" or promotion_decision == "NEED_MORE_DATA":
        return (
            "NEED_MORE_DATA",
            str(comparability.get("reason") or "comparison_needs_more_data"),
            "Repeat with the same fixed universe, longer duration, or a narrower candidate.",
        )
    if assessment_status == "need_more_data":
        return (
            "NEED_MORE_DATA",
            str(assessment.get("reason") or "restricted_assessment_needs_more_data"),
            "Repeat restricted observation before changing thresholds.",
        )
    if (
        promotion_decision == "PROMOTE"
        and assessment_status == "accepted_for_observation"
    ):
        return (
            "REPEAT_OBSERVATION",
            "accepted_for_observation_requires_repeat",
            "Repeat the restricted run with the same fixed universe before promotion.",
        )
    return (
        "NEED_MORE_DATA",
        "insufficient_restricted_decision_evidence",
        "Inspect comparison.json and research_promotion_decision.json.",
    )


def read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def typed_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def main() -> int:
    parser = argparse.ArgumentParser(prog="restricted-blocklist-decision")
    parser.add_argument("--observation-root", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    payload = finalize_restricted_blocklist_decision(
        args.observation_root,
        output=args.output,
    )
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
