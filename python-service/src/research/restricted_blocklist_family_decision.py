import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.research.restricted_blocklist_ranking import read_json, typed_list, write_json


REPORT_VERSION = "restricted_blocklist_family_decision_v1"


def decide_restricted_blocklist_family(history_path: Path) -> dict[str, object]:
    history = read_json(history_path)
    families = [
        item for item in typed_list(history.get("variant_family_summary")) if isinstance(item, dict)
    ]
    decision, reason, selected_family = family_decision(families)
    return {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "history_path": str(history_path),
        "decision": decision,
        "reason": reason,
        "selected_family": selected_family,
        "family_count": len(families),
        "families": compact_families(families),
        "can_execute_trades": False,
    }


def family_decision(
    families: list[dict[str, Any]],
) -> tuple[str, str, dict[str, object] | None]:
    all_migrated = family_by_name(families, "restricted_input_plus_all_migrated_risk")
    if all_migrated is not None and positive_rate(
        all_migrated.get("risk_migration_detected_rate")
    ):
        return (
            "REDESIGN_STRATEGY",
            "all_migrated_variant_still_has_risk_migration",
            compact_family(all_migrated),
        )
    repeatable = [
        item
        for item in families
        if item.get("stable_recommendation") == "REPEAT"
        and not positive_rate(item.get("risk_migration_detected_rate"))
        and not positive_rate(item.get("unexpected_blocked_segments_rate"))
    ]
    if repeatable:
        selected = sorted(
            repeatable,
            key=lambda item: numeric_or_default(item.get("score_avg"), -1_000_000.0),
            reverse=True,
        )[0]
        return (
            "REPEAT_OBSERVATION",
            "repeatable_variant_without_migrated_risk",
            compact_family(selected),
        )
    top_migrated = family_by_name(families, "restricted_input_plus_top_migrated_risk")
    candidate = family_by_name(families, "candidate")
    if all_migrated is None and (
        has_migration(candidate) or has_migration(top_migrated)
    ):
        migration_family = top_migrated or candidate
        return (
            "TEST_ALL_MIGRATED",
            "migration_seen_before_all_migrated_variant",
            compact_family(migration_family) if migration_family else None,
        )
    if any(item.get("complete_observations") for item in families):
        selected = sorted(
            families,
            key=lambda item: numeric_or_default(item.get("score_avg"), -1_000_000.0),
            reverse=True,
        )[0]
        return (
            "CONTINUE_BLOCKLIST_TESTS",
            "complete_history_without_stable_decision",
            compact_family(selected),
        )
    return ("STOP_PRELIVE", "no_complete_restricted_observations", None)


def compact_families(families: list[dict[str, Any]]) -> list[dict[str, object]]:
    return [compact_family(item) for item in families]


def compact_family(family: dict[str, Any]) -> dict[str, object]:
    return {
        "variant_family": family.get("variant_family"),
        "complete_observations": family.get("complete_observations"),
        "score_avg": family.get("score_avg"),
        "risk_migration_detected_rate": family.get("risk_migration_detected_rate"),
        "unexpected_blocked_segments_rate": family.get(
            "unexpected_blocked_segments_rate"
        ),
        "realized_edge_delta_avg": family.get("realized_edge_delta_avg"),
        "fill_rate_delta_avg": family.get("fill_rate_delta_avg"),
        "drawdown_delta_avg": family.get("drawdown_delta_avg"),
        "adverse_selection_delta_avg": family.get("adverse_selection_delta_avg"),
        "stable_recommendation": family.get("stable_recommendation"),
    }


def family_by_name(
    families: list[dict[str, Any]],
    name: str,
) -> dict[str, Any] | None:
    for item in families:
        if item.get("variant_family") == name:
            return item
    return None


def has_migration(family: dict[str, Any] | None) -> bool:
    return family is not None and positive_rate(family.get("risk_migration_detected_rate"))


def positive_rate(value: object) -> bool:
    return numeric_or_default(value, 0.0) > 0.0


def numeric_or_default(value: object, default: float) -> float:
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return default


def main() -> int:
    parser = argparse.ArgumentParser(prog="restricted-blocklist-family-decision")
    parser.add_argument("--history", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    payload = decide_restricted_blocklist_family(args.history)
    if args.output:
        write_json(args.output, payload)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
