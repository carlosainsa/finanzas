import argparse
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.research.restricted_blocklist_ranking import (
    numeric_or_default,
    observation_row,
    typed_list,
    write_json,
)


REPORT_VERSION = "restricted_blocklist_observation_history_v1"


def build_restricted_blocklist_history(
    observation_roots: list[Path],
) -> dict[str, object]:
    observations = [observation_row(path) for path in observation_roots]
    by_kind: dict[str, list[dict[str, object]]] = defaultdict(list)
    for item in observations:
        by_kind[str(item.get("blocklist_kind") or "unknown")].append(item)
    return {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "observation_roots": [str(path) for path in observation_roots],
        "summary": history_summary(observations),
        "counts": {
            "by_status": count_values(observations, "status"),
            "by_recommendation": count_values(observations, "recommendation"),
            "by_failure_classification": count_values(
                observations,
                "failure_classification",
                include_missing=False,
            ),
            "by_blocklist_kind": count_values(observations, "blocklist_kind"),
        },
        "blocklist_kind_stability": [
            blocklist_kind_stability(kind, rows) for kind, rows in sorted(by_kind.items())
        ],
        "variant_family_summary": [
            variant_family_summary(kind, rows) for kind, rows in sorted(by_kind.items())
        ],
        "observations": [compact_observation(item) for item in observations],
        "can_execute_trades": False,
    }


def history_summary(observations: list[dict[str, object]]) -> dict[str, object]:
    complete = [item for item in observations if item.get("status") == "complete"]
    insufficient = [
        item for item in observations if item.get("status") == "insufficient_evidence"
    ]
    kind_groups: dict[str, list[dict[str, object]]] = defaultdict(list)
    for item in observations:
        kind_groups[str(item.get("blocklist_kind") or "unknown")].append(item)
    stability_reports = [
        blocklist_kind_stability(kind, rows) for kind, rows in kind_groups.items()
    ]
    stable_kinds = sum(
        1
        for item in stability_reports
        if item.get("stability_status")
        not in {"insufficient_history", "unstable"}
    )
    return {
        "observations": len(observations),
        "complete_observations": len(complete),
        "insufficient_evidence_observations": len(insufficient),
        "missing_artifacts_observations": sum(
            1 for item in observations if item.get("status") == "missing_artifacts"
        ),
        "blocklist_kinds": len(kind_groups),
        "stable_blocklist_kinds": stable_kinds,
        "unstable_blocklist_kinds": max(0, len(kind_groups) - stable_kinds),
        "blocked_observations": sum(
            1 for item in observations if typed_list(item.get("blockers"))
        ),
    }


def blocklist_kind_stability(
    blocklist_kind: str,
    observations: list[dict[str, object]],
) -> dict[str, object]:
    recommendations = count_values(observations, "recommendation")
    statuses = count_values(observations, "status")
    failure_classifications = count_values(
        observations,
        "failure_classification",
        include_missing=False,
    )
    scores = [
        numeric_or_default(item.get("score"), 0.0)
        for item in observations
        if item.get("score") is not None
    ]
    latest = sorted(
        observations,
        key=lambda item: str(item.get("observation_root") or ""),
    )[-1]
    recommendation_values = set(recommendations)
    status_values = set(statuses)
    failure_values = set(failure_classifications)
    stable_failure_classification = len(failure_values) == 1 and bool(failure_values)
    return {
        "blocklist_kind": blocklist_kind,
        "observations": len(observations),
        "statuses": statuses,
        "recommendations": recommendations,
        "failure_classifications": failure_classifications,
        "latest_observation_root": latest.get("observation_root"),
        "latest_status": latest.get("status"),
        "latest_recommendation": latest.get("recommendation"),
        "latest_failure_classification": latest.get("failure_classification"),
        "stable_recommendation": len(recommendation_values) == 1,
        "stable_status": len(status_values) == 1,
        "stable_failure_classification": stable_failure_classification,
        "stability_status": stability_status(
            observations=observations,
            stable_status=len(status_values) == 1,
            stable_recommendation=len(recommendation_values) == 1,
            stable_failure_classification=stable_failure_classification,
        ),
        "score_min": min(scores) if scores else None,
        "score_max": max(scores) if scores else None,
        "score_avg": sum(scores) / len(scores) if scores else None,
        "complete_observations": statuses.get("complete", 0),
        "insufficient_evidence_observations": statuses.get("insufficient_evidence", 0),
        "can_execute_trades": False,
    }


def variant_family_summary(
    variant_family: str,
    observations: list[dict[str, object]],
) -> dict[str, object]:
    complete = [item for item in observations if item.get("status") == "complete"]
    scores = numeric_values(observations, "score")
    risk_migration_count = sum(
        1
        for item in complete
        if item.get("risk_migration_status") == "risk_migration_detected"
    )
    unexpected_count = sum(
        1
        for item in complete
        if numeric_or_default(item.get("unexpected_blocked_segments"), 0.0) > 0
    )
    unexpected_values = numeric_values(complete, "unexpected_blocked_segments")
    recommendation = stable_variant_recommendation(
        variant_family=variant_family,
        complete=complete,
        risk_migration_count=risk_migration_count,
        unexpected_count=unexpected_count,
    )
    return {
        "variant_family": variant_family,
        "observations": len(observations),
        "complete_observations": len(complete),
        "score_min": min(scores) if scores else None,
        "score_max": max(scores) if scores else None,
        "score_avg": sum(scores) / len(scores) if scores else None,
        "risk_migration_detected_count": risk_migration_count,
        "risk_migration_detected_rate": (
            risk_migration_count / len(complete) if complete else None
        ),
        "unexpected_blocked_segments_count": unexpected_count,
        "unexpected_blocked_segments_rate": (
            unexpected_count / len(complete) if complete else None
        ),
        "unexpected_blocked_segments_total": (
            sum(unexpected_values) if unexpected_values else None
        ),
        "unexpected_blocked_segments_avg": (
            sum(unexpected_values) / len(unexpected_values)
            if unexpected_values
            else None
        ),
        "realized_edge_delta_avg": average_metric(complete, "realized_edge_delta"),
        "fill_rate_delta_avg": average_metric(complete, "fill_rate_delta"),
        "drawdown_delta_avg": average_metric(complete, "drawdown_delta"),
        "adverse_selection_delta_avg": average_metric(
            complete, "adverse_selection_delta"
        ),
        "stable_recommendation": recommendation,
        "can_execute_trades": False,
    }


def stable_variant_recommendation(
    *,
    variant_family: str,
    complete: list[dict[str, object]],
    risk_migration_count: int,
    unexpected_count: int,
) -> str:
    if not complete:
        return "NEED_MORE_EVIDENCE"
    if risk_migration_count or unexpected_count:
        if variant_family == "restricted_input_plus_all_migrated_risk":
            return "REDESIGN_STRATEGY"
        return "TRY_ALL_MIGRATED"
    decisions = {item.get("restricted_decision") for item in complete}
    recommendations = {item.get("recommendation") for item in complete}
    if decisions == {"REPEAT_OBSERVATION"} or recommendations == {"repeat_observation"}:
        return "REPEAT"
    if decisions == {"REJECT"}:
        return "REJECT"
    return "REDESIGN_STRATEGY"


def compact_observation(row: dict[str, object]) -> dict[str, object]:
    return {
        "observation_root": row.get("observation_root"),
        "status": row.get("status"),
        "blocklist_kind": row.get("blocklist_kind"),
        "recommendation": row.get("recommendation"),
        "restricted_decision": row.get("restricted_decision"),
        "failure_classification": row.get("failure_classification"),
        "score": row.get("score"),
        "realized_edge_delta": row.get("realized_edge_delta"),
        "fill_rate_delta": row.get("fill_rate_delta"),
        "drawdown_delta": row.get("drawdown_delta"),
        "adverse_selection_delta": row.get("adverse_selection_delta"),
        "can_execute_trades": False,
    }


def stability_status(
    *,
    observations: list[dict[str, object]],
    stable_status: bool,
    stable_recommendation: bool,
    stable_failure_classification: bool,
) -> str:
    if len(observations) < 2:
        return "insufficient_history"
    statuses = {item.get("status") for item in observations}
    recommendations = {item.get("recommendation") for item in observations}
    if statuses == {"insufficient_evidence"} and stable_failure_classification:
        return "stable_insufficient_evidence"
    if statuses == {"complete"} and recommendations == {"repeat_observation"}:
        return "stable_repeat_candidate"
    if statuses == {"complete"} and (
        recommendations == {"test_migrated_risk_variant"}
        or {item.get("risk_migration_status") for item in observations}
        == {"risk_migration_detected"}
    ):
        return "stable_migrated_risk"
    if stable_status and stable_recommendation:
        return "stable_other"
    return "unstable"


def count_values(
    observations: list[dict[str, object]],
    key: str,
    *,
    include_missing: bool = True,
) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for item in observations:
        value = item.get(key)
        if value is None or value == "":
            if not include_missing:
                continue
            value = "missing"
        counter[str(value)] += 1
    return dict(sorted(counter.items()))


def numeric_values(observations: list[dict[str, object]], key: str) -> list[float]:
    return [
        numeric_or_default(item.get(key), 0.0)
        for item in observations
        if item.get(key) is not None
    ]


def average_metric(observations: list[dict[str, object]], key: str) -> float | None:
    values = numeric_values(observations, key)
    return sum(values) / len(values) if values else None


def main() -> int:
    parser = argparse.ArgumentParser(prog="restricted-blocklist-history")
    parser.add_argument(
        "--observation-root",
        type=Path,
        action="append",
        required=True,
        help="restricted observation report root; repeat for multiple variants",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    report = build_restricted_blocklist_history(args.observation_root)
    if args.output:
        write_json(args.output, report)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
