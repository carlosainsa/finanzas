import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPORT_VERSION = "pre_live_candidate_report_v1"
REQUIRED_ARTIFACTS = (
    "research_summary.json",
    "go_no_go.json",
    "pre_live_promotion.json",
    "market_opportunity_selector.json",
    "execution_quality.json",
    "real_dry_run_evidence.json",
)


def create_pre_live_candidate_report(
    report_root: Path,
    output_path: Path | None = None,
) -> dict[str, object]:
    artifacts = {name: artifact_status(report_root, name) for name in REQUIRED_ARTIFACTS}
    summary = read_json(report_root / "research_summary.json")
    go_no_go = read_json(report_root / "go_no_go.json")
    promotion = read_json(report_root / "pre_live_promotion.json")
    market_selection = read_json(report_root / "market_opportunity_selector.json")
    execution_quality = read_json(report_root / "execution_quality.json")
    candidate_market_ranking = read_json(report_root / "candidate_market_ranking.json")
    dry_run_evidence = read_json(report_root / "real_dry_run_evidence.json")
    readiness = read_json(report_root / "pre_live_readiness.json")
    manifest = read_json(report_root / "research_manifest.json")

    missing = [name for name, status in artifacts.items() if not status["available"]]
    blockers = candidate_blockers(
        missing,
        readiness,
        go_no_go,
        promotion,
        market_selection,
        execution_quality,
    )
    status = candidate_status(missing, readiness, blockers)
    report: dict[str, object] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "status": status,
        "recommendation": recommendation(status, readiness, go_no_go, market_selection),
        "run_id": manifest.get("run_id") or dry_run_evidence.get("run_id") or report_root.name,
        "created_at": manifest.get("created_at") or dry_run_evidence.get("created_at"),
        "report_root": str(report_root),
        "can_execute_trades": False,
        "decision_policy": "offline_operator_candidate_packet_only",
        "candidate": {
            "summary": {
                "passed": bool(summary.get("passed", False)),
                "pre_live_promotion_passed": bool(
                    summary.get("pre_live_promotion_passed", False)
                ),
                "go_no_go_passed": bool(summary.get("go_no_go_passed", False)),
                "agent_advisory_acceptable": bool(
                    summary.get("agent_advisory_acceptable", False)
                ),
            },
            "go_no_go": {
                "decision": go_no_go.get("decision", "NO_GO"),
                "profile": go_no_go.get("profile"),
                "passed": bool(go_no_go.get("passed", False)),
                "threshold_set_version": go_no_go.get("threshold_set_version"),
                "metrics": go_no_go.get("metrics", {}),
            },
            "readiness": {
                "status": readiness.get("status", "missing"),
                "blocker_count": len(typed_list(readiness.get("blockers"))),
            },
            "promotion": {
                "passed": bool(promotion.get("passed", False)),
                "metrics": promotion.get("metrics", {}),
                "checks": promotion.get("checks", []),
            },
            "market_selection": {
                "selected_market_asset_ids": market_selection.get(
                    "selected_market_asset_ids", []
                ),
                "counts": market_selection.get("counts", {}),
                "decision_policy": market_selection.get("decision_policy"),
            },
            "execution_quality": {
                "top_asset_ids": execution_quality.get("top_asset_ids", []),
                "counts": execution_quality.get("counts", {}),
                "decision_policy": execution_quality.get("decision_policy"),
            },
            "candidate_market_ranking": {
                "selected_market_asset_ids": candidate_market_ranking.get(
                    "selected_market_asset_ids", []
                ),
                "counts": candidate_market_ranking.get("counts", {}),
                "decision_policy": candidate_market_ranking.get("decision_policy"),
            },
            "dry_run_evidence": {
                "status": dry_run_evidence.get("status"),
                "execution_mode": dry_run_evidence.get("execution_mode"),
                "capture_seconds": dry_run_evidence.get("capture_seconds"),
                "stream_lengths": dry_run_evidence.get("stream_lengths", {}),
            },
        },
        "blockers": blockers,
        "artifacts": {
            **artifacts,
            "pre_live_readiness.json": artifact_status(
                report_root, "pre_live_readiness.json"
            ),
            "research_manifest.json": artifact_status(report_root, "research_manifest.json"),
            "candidate_market_ranking.json": artifact_status(
                report_root, "candidate_market_ranking.json"
            ),
        },
    }
    target = output_path or report_root / "pre_live_candidate_report.json"
    target.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def candidate_blockers(
    missing: list[str],
    readiness: dict[str, object],
    go_no_go: dict[str, object],
    promotion: dict[str, object],
    market_selection: dict[str, object],
    execution_quality: dict[str, object],
) -> list[dict[str, object]]:
    blockers = [
        {
            "check_name": "artifact_available",
            "artifact": name,
            "passed": False,
            "source": name,
        }
        for name in missing
    ]
    for item in typed_list(readiness.get("blockers")):
        if isinstance(item, dict):
            blockers.append(
                {
                    "check_name": item.get("check_name", "readiness_blocker"),
                    "passed": bool(item.get("passed", False)),
                    "source": "pre_live_readiness",
                    "metric_value": item.get("metric_value"),
                    "threshold": item.get("threshold"),
                }
            )
    if go_no_go and not bool(go_no_go.get("passed", False)):
        blockers.append(
            {
                "check_name": "go_no_go_passed",
                "passed": False,
                "source": "go_no_go",
                "metric_value": go_no_go.get("decision", "NO_GO"),
                "threshold": "GO",
            }
        )
    if promotion and not bool(promotion.get("passed", False)):
        blockers.append(
            {
                "check_name": "pre_live_promotion_passed",
                "passed": False,
                "source": "pre_live_promotion",
                "threshold": True,
            }
        )
    selected_assets = typed_list(market_selection.get("selected_market_asset_ids"))
    if market_selection and not selected_assets:
        blockers.append(
            {
                "check_name": "market_selection_has_candidates",
                "passed": False,
                "source": "market_opportunity_selector",
                "threshold": "at least one selected asset",
            }
        )
    top_assets = typed_list(execution_quality.get("top_asset_ids"))
    if execution_quality and not top_assets:
        blockers.append(
            {
                "check_name": "execution_quality_has_candidates",
                "passed": False,
                "source": "execution_quality",
                "threshold": "at least one ranked asset",
            }
        )
    return blockers


def candidate_status(
    missing: list[str],
    readiness: dict[str, object],
    blockers: list[dict[str, object]],
) -> str:
    if missing and len(missing) == len(REQUIRED_ARTIFACTS):
        return "missing"
    readiness_status = readiness.get("status")
    if readiness_status == "ready" and not blockers:
        return "ready"
    if readiness_status == "missing" and missing:
        return "missing"
    return "blocked" if blockers else "ready"


def recommendation(
    status: str,
    readiness: dict[str, object],
    go_no_go: dict[str, object],
    market_selection: dict[str, object],
) -> str:
    if status == "ready":
        return "advance_to_second_comparable_pre_live_run"
    if status == "missing":
        return "generate_pre_live_dry_run_artifacts"
    if readiness.get("status") == "blocked" or go_no_go.get("decision") == "NO_GO":
        return "investigate_blockers_before_repeat"
    if not typed_list(market_selection.get("selected_market_asset_ids")):
        return "collect_or_select_executable_market_candidates"
    return "investigate_blockers_before_repeat"


def artifact_status(report_root: Path, name: str) -> dict[str, object]:
    path = report_root / name
    return {
        "path": str(path),
        "available": path.exists(),
        "valid_json_object": bool(read_json(path)) if path.exists() else False,
    }


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def typed_list(value: object) -> list[object]:
    return value if isinstance(value, list) else []


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build a unified pre-live candidate report")
    parser.add_argument("--report-root", required=True)
    parser.add_argument("--output")
    args = parser.parse_args(argv)

    report = create_pre_live_candidate_report(
        Path(args.report_root),
        Path(args.output) if args.output else None,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "ready" else 2


if __name__ == "__main__":
    raise SystemExit(main())
