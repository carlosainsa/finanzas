import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

import asyncpg  # type: ignore[import-untyped]

from src.api.research_service import (
    latest_manifest,
    read_report_json,
    resolved_manifest_root,
)
from src.config import settings

REPORT_VERSION = "pre_live_readiness_v1"
STRICT_PROFILES = {"pre_live", "live_candidate"}


def build_pre_live_readiness(
    manifest_root: Path | None = None,
    audit_summary: dict[str, object] | None = None,
) -> dict[str, object]:
    resolved_root = resolved_manifest_root(manifest_root)
    index_path = resolved_root / "research_runs.jsonl"
    audit = audit_summary or {"status": "not_checked", "source": "postgres"}
    if not index_path.exists():
        return missing_readiness(index_path, audit)

    latest = latest_manifest(index_path)
    if latest is None:
        return missing_readiness(index_path, audit)

    report_root_value = latest.get("report_root")
    report_root = Path(report_root_value) if isinstance(report_root_value, str) else None
    go_no_go = read_report_json(report_root / "go_no_go.json") if report_root else {}
    dry_run_evidence = (
        read_report_json(report_root / "real_dry_run_evidence.json") if report_root else {}
    )
    research_summary = (
        read_report_json(report_root / "research_summary.json") if report_root else {}
    )
    pre_live_promotion = (
        read_report_json(report_root / "pre_live_promotion.json") if report_root else {}
    )

    profile = string_value(go_no_go.get("profile") or metric_value(latest, "go_no_go_profile"))
    decision = string_value(go_no_go.get("decision") or metric_value(latest, "go_no_go_decision"))
    go_no_go_passed = bool(go_no_go.get("passed", latest.get("go_no_go_passed", False)))
    blockers = blockers_from(go_no_go)
    checks = [
        readiness_check(
            "research_manifest_available",
            True,
            "latest research run found",
            source=str(index_path),
        ),
        readiness_check(
            "research_summary_available",
            artifact_exists(report_root, "research_summary.json"),
            "research_summary.json exists",
            source=artifact_path(report_root, "research_summary.json"),
        ),
        readiness_check(
            "real_dry_run_evidence_available",
            artifact_exists(report_root, "real_dry_run_evidence.json"),
            "real_dry_run_evidence.json exists",
            source=artifact_path(report_root, "real_dry_run_evidence.json"),
        ),
        readiness_check(
            "dry_run_execution_mode",
            dry_run_evidence.get("execution_mode") == "dry_run",
            "real market run stayed in dry_run mode",
            metric_value=dry_run_evidence.get("execution_mode"),
            threshold="dry_run",
        ),
        readiness_check(
            "go_no_go_report_available",
            artifact_exists(report_root, "go_no_go.json"),
            "go_no_go.json exists",
            source=artifact_path(report_root, "go_no_go.json"),
        ),
        readiness_check(
            "go_no_go_profile_pre_live",
            profile in STRICT_PROFILES,
            "go/no-go used a pre-live or stricter profile",
            metric_value=profile,
            threshold="pre_live|live_candidate",
        ),
        readiness_check(
            "go_no_go_passed",
            go_no_go_passed,
            "quantitative pre-live gate passed",
            metric_value=decision,
            threshold="GO",
        ),
        readiness_check(
            "pre_live_promotion_available",
            artifact_exists(report_root, "pre_live_promotion.json"),
            "pre_live_promotion.json exists",
            source=artifact_path(report_root, "pre_live_promotion.json"),
        ),
        readiness_check(
            "postgres_audit_available",
            audit.get("status") == "ok",
            "Postgres operational audit is readable",
            metric_value=audit.get("status"),
            threshold="ok",
        ),
    ]
    status = "ready" if all(bool(check["passed"]) for check in checks) else "blocked"
    readiness_blockers = [
        check for check in checks if not bool(check.get("passed"))
    ] + blockers
    return {
        "report_version": REPORT_VERSION,
        "status": status,
        "source": str(index_path),
        "run_id": latest.get("run_id"),
        "created_at": latest.get("created_at"),
        "report_root": str(report_root) if report_root else None,
        "can_execute_trades": False,
        "go_no_go": {
            "decision": decision or "NO_GO",
            "profile": profile,
            "passed": go_no_go_passed,
            "reason": go_no_go.get("reason"),
            "threshold_set_version": go_no_go.get("threshold_set_version"),
        },
        "checks": checks,
        "blockers": readiness_blockers,
        "audit": audit,
        "artifacts": {
            "go_no_go": artifact_status(report_root, "go_no_go.json", go_no_go),
            "research_summary": artifact_status(
                report_root, "research_summary.json", research_summary
            ),
            "real_dry_run_evidence": artifact_status(
                report_root, "real_dry_run_evidence.json", dry_run_evidence
            ),
            "pre_live_promotion": artifact_status(
                report_root, "pre_live_promotion.json", pre_live_promotion
            ),
        },
    }


async def postgres_audit_summary(database_url: str) -> dict[str, object]:
    conn = await asyncpg.connect(database_url)
    try:
        return {
            "status": "ok",
            "source": "postgres",
            "orders": await table_count(conn, "orders"),
            "execution_reports": await table_count(conn, "execution_reports"),
            "trade_signals": await table_count(conn, "trade_signals"),
            "positions": await table_count(conn, "positions"),
            "reconciliation_events": await table_count(conn, "reconciliation_events"),
            "control_commands": await table_count(conn, "control_commands"),
            "control_results": await table_count(conn, "control_results"),
        }
    except Exception as exc:
        return {"status": "error", "source": "postgres", "error": str(exc)}
    finally:
        await conn.close()


async def table_count(conn: asyncpg.Connection, table: str) -> int:
    exists = await conn.fetchval("select to_regclass($1) is not null", f"public.{table}")
    if not exists:
        raise RuntimeError(f"missing table: {table}")
    value = await conn.fetchval(f"select count(*) from {table}")
    return int(value or 0)


def missing_readiness(index_path: Path, audit: dict[str, object]) -> dict[str, object]:
    check = readiness_check(
        "research_manifest_available",
        False,
        "no research_runs.jsonl entry found",
        source=str(index_path),
    )
    return {
        "report_version": REPORT_VERSION,
        "status": "missing",
        "source": str(index_path),
        "run_id": None,
        "created_at": None,
        "report_root": None,
        "can_execute_trades": False,
        "go_no_go": {"decision": "NO_GO", "profile": None, "passed": False},
        "checks": [check],
        "blockers": [check],
        "audit": audit,
        "artifacts": {},
    }


def readiness_check(
    check_name: str,
    passed: bool,
    description: str,
    **fields: object,
) -> dict[str, object]:
    return {
        "check_name": check_name,
        "passed": passed,
        "description": description,
        **fields,
    }


def artifact_status(
    report_root: Path | None, file_name: str, payload: dict[str, object]
) -> dict[str, object]:
    return {
        "path": artifact_path(report_root, file_name),
        "available": artifact_exists(report_root, file_name),
        "valid_json_object": bool(payload),
    }


def artifact_path(report_root: Path | None, file_name: str) -> str | None:
    return str(report_root / file_name) if report_root else None


def artifact_exists(report_root: Path | None, file_name: str) -> bool:
    return bool(report_root and (report_root / file_name).exists())


def metric_value(manifest: dict[str, object], key: str) -> object:
    metrics = manifest.get("metrics")
    if isinstance(metrics, dict):
        return metrics.get(key)
    return None


def blockers_from(go_no_go: dict[str, object]) -> list[object]:
    blockers = go_no_go.get("blockers")
    return blockers if isinstance(blockers, list) else []


def string_value(value: object) -> str | None:
    return value if isinstance(value, str) else None


def load_readiness_report(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid readiness report: {path}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"readiness report must be a JSON object: {path}")
    return value


def summarize_readiness_report(report: dict[str, object]) -> dict[str, object]:
    go_no_go = object_dict(report.get("go_no_go"))
    audit = object_dict(report.get("audit"))
    blockers = object_list(report.get("blockers"))
    artifacts = object_dict(report.get("artifacts"))
    status = string_value(report.get("status")) or "missing"
    return {
        "status": status,
        "recommendation": readiness_recommendation(status, blockers),
        "run_id": report.get("run_id"),
        "report_root": report.get("report_root"),
        "can_execute_trades": False,
        "go_no_go_decision": go_no_go.get("decision"),
        "go_no_go_profile": go_no_go.get("profile"),
        "audit_status": audit.get("status"),
        "blocker_count": len(blockers),
        "blockers": blocker_names(blockers),
        "artifact_paths": artifact_paths(artifacts),
    }


def format_readiness_summary(summary: dict[str, object]) -> str:
    lines = [
        "pre_live_readiness_summary",
        f"status={summary.get('status')}",
        f"recommendation={summary.get('recommendation')}",
        f"run_id={summary.get('run_id')}",
        f"profile={summary.get('go_no_go_profile')}",
        f"decision={summary.get('go_no_go_decision')}",
        f"audit={summary.get('audit_status')}",
        f"blockers={summary.get('blocker_count')}",
        "can_execute_trades=false",
    ]
    blockers = object_list(summary.get("blockers"))
    if blockers:
        lines.append("blocker_names=" + ",".join(str(item) for item in blockers))
    artifact_paths_value = object_dict(summary.get("artifact_paths"))
    for name, path in sorted(artifact_paths_value.items()):
        lines.append(f"artifact.{name}={path}")
    return "\n".join(lines) + "\n"


def readiness_recommendation(status: str, blockers: list[object]) -> str:
    if status == "ready":
        return "advance_to_second_comparable_pre_live_run"
    if status == "missing":
        return "generate_pre_live_dry_run_artifacts"
    if blockers:
        return "investigate_blockers_before_repeat"
    return "repeat_pre_live_dry_run"


def blocker_names(blockers: list[object]) -> list[str]:
    names: list[str] = []
    for blocker in blockers:
        blocker_dict = object_dict(blocker)
        name = blocker_dict.get("check_name") or blocker_dict.get("name")
        if name is not None:
            names.append(str(name))
        else:
            names.append(str(blocker))
    return names


def artifact_paths(artifacts: dict[str, object]) -> dict[str, str | None]:
    paths: dict[str, str | None] = {}
    for name, value in artifacts.items():
        item = object_dict(value)
        path = item.get("path")
        paths[name] = str(path) if path is not None else None
    return paths


def object_dict(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


def object_list(value: object) -> list[object]:
    return value if isinstance(value, list) else []


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pre-live-readiness")
    parser.add_argument("--input", type=Path)
    parser.add_argument("--manifest-root", type=Path)
    parser.add_argument("--database-url", default=settings.database_url)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--format",
        choices=("full-json", "summary", "summary-json"),
        default="full-json",
    )
    args = parser.parse_args(argv)

    if args.input:
        report = load_readiness_report(args.input)
    else:
        audit = None
        if args.database_url:
            audit = asyncio.run(postgres_audit_summary(args.database_url))
        report = build_pre_live_readiness(args.manifest_root, audit_summary=audit)
    payload = format_report_payload(report, args.format)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload, encoding="utf-8")
    else:
        print(payload, end="")
    return 0 if report["status"] == "ready" else 2


def format_report_payload(report: dict[str, object], output_format: str) -> str:
    if output_format == "summary":
        return format_readiness_summary(summarize_readiness_report(report))
    if output_format == "summary-json":
        return json.dumps(
            summarize_readiness_report(report), indent=2, sort_keys=True
        ) + "\n"
    return json.dumps(report, indent=2, sort_keys=True) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
