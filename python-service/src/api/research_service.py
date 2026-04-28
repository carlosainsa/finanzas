import json
import re
from pathlib import Path
from typing import Any

from src.config import settings

RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")


def latest_nim_budget(root: Path | None = None) -> dict[str, object]:
    manifest_root = resolved_manifest_root(root)
    index_path = manifest_root / "research_runs.jsonl"
    if not index_path.exists():
        return empty_nim_budget(index_path)

    latest = latest_manifest(index_path)
    if latest is None:
        return empty_nim_budget(index_path)

    counts = typed_dict(latest.get("counts"))
    versions = typed_dict(latest.get("versions"))
    return {
        "status": "ok",
        "source": str(index_path),
        "run_id": latest.get("run_id"),
        "report_root": latest.get("report_root"),
        "enabled": counts.get("nim_advisory_enabled"),
        "nim_model": versions.get("nim_advisory_model"),
        "annotations": counts.get("nim_advisory_annotations"),
        "failures": counts.get("nim_advisory_failures"),
        "prompt_tokens": counts.get("nim_advisory_prompt_tokens"),
        "completion_tokens": counts.get("nim_advisory_completion_tokens"),
        "total_tokens": counts.get("nim_advisory_total_tokens"),
        "latency_ms_avg": counts.get("nim_advisory_latency_ms_avg"),
        "estimated_cost": counts.get("nim_advisory_estimated_cost"),
        "budget_status": counts.get("nim_advisory_budget_status"),
        "budget_violations": parse_violations(
            counts.get("nim_advisory_budget_violations")
        ),
        "can_execute_trades": False,
        "updated_at": latest.get("created_at"),
    }


def latest_go_no_go(root: Path | None = None) -> dict[str, object]:
    manifest_root = resolved_manifest_root(root)
    index_path = manifest_root / "research_runs.jsonl"
    if not index_path.exists():
        return empty_go_no_go(index_path)

    latest = latest_manifest(index_path)
    if latest is None:
        return empty_go_no_go(index_path)

    report_root_value = latest.get("report_root")
    report_root = Path(report_root_value) if isinstance(report_root_value, str) else None
    go_no_go = read_report_json(report_root / "go_no_go.json") if report_root else {}
    metrics = typed_dict(latest.get("metrics"))
    counts = typed_dict(latest.get("counts"))
    blockers = parse_violations(metrics.get("go_no_go_blockers"))
    return {
        "status": "ok" if go_no_go else "missing_report",
        "source": str(report_root / "go_no_go.json") if report_root else str(index_path),
        "run_id": latest.get("run_id"),
        "created_at": latest.get("created_at"),
        "decision": go_no_go.get("decision") or metrics.get("go_no_go_decision"),
        "passed": bool(go_no_go.get("passed", latest.get("go_no_go_passed", False))),
        "can_execute_trades": False,
        "reason": go_no_go.get("reason"),
        "blockers": go_no_go.get("blockers") if isinstance(go_no_go.get("blockers"), list) else blockers,
        "metrics": go_no_go.get("metrics") if isinstance(go_no_go.get("metrics"), dict) else {
            "realized_edge": metrics.get("realized_edge"),
            "fill_rate": metrics.get("fill_rate"),
            "slippage": metrics.get("slippage"),
            "adverse_selection": metrics.get("adverse_selection"),
            "drawdown": metrics.get("drawdown"),
            "stale_data_rate": metrics.get("stale_data_rate"),
            "reconciliation_divergence_rate": metrics.get("reconciliation_divergence_rate"),
            "test_brier_score": metrics.get("test_brier_score"),
            "test_log_loss": metrics.get("test_log_loss"),
        },
        "checks": go_no_go.get("checks") if isinstance(go_no_go.get("checks"), list) else [],
        "pre_live_gate_passed": latest.get("pre_live_gate_passed"),
        "calibration_passed": latest.get("calibration_passed"),
        "pre_live_promotion_passed": latest.get("pre_live_promotion_passed"),
        "agent_advisory_acceptable": latest.get("agent_advisory_acceptable"),
        "nim_budget_status": counts.get("nim_advisory_budget_status"),
    }


def list_research_runs(root: Path | None = None, limit: int = 20) -> dict[str, object]:
    manifest_root = resolved_manifest_root(root)
    index_path = manifest_root / "research_runs.jsonl"
    if not index_path.exists():
        return {"runs": [], "source": str(index_path)}
    bounded_limit = max(1, min(limit, 200))
    runs = [summarize_run(item) for item in read_manifest_index(index_path)]
    return {"runs": list(reversed(runs))[:bounded_limit], "source": str(index_path)}


def get_research_run(run_id: str, root: Path | None = None) -> dict[str, object]:
    manifest_root = resolved_manifest_root(root)
    if not RUN_ID_PATTERN.fullmatch(run_id):
        return {
            "status": "invalid_run_id",
            "source": str(manifest_root / "runs"),
            "run": None,
            "can_execute_trades": False,
        }
    run_path = manifest_root / "runs" / f"{run_id}.json"
    if not run_path.exists():
        return {
            "status": "missing",
            "source": str(run_path),
            "run": None,
            "can_execute_trades": False,
        }
    try:
        payload = json.loads(run_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {
            "status": "invalid",
            "source": str(run_path),
            "run": None,
            "can_execute_trades": False,
        }
    if not isinstance(payload, dict):
        return {
            "status": "invalid",
            "source": str(run_path),
            "run": None,
            "can_execute_trades": False,
        }
    return {
        "status": "ok",
        "source": str(run_path),
        "run": payload,
        "can_execute_trades": False,
    }


def read_manifest_index(index_path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for line in index_path.read_text(encoding="utf-8").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows


def read_report_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def latest_manifest(index_path: Path) -> dict[str, object] | None:
    rows = read_manifest_index(index_path)
    return rows[-1] if rows else None


def summarize_run(item: dict[str, object]) -> dict[str, object]:
    counts = typed_dict(item.get("counts"))
    metrics = typed_dict(item.get("metrics"))
    versions = typed_dict(item.get("versions"))
    return {
        "run_id": item.get("run_id"),
        "created_at": item.get("created_at"),
        "source": item.get("source"),
        "report_root": item.get("report_root"),
        "passed": item.get("passed"),
        "pre_live_gate_passed": item.get("pre_live_gate_passed"),
        "calibration_passed": item.get("calibration_passed"),
        "pre_live_promotion_passed": item.get("pre_live_promotion_passed"),
        "go_no_go_passed": item.get("go_no_go_passed"),
        "feature_research_decision": item.get("feature_research_decision"),
        "go_no_go_decision": metrics.get("go_no_go_decision"),
        "realized_edge": metrics.get("realized_edge"),
        "fill_rate": metrics.get("fill_rate"),
        "nim_budget_status": counts.get("nim_advisory_budget_status"),
        "nim_total_tokens": counts.get("nim_advisory_total_tokens"),
        "nim_estimated_cost": counts.get("nim_advisory_estimated_cost"),
        "nim_model": versions.get("nim_advisory_model"),
        "can_execute_trades": False,
    }


def empty_nim_budget(index_path: Path) -> dict[str, object]:
    return {
        "status": "missing",
        "source": str(index_path),
        "run_id": None,
        "report_root": None,
        "enabled": None,
        "nim_model": None,
        "annotations": None,
        "failures": None,
        "prompt_tokens": None,
        "completion_tokens": None,
        "total_tokens": None,
        "latency_ms_avg": None,
        "estimated_cost": None,
        "budget_status": None,
        "budget_violations": [],
        "can_execute_trades": False,
        "updated_at": None,
    }


def empty_go_no_go(index_path: Path) -> dict[str, object]:
    return {
        "status": "missing",
        "source": str(index_path),
        "run_id": None,
        "created_at": None,
        "decision": "NO_GO",
        "passed": False,
        "can_execute_trades": False,
        "reason": "missing_research_run",
        "blockers": [{"check_name": "research_run_available", "passed": False}],
        "metrics": {},
        "checks": [],
        "pre_live_gate_passed": None,
        "calibration_passed": None,
        "pre_live_promotion_passed": None,
        "agent_advisory_acceptable": None,
        "nim_budget_status": None,
    }


def resolved_manifest_root(root: Path | None = None) -> Path:
    return root or Path(settings.data_lake_root) / "research_runs"


def parse_violations(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if not isinstance(value, str) or not value:
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return [value]
    if isinstance(parsed, list):
        return [str(item) for item in parsed]
    return []


def typed_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}
