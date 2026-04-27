import json
from pathlib import Path
from typing import Any

from src.config import settings


def latest_nim_budget(root: Path | None = None) -> dict[str, object]:
    manifest_root = root or Path(settings.data_lake_root) / "research_runs"
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


def latest_manifest(index_path: Path) -> dict[str, object] | None:
    selected: dict[str, object] | None = None
    for line in index_path.read_text(encoding="utf-8").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            selected = value
    return selected


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
