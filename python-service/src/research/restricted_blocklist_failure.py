import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPORT_VERSION = "restricted_blocklist_observation_failure_v1"


def write_restricted_blocklist_failure(
    *,
    plan: dict[str, object],
    output_dir: Path,
    candidate_report_root: Path | None,
    exit_code: int,
    reason: str,
    stage: str,
    output_tail: str | None = None,
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = build_restricted_blocklist_failure(
        plan=plan,
        output_dir=output_dir,
        candidate_report_root=candidate_report_root,
        exit_code=exit_code,
        reason=reason,
        stage=stage,
        output_tail=output_tail,
    )
    write_json_atomic(output_dir / "restricted_blocklist_observation_failure.json", payload)
    return payload


def build_restricted_blocklist_failure(
    *,
    plan: dict[str, object],
    output_dir: Path,
    candidate_report_root: Path | None,
    exit_code: int,
    reason: str,
    stage: str,
    output_tail: str | None = None,
) -> dict[str, object]:
    report_root = candidate_report_root or inferred_candidate_report_root(plan)
    data_lake_root = inferred_data_lake_root(report_root)
    return {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "insufficient_evidence",
        "stage": stage,
        "reason": reason,
        "exit_code": exit_code,
        "dry_run_exit_code": exit_code,
        "exit_code_policy": "preserved",
        "can_execute_trades": False,
        "output_tail": output_tail or "",
        "plan": sanitized_plan(plan),
        "candidate_report_root": str(report_root) if report_root else None,
        "output_dir": str(output_dir),
        "diagnostics": {
            "classification": classify_failure(reason, report_root),
            "diagnosis_hints": diagnosis_hints(reason, report_root),
            "candidate_report_root_exists": bool(report_root and report_root.exists()),
            "data_lake_root": str(data_lake_root) if data_lake_root else None,
            "data_lake_root_exists": bool(data_lake_root and data_lake_root.exists()),
            "report_files": file_names(report_root),
            "data_lake_partitions": partition_summary(data_lake_root),
            "real_dry_run_evidence": read_json(report_root / "real_dry_run_evidence.json")
            if report_root
            else {},
        },
    }


def inferred_candidate_report_root(plan: dict[str, object]) -> Path | None:
    candidate = plan.get("candidate_report_root")
    if isinstance(candidate, str) and candidate:
        return Path(candidate)
    output = plan.get("output_dir")
    if isinstance(output, str) and output:
        return Path(output)
    return None


def inferred_data_lake_root(report_root: Path | None) -> Path | None:
    if report_root is None:
        return None
    parts = report_root.parts
    if "reports" not in parts:
        return None
    index = parts.index("reports")
    if index == 0:
        return None
    return Path(*parts[:index])


def classify_failure(reason: str, report_root: Path | None) -> str:
    normalized = reason.lower()
    if "no dry-run execution report found" in normalized:
        return "no_dry_run_execution_reports"
    if "missing real dry-run stream data" in normalized:
        return "missing_stream_data"
    if report_root is not None and not report_root.exists():
        return "missing_candidate_report_root"
    return "restricted_observation_failed"


def diagnosis_hints(reason: str, report_root: Path | None) -> list[str]:
    hints = [classify_failure(reason, report_root)]
    if report_root is not None and not report_root.exists():
        hints.append("report_root_missing_or_evidence_not_written")
    if "no dry-run execution report found" in reason.lower():
        hints.extend(
            [
                "check_signals_stream_for_eligible_signals",
                "check_rust_executor_rejections",
                "check_if_blocklist_removed_all_candidate_segments",
            ]
        )
    return sorted(set(hints))


def sanitized_plan(plan: dict[str, object]) -> dict[str, object]:
    keys = (
        "baseline_report_root",
        "diagnostics_path",
        "blocklist_kind",
        "blocklist_path",
        "market_asset_ids_count",
        "market_asset_ids_sha256",
        "duration_seconds",
        "candidate_report_root",
        "output_dir",
        "can_execute_trades",
    )
    return {key: plan.get(key) for key in keys if key in plan}


def partition_summary(data_lake_root: Path | None) -> dict[str, object]:
    if data_lake_root is None or not data_lake_root.exists():
        return {}
    output: dict[str, object] = {}
    for name in (
        "orderbook_snapshots",
        "orderbook_levels",
        "signals",
        "execution_reports",
        "market_metadata",
        "reports",
        "research_runs",
    ):
        path = data_lake_root / name
        output[name] = {
            "exists": path.exists(),
            "files": count_files(path),
            "parquet_files": count_files(path, suffix=".parquet"),
        }
    return output


def file_names(path: Path | None) -> list[str]:
    if path is None or not path.exists():
        return []
    return sorted(item.name for item in path.iterdir() if item.is_file())


def count_files(path: Path, suffix: str | None = None) -> int:
    if not path.exists():
        return 0
    return sum(
        1
        for item in path.rglob("*")
        if item.is_file() and (suffix is None or item.suffix == suffix)
    )


def read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(prog="restricted-blocklist-failure")
    parser.add_argument("--plan-json", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--candidate-report-root", type=Path)
    parser.add_argument("--exit-code", type=int, required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--stage", required=True)
    parser.add_argument("--output-tail")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    plan = json.loads(args.plan_json)
    if not isinstance(plan, dict):
        raise SystemExit("plan-json must decode to an object")
    payload = write_restricted_blocklist_failure(
        plan=plan,
        output_dir=args.output_dir,
        candidate_report_root=args.candidate_report_root,
        exit_code=args.exit_code,
        reason=args.reason,
        stage=args.stage,
        output_tail=args.output_tail,
    )
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
