import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.research.restricted_blocklist_diagnostics import (
    write_migrated_risk_variants,
)


REPORT_VERSION = "restricted_blocklist_next_variant_v1"
TARGET_VARIANT = "restricted_input_plus_top_migrated_risk"


def build_next_migrated_risk_variant(
    ranking_path: Path,
    output_dir: Path,
) -> dict[str, object]:
    ranking = read_json(ranking_path)
    candidate = select_candidate(ranking)
    if not candidate:
        report = base_report(ranking_path, "missing_candidate")
        report["reason"] = "ranking_has_no_migrated_risk_candidate"
        write_json_atomic(output_dir / "restricted_blocklist_next_variant.json", report)
        return report

    observation_root_value = candidate.get("observation_root")
    observation_root = (
        Path(observation_root_value) if isinstance(observation_root_value, str) else None
    )
    if observation_root is None:
        report = base_report(ranking_path, "invalid_candidate")
        report["reason"] = "candidate_observation_root_missing"
        report["selected_observation"] = candidate
        write_json_atomic(output_dir / "restricted_blocklist_next_variant.json", report)
        return report

    diagnostics_path = observation_root / "restricted_blocklist_diagnostics.json"
    diagnostics = read_json(diagnostics_path)
    variants_summary = write_migrated_risk_variants(diagnostics, output_dir)
    variant = variant_by_name(variants_summary, TARGET_VARIANT)
    if not variant:
        report = base_report(ranking_path, "missing_variant")
        report["reason"] = "target_variant_not_generated"
        report["selected_observation"] = candidate
        report["source_diagnostics_path"] = str(diagnostics_path)
        report["variants_summary"] = variants_summary
        write_json_atomic(output_dir / "restricted_blocklist_next_variant.json", report)
        return report

    variant_payload = read_json(Path(str(variant.get("path") or "")))
    validate_research_only_variant(variant, variant_payload)
    report = base_report(ranking_path, "generated")
    report.update(
        {
            "reason": "migrated_risk_candidate_selected_from_ranking",
            "selected_observation": candidate,
            "source_diagnostics_path": str(diagnostics_path),
            "variant": variant,
            "variant_payload": {
                "path": variant.get("path"),
                "variant_name": variant_payload.get("variant_name"),
                "can_apply_live": variant_payload.get("can_apply_live"),
                "can_execute_trades": variant_payload.get("can_execute_trades"),
                "decision_policy": variant_payload.get("decision_policy"),
                "evaluation_contract": variant_payload.get("evaluation_contract"),
            },
            "next_command": variant.get("next_command"),
            "variants_summary_path": str(output_dir / "migrated_risk_blocklist_variants.json"),
        }
    )
    write_json_atomic(output_dir / "restricted_blocklist_next_variant.json", report)
    return report


def select_candidate(ranking: dict[str, object]) -> dict[str, object] | None:
    observations = ranking.get("observations")
    if not isinstance(observations, list):
        return None
    for item in observations:
        if not isinstance(item, dict):
            continue
        if item.get("recommendation") != "test_migrated_risk_variant":
            continue
        if item.get("status") != "complete":
            continue
        if item.get("risk_migration_status") != "risk_migration_detected":
            continue
        return item
    return None


def base_report(ranking_path: Path, status: str) -> dict[str, object]:
    return {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "ranking_path": str(ranking_path),
        "status": status,
        "target_variant": TARGET_VARIANT,
        "decision_policy": "offline_restricted_blocklist_variant_only",
        "can_apply_live": False,
        "can_execute_trades": False,
    }


def variant_by_name(
    variants_summary: dict[str, object],
    name: str,
) -> dict[str, object] | None:
    variants = variants_summary.get("variants")
    if not isinstance(variants, list):
        return None
    for item in variants:
        if isinstance(item, dict) and item.get("name") == name:
            return item
    return None


def validate_research_only_variant(
    variant: dict[str, object],
    payload: dict[str, object],
) -> None:
    contract = typed_dict(payload.get("evaluation_contract"))
    if payload.get("can_apply_live") is not False:
        raise ValueError("restricted blocklist variant must keep can_apply_live=false")
    if payload.get("can_execute_trades") is not False:
        raise ValueError("restricted blocklist variant must keep can_execute_trades=false")
    if contract.get("can_promote_live") is not False:
        raise ValueError("restricted blocklist variant contract must block live promotion")
    command = str(variant.get("next_command") or "")
    if "EXECUTION_MODE=live" in command:
        raise ValueError("restricted blocklist variant next command must not enable live")


def read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def typed_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def main() -> int:
    parser = argparse.ArgumentParser(prog="restricted-blocklist-next-variant")
    parser.add_argument("--ranking", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    report = build_next_migrated_risk_variant(args.ranking, args.output_dir)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
