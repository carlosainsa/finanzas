import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.research.compare_runs import compare_report_roots, typed_dict


def create_restricted_blocklist_diagnostics(
    baseline_report_root: Path,
    candidate_report_root: Path,
) -> dict[str, object]:
    report = compare_report_roots(baseline_report_root, candidate_report_root)
    comparison = typed_dict(report.get("comparison"))
    diagnostics = typed_dict(comparison.get("restricted_blocklist_diagnostics"))
    return {
        "baseline_report_root": str(baseline_report_root),
        "candidate_report_root": str(candidate_report_root),
        **diagnostics,
    }


def write_restricted_blocklist_diagnostics(
    baseline_report_root: Path,
    candidate_report_root: Path,
    output: Path,
) -> dict[str, object]:
    payload = create_restricted_blocklist_diagnostics(
        baseline_report_root,
        candidate_report_root,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.with_suffix(output.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(output)
    return payload


def write_migrated_risk_variants(
    diagnostics: dict[str, object],
    output_dir: Path,
) -> dict[str, object]:
    variants_dir = output_dir / "migrated_risk_variants"
    variants_dir.mkdir(parents=True, exist_ok=True)
    restricted_input = load_restricted_input_segments(diagnostics)
    migrated_risk = migrated_risk_segments(diagnostics)
    fixed_universe = fixed_universe_from_diagnostics(diagnostics)
    source_report_root = str(diagnostics.get("candidate_report_root") or "")
    if not migrated_risk:
        summary = {
            "report_version": "migrated_risk_blocklist_variants_v1",
            "generated_at": datetime.now(UTC).isoformat(),
            "source_diagnostics_status": diagnostics.get("status"),
            "source_diagnostics_path": str(
                output_dir / "restricted_blocklist_diagnostics.json"
            ),
            "variants": [],
            "can_execute_trades": False,
        }
        write_json_atomic(output_dir / "migrated_risk_blocklist_variants.json", summary)
        return summary
    variants = [
        variant_spec(
            "restricted_input_plus_top_migrated_risk",
            restricted_input + migrated_risk[:1],
            variants_dir,
            fixed_universe,
            source_report_root,
            diagnostics,
        ),
        variant_spec(
            "restricted_input_plus_all_migrated_risk",
            restricted_input + migrated_risk,
            variants_dir,
            fixed_universe,
            source_report_root,
            diagnostics,
        ),
        variant_spec(
            "migrated_risk_only",
            migrated_risk,
            variants_dir,
            fixed_universe,
            source_report_root,
            diagnostics,
        ),
    ]
    summary = {
        "report_version": "migrated_risk_blocklist_variants_v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "source_diagnostics_status": diagnostics.get("status"),
        "source_diagnostics_path": str(output_dir / "restricted_blocklist_diagnostics.json"),
        "variants": variants,
        "can_execute_trades": False,
    }
    write_json_atomic(output_dir / "migrated_risk_blocklist_variants.json", summary)
    return summary


def variant_spec(
    name: str,
    segments: list[dict[str, object]],
    variants_dir: Path,
    fixed_universe: dict[str, object],
    source_report_root: str,
    diagnostics: dict[str, object],
) -> dict[str, object]:
    path = variants_dir / f"blocked_segments_{name}.json"
    unique_segments = unique_blocklist_segments(segments)
    payload: dict[str, object] = {
        "version": "blocked_segments_v1",
        "source_report_version": "restricted_blocklist_diagnostics_v1",
        "source_report_root": source_report_root,
        "generated_at": datetime.now(UTC).isoformat(),
        "decision_policy": "migrated_risk_variant_requires_restricted_run_comparison",
        "variant_name": name,
        "can_apply_live": False,
        "can_execute_trades": False,
        "config": {
            "source": "restricted_blocklist_diagnostics",
            "source_status": diagnostics.get("status"),
            "source_efficacy_status": typed_dict(diagnostics.get("efficacy")).get(
                "status"
            ),
        },
        "segments": unique_segments,
        "evaluation_contract": {
            "version": "blocked_segments_evaluation_contract_v1",
            "comparability_policy_version": "segment_comparability_v2",
            "source_report_root": source_report_root,
            "can_promote_live": False,
            "required_outcome": "restricted_run_must_remain_comparable",
            "fixed_market_universe": fixed_universe,
            "expected_removed_segments": [
                segment_identity(segment) for segment in unique_segments
            ],
            "expected_removed_segments_count": len(unique_segments),
            "acceptance_criteria": [
                "candidate run keeps can_execute_trades false",
                "restricted run uses the fixed MARKET_ASSET_IDS universe recorded in this contract",
                "all missing candidate segments are listed in expected_removed_segments",
                "migrated risk does not reappear as unexpected candidate-generated blocklist",
                "protected metrics do not regress",
            ],
            "rejection_criteria": [
                "risk_migration_detected",
                "unexpected candidate-generated blocklist segments",
                "fixed market universe mismatch",
                "single-run-only evidence",
            ],
        },
    }
    write_json_atomic(path, payload)
    return {
        "name": name,
        "path": str(path),
        "blocked_segments": len(unique_segments),
        "can_execute_trades": False,
        "next_command": next_command(path, fixed_universe),
    }


def load_restricted_input_segments(
    diagnostics: dict[str, object],
) -> list[dict[str, object]]:
    source_paths = typed_dict(diagnostics.get("source_paths"))
    path_value = source_paths.get("restricted_input_blocklist_path")
    if not isinstance(path_value, str) or not path_value:
        path_value = source_paths.get("expected_restricted_input")
    if not isinstance(path_value, str) or not path_value:
        return []
    payload = read_json(Path(path_value))
    segments = payload.get("segments")
    return normalize_blocklist_segments(segments, fallback_reason="restricted_input")


def migrated_risk_segments(diagnostics: dict[str, object]) -> list[dict[str, object]]:
    unexpected = typed_dict(diagnostics.get("unexpected"))
    segments = unexpected.get("segments")
    return normalize_blocklist_segments(segments, fallback_reason="migrated_risk")


def normalize_blocklist_segments(
    value: object,
    *,
    fallback_reason: str,
) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    output: list[dict[str, object]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        metrics = typed_dict(item.get("metrics"))
        if not metrics:
            metrics = typed_dict(item.get("candidate_metrics"))
        if not metrics:
            metrics = typed_dict(item.get("baseline_metrics"))
        output.append(
            {
                "market_id": str(item.get("market_id") or ""),
                "asset_id": str(item.get("asset_id") or ""),
                "side": str(item.get("side") or ""),
                "strategy": str(item.get("strategy") or ""),
                "model_version": str(item.get("model_version") or ""),
                "reason": str(item.get("reason") or fallback_reason),
                "metrics": metrics,
            }
        )
    return output


def unique_blocklist_segments(
    segments: list[dict[str, object]],
) -> list[dict[str, object]]:
    output: dict[tuple[str, str, str, str, str], dict[str, object]] = {}
    for segment in segments:
        key = segment_key(segment)
        if key not in output:
            output[key] = segment
            continue
        current = output[key]
        current_reasons = reason_set(current.get("reason"))
        current_reasons.update(reason_set(segment.get("reason")))
        current["reason"] = ",".join(sorted(current_reasons))
        metrics = typed_dict(current.get("metrics"))
        metrics.update(typed_dict(segment.get("metrics")))
        current["metrics"] = metrics
    return list(output.values())


def fixed_universe_from_diagnostics(diagnostics: dict[str, object]) -> dict[str, object]:
    source_paths = typed_dict(diagnostics.get("source_paths"))
    path_value = source_paths.get("restricted_input_blocklist_path")
    if not isinstance(path_value, str) or not path_value:
        path_value = source_paths.get("expected_restricted_input")
    if isinstance(path_value, str) and path_value:
        payload = read_json(Path(path_value))
        contract = typed_dict(payload.get("evaluation_contract"))
        fixed = typed_dict(contract.get("fixed_market_universe"))
        if fixed:
            return fixed
    return {"version": "fixed_market_universe_v1", "market_asset_ids": []}


def next_command(path: Path, fixed_universe: dict[str, object]) -> str:
    prefix = fixed_universe_command_prefix(fixed_universe)
    return (
        f"{prefix}PREDICTOR_BLOCKED_SEGMENTS_PATH={path} "
        "scripts/run_pre_live_dry_run.sh --duration-seconds 900"
    )


def fixed_universe_command_prefix(fixed_universe: dict[str, object]) -> str:
    value = fixed_universe.get("market_asset_ids_csv")
    if not isinstance(value, str) or not value:
        return ""
    return f"MARKET_ASSET_IDS={shell_quote(value)} "


def shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def segment_identity(segment: dict[str, object]) -> dict[str, str]:
    return {
        "market_id": str(segment.get("market_id") or ""),
        "asset_id": str(segment.get("asset_id") or ""),
        "side": str(segment.get("side") or ""),
        "strategy": str(segment.get("strategy") or ""),
        "model_version": str(segment.get("model_version") or ""),
    }


def segment_key(segment: dict[str, object]) -> tuple[str, str, str, str, str]:
    identity = segment_identity(segment)
    return (
        identity["market_id"],
        identity["asset_id"],
        identity["side"],
        identity["strategy"],
        identity["model_version"],
    )


def reason_set(value: object) -> set[str]:
    return {item for item in str(value or "").split(",") if item}


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


def main() -> int:
    parser = argparse.ArgumentParser(prog="restricted-blocklist-diagnostics")
    parser.add_argument("--baseline-report-root", type=Path, required=True)
    parser.add_argument("--candidate-report-root", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--variants-output-dir", type=Path)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    if args.output:
        payload = write_restricted_blocklist_diagnostics(
            args.baseline_report_root,
            args.candidate_report_root,
            args.output,
        )
    else:
        payload = create_restricted_blocklist_diagnostics(
            args.baseline_report_root,
            args.candidate_report_root,
        )
    if args.variants_output_dir:
        payload["migrated_risk_variants"] = write_migrated_risk_variants(
            payload,
            args.variants_output_dir,
        )
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
