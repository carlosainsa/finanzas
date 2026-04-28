import argparse
import json
from pathlib import Path

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


def main() -> int:
    parser = argparse.ArgumentParser(prog="restricted-blocklist-diagnostics")
    parser.add_argument("--baseline-report-root", type=Path, required=True)
    parser.add_argument("--candidate-report-root", type=Path, required=True)
    parser.add_argument("--output", type=Path)
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
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
