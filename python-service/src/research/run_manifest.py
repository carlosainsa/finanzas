import json
import os
import hashlib
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd  # type: ignore[import-untyped]


MANIFEST_SCHEMA_VERSION = "research_run_manifest_v1"


def create_run_manifest(
    report_root: Path,
    manifest_root: Path,
    run_id: str | None = None,
    source: str = "research_loop",
) -> dict[str, object]:
    resolved_run_id = run_id or report_root.name
    summary = read_json(report_root / "research_summary.json")
    promotion = read_json(report_root / "pre_live_promotion.json")
    advisory = read_json(report_root / "agent_advisory.json")
    baseline = read_json(report_root / "baseline.json")
    calibration = read_json(report_root / "calibration.json")
    backtest = read_json(report_root / "backtest.json")

    manifest: dict[str, object] = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "run_id": resolved_run_id,
        "source": source,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "git_commit": git_commit(report_root),
        "report_root": str(report_root),
        "passed": bool(summary.get("passed", False)),
        "pre_live_gate_passed": bool(summary.get("pre_live_gate_passed", False)),
        "calibration_passed": bool(summary.get("calibration_passed", False)),
        "pre_live_promotion_passed": bool(summary.get("pre_live_promotion_passed", False)),
        "agent_advisory_acceptable": bool(summary.get("agent_advisory_acceptable", False)),
        "versions": {
            "promotion_report": promotion.get("report_version"),
            "advisory_report": advisory.get("report_version"),
            "advisory_model": advisory.get("model_version"),
            "advisory_data": advisory.get("data_version"),
            "advisory_feature": advisory.get("feature_version"),
            "baseline_model": baseline.get("model_version"),
            "baseline_data": baseline.get("data_version"),
            "baseline_feature": baseline.get("feature_version"),
        },
        "metrics": manifest_metrics(promotion, advisory, calibration, backtest),
        "counts": manifest_counts(summary, baseline, backtest),
        "artifacts": artifact_metadata(report_root),
    }
    write_manifest(manifest_root, manifest)
    return manifest


def write_manifest(manifest_root: Path, manifest: dict[str, object]) -> None:
    run_id = str(manifest["run_id"])
    runs_dir = manifest_root / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    (runs_dir / f"{run_id}.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    rebuild_manifest_index(manifest_root)


def rebuild_manifest_index(manifest_root: Path) -> None:
    runs_dir = manifest_root / "runs"
    manifests = [
        read_json(path)
        for path in sorted(runs_dir.glob("*.json"))
        if read_json(path).get("schema_version") == MANIFEST_SCHEMA_VERSION
    ]
    manifest_root.mkdir(parents=True, exist_ok=True)
    jsonl_path = manifest_root / "research_runs.jsonl"
    jsonl_path.write_text(
        "".join(json.dumps(item, sort_keys=True) + "\n" for item in manifests),
        encoding="utf-8",
    )
    if manifests:
        pd.DataFrame([flatten_manifest(item) for item in manifests]).to_parquet(
            manifest_root / "research_runs.parquet", index=False
        )


def manifest_metrics(
    promotion: dict[str, object],
    advisory: dict[str, object],
    calibration: dict[str, object],
    backtest: dict[str, object],
) -> dict[str, object]:
    promotion_metrics = typed_dict(promotion.get("metrics"))
    advisory_summary = typed_dict(advisory.get("summary"))
    pre_live_gate = typed_dict(backtest.get("pre_live_gate"))
    return {
        "realized_edge": promotion_metrics.get("realized_edge"),
        "fill_rate": promotion_metrics.get("fill_rate"),
        "slippage": promotion_metrics.get("slippage"),
        "adverse_selection": promotion_metrics.get("adverse_selection"),
        "drawdown": promotion_metrics.get("drawdown"),
        "stale_data_rate": promotion_metrics.get("stale_data_rate"),
        "reconciliation_divergence_rate": promotion_metrics.get(
            "reconciliation_divergence_rate"
        ),
        "test_brier_score": promotion_metrics.get("test_brier_score"),
        "test_log_loss": promotion_metrics.get("test_log_loss"),
        "advisory_failed": advisory_summary.get("failed"),
        "advisory_warned": advisory_summary.get("warned"),
        "legacy_pre_live_fill_rate": pre_live_gate.get("fill_rate"),
        "calibration_metrics": calibration.get("metrics", []),
    }


def manifest_counts(
    summary: dict[str, object], baseline: dict[str, object], backtest: dict[str, object]
) -> dict[str, object]:
    data_lake = typed_dict(summary.get("data_lake"))
    baseline_counts = typed_dict(baseline.get("counts"))
    backtest_exports = typed_dict(summary.get("backtest_exports"))
    return {
        "orderbook_snapshots": data_lake.get("orderbook_snapshots"),
        "signals": data_lake.get("signals"),
        "execution_reports": data_lake.get("execution_reports"),
        "baseline_signals": baseline_counts.get("baseline_signals"),
        "backtest_trades": backtest_exports.get("backtest_trades"),
        "backtest_summary": backtest_exports.get("backtest_summary"),
        "pre_live_gate_signals": typed_dict(backtest.get("pre_live_gate")).get("signals"),
    }


def artifact_metadata(report_root: Path) -> list[dict[str, object]]:
    names = (
        "research_summary.json",
        "data_lake_export.json",
        "baseline.json",
        "backtest.json",
        "game_theory.json",
        "calibration.json",
        "pre_live_promotion.json",
        "agent_advisory.json",
    )
    artifacts: list[dict[str, object]] = []
    for name in names:
        path = report_root / name
        if path.exists():
            artifacts.append(file_artifact(path, report_root))
    for path in sorted(report_root.glob("**/*.parquet")):
        artifacts.append(file_artifact(path, report_root))
    return artifacts


def file_artifact(path: Path, report_root: Path) -> dict[str, object]:
    return {
        "path": str(path),
        "relative_path": path.relative_to(report_root).as_posix(),
        "kind": path.suffix.removeprefix(".") or "file",
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def flatten_manifest(manifest: dict[str, object]) -> dict[str, object]:
    metrics = typed_dict(manifest.get("metrics"))
    counts = typed_dict(manifest.get("counts"))
    versions = typed_dict(manifest.get("versions"))
    return {
        "schema_version": manifest.get("schema_version"),
        "run_id": manifest.get("run_id"),
        "source": manifest.get("source"),
        "created_at": manifest.get("created_at"),
        "git_commit": manifest.get("git_commit"),
        "report_root": manifest.get("report_root"),
        "passed": manifest.get("passed"),
        "pre_live_gate_passed": manifest.get("pre_live_gate_passed"),
        "calibration_passed": manifest.get("calibration_passed"),
        "pre_live_promotion_passed": manifest.get("pre_live_promotion_passed"),
        "agent_advisory_acceptable": manifest.get("agent_advisory_acceptable"),
        "realized_edge": metrics.get("realized_edge"),
        "fill_rate": metrics.get("fill_rate"),
        "slippage": metrics.get("slippage"),
        "adverse_selection": metrics.get("adverse_selection"),
        "drawdown": metrics.get("drawdown"),
        "stale_data_rate": metrics.get("stale_data_rate"),
        "reconciliation_divergence_rate": metrics.get("reconciliation_divergence_rate"),
        "test_brier_score": metrics.get("test_brier_score"),
        "test_log_loss": metrics.get("test_log_loss"),
        "legacy_pre_live_fill_rate": metrics.get("legacy_pre_live_fill_rate"),
        "advisory_failed": metrics.get("advisory_failed"),
        "advisory_warned": metrics.get("advisory_warned"),
        "orderbook_snapshots": counts.get("orderbook_snapshots"),
        "signals": counts.get("signals"),
        "execution_reports": counts.get("execution_reports"),
        "baseline_signals": counts.get("baseline_signals"),
        "backtest_trades": counts.get("backtest_trades"),
        "backtest_summary": counts.get("backtest_summary"),
        "pre_live_gate_signals": counts.get("pre_live_gate_signals"),
        "promotion_report_version": versions.get("promotion_report"),
        "advisory_report_version": versions.get("advisory_report"),
        "advisory_model_version": versions.get("advisory_model"),
        "advisory_data_version": versions.get("advisory_data"),
        "advisory_feature_version": versions.get("advisory_feature"),
        "baseline_model_version": versions.get("baseline_model"),
        "baseline_data_version": versions.get("baseline_data"),
        "baseline_feature_version": versions.get("baseline_feature"),
        "artifact_count": artifact_count(manifest),
        "artifact_bytes_total": artifact_bytes_total(manifest),
    }


def artifact_count(manifest: dict[str, object]) -> int:
    artifacts = manifest.get("artifacts")
    return len(artifacts) if isinstance(artifacts, list) else 0


def artifact_bytes_total(manifest: dict[str, object]) -> int:
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list):
        return 0
    total = 0
    for artifact in artifacts:
        if isinstance(artifact, dict) and isinstance(artifact.get("bytes"), (int, float)):
            total += int(artifact["bytes"])
    return total


def read_json(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def typed_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def git_commit(path: Path) -> str | None:
    env_commit = os.getenv("GIT_COMMIT")
    if env_commit:
        return env_commit
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=path,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip() or None


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="research-run-manifest")
    parser.add_argument("--report-root", required=True)
    parser.add_argument("--manifest-root", required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--source", default="research_loop")
    args = parser.parse_args()

    manifest = create_run_manifest(
        report_root=Path(args.report_root),
        manifest_root=Path(args.manifest_root),
        run_id=args.run_id,
        source=args.source,
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
