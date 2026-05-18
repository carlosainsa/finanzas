import argparse
import json
import shlex
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd  # type: ignore[import-untyped]

from src.research.runtime_touch_ranking import normalize_records


REPORT_VERSION = "runtime_touch_discovery_batch_comparison_v1"


@dataclass(frozen=True)
class RuntimeTouchDiscoveryBatchComparisonConfig:
    min_assets: int = 2
    ab_observation_seconds: int = 3600
    ab_fresh_capture_seconds: int = 1800
    profile_a: str = "execution_probe_v11"
    profile_b: str = "execution_probe_v12"

    def __post_init__(self) -> None:
        if self.min_assets <= 0:
            raise ValueError("min_assets must be positive")
        if self.ab_observation_seconds < 1800 or self.ab_observation_seconds > 5400:
            raise ValueError("ab_observation_seconds must be between 1800 and 5400")
        if self.ab_fresh_capture_seconds < 1800 or self.ab_fresh_capture_seconds > 5400:
            raise ValueError("ab_fresh_capture_seconds must be between 1800 and 5400")
        if self.profile_a not in {"execution_probe_v11", "execution_probe_v12"}:
            raise ValueError("profile_a must be execution_probe_v11 or execution_probe_v12")
        if self.profile_b not in {"execution_probe_v11", "execution_probe_v12"}:
            raise ValueError("profile_b must be execution_probe_v11 or execution_probe_v12")
        if self.profile_a == self.profile_b:
            raise ValueError("profile_a and profile_b must be different")


def create_runtime_touch_discovery_batch_comparison(
    discovery_batches_path: Path,
    batch_results_root: Path,
    output_dir: Path,
    config: RuntimeTouchDiscoveryBatchComparisonConfig = (
        RuntimeTouchDiscoveryBatchComparisonConfig()
    ),
) -> dict[str, Any]:
    discovery_batches = read_json(discovery_batches_path)
    rows = [
        batch_summary(batch, batch_results_root, config)
        for batch in discovery_batches.get("batches", [])
        if isinstance(batch, dict)
    ]
    selected = select_best_batch(rows, config)
    recommended_next_run = build_recommended_next_run(selected, config)
    recommended_next_command = (
        build_recommended_next_command(recommended_next_run)
        if recommended_next_run is not None
        else None
    )
    status = "ready" if selected is not None else "no_ready_batch"
    output_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(
        output_dir / "runtime_touch_discovery_batch_comparison.parquet",
        index=False,
    )
    payload: dict[str, Any] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "can_promote_live": False,
        "decision_policy": "offline_runtime_touch_discovery_batch_comparison_only",
        "selection_source": "runtime_touch_discovery_batches",
        "status": status,
        "next_action": next_action_for_selected_batch(selected),
        "risk_contract": {
            "execution_mode": "dry_run",
            "does_not_modify_quote_policy": True,
            "does_not_modify_risk_limits": True,
            "does_not_publish_signals": True,
        },
        "config": {
            "min_assets": config.min_assets,
            "ab_observation_seconds": config.ab_observation_seconds,
            "ab_fresh_capture_seconds": config.ab_fresh_capture_seconds,
            "profile_a": config.profile_a,
            "profile_b": config.profile_b,
        },
        "source_discovery_batches": str(discovery_batches_path),
        "batch_results_root": str(batch_results_root),
        "counts": {
            "batches": len(rows),
            "ready_batches": sum(1 for row in rows if row["batch_decision"] == "READY"),
            "expanded_discovery_batches": sum(
                1 for row in rows if row["route_next_action"] == "EXPAND_MARKET_DISCOVERY"
            ),
            "change_time_window_batches": sum(
                1 for row in rows if row["route_next_action"] == "CHANGE_TIME_WINDOW"
            ),
            "ab_retry_ready_batches": sum(
                1 for row in rows if row["route_next_action"] == "READY_FOR_AB_RETRY"
            ),
        },
        "selected_batch": selected,
        "recommended_next_command": recommended_next_command,
        "recommended_next_run": recommended_next_run,
        "batches": normalize_records(rows),
        "outputs": [
            "runtime_touch_discovery_batch_comparison.parquet",
            "runtime_touch_discovery_batch_comparison.json",
        ],
    }
    (output_dir / "runtime_touch_discovery_batch_comparison.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload


def batch_summary(
    batch: dict[str, Any],
    batch_results_root: Path,
    config: RuntimeTouchDiscoveryBatchComparisonConfig,
) -> dict[str, Any]:
    batch_id = str(batch.get("batch_id") or "")
    batch_root = batch_results_root / batch_id
    scout = read_optional_json(
        batch_root / "runtime_touch_market_timing_scout" / "runtime_touch_market_timing_scout.json"
    )
    route = read_optional_json(batch_root / "runtime_touch_route_decision.json")
    selection_summary = read_optional_json(
        batch_root / "runtime_touch_selection_probe_summary.json"
    )
    raw_scout_counts = scout.get("counts")
    scout_counts: dict[str, Any] = (
        raw_scout_counts if isinstance(raw_scout_counts, dict) else {}
    )
    raw_route_status = route.get("input_status")
    route_status: dict[str, Any] = (
        raw_route_status if isinstance(raw_route_status, dict) else {}
    )
    selected_assets = int_value(
        scout.get("market_asset_ids_count")
        or route_status.get("market_timing_scout_assets")
    )
    eligible_windows = int_value(scout_counts.get("eligible_windows"))
    route_next_action = str(route.get("next_action") or "MISSING_ROUTE_DECISION")
    batch_decision = (
        "READY"
        if selected_assets >= config.min_assets and route_next_action != "BLOCK_LIVE"
        else "REJECT"
    )
    rejection_reason = classify_rejection_reason(
        batch_decision,
        route_next_action,
        selected_assets,
        eligible_windows,
        config,
    )
    return {
        "batch_id": batch_id,
        "batch_index": int_value(batch.get("batch_index")),
        "markets_count": int_value(batch.get("markets_count")),
        "asset_ids_count": int_value(batch.get("asset_ids_count")),
        "market_asset_ids_csv": str(batch.get("market_asset_ids_csv") or ""),
        "scout_status": str(scout.get("status") or "missing"),
        "scout_asset_windows": int_value(scout_counts.get("asset_windows")),
        "scout_windows": int_value(scout_counts.get("windows")),
        "eligible_windows": eligible_windows,
        "selected_assets": selected_assets,
        "route_next_action": route_next_action,
        "fresh_duckdb": str(selection_summary.get("fresh_duckdb") or ""),
        "fresh_report_root": str(selection_summary.get("fresh_report_root") or ""),
        "batch_decision": batch_decision,
        "rejection_reason": rejection_reason,
        "batch_root": str(batch_root),
    }


def next_action_for_selected_batch(selected: dict[str, Any] | None) -> str:
    if selected is None:
        return "EXPAND_MARKET_DISCOVERY"
    if selected.get("route_next_action") == "READY_FOR_AB_RETRY":
        return "RUN_RUNTIME_TOUCH_AB_RETRY_LADDER"
    return "RUN_SELECTION_PROBE_ON_BEST_BATCH"


def build_recommended_next_run(
    selected: dict[str, Any] | None,
    config: RuntimeTouchDiscoveryBatchComparisonConfig,
) -> dict[str, Any] | None:
    if selected is None or selected.get("route_next_action") != "READY_FOR_AB_RETRY":
        return None
    fresh_duckdb = str(selected.get("fresh_duckdb") or "")
    if not fresh_duckdb:
        return None
    args = [
        "--fresh-duckdb",
        fresh_duckdb,
        "--duration-seconds",
        str(config.ab_observation_seconds),
        "--fresh-capture-seconds",
        str(config.ab_fresh_capture_seconds),
        "--min-assets",
        str(config.min_assets),
        "--profile-a",
        config.profile_a,
        "--profile-b",
        config.profile_b,
    ]
    fresh_report_root = str(selected.get("fresh_report_root") or "")
    if fresh_report_root:
        args[2:2] = ["--fresh-report-root", fresh_report_root]
    return {
        "script": "scripts/run_runtime_touch_ab_retry_ladder.sh",
        "env": {"EXECUTION_MODE": "dry_run"},
        "args": args,
        "source_batch_id": selected["batch_id"],
        "source_batch_root": selected["batch_root"],
        "route_next_action": selected["route_next_action"],
        "can_execute_trades": False,
        "can_promote_live": False,
        "research_only": True,
        "requires_manual_operator_execution": True,
    }


def build_recommended_next_command(next_run: dict[str, Any]) -> str:
    env = object_value(next_run.get("env"))
    args = list_value(next_run.get("args"))
    env_prefix = " ".join(
        f"{key}={shlex.quote(str(value))}" for key, value in sorted(env.items())
    )
    script = shlex.quote(str(next_run["script"]))
    quoted_args = " ".join(shlex.quote(str(arg)) for arg in args)
    return " ".join(part for part in [env_prefix, script, quoted_args] if part)


def classify_rejection_reason(
    batch_decision: str,
    route_next_action: str,
    selected_assets: int,
    eligible_windows: int,
    config: RuntimeTouchDiscoveryBatchComparisonConfig,
) -> str:
    if batch_decision == "READY":
        return "ready_for_selection_probe"
    if route_next_action == "BLOCK_LIVE":
        return "unsafe_or_invalid_research_contract"
    if eligible_windows <= 0:
        return "no_eligible_runtime_windows"
    if selected_assets < config.min_assets:
        return "insufficient_selected_assets"
    return "not_ready"


def select_best_batch(
    rows: list[dict[str, Any]],
    config: RuntimeTouchDiscoveryBatchComparisonConfig,
) -> dict[str, Any] | None:
    ready = [
        row
        for row in rows
        if row["batch_decision"] == "READY"
        and int_value(row["selected_assets"]) >= config.min_assets
    ]
    if not ready:
        return None
    return dict(
        sorted(
            ready,
            key=lambda row: (
                int_value(row["selected_assets"]),
                int_value(row["eligible_windows"]),
                int_value(row["scout_asset_windows"]),
            ),
            reverse=True,
        )[0]
    )


def read_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"expected JSON object: {path}")
    return data


def read_optional_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return read_json(path)


def object_value(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def list_value(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def int_value(value: Any) -> int:
    if value is None or value == "":
        return 0
    return int(value)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--discovery-batches", required=True)
    parser.add_argument("--batch-results-root", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--min-assets", type=int, default=2)
    args = parser.parse_args()
    report = create_runtime_touch_discovery_batch_comparison(
        Path(args.discovery_batches),
        Path(args.batch_results_root),
        Path(args.output_dir),
        RuntimeTouchDiscoveryBatchComparisonConfig(min_assets=args.min_assets),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
