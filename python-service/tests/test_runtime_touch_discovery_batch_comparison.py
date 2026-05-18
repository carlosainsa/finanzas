import json
from pathlib import Path

from src.research.runtime_touch_discovery_batch_comparison import (
    REPORT_VERSION,
    RuntimeTouchDiscoveryBatchComparisonConfig,
    create_runtime_touch_discovery_batch_comparison,
)


def test_discovery_batch_comparison_selects_best_ready_batch(
    tmp_path: Path,
) -> None:
    discovery = write_discovery_batches(tmp_path)
    write_batch_result(
        tmp_path,
        "batch-01",
        scout_assets=0,
        eligible_windows=0,
        route_next_action="EXPAND_MARKET_DISCOVERY",
    )
    write_batch_result(
        tmp_path,
        "batch-02",
        scout_assets=2,
        eligible_windows=1,
        route_next_action="CHANGE_TIME_WINDOW",
    )

    report = create_runtime_touch_discovery_batch_comparison(
        discovery,
        tmp_path / "batches",
        tmp_path / "comparison",
        RuntimeTouchDiscoveryBatchComparisonConfig(min_assets=2),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert report["can_promote_live"] is False
    assert report["status"] == "ready"
    assert report["next_action"] == "RUN_SELECTION_PROBE_ON_BEST_BATCH"
    assert report["selected_batch"]["batch_id"] == "batch-02"
    assert report["recommended_next_run"] is None
    assert report["recommended_next_command"] is None
    assert report["counts"]["ready_batches"] == 1
    assert (
        tmp_path
        / "comparison"
        / "runtime_touch_discovery_batch_comparison.parquet"
    ).exists()


def test_discovery_batch_comparison_handles_missing_artifacts(
    tmp_path: Path,
) -> None:
    discovery = write_discovery_batches(tmp_path)
    write_batch_result(
        tmp_path,
        "batch-01",
        scout_assets=1,
        eligible_windows=0,
        route_next_action="EXPAND_MARKET_DISCOVERY",
    )

    report = create_runtime_touch_discovery_batch_comparison(
        discovery,
        tmp_path / "batches",
        tmp_path / "comparison",
    )

    assert report["status"] == "no_ready_batch"
    assert report["next_action"] == "EXPAND_MARKET_DISCOVERY"
    missing = next(row for row in report["batches"] if row["batch_id"] == "batch-02")
    assert missing["route_next_action"] == "MISSING_ROUTE_DECISION"
    assert missing["batch_decision"] == "REJECT"
    assert report["recommended_next_run"] is None
    assert report["recommended_next_command"] is None


def test_discovery_batch_comparison_emits_ab_retry_ladder_command(
    tmp_path: Path,
) -> None:
    discovery = write_discovery_batches(tmp_path)
    write_batch_result(
        tmp_path,
        "batch-01",
        scout_assets=1,
        eligible_windows=0,
        route_next_action="EXPAND_MARKET_DISCOVERY",
    )
    write_batch_result(
        tmp_path,
        "batch-02",
        scout_assets=2,
        eligible_windows=3,
        route_next_action="READY_FOR_AB_RETRY",
    )

    report = create_runtime_touch_discovery_batch_comparison(
        discovery,
        tmp_path / "batches",
        tmp_path / "comparison",
        RuntimeTouchDiscoveryBatchComparisonConfig(min_assets=2),
    )

    assert report["next_action"] == "RUN_RUNTIME_TOUCH_AB_RETRY_LADDER"
    next_run = report["recommended_next_run"]
    assert next_run["script"] == "scripts/run_runtime_touch_ab_retry_ladder.sh"
    assert next_run["source_batch_id"] == "batch-02"
    assert next_run["can_execute_trades"] is False
    assert next_run["can_promote_live"] is False
    assert next_run["env"] == {"EXECUTION_MODE": "dry_run"}
    assert "--fresh-duckdb" in next_run["args"]
    assert str(tmp_path / "batches" / "batch-02" / "research.duckdb") in next_run["args"]
    assert "--fresh-report-root" in next_run["args"]
    assert "--min-assets" in next_run["args"]
    command = report["recommended_next_command"]
    assert "EXECUTION_MODE=dry_run" in command
    assert "scripts/run_runtime_touch_ab_retry_ladder.sh" in command
    assert "EXECUTION_MODE=live" not in command


def write_discovery_batches(tmp_path: Path) -> Path:
    path = tmp_path / "discovery" / "runtime_touch_discovery_batches.json"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "can_execute_trades": False,
                "decision_policy": "offline_runtime_touch_discovery_batching_only",
                "batches": [
                    {
                        "batch_id": "batch-01",
                        "batch_index": 1,
                        "markets_count": 1,
                        "asset_ids_count": 2,
                        "market_asset_ids_csv": "a,b",
                    },
                    {
                        "batch_id": "batch-02",
                        "batch_index": 2,
                        "markets_count": 1,
                        "asset_ids_count": 2,
                        "market_asset_ids_csv": "c,d",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def write_batch_result(
    tmp_path: Path,
    batch_id: str,
    *,
    scout_assets: int,
    eligible_windows: int,
    route_next_action: str,
) -> None:
    batch_root = tmp_path / "batches" / batch_id
    scout_root = batch_root / "runtime_touch_market_timing_scout"
    scout_root.mkdir(parents=True)
    (scout_root / "runtime_touch_market_timing_scout.json").write_text(
        json.dumps(
            {
                "can_execute_trades": False,
                "decision_policy": "offline_runtime_touch_market_timing_scout_only",
                "status": "ready" if scout_assets >= 2 else "insufficient_assets",
                "market_asset_ids_count": scout_assets,
                "counts": {
                    "asset_windows": 10,
                    "windows": 2,
                    "eligible_windows": eligible_windows,
                },
            }
        ),
        encoding="utf-8",
    )
    (batch_root / "runtime_touch_route_decision.json").write_text(
        json.dumps(
            {
                "can_execute_trades": False,
                "decision_policy": "offline_runtime_touch_route_decision_only",
                "next_action": route_next_action,
                "input_status": {
                    "market_timing_scout_assets": scout_assets,
                    "market_timing_scout_status": "ready"
                    if scout_assets >= 2
                    else "insufficient_assets",
                },
            }
        ),
        encoding="utf-8",
    )
    (batch_root / "runtime_touch_selection_probe_summary.json").write_text(
        json.dumps(
            {
                "can_execute_trades": False,
                "decision_policy": "offline_runtime_touch_selection_probe_only",
                "fresh_duckdb": str(batch_root / "research.duckdb"),
                "fresh_report_root": str(batch_root / "reports"),
            }
        ),
        encoding="utf-8",
    )
