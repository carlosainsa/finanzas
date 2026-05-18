import json
from pathlib import Path

from src.research.runtime_touch_discovery_batch_diagnostics import (
    REPORT_VERSION,
    RuntimeTouchDiscoveryBatchDiagnosticsConfig,
    create_runtime_touch_discovery_batch_diagnostics,
)


def test_discovery_batch_diagnostics_explains_insufficient_assets(
    tmp_path: Path,
) -> None:
    discovery = write_discovery_batches(tmp_path)
    batch_root = tmp_path / "batches" / "batch-01"
    write_batch_artifacts(batch_root)

    report = create_runtime_touch_discovery_batch_diagnostics(
        discovery,
        tmp_path / "batches",
        tmp_path / "diagnostics",
        config=RuntimeTouchDiscoveryBatchDiagnosticsConfig(min_assets=2),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert report["can_promote_live"] is False
    assert report["counts"]["processed_batches"] == 1
    batch = report["batches"][0]
    assert batch["readiness_status"] == "DISCOVERY_EXPANSION_REQUIRED"
    assert batch["ready_for_ab_retry"] is False
    assert batch["primary_blocker"] == "signalability_signalable_density"
    assert batch["blockers"][0]["stage"] == "signalability"
    assert batch["selector_adjustment_recommendations"][0]["selector"] == (
        "runtime_touch_discovery_batches"
    )
    assert report["recommended_selector_adjustment"]["can_execute_trades"] is False
    assert (
        tmp_path
        / "diagnostics"
        / "runtime_touch_discovery_batch_diagnostics.parquet"
    ).exists()


def test_discovery_batch_diagnostics_identifies_ready_batch(tmp_path: Path) -> None:
    discovery = write_discovery_batches(tmp_path)
    write_batch_artifacts(
        tmp_path / "batches" / "batch-01",
        route_next_action="READY_FOR_AB_RETRY",
        signalable_assets=2,
        candidate_assets=2,
        opportunity_assets=2,
    )

    report = create_runtime_touch_discovery_batch_diagnostics(
        discovery,
        tmp_path / "batches",
        tmp_path / "diagnostics",
    )

    batch = report["batches"][0]
    assert batch["readiness_status"] == "READY_FOR_AB_RETRY"
    assert batch["ready_for_ab_retry"] is True
    assert batch["blockers"] == []
    assert report["recommended_selector_adjustment"]["action"] == "RUN_RETRY_LADDER"


def test_discovery_batch_diagnostics_marks_unprocessed_batches(tmp_path: Path) -> None:
    discovery = write_discovery_batches(tmp_path, batch_count=2)
    write_batch_artifacts(tmp_path / "batches" / "batch-01")

    report = create_runtime_touch_discovery_batch_diagnostics(
        discovery,
        tmp_path / "batches",
        tmp_path / "diagnostics",
    )

    assert report["counts"]["processed_batches"] == 1
    assert report["counts"]["not_processed_batches"] == 1
    skipped = report["batches"][1]
    assert skipped["batch_id"] == "batch-02"
    assert skipped["is_processed"] is False
    assert skipped["readiness_status"] == "NOT_PROCESSED"
    assert skipped["primary_blocker"] == "not_processed_by_loop_limit"


def write_discovery_batches(tmp_path: Path, *, batch_count: int = 1) -> Path:
    path = tmp_path / "discovery" / "runtime_touch_discovery_batches.json"
    path.parent.mkdir(parents=True)
    batches = [
        {
            "batch_id": f"batch-{index:02d}",
            "batch_index": index,
            "markets_count": 2,
            "asset_ids_count": 4,
            "families_count": 2,
            "market_families": ["tag:politics", "tag:sports"],
            "selection_modes": ["exploit", "explore"],
            "fillability_covered_assets_count": 4,
        }
        for index in range(1, batch_count + 1)
    ]
    path.write_text(
        json.dumps(
            {
                "can_execute_trades": False,
                "batches": batches,
            }
        ),
        encoding="utf-8",
    )
    return path


def write_batch_artifacts(
    batch_root: Path,
    *,
    route_next_action: str = "EXPAND_MARKET_DISCOVERY",
    signalable_assets: int = 0,
    candidate_assets: int = 1,
    opportunity_assets: int = 1,
) -> None:
    (batch_root / "runtime_touch_signalability_diagnostic").mkdir(parents=True)
    (batch_root / "runtime_touch_candidate_expansion").mkdir(parents=True)
    (batch_root / "runtime_touch_opportunity_windows").mkdir(parents=True)
    (batch_root / "runtime_touch_market_timing_scout").mkdir(parents=True)
    (
        batch_root
        / "runtime_touch_signalability_diagnostic"
        / "runtime_touch_signalability_diagnostic.json"
    ).write_text(
        json.dumps(
            {
                "can_execute_trades": False,
                "decision_policy": "offline_runtime_touch_signalability_diagnostic_only",
                "status": "ready"
                if signalable_assets >= 2
                else "insufficient_signalable_assets",
                "signalable_assets_count": signalable_assets,
                "blocker_counts": {"signalable_density": 3, "stale_rate": 1},
            }
        ),
        encoding="utf-8",
    )
    (
        batch_root
        / "runtime_touch_candidate_expansion"
        / "runtime_touch_candidate_expansion.json"
    ).write_text(
        json.dumps(
            {
                "can_execute_trades": False,
                "status": "ready" if candidate_assets >= 2 else "insufficient_assets",
                "market_asset_ids_count": candidate_assets,
                "counts": {"ranked_assets": 4, "selected_assets": candidate_assets},
            }
        ),
        encoding="utf-8",
    )
    (
        batch_root
        / "runtime_touch_opportunity_windows"
        / "runtime_touch_opportunity_windows.json"
    ).write_text(
        json.dumps(
            {
                "can_execute_trades": False,
                "status": "ready"
                if opportunity_assets >= 2
                else "insufficient_assets",
                "market_asset_ids_count": opportunity_assets,
                "counts": {
                    "ranked_assets": 4,
                    "selected_assets": opportunity_assets,
                    "windows": 2,
                },
            }
        ),
        encoding="utf-8",
    )
    (
        batch_root
        / "runtime_touch_market_timing_scout"
        / "runtime_touch_market_timing_scout.json"
    ).write_text(
        json.dumps(
            {
                "can_execute_trades": False,
                "status": "insufficient_assets",
                "market_asset_ids_count": 0,
                "counts": {
                    "asset_windows": 1,
                    "windows": 1,
                    "eligible_windows": 0,
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
                "candidate_expansion_assets": candidate_assets,
                "opportunity_window_assets": opportunity_assets,
                "signalable_assets_count": signalable_assets,
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
                "rationale": ["candidate_expansion_has_insufficient_assets"],
                "input_status": {
                    "market_timing_scout_assets": 0,
                    "market_timing_scout_status": "insufficient_assets",
                },
            }
        ),
        encoding="utf-8",
    )
