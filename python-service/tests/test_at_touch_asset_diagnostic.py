import json
from pathlib import Path
from typing import Any, cast

import pandas as pd  # type: ignore[import-untyped]

from src.research.at_touch_asset_diagnostic import (
    REPORT_VERSION,
    AtTouchAssetDiagnosticConfig,
    create_at_touch_asset_diagnostic_report,
)


def test_at_touch_asset_diagnostic_classifies_runtime_stale_assets(
    tmp_path: Path,
) -> None:
    report_root = seed_at_touch_report(tmp_path)

    report = create_at_touch_asset_diagnostic_report(
        report_root,
        config=AtTouchAssetDiagnosticConfig(min_asset_signals=5),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    summary = cast(dict[str, Any], report["summary"])
    assert summary["assets"] == 2
    assert summary["signals"] == 30
    assets = cast(list[dict[str, Any]], report["assets"])
    decisions = {str(row["asset_id"]): row["decision"] for row in assets}
    assert decisions["asset-stale"] == "DROP_RUNTIME_STALE"
    assert decisions["asset-touch"] == "RETUNE_TO_AT_TOUCH"
    assert (
        report_root
        / "at_touch_asset_diagnostic"
        / "at_touch_asset_diagnostic.parquet"
    ).exists()


def test_at_touch_asset_diagnostic_can_filter_assets(tmp_path: Path) -> None:
    report_root = seed_at_touch_report(tmp_path)

    report = create_at_touch_asset_diagnostic_report(
        report_root,
        asset_ids=["asset-touch"],
        config=AtTouchAssetDiagnosticConfig(min_asset_signals=5),
    )

    assets = cast(list[dict[str, Any]], report["assets"])
    assert [row["asset_id"] for row in assets] == ["asset-touch"]


def seed_at_touch_report(tmp_path: Path) -> Path:
    report_root = tmp_path / "report"
    diagnostics_dir = report_root / "quote_execution_diagnostics"
    diagnostics_dir.mkdir(parents=True)
    (report_root / "real_dry_run_evidence.json").write_text(
        json.dumps({"market_asset_ids": ["asset-stale", "asset-touch"]}),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "market_id": "market-stale",
                "asset_id": "asset-stale",
                "side": "BUY",
                "strategy": "probe",
                "model_version": "model",
                "signals": 20,
                "dry_run_signal_lifecycles": 20,
                "dry_run_filled_signals": 0,
                "synthetic_filled_signals": 0,
                "at_touch_rate": 0.0,
                "inside_spread_rate": 1.0,
                "behind_touch_rate": 0.0,
                "future_touch_rate": 0.0,
                "avg_distance_to_touch": 0.02,
                "avg_required_quote_move": 0.02,
            },
            {
                "market_id": "market-touch",
                "asset_id": "asset-touch",
                "side": "BUY",
                "strategy": "probe",
                "model_version": "model",
                "signals": 10,
                "dry_run_signal_lifecycles": 10,
                "dry_run_filled_signals": 0,
                "synthetic_filled_signals": 0,
                "at_touch_rate": 0.0,
                "inside_spread_rate": 1.0,
                "behind_touch_rate": 0.0,
                "future_touch_rate": 0.20,
                "avg_distance_to_touch": 0.002,
                "avg_required_quote_move": 0.002,
            },
        ]
    ).to_parquet(diagnostics_dir / "quote_execution_by_market_asset.parquet", index=False)
    pd.DataFrame(
        [
            {
                "strategy": "probe",
                "model_version": "model",
                "feature_version": "feature",
                "market_id": "market-stale",
                "asset_id": "asset-stale",
                "side": "BUY",
                "quote_relation": "inside_spread",
                "root_cause": "dry_run_created_unmatched",
                "signals": 20,
                "avg_ms_to_first_future_touch": None,
            }
        ]
    ).to_parquet(
        diagnostics_dir / "quote_execution_no_fill_diagnostics.parquet", index=False
    )
    return report_root
