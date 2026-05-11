import json
import os
import subprocess
from pathlib import Path
from typing import Any, cast

import pandas as pd  # type: ignore[import-untyped]

from src.research.asset_execution_decision import (
    REPORT_VERSION,
    create_asset_execution_decision_report,
)


ROOT_DIR = Path(__file__).resolve().parents[2]


def test_asset_execution_decision_classifies_repeat_retune_and_block(
    tmp_path: Path,
) -> None:
    report_root = seed_asset_report(tmp_path)

    report = create_asset_execution_decision_report(report_root)

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    summary = cast(dict[str, Any], report["summary"])
    assert summary == {
        "assets": 3,
        "repeat_assets": 1,
        "retune_assets": 1,
        "block_assets": 1,
    }
    decisions = {
        str(item["asset_id"]): item for item in cast(list[dict[str, Any]], report["assets"])
    }
    assert decisions["repeat"]["decision"] == "REPEAT_ASSET"
    assert decisions["retune"]["decision"] == "RETUNE_ASSET"
    assert (
        decisions["retune"]["primary_reason"]
        == "at_touch_orders_unmatched_low_future_touch"
    )
    assert decisions["block"]["decision"] == "BLOCK_ASSET"
    blocked = json.loads(
        (
            report_root
            / "asset_execution_decision"
            / "blocked_asset_segments_candidate.json"
        ).read_text(encoding="utf-8")
    )
    assert blocked["version"] == "blocked_segments_v1"
    assert blocked["can_apply_live"] is False
    assert blocked["can_execute_trades"] is False
    assert len(blocked["segments"]) == 1
    assert blocked["segments"][0]["asset_id"] == "block"
    assert (
        report_root / "asset_execution_decision" / "asset_execution_decisions.parquet"
    ).exists()
    assert (
        report_root / "asset_execution_decision" / "asset_execution_evidence.parquet"
    ).exists()


def test_asset_execution_decision_cli_writes_output(tmp_path: Path) -> None:
    report_root = seed_asset_report(tmp_path)
    output_dir = tmp_path / "custom_output"

    completed = subprocess.run(
        [
            "python3",
            "-m",
            "src.research.asset_execution_decision",
            "--report-root",
            str(report_root),
            "--output-dir",
            str(output_dir),
            "--json",
        ],
        cwd=ROOT_DIR,
        env={**os.environ, "PYTHONPATH": "python-service"},
        check=True,
        capture_output=True,
        text=True,
    )

    stdout_report = json.loads(completed.stdout)
    output_report = json.loads(
        (output_dir / "asset_execution_decision.json").read_text(encoding="utf-8")
    )
    assert stdout_report["summary"] == output_report["summary"]
    assert (output_dir / "blocked_asset_segments_candidate.json").exists()


def seed_asset_report(tmp_path: Path) -> Path:
    report_root = tmp_path / "report"
    quote_dir = report_root / "quote_execution_diagnostics"
    signal_dir = report_root / "signal_to_order_conversion"
    promotion_dir = report_root / "pre_live_promotion"
    quote_dir.mkdir(parents=True)
    signal_dir.mkdir(parents=True)
    promotion_dir.mkdir(parents=True)
    (report_root / "real_dry_run_evidence.json").write_text(
        json.dumps({"predictor_strategy_profile": "execution_probe_v8"}),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            quote_row("repeat", dry_run_filled_signals=2, future_touch_rate=0.4),
            quote_row(
                "retune",
                dry_run_filled_signals=0,
                future_touch_rate=0.02,
                avg_required_quote_move=0.01,
            ),
            quote_row("block", dry_run_filled_signals=0, future_touch_rate=0.4),
        ]
    ).to_parquet(quote_dir / "quote_execution_by_market_asset.parquet", index=False)
    pd.DataFrame(
        [
            signal_row("repeat", filled_signals=2),
            signal_row("retune", filled_signals=0),
            signal_row("block", filled_signals=0),
        ]
    ).to_parquet(signal_dir / "signal_to_order_by_asset_strategy.parquet", index=False)
    pd.DataFrame(
        [
            promotion_row("repeat", filled_signals=2, dry_run_observed_fill_rate=0.1),
            promotion_row("retune", filled_signals=0, dry_run_observed_fill_rate=0.0),
            promotion_row("block", filled_signals=0, dry_run_observed_fill_rate=0.0),
        ]
    ).to_parquet(promotion_dir / "pre_live_promotion_segments.parquet", index=False)
    pd.DataFrame(
        [
            {
                **promotion_row(
                    "block", filled_signals=0, dry_run_observed_fill_rate=0.0
                ),
                "block_reason": "negative_realized_edge",
            }
        ]
    ).to_parquet(promotion_dir / "pre_live_blocked_segments.parquet", index=False)
    return report_root


def quote_row(
    asset_id: str,
    *,
    dry_run_filled_signals: int,
    future_touch_rate: float,
    avg_required_quote_move: float = 0.0,
) -> dict[str, object]:
    return {
        **segment(asset_id),
        "signals": 20,
        "dry_run_signal_lifecycles": 20,
        "dry_run_filled_signals": dry_run_filled_signals,
        "synthetic_filled_signals": dry_run_filled_signals,
        "future_touch_rate": future_touch_rate,
        "avg_required_quote_move": avg_required_quote_move,
    }


def signal_row(asset_id: str, *, filled_signals: int) -> dict[str, object]:
    return {
        **segment(asset_id),
        "signals": 20,
        "reports": 20,
        "orders_created": 20,
        "filled_signals": filled_signals,
        "report_rate": 1.0,
        "order_creation_rate": 1.0,
        "missing_report_rate": 0.0,
        "error_rate": 0.0,
        "consumption_rate": 1.0,
        "rejection_rate": 0.0,
    }


def promotion_row(
    asset_id: str,
    *,
    filled_signals: int,
    dry_run_observed_fill_rate: float,
) -> dict[str, object]:
    return {
        **segment(asset_id),
        "signals": 20,
        "filled_signals": filled_signals,
        "realized_edge": 0.01 if asset_id != "block" else -0.01,
        "pnl": 0.01 if asset_id != "block" else -0.01,
        "max_drawdown": 0.0,
        "dry_run_reports": 20,
        "dry_run_observed_fill_rate": dry_run_observed_fill_rate,
        "synthetic_fill_rate": dry_run_observed_fill_rate,
    }


def segment(asset_id: str) -> dict[str, object]:
    return {
        "market_id": f"market-{asset_id}",
        "asset_id": asset_id,
        "side": "BUY",
        "strategy": "passive_spread_capture_execution_probe_near_touch_v8",
        "model_version": "passive_spread_capture_execution_probe_near_touch_v8",
    }
