import json
from pathlib import Path
from typing import cast

import pandas as pd  # type: ignore[import-untyped]

from src.research.pre_live_blocker_analysis import (
    create_blocker_diagnostics,
    format_summary,
)


def test_blocker_diagnostics_exports_candidate_blocklist(tmp_path: Path) -> None:
    report_root = seed_report_root(tmp_path)

    report = create_blocker_diagnostics(
        report_root,
        max_candidates=10,
        min_segment_signals=10,
        min_adverse_filled_events=10,
    )

    summary = report["summary"]
    assert isinstance(summary, dict)
    assert summary["drawdown_segments"] == 1
    assert summary["adverse_selection_segments"] == 2
    assert summary["adverse_selection_candidate_segments"] == 2
    assert summary["candidate_blocked_segments"] == 2
    blocked_path = Path(str(report["blocked_segments_path"]))
    payload = json.loads(blocked_path.read_text(encoding="utf-8"))
    assert payload["version"] == "blocked_segments_v1"
    assert payload["can_apply_live"] is False
    contract = payload["evaluation_contract"]
    assert contract["version"] == "blocked_segments_evaluation_contract_v1"
    assert contract["comparability_policy_version"] == "segment_comparability_v2"
    assert contract["expected_removed_segments_count"] == 2
    coverage = contract["expected_coverage_impact"]
    assert coverage["baseline_signals"] == 63.0
    assert coverage["candidate_blocked_signals"] == 55.0
    assert coverage["signal_coverage_rate"] == 8.0 / 63.0
    fixed_universe = contract["fixed_market_universe"]
    assert fixed_universe["market_asset_ids"] == ["asset-1", "asset-2", "asset-3"]
    next_run = cast(dict[str, object], report["next_restricted_run"])
    assert "MARKET_ASSET_IDS=" in str(next_run["command"])
    assert Path(str(report["fixed_market_universe_path"])).exists()
    variants = report["narrow_candidate_variants"]
    assert isinstance(variants, list)
    assert len(variants) == 1
    variant = variants[0]
    assert isinstance(variant, dict)
    variant_path = Path(str(variant["path"]))
    variant_payload = json.loads(variant_path.read_text(encoding="utf-8"))
    assert variant_payload["evaluation_contract"]["expected_removed_segments_count"] == 1
    assert len(variant_payload["segments"]) == 1
    reasons = {segment["reason"] for segment in payload["segments"]}
    assert "adverse_selection,bounded_drawdown" in reasons
    assert "adverse_selection" in reasons
    drawdown_segments = cast(list[object], report["top_drawdown_segments"])
    drawdown_segment = drawdown_segments[0]
    assert isinstance(drawdown_segment, dict)
    drawdown_metrics = drawdown_segment["metrics"]
    assert isinstance(drawdown_metrics, dict)
    assert drawdown_metrics["diagnostic_score"] == 0.4
    buckets = cast(list[object], report["top_explanatory_buckets"])
    assert buckets
    market_bucket = next(
        item
        for item in buckets
        if isinstance(item, dict)
        and item["bucket_type"] == "market"
        and item["bucket"] == {"market_id": "market-1"}
    )
    assert isinstance(market_bucket, dict)
    assert market_bucket["candidate_segment_count"] == 2
    defensive_path = Path(str(report["defensive_blocked_segments_path"]))
    defensive_payload = json.loads(defensive_path.read_text(encoding="utf-8"))
    assert defensive_payload["can_apply_live"] is False
    assert defensive_payload["decision_policy"] == (
        "candidate_requires_restricted_run_comparison"
    )


def test_blocker_diagnostics_summary_includes_next_command(tmp_path: Path) -> None:
    report = create_blocker_diagnostics(seed_report_root(tmp_path))

    output = format_summary(report)

    assert "pre_live_blocker_diagnostics" in output
    assert "candidate_blocked_segments=2" in output
    assert "variant_top_1_command=" in output
    assert "PREDICTOR_BLOCKED_SEGMENTS_PATH=" in output


def seed_report_root(tmp_path: Path) -> Path:
    report_root = tmp_path / "reports" / "run-1"
    (report_root / "pre_live_promotion").mkdir(parents=True)
    (report_root / "game_theory").mkdir()
    (report_root / "go_no_go.json").write_text(
        json.dumps(
            {
                "decision": "NO_GO",
                "config": {
                    "max_drawdown": 0.1,
                    "max_adverse_selection_rate": 0.4,
                },
                "blockers": [
                    {"check_name": "bounded_drawdown"},
                    {"check_name": "no_persistent_adverse_selection"},
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            segment_row(
                "market-1",
                "asset-1",
                signals=30,
                filled_signals=20,
                max_drawdown=0.5,
            ),
            segment_row(
                "market-1",
                "asset-2",
                signals=25,
                filled_signals=10,
                max_drawdown=0.0,
            ),
            segment_row(
                "market-2",
                "asset-3",
                signals=8,
                filled_signals=8,
                max_drawdown=0.9,
            ),
        ]
    ).to_parquet(report_root / "pre_live_promotion" / "pre_live_promotion_segments.parquet")
    pd.DataFrame(
        [
            {
                "strategy": "strategy-v1",
                "market_id": "market-1",
                "side": "BUY",
                "filled_events": 30,
                "avg_pnl_5s": -0.01,
                "avg_pnl_30s": -0.02,
                "avg_pnl_300s": -0.03,
                "adverse_30s_count": 27,
                "adverse_30s_rate": 0.9,
            },
            {
                "strategy": "strategy-v1",
                "market_id": "market-2",
                "side": "BUY",
                "filled_events": 5,
                "avg_pnl_5s": -0.01,
                "avg_pnl_30s": -0.02,
                "avg_pnl_300s": -0.03,
                "adverse_30s_count": 5,
                "adverse_30s_rate": 1.0,
            },
        ]
    ).to_parquet(report_root / "game_theory" / "adverse_selection_by_strategy.parquet")
    return report_root


def segment_row(
    market_id: str,
    asset_id: str,
    *,
    signals: int,
    filled_signals: int,
    max_drawdown: float,
) -> dict[str, object]:
    return {
        "market_id": market_id,
        "asset_id": asset_id,
        "side": "BUY",
        "strategy": "strategy-v1",
        "model_version": "strategy-v1",
        "signals": signals,
        "filled_signals": filled_signals,
        "fill_rate": filled_signals / signals,
        "avg_slippage": 0.0,
        "realized_edge": -0.01,
        "filled_notional": 1.0,
        "pnl": -0.1,
        "pnl_per_signal": -0.1 / signals,
        "pnl_per_filled_signal": -0.1 / filled_signals,
        "pnl_per_filled_notional": -0.1,
        "dry_run_reports": filled_signals,
        "dry_run_filled_signals": filled_signals,
        "dry_run_observed_fill_rate": 1.0,
        "synthetic_fill_rate": 1.0,
        "simulator_fill_rate_delta": 0.0,
        "abs_simulator_fill_rate_delta": 0.0,
        "max_drawdown": max_drawdown,
        "drawdown_per_signal": max_drawdown / signals,
        "drawdown_per_filled_signal": max_drawdown / filled_signals,
        "drawdown_per_filled_notional": max_drawdown,
    }
