import json
from pathlib import Path
from typing import Any, cast

import pandas as pd  # type: ignore[import-untyped]

from src.research.strategy_family_comparison import (
    REPORT_VERSION,
    create_strategy_family_comparison,
)


def test_strategy_family_comparison_repeats_conservative_observation(
    tmp_path: Path,
) -> None:
    baseline = seed_report_root(
        tmp_path / "baseline",
        run_id="baseline-run",
        profile="baseline",
        strategy="passive_spread_capture_v1",
        model_version="passive_spread_capture_v1",
        signals=100,
        filled_signals=20,
        realized_edge=0.02,
        drawdown=0.08,
        adverse_selection=0.35,
    )
    conservative = seed_report_root(
        tmp_path / "conservative",
        run_id="conservative-run",
        profile="conservative_v1",
        strategy="passive_spread_capture_conservative_v1",
        model_version="passive_spread_capture_conservative_v1",
        signals=100,
        filled_signals=14,
        realized_edge=0.03,
        drawdown=0.03,
        adverse_selection=0.20,
    )

    report = create_strategy_family_comparison([baseline, conservative])

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert report["decision"] == "REPEAT_CONSERVATIVE_V1_OBSERVATION"
    family = conservative_family(report)
    assert family["latest_run_id"] == "conservative-run"
    assert cast(dict[str, Any], family["metrics"])["fill_rate"] == 0.14
    drawdown_delta = metric_delta_by_name(family, "drawdown")
    assert drawdown_delta["improved"] is True
    stale_delta = metric_delta_by_name(family, "stale_data_rate")
    assert stale_delta["direction"] == "lower_is_better"


def test_strategy_family_comparison_redesigns_when_fill_rate_is_killed(
    tmp_path: Path,
) -> None:
    baseline = seed_report_root(
        tmp_path / "baseline",
        run_id="baseline-run",
        profile="baseline",
        strategy="passive_spread_capture_v1",
        model_version="passive_spread_capture_v1",
        signals=100,
        filled_signals=20,
        realized_edge=0.02,
        drawdown=0.08,
        adverse_selection=0.35,
    )
    conservative = seed_report_root(
        tmp_path / "conservative",
        run_id="conservative-run",
        profile="conservative_v1",
        strategy="passive_spread_capture_conservative_v1",
        model_version="passive_spread_capture_conservative_v1",
        signals=100,
        filled_signals=4,
        realized_edge=0.03,
        drawdown=0.03,
        adverse_selection=0.20,
    )

    report = create_strategy_family_comparison([baseline, conservative])

    assert report["decision"] == "REDESIGN_STRATEGY_AGAIN"


def test_strategy_family_comparison_evaluates_latest_balanced_family(
    tmp_path: Path,
) -> None:
    near_touch = seed_report_root(
        tmp_path / "near-touch",
        run_id="near-touch-run",
        profile="unknown",
        strategy="passive_spread_capture_near_touch_v1",
        model_version="passive_spread_capture_near_touch_v1",
        signals=100,
        filled_signals=30,
        realized_edge=0.02,
        drawdown=0.10,
        adverse_selection=0.50,
    )
    conservative = seed_report_root(
        tmp_path / "conservative",
        run_id="conservative-run",
        profile="conservative_v1",
        strategy="passive_spread_capture_conservative_v1",
        model_version="passive_spread_capture_conservative_v1",
        signals=100,
        filled_signals=20,
        realized_edge=0.03,
        drawdown=0.04,
        adverse_selection=0.25,
    )
    balanced = seed_report_root(
        tmp_path / "balanced",
        run_id="balanced-run",
        profile="balanced_v1",
        strategy="passive_spread_capture_balanced_v1",
        model_version="passive_spread_capture_balanced_v1",
        signals=100,
        filled_signals=4,
        realized_edge=0.03,
        drawdown=0.03,
        adverse_selection=0.20,
    )

    report = create_strategy_family_comparison([near_touch, conservative, balanced])

    assert report["decision"] == "REDESIGN_STRATEGY_AGAIN"
    assert "balanced_v1 did not show enough evidence" in str(
        report["decision_reason"]
    )
    families = cast(list[dict[str, Any]], report["families"])
    family = [
        item
        for item in families
        if item["strategy_family"] == "balanced_v1"
    ][0]
    fill_rate_delta = metric_delta_by_name(family, "fill_rate")
    assert fill_rate_delta["baseline"] == 0.3
    assert fill_rate_delta["candidate"] == 0.04
    assert fill_rate_delta["improved"] is False


def test_strategy_family_comparison_handles_missing_segments(tmp_path: Path) -> None:
    report_root = tmp_path / "missing"
    report_root.mkdir()
    write_json(
        report_root / "real_dry_run_evidence.json",
        {"predictor_strategy_profile": "conservative_v1"},
    )
    write_json(
        report_root / "pre_live_promotion.json",
        {"metrics": {"stale_data_rate": 0.01, "reconciliation_divergence_rate": 0.0}},
    )

    output = report_root / "strategy_family_comparison.json"
    report = create_strategy_family_comparison([report_root], output_path=output)

    assert output.exists()
    family = conservative_family(report)
    assert cast(dict[str, Any], family["metrics"])["missing_segments"] is True
    assert report["decision"] == "REDESIGN_STRATEGY_AGAIN"


def conservative_family(report: dict[str, object]) -> dict[str, Any]:
    families = cast(list[dict[str, Any]], report["families"])
    return [
        family
        for family in families
        if family["strategy_family"] == "conservative_v1"
    ][0]


def metric_delta_by_name(family: dict[str, Any], metric: str) -> dict[str, Any]:
    deltas = cast(list[dict[str, Any]], family["deltas_vs_baseline"])
    return [delta for delta in deltas if delta["metric"] == metric][0]


def seed_report_root(
    report_root: Path,
    *,
    run_id: str,
    profile: str,
    strategy: str,
    model_version: str,
    signals: int,
    filled_signals: int,
    realized_edge: float,
    drawdown: float,
    adverse_selection: float,
) -> Path:
    (report_root / "pre_live_promotion").mkdir(parents=True)
    (report_root / "game_theory").mkdir()
    write_json(report_root / "research_manifest.json", {"run_id": run_id})
    write_json(
        report_root / "real_dry_run_evidence.json",
        {"predictor_strategy_profile": profile},
    )
    write_json(
        report_root / "pre_live_promotion.json",
        {
            "metrics": {
                "stale_data_rate": 0.01,
                "reconciliation_divergence_rate": 0.0,
            }
        },
    )
    write_json(
        report_root / "go_no_go.json",
        {"decision": "NO_GO", "passed": False, "profile": "pre_live", "blockers": []},
    )
    pd.DataFrame(
        [
            {
                "market_id": "m1",
                "asset_id": "a1",
                "side": "BUY",
                "strategy": strategy,
                "model_version": model_version,
                "signals": signals,
                "filled_signals": filled_signals,
                "realized_edge": realized_edge,
                "max_drawdown": drawdown,
                "pnl": realized_edge * filled_signals,
                "filled_notional": filled_signals,
            }
        ]
    ).to_parquet(
        report_root / "pre_live_promotion" / "pre_live_promotion_segments.parquet",
        index=False,
    )
    pd.DataFrame(
        [{"strategy": strategy, "adverse_30s_rate": adverse_selection}]
    ).to_parquet(
        report_root / "game_theory" / "adverse_selection_by_strategy.parquet",
        index=False,
    )
    return report_root


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
