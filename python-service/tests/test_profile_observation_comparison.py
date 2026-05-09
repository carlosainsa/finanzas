import json
from pathlib import Path
from typing import Any, cast

import pytest

from src.research.profile_observation_comparison import (
    REPORT_VERSION,
    create_profile_observation_comparison,
)


def test_profile_observation_comparison_compares_activity_fills_and_blockers(
    tmp_path: Path,
) -> None:
    v1 = seed_profile_report(
        tmp_path / "v1",
        profile="execution_probe_v1",
        signals=100,
        filled_signals=10,
        observed_fill_rate=0.10,
        synthetic_fill_rate=0.90,
        blockers=("no_persistent_adverse_selection",),
    )
    v2 = seed_profile_report(
        tmp_path / "v2",
        profile="execution_probe_v2",
        signals=20,
        filled_signals=0,
        observed_fill_rate=0.0,
        synthetic_fill_rate=0.0,
        blockers=("has_fills",),
        market_timing_filter="future_touch",
    )

    report = create_profile_observation_comparison([v1, v2])

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    observations = cast(list[dict[str, Any]], report["observations"])
    assert [item["profile"] for item in observations] == [
        "execution_probe_v1",
        "execution_probe_v2",
    ]
    assert cast(dict[str, Any], observations[0]["fills"])["fill_rate_gap"] == 0.8
    assert (
        cast(dict[str, Any], observations[0]["fills"])[
            "adjusted_synthetic_fill_rate"
        ]
        == 0.25
    )
    deltas = cast(list[dict[str, Any]], report["pairwise_deltas"])
    activity = cast(list[dict[str, Any]], deltas[0]["activity_deltas"])
    fill = cast(list[dict[str, Any]], deltas[0]["fill_deltas"])
    assert find_metric(activity, "signals")["delta"] == -80.0
    assert find_metric(fill, "synthetic_fill_rate")["delta"] == -0.9
    assert find_metric(fill, "adjusted_fill_rate_gap")["delta"] == -0.15
    quote_policy = cast(list[dict[str, Any]], deltas[0]["quote_policy_deltas"])
    assert find_metric(quote_policy, "avg_required_quote_move")[
        "delta"
    ] == pytest.approx(0.01)
    timing_selection = cast(dict[str, Any], observations[1]["market_timing_selection"])
    assert timing_selection["market_timing_filter"] == "future_touch"
    assert timing_selection["min_future_touch_rate"] == 0.1
    assert timing_selection["min_timing_signals"] == 5
    assert timing_selection["min_avg_opportunity_spread"] == 0.01
    assert timing_selection["market_asset_ids_count"] == 2
    unmatched = cast(dict[str, Any], observations[1]["unmatched_diagnostics"])
    signal_to_order = cast(dict[str, Any], unmatched["signal_to_order_summary"])
    assert signal_to_order["consumption_rate"] == 1.0
    no_fill = cast(list[dict[str, Any]], unmatched["no_fill_diagnostics"])
    assert no_fill[0]["root_cause"] == "dry_run_created_unmatched"


def test_profile_observation_comparison_extracts_effective_quote_policy(
    tmp_path: Path,
) -> None:
    baseline = seed_profile_report(
        tmp_path / "baseline",
        profile="execution_probe_v7",
        signals=100,
        filled_signals=0,
        observed_fill_rate=0.0,
        synthetic_fill_rate=0.0,
        blockers=("has_fills",),
        near_touch_fraction=0.85,
        offset_ticks=1,
        fraction_selection_path="/tmp/v7-fraction.json",
    )
    candidate = seed_profile_report(
        tmp_path / "candidate",
        profile="execution_probe_v8",
        signals=120,
        filled_signals=1,
        observed_fill_rate=0.01,
        synthetic_fill_rate=0.01,
        blockers=(),
        near_touch_fraction=1.0,
        offset_ticks=0,
    )

    report = create_profile_observation_comparison([baseline, candidate])

    observations = cast(list[dict[str, Any]], report["observations"])
    baseline_quote = cast(dict[str, Any], observations[0]["quote_policy"])
    candidate_quote = cast(dict[str, Any], observations[1]["quote_policy"])
    assert baseline_quote["near_touch_max_spread_fraction"] == 0.85
    assert baseline_quote["offset_ticks"] == 1.0
    assert baseline_quote["fraction_selection_path"] == "/tmp/v7-fraction.json"
    assert candidate_quote["near_touch_max_spread_fraction"] == 1.0
    assert candidate_quote["offset_ticks"] == 0.0
    deltas = cast(list[dict[str, Any]], report["pairwise_deltas"])
    quote_policy = cast(list[dict[str, Any]], deltas[0]["quote_policy_deltas"])
    assert find_metric(quote_policy, "near_touch_max_spread_fraction")[
        "delta"
    ] == pytest.approx(0.15)
    assert find_metric(quote_policy, "offset_ticks")["delta"] == -1.0


def find_metric(rows: list[dict[str, Any]], metric: str) -> dict[str, Any]:
    return next(row for row in rows if row["metric"] == metric)


def seed_profile_report(
    root: Path,
    *,
    profile: str,
    signals: int,
    filled_signals: int,
    observed_fill_rate: float,
    synthetic_fill_rate: float,
    blockers: tuple[str, ...],
    market_timing_filter: str | None = None,
    near_touch_fraction: float | None = None,
    offset_ticks: int | None = None,
    fraction_selection_path: str | None = None,
) -> Path:
    root.mkdir(parents=True)
    universe_selection_path = ""
    if market_timing_filter is not None:
        universe_selection_path = str(root / "execution_probe_universe_selection.json")
        write_json(
            root / "execution_probe_universe_selection.json",
            {
                "status": "ready",
                "profile": profile,
                "config": {
                    "market_timing_filter": market_timing_filter,
                    "min_future_touch_rate": 0.1,
                    "min_timing_signals": 5,
                    "min_avg_opportunity_spread": 0.01,
                    "max_avg_opportunity_spread": None,
                },
                "market_asset_ids_count": 2,
                "market_asset_ids_sha256": "timing-hash",
                "selection_reason": "ranked_multi_market_universe_meets_minimum_asset_coverage",
            },
        )
    evidence: dict[str, object] = {
            "predictor_strategy_profile": profile,
            "predictor_quote_placement": "near_touch",
            "execution_probe_universe_selection_path": universe_selection_path,
            "capture_seconds": 3600,
            "market_asset_ids_count": 2,
            "market_asset_ids_sha256": "hash",
            "stream_lengths": {"signals": signals},
            "recent_report_status_counts": {"MATCHED": filled_signals},
    }
    if near_touch_fraction is not None:
        evidence[f"predictor_{profile}_near_touch_max_spread_fraction"] = str(
            near_touch_fraction
        )
    if offset_ticks is not None:
        evidence[f"predictor_{profile}_offset_ticks"] = str(offset_ticks)
    if fraction_selection_path is not None:
        evidence[f"predictor_{profile}_fraction_selection_path"] = (
            fraction_selection_path
        )
    write_json(root / "real_dry_run_evidence.json", evidence)
    write_json(
        root / "pre_live_promotion.json",
        {
            "metrics": {
                "signals": float(signals),
                "filled_signals": float(filled_signals),
                "fill_rate": filled_signals / signals,
                "dry_run_observed_fill_rate": observed_fill_rate,
                "realized_edge": 0.01,
                "adverse_selection": 0.5,
                "drawdown": 0.0,
                "stale_data_rate": 0.01,
                "test_brier_score": 0.2,
                "reconciliation_divergence_rate": 0.0,
            }
        },
    )
    write_json(
        root / "quote_execution_diagnostics.json",
        {
            "summary": {
                "signals": signals,
                "observed_fill_rate": observed_fill_rate,
                "synthetic_fill_rate": synthetic_fill_rate,
                "adjusted_synthetic_fill_rate": (
                    0.25 if synthetic_fill_rate > observed_fill_rate else synthetic_fill_rate
                ),
                "adjusted_fill_rate_gap": (
                    0.25 if synthetic_fill_rate > observed_fill_rate else synthetic_fill_rate
                )
                - observed_fill_rate,
                "synthetic_only_signals": signals - filled_signals,
                "signals_without_observed_report": signals - filled_signals,
                "dry_run_unfilled_but_synthetic_available": signals - filled_signals,
                "avg_no_fill_distance_to_touch": 0.02,
                "avg_no_fill_distance_to_mid": 0.01,
                "avg_no_fill_spread": 0.05,
                "no_fill_future_touch_rate": 0.0,
                "avg_required_quote_move": 0.03 if profile.endswith("v2") else 0.02,
            },
            "synthetic_vs_observed_gap": [],
            "no_fill_diagnostics": [
                {
                    "asset_id": "asset-1",
                    "quote_relation": "inside_spread",
                    "root_cause": "dry_run_created_unmatched",
                    "signals": signals,
                    "avg_distance_to_touch": 0.01,
                    "future_touch_rate": 0.0,
                    "avg_required_quote_move": 0.02,
                }
            ],
        },
    )
    write_json(
        root / "signal_to_order_conversion.json",
        {
            "summary": {
                "signals": signals,
                "consumed_signals": signals,
                "unconsumed_signals": 0,
                "rejected_consumed_signals": 0,
                "consumption_rate": 1.0,
                "rejection_rate": 0.0,
            },
            "top_root_causes": [
                {
                    "root_cause": "order_created_unfilled",
                    "consumption_state": "consumed_order_created",
                    "signals": signals,
                }
            ],
        },
    )
    write_json(
        root / "signal_rejection_diagnostics.json",
        {
            "summary": [],
            "profile_comparison": {"status": "compared"},
        },
    )
    write_json(
        root / "go_no_go.json",
        {"blockers": [{"check_name": blocker} for blocker in blockers]},
    )
    return root


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")
