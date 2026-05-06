import json
from pathlib import Path
from typing import Any, cast

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
    deltas = cast(list[dict[str, Any]], report["pairwise_deltas"])
    activity = cast(list[dict[str, Any]], deltas[0]["activity_deltas"])
    fill = cast(list[dict[str, Any]], deltas[0]["fill_deltas"])
    assert find_metric(activity, "signals")["delta"] == -80.0
    assert find_metric(fill, "synthetic_fill_rate")["delta"] == -0.9


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
) -> Path:
    root.mkdir(parents=True)
    write_json(
        root / "real_dry_run_evidence.json",
        {
            "predictor_strategy_profile": profile,
            "predictor_quote_placement": "near_touch",
            "capture_seconds": 3600,
            "market_asset_ids_count": 2,
            "market_asset_ids_sha256": "hash",
            "stream_lengths": {"signals": signals},
            "recent_report_status_counts": {"MATCHED": filled_signals},
        },
    )
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
                "synthetic_only_signals": signals - filled_signals,
                "signals_without_observed_report": signals - filled_signals,
                "dry_run_unfilled_but_synthetic_available": signals - filled_signals,
            },
            "synthetic_vs_observed_gap": [],
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
