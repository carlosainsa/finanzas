import json
from pathlib import Path
from typing import Any, cast

from src.research.execution_probe_touch_comparison import (
    REPORT_VERSION,
    create_execution_probe_touch_comparison,
)


def test_execution_probe_touch_comparison_diagnoses_quote_aggressiveness(
    tmp_path: Path,
) -> None:
    baseline = seed_profile_report(
        tmp_path / "v10",
        profile="execution_probe_v10",
        signals=100,
        filled_signals=0,
        observed_fill_rate=0.0,
        synthetic_fill_rate=0.0,
        blockers=("has_fills",),
    )
    candidate = seed_profile_report(
        tmp_path / "v11",
        profile="execution_probe_v11",
        signals=120,
        filled_signals=0,
        observed_fill_rate=0.0,
        synthetic_fill_rate=0.0,
        blockers=("has_fills",),
        near_touch_fraction=0.90,
    )
    (candidate / "at_touch_asset_diagnostic").mkdir()
    write_json(
        candidate / "at_touch_asset_diagnostic" / "at_touch_asset_diagnostic.json",
        {
            "summary": {
                "assets": 2,
                "signals": 120,
                "observed_fill_rate": 0.0,
                "future_touch_rate": 0.2,
                "decisions": {"RETUNE_TO_AT_TOUCH": 2},
            }
        },
    )

    report = create_execution_probe_touch_comparison([baseline, candidate])

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    diagnosis = cast(dict[str, Any], report["diagnosis"])
    assert diagnosis["next_action"] == "RETUNE_TO_AT_TOUCH"
    observations = cast(list[dict[str, Any]], report["observations"])
    assert cast(dict[str, Any], observations[1]["at_touch_diagnostic"])["summary"][
        "assets"
    ] == 2


def test_execution_probe_touch_comparison_diagnoses_runtime_stale(
    tmp_path: Path,
) -> None:
    candidate = seed_profile_report(
        tmp_path / "v11",
        profile="execution_probe_v11",
        signals=120,
        filled_signals=0,
        observed_fill_rate=0.0,
        synthetic_fill_rate=0.0,
        blockers=("has_fills",),
    )
    (candidate / "at_touch_asset_diagnostic").mkdir()
    write_json(
        candidate / "at_touch_asset_diagnostic" / "at_touch_asset_diagnostic.json",
        {
            "summary": {
                "assets": 2,
                "signals": 120,
                "observed_fill_rate": 0.0,
                "future_touch_rate": 0.0,
                "decisions": {"DROP_RUNTIME_STALE": 2},
            }
        },
    )

    report = create_execution_probe_touch_comparison([candidate])

    diagnosis = cast(dict[str, Any], report["diagnosis"])
    assert diagnosis["diagnosis"] == "runtime_touch_stale_or_quotes_not_reachable"
    assert diagnosis["next_action"] == "CHANGE_MARKET_OR_TIMING_FILTERS"


def seed_profile_report(
    root: Path,
    *,
    profile: str,
    signals: int,
    filled_signals: int,
    observed_fill_rate: float,
    synthetic_fill_rate: float,
    blockers: tuple[str, ...],
    near_touch_fraction: float | None = None,
) -> Path:
    root.mkdir(parents=True)
    evidence: dict[str, object] = {
        "predictor_strategy_profile": profile,
        "predictor_quote_placement": "near_touch",
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
    write_json(root / "real_dry_run_evidence.json", evidence)
    write_json(
        root / "pre_live_promotion.json",
        {
            "metrics": {
                "signals": float(signals),
                "filled_signals": float(filled_signals),
                "fill_rate": filled_signals / signals,
                "dry_run_observed_fill_rate": observed_fill_rate,
                "realized_edge": 0.0,
                "adverse_selection": 0.0,
                "drawdown": 0.0,
                "stale_data_rate": 0.0,
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
                "adjusted_synthetic_fill_rate": synthetic_fill_rate,
                "adjusted_fill_rate_gap": synthetic_fill_rate - observed_fill_rate,
                "signals_without_observed_report": signals - filled_signals,
                "avg_no_fill_distance_to_touch": 0.02,
                "no_fill_future_touch_rate": 0.0,
                "avg_required_quote_move": 0.02,
            },
            "synthetic_vs_observed_gap": [],
            "no_fill_diagnostics": [],
        },
    )
    write_json(root / "fill_toxicity.json", {})
    write_json(root / "signal_to_order_conversion.json", {"summary": {}})
    write_json(root / "signal_rejection_diagnostics.json", {})
    write_json(
        root / "go_no_go.json",
        {"blockers": [{"check_name": blocker} for blocker in blockers]},
    )
    return root


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")
