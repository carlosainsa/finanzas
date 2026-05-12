import json
from pathlib import Path
from typing import Any, cast

from src.research.toxicity_filter_impact import (
    REPORT_VERSION,
    create_toxicity_filter_impact_report,
)


def test_toxicity_filter_impact_keeps_effective_filter(tmp_path: Path) -> None:
    baseline = seed_report(
        tmp_path / "baseline",
        signals=1000,
        filled_signals=20,
        observed_fill_rate=0.02,
        synthetic_fill_rate=0.04,
        adverse_selection=0.80,
        toxicity_enabled=False,
    )
    candidate = seed_report(
        tmp_path / "candidate",
        signals=980,
        filled_signals=19,
        observed_fill_rate=0.019,
        synthetic_fill_rate=0.03,
        adverse_selection=0.70,
        toxicity_enabled=True,
        blocked_segment_rejections=50,
    )

    output = candidate / "toxicity_filter_impact.json"
    report = create_toxicity_filter_impact_report(
        candidate,
        baseline_report_root=baseline,
        output_path=output,
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert output.exists()
    assert cast(dict[str, Any], report["filter"])["blocked_segments"] == 1
    runtime = cast(dict[str, Any], report["runtime_rejection_impact"])
    assert runtime["blocked_segment_rejections"] == 50
    assert runtime["blocked_segment_rate"] == 0.05
    assert cast(dict[str, Any], report["decision"])["recommendation"] == "KEEP_FILTER"


def test_toxicity_filter_impact_relaxes_activity_killing_filter(tmp_path: Path) -> None:
    baseline = seed_report(
        tmp_path / "baseline",
        signals=100,
        filled_signals=5,
        observed_fill_rate=0.05,
        synthetic_fill_rate=0.05,
        adverse_selection=0.50,
        toxicity_enabled=False,
    )
    candidate = seed_report(
        tmp_path / "candidate",
        signals=0,
        filled_signals=0,
        observed_fill_rate=0.0,
        synthetic_fill_rate=0.0,
        adverse_selection=0.50,
        toxicity_enabled=True,
        blocked_segment_rejections=100,
        diagnostic_snapshots=100,
    )

    report = create_toxicity_filter_impact_report(
        candidate,
        baseline_report_root=baseline,
    )

    assert cast(dict[str, Any], report["decision"])["recommendation"] == "RELAX_FILTER"


def test_toxicity_filter_impact_requires_baseline_for_enabled_filter(
    tmp_path: Path,
) -> None:
    candidate = seed_report(
        tmp_path / "candidate",
        signals=10,
        filled_signals=1,
        observed_fill_rate=0.10,
        synthetic_fill_rate=0.10,
        adverse_selection=0.40,
        toxicity_enabled=True,
    )

    report = create_toxicity_filter_impact_report(candidate)

    assert (
        cast(dict[str, Any], report["decision"])["recommendation"]
        == "NEED_BASELINE_COMPARISON"
    )


def seed_report(
    root: Path,
    *,
    signals: int,
    filled_signals: int,
    observed_fill_rate: float,
    synthetic_fill_rate: float,
    adverse_selection: float,
    toxicity_enabled: bool,
    blocked_segment_rejections: int = 0,
    diagnostic_snapshots: int = 1000,
) -> Path:
    root.mkdir(parents=True)
    blocklist_path = root / "blocked_segments.json"
    blocklist_path.write_text(
        json.dumps(
            {
                "version": "blocked_segments_v1",
                "can_execute_trades": False,
                "segments": [
                    {
                        "market_id": "market-1",
                        "asset_id": "asset-1",
                        "side": "BUY",
                        "strategy": "passive_spread_capture_execution_probe_v9",
                        "model_version": "passive_spread_capture_execution_probe_v9",
                        "spread_bucket": "250_500bps",
                        "timing_bucket": "stable",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    universe_path = root / "execution_probe_universe_selection.json"
    write_json(
        universe_path,
        {
            "status": "ready",
            "profile": "execution_probe_v9",
            "config": {
                "selection_source": "fillability",
                "market_timing_filter": "future_touch",
            },
            "toxicity_filter": {
                "enabled": toxicity_enabled,
                "mode": "segment" if toxicity_enabled else "none",
                "blocked_segments_path": str(blocklist_path) if toxicity_enabled else None,
                "filtered_count": 1 if toxicity_enabled else 0,
                "quality_rows": 1 if toxicity_enabled else 0,
            },
            "market_asset_ids_count": 1,
            "market_asset_ids_sha256": "hash",
            "selection_reason": "selection_source=fillability",
        },
    )
    write_json(
        root / "real_dry_run_evidence.json",
        {
            "predictor_strategy_profile": "execution_probe_v9",
            "predictor_quote_placement": "near_touch",
            "execution_probe_universe_selection_path": str(universe_path),
            "blocked_segments_enabled": toxicity_enabled,
            "blocked_segments_path": str(blocklist_path) if toxicity_enabled else None,
            "capture_seconds": 3600,
            "market_asset_ids_count": 1,
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
                "fill_rate": observed_fill_rate,
                "dry_run_observed_fill_rate": observed_fill_rate,
                "realized_edge": 0.01,
                "adverse_selection": adverse_selection,
                "drawdown": 0.02,
                "stale_data_rate": 0.0,
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
                "signals_without_observed_report": signals - filled_signals,
            },
            "synthetic_vs_observed_gap": [],
        },
    )
    write_json(
        root / "fill_toxicity.json",
        {
            "report_version": "fill_toxicity_v1",
            "counts": {
                "fill_toxicity_events": filled_signals,
                "fill_toxicity_by_asset_strategy": 1,
            },
            "summary": {
                "segments": 1,
                "signals": signals,
                "filled_events": filled_signals,
                "fill_rate": observed_fill_rate,
                "avg_pnl_30s": 0.01,
                "adverse_30s_rate": adverse_selection,
                "rejected_segments": 1 if toxicity_enabled else 0,
                "promoted_segments": 0 if toxicity_enabled else 1,
                "diagnostic_segments": 0,
                "insufficient_sample_segments": 0,
            },
        },
    )
    write_json(
        root / "signal_rejection_diagnostics.json",
        {
            "summary": [
                {
                    "profile": "execution_probe_v9",
                    "snapshots": diagnostic_snapshots,
                    "accepted": max(0, diagnostic_snapshots - blocked_segment_rejections),
                    "acceptance_rate": 0.95,
                    "rejection_counts": {
                        "accepted": max(
                            0,
                            diagnostic_snapshots - blocked_segment_rejections,
                        ),
                        "blocked_segment": blocked_segment_rejections,
                    },
                }
            ],
            "profile_comparison": {"status": "compared"},
        },
    )
    write_json(root / "signal_to_order_conversion.json", {"summary": {}})
    write_json(root / "signal_rejection_diagnostics.json", read_json(root / "signal_rejection_diagnostics.json"))
    write_json(root / "go_no_go.json", {"blockers": []})
    return root


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def read_json(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    return value if isinstance(value, dict) else {}
