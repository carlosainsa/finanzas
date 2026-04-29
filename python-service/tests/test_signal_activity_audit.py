import json
from pathlib import Path
from typing import Any, cast

import pandas as pd  # type: ignore[import-untyped]

from src.research.signal_activity_audit import (
    REPORT_VERSION,
    create_signal_activity_audit,
)


def test_signal_activity_audit_classifies_market_window_gap(tmp_path: Path) -> None:
    baseline = seed_report_root(
        tmp_path / "baseline",
        run_id="conservative",
        signals=35,
        snapshots=100,
        capture_duration_ms=600_000,
        passes_spread=35,
        all_pass=30,
    )
    candidate = seed_report_root(
        tmp_path / "candidate",
        run_id="balanced",
        signals=19,
        snapshots=120,
        capture_duration_ms=600_000,
        passes_spread=19,
        all_pass=13,
        rejection_comparison={
            "status": "compared",
            "candidate_less_active": False,
            "primary_gap_reason": None,
        },
    )

    output = candidate / "signal_activity_audit.json"
    report = create_signal_activity_audit(baseline, candidate, output_path=output)

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert output.exists()
    comparison = cast(dict[str, Any], report["comparison"])
    assert comparison["candidate_less_active"] is True
    assert comparison["signals_delta"] == -16
    assert comparison["primary_reason"] == "market_window_fewer_spread_candidates"


def test_signal_activity_audit_uses_profile_rejection_gap(tmp_path: Path) -> None:
    baseline = seed_report_root(
        tmp_path / "baseline",
        run_id="conservative",
        signals=20,
        snapshots=100,
        capture_duration_ms=600_000,
        passes_spread=20,
        all_pass=20,
    )
    candidate = seed_report_root(
        tmp_path / "candidate",
        run_id="balanced",
        signals=10,
        snapshots=100,
        capture_duration_ms=600_000,
        passes_spread=20,
        all_pass=20,
        rejection_comparison={
            "status": "compared",
            "candidate_less_active": True,
            "primary_gap_reason": "top_rotation",
        },
    )

    report = create_signal_activity_audit(baseline, candidate)

    comparison = cast(dict[str, Any], report["comparison"])
    assert comparison["primary_reason"] == "profile_rejection_gap:top_rotation"


def seed_report_root(
    report_root: Path,
    *,
    run_id: str,
    signals: int,
    snapshots: int,
    capture_duration_ms: int,
    passes_spread: int,
    all_pass: int,
    rejection_comparison: dict[str, object] | None = None,
) -> Path:
    (report_root / "baseline").mkdir(parents=True)
    write_json(report_root / "research_manifest.json", {"run_id": run_id})
    write_json(
        report_root / "research_summary.json",
        {
            "data_lake": {
                "signals": signals,
                "execution_reports": 0,
                "orderbook_snapshots": snapshots,
            },
            "pre_live_promotion": {
                "metrics": {"capture_duration_ms": capture_duration_ms}
            },
            "baseline": {"counts": {"baseline_signals": all_pass}},
        },
    )
    if rejection_comparison is not None:
        write_json(
            report_root / "signal_rejection_diagnostics.json",
            {"profile_comparison": rejection_comparison, "summary": []},
        )
    rows = [
        {
            "passes_spread": index < passes_spread,
            "passes_depth": index < all_pass,
            "passes_imbalance": index < all_pass,
            "passes_momentum": index < all_pass,
            "passes_stale": True,
            "passes_adverse_selection": True,
        }
        for index in range(snapshots)
    ]
    pd.DataFrame(rows).to_parquet(
        report_root / "baseline" / "baseline_filter_decisions.parquet",
        index=False,
    )
    return report_root


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
