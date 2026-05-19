from typing import Any, cast

from src.research.runtime_touch_ab_decision import (
    REPORT_VERSION,
    create_runtime_touch_ab_decision,
    create_runtime_touch_ab_preflight_failure_decision,
    create_runtime_touch_ab_signalability_gate_failure_decision,
)


def test_runtime_touch_ab_decision_repeats_clean_v12() -> None:
    report = create_runtime_touch_ab_decision(
        profile_comparison(
            baseline=observation("execution_probe_v11", signals=100, fill_rate=0.0),
            candidate=observation(
                "execution_probe_v12",
                signals=120,
                fill_rate=0.02,
                synthetic_fill_rate=0.03,
                adverse_selection=-0.01,
            ),
        ),
        touch_comparison(next_action="REPEAT_LONGER_OBSERVATION"),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert report["can_promote_live"] is False
    assert report["recommendation"] == "REPEAT_V12_LONGER"
    templates = "\n".join(cast(list[str], report["next_command_templates"]))
    assert "execution_probe_v12" in templates


def test_runtime_touch_ab_decision_changes_market_when_v12_unmatched() -> None:
    report = create_runtime_touch_ab_decision(
        profile_comparison(
            baseline=observation("execution_probe_v11", signals=100, fill_rate=0.0),
            candidate=observation(
                "execution_probe_v12",
                signals=120,
                fill_rate=0.0,
                failure_family="UNMATCHED",
                no_fill_future_touch_rate=0.0,
            ),
        ),
        touch_comparison(next_action="CHANGE_MARKET_OR_TIMING_FILTERS"),
    )

    assert report["recommendation"] == "CHANGE_MARKET_OR_TIMING_FILTERS"
    assert "no_fill_future_touch_rate=0.0" in cast(list[str], report["rationale"])


def test_runtime_touch_ab_decision_fixes_errors_before_quote_tuning() -> None:
    report = create_runtime_touch_ab_decision(
        profile_comparison(
            baseline=observation("execution_probe_v11", signals=100, fill_rate=0.0),
            candidate=observation(
                "execution_probe_v12",
                signals=120,
                fill_rate=0.0,
                error_rate=0.20,
                failure_family="ERROR",
            ),
        ),
        touch_comparison(next_action="RETUNE_TO_AT_TOUCH"),
    )

    assert report["recommendation"] == "FIX_RISK_OR_EXECUTOR_ERRORS"
    checks = cast(list[dict[str, Any]], report["checks"])
    error_check = next(item for item in checks if item["check_name"] == "execution_error_rate")
    assert error_check["status"] == "FAIL"


def test_runtime_touch_ab_decision_requires_comparable_universe() -> None:
    candidate = observation(
        "execution_probe_v12",
        signals=120,
        fill_rate=0.02,
        synthetic_fill_rate=0.02,
    )
    candidate["market_asset_ids_sha256"] = "different-universe"

    report = create_runtime_touch_ab_decision(
        profile_comparison(
            baseline=observation("execution_probe_v11", signals=100, fill_rate=0.0),
            candidate=candidate,
        ),
        touch_comparison(next_action="REPEAT_LONGER_OBSERVATION"),
    )

    assert report["recommendation"] == "REPEAT_AB_WITH_COMPARABLE_UNIVERSE"
    failed = {
        item["check_name"]: item["status"]
        for item in cast(list[dict[str, Any]], report["comparability_checks"])
    }
    assert failed["same_market_asset_universe_hash"] == "FAIL"


def test_runtime_touch_ab_preflight_failure_reranks_no_signal_universe() -> None:
    report = create_runtime_touch_ab_preflight_failure_decision(
        {
            "report_version": "real_dry_run_preflight_v1",
            "can_execute_trades": False,
            "status": "failed",
            "blockers": ["missing_signals_stream_progress"],
            "market_asset_ids_count": 2,
            "market_asset_ids_sha256": "hash",
            "capture_seconds": 1800,
        },
        failed_profile="execution_probe_v11",
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["recommendation"] == "RERANK_RUNTIME_TOUCH_UNIVERSE"
    assert report["can_promote_live"] is False
    assert "missing_signals_stream_progress" in cast(list[str], report["rationale"])


def test_runtime_touch_ab_preflight_failure_explains_low_depth_rejections() -> None:
    report = create_runtime_touch_ab_preflight_failure_decision(
        {
            "report_version": "real_dry_run_preflight_v1",
            "can_execute_trades": False,
            "status": "failed",
            "blockers": ["missing_signals_stream_progress"],
            "market_asset_ids_count": 2,
            "market_asset_ids_sha256": "hash",
            "capture_seconds": 1800,
            "predictor_decision_diagnostics": {
                "decisions": 4,
                "accepted": 0,
                "rejected": 4,
                "primary_rejection_reason": "low_depth",
            },
        },
        failed_profile="execution_probe_v11",
    )

    assert report["recommendation"] == "RERANK_RUNTIME_TOUCH_UNIVERSE"
    assert "low_depth" in str(report["next_step"])


def test_runtime_touch_ab_preflight_failure_detects_missing_decision_trace() -> None:
    report = create_runtime_touch_ab_preflight_failure_decision(
        {
            "report_version": "real_dry_run_preflight_v1",
            "can_execute_trades": False,
            "status": "failed",
            "blockers": ["missing_predictor_decisions_stream_progress"],
            "market_asset_ids_count": 2,
            "market_asset_ids_sha256": "hash",
            "capture_seconds": 1800,
        },
        failed_profile="execution_probe_v11",
    )

    assert report["recommendation"] == "FIX_PREDICTOR_DECISION_TRACE"
    assert "consumer tracing" in str(report["next_step"])


def test_runtime_touch_ab_signalability_gate_failure_reranks_universe() -> None:
    report = create_runtime_touch_ab_signalability_gate_failure_decision(
        {
            "report_version": "runtime_touch_signalability_diagnostic_v1",
            "can_execute_trades": False,
            "status": "insufficient_signalable_assets",
            "signalable_assets_count": 1,
            "min_assets": 2,
            "assets_analyzed": 8,
            "blocker_counts": {"signalable_density": 2, "signalable_snapshots": 1},
        }
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert report["can_promote_live"] is False
    assert report["recommendation"] == "RERANK_RUNTIME_TOUCH_UNIVERSE"
    checks = {
        item["check_name"]: item["status"]
        for item in cast(list[dict[str, Any]], report["checks"])
    }
    assert checks["signalability_gate_status_ready"] == "FAIL"
    assert checks["minimum_signalable_assets"] == "FAIL"
    assert "signalable_density=2" in cast(list[str], report["rationale"])


def profile_comparison(
    *,
    baseline: dict[str, object],
    candidate: dict[str, object],
) -> dict[str, object]:
    return {
        "report_version": "profile_observation_comparison_v1",
        "can_execute_trades": False,
        "observations": [baseline, candidate],
    }


def touch_comparison(*, next_action: str) -> dict[str, object]:
    return {
        "report_version": "execution_probe_touch_comparison_v1",
        "diagnosis": {
            "next_action": next_action,
            "diagnosis": "test",
            "can_promote_live": False,
        },
    }


def observation(
    profile: str,
    *,
    signals: int,
    fill_rate: float,
    synthetic_fill_rate: float = 0.0,
    adverse_selection: float = 0.0,
    drawdown: float = 0.0,
    error_rate: float = 0.0,
    failure_family: str = "UNMATCHED",
    no_fill_future_touch_rate: float = 0.25,
) -> dict[str, object]:
    return {
        "run_id": profile,
        "profile": profile,
        "capture_seconds": 3600,
        "market_asset_ids_count": 2,
        "market_asset_ids_sha256": "same-universe",
        "activity": {
            "signals": signals,
            "filled_signals": int(signals * fill_rate),
        },
        "fills": {
            "observed_fill_rate": fill_rate,
            "synthetic_fill_rate": synthetic_fill_rate,
            "fill_rate_gap": synthetic_fill_rate - fill_rate,
        },
        "risk": {
            "adverse_selection": adverse_selection,
            "drawdown": drawdown,
        },
        "quote_policy": {
            "no_fill_future_touch_rate": no_fill_future_touch_rate,
        },
        "execution_failure_diagnostics": {
            "summary": {
                "error_rate": error_rate,
                "unmatched_rate": 1 - fill_rate,
                "dominant_failure_family": failure_family,
            },
            "counts": {
                "signals": signals,
                "filled": int(signals * fill_rate),
            },
            "recommended_next_action": "CHANGE_MARKET_OR_TIMING_FILTERS",
        },
    }
