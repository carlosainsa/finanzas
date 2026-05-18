from pathlib import Path
from typing import Any

from src.research.runtime_touch_ab_root_cause import (
    REPORT_VERSION,
    create_runtime_touch_ab_root_cause,
    write_runtime_touch_ab_root_cause,
)


def test_root_cause_classifies_signalability_gate_failure() -> None:
    report = create_runtime_touch_ab_root_cause(
        cycle_summary={
            "status": "blocked",
            "blocker": "runtime_touch_signalability_gate",
            "signalability_gate": {
                "status": "insufficient_signalable_assets",
                "signalable_assets_count": 1,
                "min_assets": 2,
                "blocker_counts": {"signalable_density": 2},
            },
        }
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert report["root_cause_category"] == "NO_SIGNALABLE_ASSETS"
    assert report["recommended_next_action"] == "RERANK_OR_EXPAND_RUNTIME_TOUCH_UNIVERSE"


def test_root_cause_classifies_preflight_signal_and_report_failures() -> None:
    signal_report = create_runtime_touch_ab_root_cause(
        cycle_summary={
            "failed_profile": "execution_probe_v11",
            "failed_preflight": {"blockers": ["missing_signals_stream_progress"]},
        }
    )
    report_report = create_runtime_touch_ab_root_cause(
        cycle_summary={
            "failed_profile": "execution_probe_v11",
            "failed_preflight": {
                "blockers": ["missing_execution_reports_stream_progress"]
            },
        }
    )

    assert signal_report["root_cause_category"] == "SIGNALS_ZERO"
    assert (
        signal_report["recommended_next_action"]
        == "CHECK_CONSUMER_OR_PREDICTOR_DECISION_TRACE"
    )
    assert report_report["root_cause_category"] == "REPORTS_ZERO"
    assert report_report["recommended_next_action"] == "FIX_RISK_EXECUTOR_OR_REPORTING"


def test_root_cause_uses_predictor_rejection_diagnostics_for_zero_signals() -> None:
    report = create_runtime_touch_ab_root_cause(
        cycle_summary={
            "failed_profile": "execution_probe_v11",
            "failed_preflight": {
                "blockers": ["missing_signals_stream_progress"],
                "predictor_decision_diagnostics": {
                    "decisions": 10,
                    "accepted": 0,
                    "rejected": 10,
                    "primary_rejection_reason": "top_rotation",
                    "rejection_counts": {"top_rotation": 10},
                },
            },
        }
    )

    assert report["root_cause_category"] == "SIGNALS_ZERO"
    assert (
        report["recommended_next_action"]
        == "RERANK_OR_RETUNE_PREDICTOR_REJECTIONS:top_rotation"
    )
    evidence = report["evidence"]
    assert isinstance(evidence, dict)
    assert evidence["primary_rejection_reason"] == "top_rotation"


def test_root_cause_classifies_completed_ab_candidate_failures() -> None:
    assert classify_candidate(
        signals=120,
        reports=120,
        observed_fill_rate=0.0,
        failure_family="ERROR",
        error_rate=0.20,
    ) == ("ERRORS_RISK_EXECUTOR", "FIX_RISK_OR_EXECUTOR_ERRORS")
    assert classify_candidate(
        signals=120,
        reports=120,
        observed_fill_rate=0.0,
        failure_family="UNMATCHED",
    ) == ("UNMATCHED_QUOTES", "CHANGE_MARKET_TIMING_OR_QUOTE_POLICY")
    assert classify_candidate(
        signals=120,
        reports=120,
        observed_fill_rate=0.02,
        failure_family="FILLED",
        toxic_adverse_rate=0.90,
    ) == ("TOXIC_FILLS", "ADD_OR_REPAIR_TOXICITY_FILTERS")
    assert classify_candidate(
        signals=120,
        reports=120,
        observed_fill_rate=0.001,
        failure_family="FILLED",
    ) == ("LOW_FILL_RATE", "REPEAT_OR_RETUNE_FILLABILITY_SELECTION")
    assert classify_candidate(
        signals=120,
        reports=120,
        observed_fill_rate=0.02,
        failure_family="FILLED",
        adverse_selection=0.10,
    ) == ("ADVERSE_SELECTION", "FILTER_TOXIC_MARKET_TIMING")


def test_root_cause_cli_writes_output(tmp_path: Path) -> None:
    cycle_summary = tmp_path / "summary.json"
    output = tmp_path / "root_cause.json"
    cycle_summary.write_text(
        '{"blocker":"runtime_touch_signalability_gate","signalability_gate":{"status":"insufficient_signalable_assets","signalable_assets_count":0,"min_assets":2}}\n',
        encoding="utf-8",
    )

    report = write_runtime_touch_ab_root_cause(
        output,
        cycle_summary_path=cycle_summary,
    )

    assert output.exists()
    assert report["root_cause_category"] == "NO_SIGNALABLE_ASSETS"


def classify_candidate(
    *,
    signals: int,
    reports: int,
    observed_fill_rate: float,
    failure_family: str,
    error_rate: float = 0.0,
    adverse_selection: float = 0.0,
    toxic_adverse_rate: float | None = None,
) -> tuple[object, object]:
    report = create_runtime_touch_ab_root_cause(
        profile_observation_comparison={
            "report_version": "profile_observation_comparison_v1",
            "observations": [
                observation(
                    signals=100,
                    reports=100,
                    observed_fill_rate=0.0,
                    failure_family="UNMATCHED",
                ),
                observation(
                    signals=signals,
                    reports=reports,
                    observed_fill_rate=observed_fill_rate,
                    failure_family=failure_family,
                    error_rate=error_rate,
                    adverse_selection=adverse_selection,
                    toxic_adverse_rate=toxic_adverse_rate,
                ),
            ],
        }
    )
    return report["root_cause_category"], report["recommended_next_action"]


def observation(
    *,
    signals: int,
    reports: int,
    observed_fill_rate: float,
    failure_family: str,
    error_rate: float = 0.0,
    adverse_selection: float = 0.0,
    toxic_adverse_rate: float | None = None,
) -> dict[str, Any]:
    return {
        "profile": "execution_probe_v12",
        "activity": {"signals": signals},
        "fills": {"observed_fill_rate": observed_fill_rate},
        "risk": {"adverse_selection": adverse_selection},
        "execution_failure_diagnostics": {
            "summary": {
                "error_rate": error_rate,
                "dominant_failure_family": failure_family,
            },
            "counts": {"reports": reports},
        },
        "fill_toxicity": {
            "adverse_30s_rate": toxic_adverse_rate,
            "rejected_segments": 0,
        },
    }
