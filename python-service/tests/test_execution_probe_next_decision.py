import json
import os
import subprocess
from pathlib import Path
from typing import Any, cast

from src.research.execution_probe_next_decision import (
    REPORT_VERSION,
    decide_execution_probe_next_step,
)


ROOT_DIR = Path(__file__).resolve().parents[2]


def test_next_decision_repeats_v6_when_fills_are_clean() -> None:
    report = decide_execution_probe_next_step(
        comparison_with_candidate(
            signals=300,
            filled_signals=12,
            observed_fill_rate=0.04,
            synthetic_fill_rate=0.06,
            adverse_selection=-0.01,
            drawdown=0.0,
        )
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert report["can_promote_live"] is False
    assert report["recommendation"] == "REPEAT_EXECUTION_PROBE_LONGER"
    assert "run_execution_probe_v6_observation.sh" in "\n".join(
        cast(list[str], report["next_command_templates"])
    )


def test_next_decision_creates_v7_when_synthetic_optimism_returns() -> None:
    report = decide_execution_probe_next_step(
        comparison_with_candidate(
            signals=300,
            filled_signals=10,
            observed_fill_rate=0.03,
            synthetic_fill_rate=0.20,
            adverse_selection=-0.01,
            drawdown=0.0,
        )
    )

    assert report["recommendation"] == "CREATE_V7_LESS_AGGRESSIVE_QUOTE"
    assert "EXECUTION_MODE=live" not in json.dumps(report)


def test_next_decision_holds_v7_when_synthetic_optimism_persists() -> None:
    report = decide_execution_probe_next_step(
        comparison_with_candidate(
            profile="execution_probe_v7",
            signals=300,
            filled_signals=10,
            observed_fill_rate=0.03,
            synthetic_fill_rate=0.20,
            adverse_selection=-0.01,
            drawdown=0.0,
        )
    )

    assert report["recommendation"] == "HOLD_RESEARCH"
    assert "synthetic-only evidence" in str(report["next_step"])


def test_next_decision_relaxes_filters_when_large_sample_has_no_fills() -> None:
    report = decide_execution_probe_next_step(
        comparison_with_candidate(
            signals=400,
            filled_signals=0,
            observed_fill_rate=0.0,
            synthetic_fill_rate=0.0,
            no_fill_future_touch_rate=0.50,
        )
    )

    assert report["recommendation"] == "RELAX_SIGNAL_FILTERS"
    assert "min_confidence" in str(report["next_step"])


def test_next_decision_changes_market_or_timing_when_future_books_never_touch() -> None:
    report = decide_execution_probe_next_step(
        comparison_with_candidate(
            signals=400,
            filled_signals=0,
            observed_fill_rate=0.0,
            synthetic_fill_rate=0.0,
            no_fill_future_touch_rate=0.0,
        )
    )

    assert report["recommendation"] == "CHANGE_MARKET_OR_TIMING_FILTERS"


def test_next_decision_repeats_v6_when_sample_is_too_small() -> None:
    report = decide_execution_probe_next_step(
        comparison_with_candidate(
            signals=12,
            filled_signals=0,
            observed_fill_rate=0.0,
            synthetic_fill_rate=0.0,
        )
    )

    assert report["recommendation"] == "REPEAT_V6_WITH_LARGER_SAMPLE"


def test_next_decision_waits_for_v6_candidate() -> None:
    report = decide_execution_probe_next_step(
        comparison_with_candidate(
            profile="execution_probe_v5",
            signals=200,
            filled_signals=0,
            observed_fill_rate=0.0,
            synthetic_fill_rate=0.0,
        )
    )

    assert report["recommendation"] == "WAIT_FOR_EXECUTION_PROBE_OBSERVATION"


def test_next_decision_holds_when_source_is_live_capable() -> None:
    comparison = comparison_with_candidate(
        signals=300,
        filled_signals=12,
        observed_fill_rate=0.04,
        synthetic_fill_rate=0.05,
        adverse_selection=-0.02,
        drawdown=0.0,
    )
    comparison["can_execute_trades"] = True

    report = decide_execution_probe_next_step(comparison)

    assert report["recommendation"] == "HOLD_RESEARCH"
    checks = {
        str(check["check_name"]): check
        for check in cast(list[dict[str, object]], report["checks"])
    }
    assert checks["source_can_execute_trades_false"]["status"] == "FAIL"


def test_next_decision_cli_writes_output(tmp_path: Path) -> None:
    comparison_path = tmp_path / "profile_observation_comparison.json"
    output_path = tmp_path / "execution_probe_next_decision.json"
    comparison_path.write_text(
        json.dumps(
            comparison_with_candidate(
                signals=300,
                filled_signals=12,
                observed_fill_rate=0.04,
                synthetic_fill_rate=0.05,
                adverse_selection=-0.02,
                drawdown=0.0,
            )
        ),
        encoding="utf-8",
    )

    completed = subprocess.run(
        [
            "python3",
            "-m",
            "src.research.execution_probe_next_decision",
            "--comparison",
            str(comparison_path),
            "--output",
            str(output_path),
            "--json",
        ],
        cwd=ROOT_DIR,
        env={**os.environ, "PYTHONPATH": "python-service"},
        check=True,
        capture_output=True,
        text=True,
    )

    stdout_report = json.loads(completed.stdout)
    output_report = json.loads(output_path.read_text(encoding="utf-8"))
    assert stdout_report["recommendation"] == "REPEAT_EXECUTION_PROBE_LONGER"
    assert output_report["recommendation"] == stdout_report["recommendation"]


def comparison_with_candidate(
    *,
    profile: str = "execution_probe_v6",
    signals: int,
    filled_signals: int,
    observed_fill_rate: float,
    synthetic_fill_rate: float,
    adverse_selection: float = 0.0,
    drawdown: float = 0.0,
    no_fill_future_touch_rate: float = 0.25,
) -> dict[str, object]:
    return {
        "report_version": "profile_observation_comparison_v1",
        "can_execute_trades": False,
        "observations": [
            observation(
                profile="execution_probe_v5",
                run_id="baseline",
                signals=100,
                filled_signals=0,
                observed_fill_rate=0.0,
                synthetic_fill_rate=0.0,
                adverse_selection=0.0,
                drawdown=0.0,
                no_fill_future_touch_rate=0.0,
            ),
            observation(
                profile=profile,
                run_id="candidate",
                signals=signals,
                filled_signals=filled_signals,
                observed_fill_rate=observed_fill_rate,
                synthetic_fill_rate=synthetic_fill_rate,
                adverse_selection=adverse_selection,
                drawdown=drawdown,
                no_fill_future_touch_rate=no_fill_future_touch_rate,
            ),
        ],
        "pairwise_deltas": [],
    }


def observation(
    *,
    profile: str,
    run_id: str,
    signals: int,
    filled_signals: int,
    observed_fill_rate: float,
    synthetic_fill_rate: float,
    adverse_selection: float,
    drawdown: float,
    no_fill_future_touch_rate: float,
) -> dict[str, object]:
    return {
        "run_id": run_id,
        "report_root": f"/tmp/{run_id}",
        "profile": profile,
        "quote_placement": "near_touch",
        "activity": {
            "signals": signals,
            "filled_signals": filled_signals,
        },
        "fills": {
            "observed_fill_rate": observed_fill_rate,
            "synthetic_fill_rate": synthetic_fill_rate,
            "fill_rate_gap": synthetic_fill_rate - observed_fill_rate,
        },
        "risk": {
            "adverse_selection": adverse_selection,
            "drawdown": drawdown,
        },
        "quote_policy": {
            "no_fill_future_touch_rate": no_fill_future_touch_rate,
            "avg_required_quote_move": 0.01,
        },
    }
