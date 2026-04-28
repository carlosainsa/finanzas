import json
from pathlib import Path

from src.research.restricted_blocklist_family_decision import (
    decide_restricted_blocklist_family,
)


def test_family_decision_redesigns_when_all_migrated_still_migrates(
    tmp_path: Path,
) -> None:
    history = write_history(
        tmp_path,
        [
            family("candidate", risk_rate=1.0, stable="TRY_ALL_MIGRATED"),
            family(
                "restricted_input_plus_all_migrated_risk",
                risk_rate=1.0,
                stable="REDESIGN_STRATEGY",
                score=10.0,
            ),
        ],
    )

    decision = decide_restricted_blocklist_family(history)

    assert decision["decision"] == "REDESIGN_STRATEGY"
    assert decision["reason"] == "all_migrated_variant_still_has_risk_migration"
    assert decision["can_execute_trades"] is False
    assert decision["selected_family"] == {
        "adverse_selection_delta_avg": -0.02,
        "complete_observations": 1,
        "drawdown_delta_avg": -0.2,
        "fill_rate_delta_avg": 0.01,
        "realized_edge_delta_avg": 0.03,
        "risk_migration_detected_rate": 1.0,
        "score_avg": 10.0,
        "stable_recommendation": "REDESIGN_STRATEGY",
        "unexpected_blocked_segments_rate": 0.0,
        "variant_family": "restricted_input_plus_all_migrated_risk",
    }


def test_family_decision_tests_all_migrated_before_all_variant_exists(
    tmp_path: Path,
) -> None:
    history = write_history(
        tmp_path,
        [
            family("candidate", risk_rate=1.0, stable="TRY_ALL_MIGRATED"),
            family(
                "restricted_input_plus_top_migrated_risk",
                risk_rate=1.0,
                stable="TRY_ALL_MIGRATED",
            ),
        ],
    )

    decision = decide_restricted_blocklist_family(history)

    assert decision["decision"] == "TEST_ALL_MIGRATED"
    assert decision["reason"] == "migration_seen_before_all_migrated_variant"


def test_family_decision_repeats_clean_repeatable_family(tmp_path: Path) -> None:
    history = write_history(
        tmp_path,
        [family("migrated_risk_only", risk_rate=0.0, stable="REPEAT", score=100.0)],
    )

    decision = decide_restricted_blocklist_family(history)

    assert decision["decision"] == "REPEAT_OBSERVATION"
    assert decision["reason"] == "repeatable_variant_without_migrated_risk"


def test_family_decision_stops_without_complete_observations(tmp_path: Path) -> None:
    path = tmp_path / "history.json"
    path.write_text(
        json.dumps(
            {
                "report_version": "restricted_blocklist_observation_history_v1",
                "variant_family_summary": [
                    {
                        "variant_family": "candidate",
                        "complete_observations": 0,
                        "stable_recommendation": "NEED_MORE_EVIDENCE",
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    decision = decide_restricted_blocklist_family(path)

    assert decision["decision"] == "STOP_PRELIVE"
    assert decision["reason"] == "no_complete_restricted_observations"


def write_history(tmp_path: Path, families: list[dict[str, object]]) -> Path:
    path = tmp_path / "history.json"
    path.write_text(
        json.dumps(
            {
                "report_version": "restricted_blocklist_observation_history_v1",
                "variant_family_summary": families,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def family(
    name: str,
    *,
    risk_rate: float,
    stable: str,
    score: float = 1.0,
) -> dict[str, object]:
    return {
        "variant_family": name,
        "complete_observations": 1,
        "score_avg": score,
        "risk_migration_detected_rate": risk_rate,
        "unexpected_blocked_segments_rate": 0.0,
        "realized_edge_delta_avg": 0.03,
        "fill_rate_delta_avg": 0.01,
        "drawdown_delta_avg": -0.2,
        "adverse_selection_delta_avg": -0.02,
        "stable_recommendation": stable,
    }
