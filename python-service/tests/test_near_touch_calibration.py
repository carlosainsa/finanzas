from pathlib import Path
from typing import Any, cast

import duckdb

from src.research.near_touch_calibration import (
    REPORT_VERSION,
    NearTouchCalibrationConfig,
    create_near_touch_calibration_report,
)


def test_near_touch_calibration_selects_lowest_viable_fraction(tmp_path: Path) -> None:
    db_path = seed_db(tmp_path)

    report = create_near_touch_calibration_report(
        db_path,
        tmp_path / "calibration",
        NearTouchCalibrationConfig(
            fractions=(0.60, 0.70, 0.80),
            min_signals=3,
            min_adjusted_synthetic_fill_rate=0.05,
            max_adjusted_synthetic_fill_rate=0.25,
            max_raw_synthetic_fill_rate=1.0,
        ),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    selected = cast(dict[str, Any], report["selected_fraction"])
    assert selected["fraction"] == 0.70
    assert selected["status"] == "candidate"
    ranking = cast(list[dict[str, Any]], report["ranking"])
    assert find_fraction(ranking, 0.60)["blockers"] == ["too_passive"]
    assert find_fraction(ranking, 0.70)["synthetic_filled_signals"] > 0
    assert (tmp_path / "calibration" / "near_touch_fraction_ranking.parquet").exists()
    assert (
        tmp_path / "calibration" / "execution_probe_v5_fraction_selection.json"
    ).exists()


def test_near_touch_calibration_rejects_invalid_config() -> None:
    try:
        NearTouchCalibrationConfig(fractions=(1.20,))
    except ValueError as exc:
        assert "fractions must be between 0 and 1" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_near_touch_calibration_selects_boundary_when_no_clean_candidate(
    tmp_path: Path,
) -> None:
    db_path = seed_db(tmp_path)

    report = create_near_touch_calibration_report(
        db_path,
        tmp_path / "calibration",
        NearTouchCalibrationConfig(
            fractions=(0.60, 0.70),
            min_signals=3,
            min_adjusted_synthetic_fill_rate=0.05,
            max_adjusted_synthetic_fill_rate=0.10,
            max_raw_synthetic_fill_rate=0.50,
        ),
    )

    selected = cast(dict[str, Any], report["selected_fraction"])
    assert selected["fraction"] == 0.70
    assert selected["status"] == "rejected"
    assert selected["selection_reason"] == (
        "lowest_fraction_with_future_touches_requires_observation_validation"
    )


def test_near_touch_calibration_aggregates_multiple_duckdb_inputs(
    tmp_path: Path,
) -> None:
    first_db = seed_db(tmp_path / "first", asset_id="asset-1")
    second_db = seed_db(tmp_path / "second", asset_id="asset-2")

    report = create_near_touch_calibration_report(
        (first_db, second_db),
        tmp_path / "calibration",
        NearTouchCalibrationConfig(
            fractions=(0.70,),
            min_signals=6,
            min_market_coverage=2,
            min_adjusted_synthetic_fill_rate=0.05,
            max_adjusted_synthetic_fill_rate=0.25,
            max_raw_synthetic_fill_rate=1.0,
        ),
    )

    counts = cast(dict[str, Any], report["counts"])
    assert counts["sources"] == 2
    selected = cast(dict[str, Any], report["selected_fraction"])
    assert selected["fraction"] == 0.70
    assert selected["covered_markets"] == 2
    assert selected["status"] == "candidate"


def test_near_touch_calibration_requires_market_coverage(tmp_path: Path) -> None:
    db_path = seed_db(tmp_path)

    report = create_near_touch_calibration_report(
        db_path,
        tmp_path / "calibration",
        NearTouchCalibrationConfig(
            fractions=(0.70,),
            min_signals=3,
            min_market_coverage=2,
            min_adjusted_synthetic_fill_rate=0.05,
            max_adjusted_synthetic_fill_rate=0.25,
            max_raw_synthetic_fill_rate=1.0,
        ),
    )

    ranking = cast(list[dict[str, Any]], report["ranking"])
    assert find_fraction(ranking, 0.70)["blockers"] == [
        "insufficient_market_coverage"
    ]
    assert report["selected_fraction"] is None


def find_fraction(rows: list[dict[str, Any]], fraction: float) -> dict[str, Any]:
    return next(row for row in rows if row["fraction"] == fraction)


def seed_db(tmp_path: Path, asset_id: str = "asset-1") -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "research.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            create table orderbook_snapshots (
                market_id varchar,
                asset_id varchar,
                event_timestamp_ms bigint,
                best_bid double,
                best_ask double,
                spread double,
                bid_depth double,
                ask_depth double
            )
            """
        )
        rows = []
        for offset in range(3):
            base_ts = 1_000 + offset * 10_000
            rows.extend(
                [
                    (
                        f"market-{asset_id}",
                        asset_id,
                        base_ts,
                        0.45,
                        0.50,
                        0.05,
                        5.0,
                        5.0,
                    ),
                    (
                        f"market-{asset_id}",
                        asset_id,
                        base_ts + 1_000,
                        0.45,
                        0.485,
                        0.035,
                        5.0,
                        5.0,
                    ),
                ]
            )
        conn.executemany(
            "insert into orderbook_snapshots values (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
    return db_path
