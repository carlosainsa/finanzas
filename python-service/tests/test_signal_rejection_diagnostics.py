from pathlib import Path
from typing import Any, cast

import duckdb

from src.research.signal_rejection_diagnostics import (
    REPORT_VERSION,
    DiagnosticsConfig,
    create_signal_rejection_diagnostics,
)


def test_signal_rejection_diagnostics_compares_profiles_on_same_snapshots(
    tmp_path: Path,
) -> None:
    db_path = seed_db(tmp_path)
    output_dir = tmp_path / "diagnostics"

    report = create_signal_rejection_diagnostics(
        db_path,
        output_dir,
        DiagnosticsConfig(
            profiles=("conservative_v1", "balanced_v1"),
            quote_placement="near_touch",
            baseline_profile="conservative_v1",
            candidate_profile="balanced_v1",
        ),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert cast(dict[str, Any], report["counts"])["snapshots"] == 4
    assert (output_dir / "signal_rejection_diagnostics.parquet").exists()
    comparison = cast(dict[str, Any], report["profile_comparison"])
    assert comparison["status"] == "compared"
    assert comparison["baseline_profile"] == "conservative_v1"
    assert comparison["candidate_profile"] == "balanced_v1"
    assert comparison["accepted_delta"] == 1
    assert comparison["candidate_less_active"] is False
    summaries = {
        str(row["profile"]): row
        for row in cast(list[dict[str, Any]], report["summary"])
    }
    assert summaries["conservative_v1"]["accepted"] == 1
    assert summaries["balanced_v1"]["accepted"] == 2
    assert cast(dict[str, int], summaries["conservative_v1"]["rejection_counts"])[
        "low_depth"
    ] == 1
    assert (
        cast(dict[str, int], summaries["balanced_v1"]["rejection_counts"]).get(
            "low_depth", 0
        )
        == 0
    )


def test_signal_rejection_diagnostics_handles_missing_orderbooks(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "empty.duckdb"
    with duckdb.connect(str(db_path)):
        pass

    report = create_signal_rejection_diagnostics(db_path, tmp_path / "diagnostics")

    assert cast(dict[str, Any], report["counts"])["snapshots"] == 0
    assert report["profile_comparison"] == {
        "status": "missing_profile",
        "baseline_profile": "conservative_v1",
        "candidate_profile": "balanced_v1",
    }


def seed_db(tmp_path: Path) -> Path:
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
                bid_depth double,
                ask_depth double
            )
            """
        )
        conn.executemany(
            """
            insert into orderbook_snapshots values (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("m1", "a1", 1_000, 0.45, 0.49, 3.0, 3.0),
                ("m1", "a1", 2_000, 0.45, 0.49, 1.6, 1.6),
                ("m1", "a1", 3_000, 0.45, 0.46, 3.0, 3.0),
                ("m1", "a1", 4_000, None, 0.49, 3.0, 3.0),
            ],
        )
    return db_path
