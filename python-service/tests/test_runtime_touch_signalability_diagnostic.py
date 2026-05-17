from pathlib import Path
from typing import Any, cast

import duckdb

from src.research.runtime_touch_signalability_diagnostic import (
    REPORT_VERSION,
    RuntimeTouchSignalabilityDiagnosticConfig,
    create_runtime_touch_signalability_diagnostic,
)


def test_signalability_diagnostic_identifies_signalable_shortfall(
    tmp_path: Path,
) -> None:
    db_path = seed_signalability_db(tmp_path)

    report = create_runtime_touch_signalability_diagnostic(
        db_path,
        tmp_path / "diagnostic",
        RuntimeTouchSignalabilityDiagnosticConfig(
            min_snapshots=3,
            min_active_minutes=1,
            min_touch_change_rate=0.10,
            min_signalable_snapshots=3,
            min_signalable_density=0.50,
            min_assets=2,
        ),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert report["status"] == "insufficient_signalable_assets"
    assert report["signalable_assets_count"] == 1
    blocker_counts = cast(dict[str, int], report["blocker_counts"])
    assert blocker_counts["signalable_snapshots"] == 1
    assert blocker_counts["signalable_density"] == 1
    assert (
        tmp_path
        / "diagnostic"
        / "runtime_touch_signalability_assets.parquet"
    ).exists()


def test_signalability_diagnostic_reports_multiple_failed_filters(
    tmp_path: Path,
) -> None:
    db_path = seed_signalability_db(tmp_path, include_flat_asset=True)

    report = create_runtime_touch_signalability_diagnostic(
        db_path,
        tmp_path / "diagnostic",
        RuntimeTouchSignalabilityDiagnosticConfig(
            min_snapshots=3,
            min_active_minutes=1,
            min_touch_change_rate=0.10,
            min_signalable_snapshots=3,
            min_signalable_density=0.50,
            min_assets=2,
        ),
    )

    rows = cast(list[dict[str, Any]], report["top_candidates"])
    flat = next(row for row in rows if row["asset_id"] == "asset-flat")
    reasons = str(flat["blocker_reasons"]).split(",")
    assert "touch_change_rate" in reasons
    assert "signalable_snapshots" in reasons
    assert "signalable_density" in reasons


def seed_signalability_db(
    tmp_path: Path,
    *,
    include_flat_asset: bool = False,
) -> Path:
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
        conn.execute(
            """
            create table market_metadata (
                market_id varchar,
                asset_id varchar,
                outcome varchar,
                question varchar,
                slug varchar,
                active boolean,
                closed boolean,
                archived boolean,
                enable_order_book boolean,
                liquidity double,
                volume double,
                ingested_at_ms bigint
            )
            """
        )
        rows = [
            ("market-good", "asset-good", 1_000, 0.40, 0.45, 0.05, 10.0, 10.0),
            ("market-good", "asset-good", 61_000, 0.41, 0.45, 0.04, 10.0, 10.0),
            ("market-good", "asset-good", 121_000, 0.41, 0.44, 0.03, 10.0, 10.0),
            ("market-thin", "asset-thin", 1_000, 0.40, 0.42, 0.02, 10.0, 10.0),
            ("market-thin", "asset-thin", 61_000, 0.41, 0.43, 0.02, 10.0, 10.0),
            ("market-thin", "asset-thin", 121_000, 0.42, 0.45, 0.03, 10.0, 10.0),
        ]
        metadata = [
            (
                "market-good",
                "asset-good",
                "YES",
                "Good question",
                "good-question",
                True,
                False,
                False,
                True,
                1000.0,
                2000.0,
                1,
            ),
            (
                "market-thin",
                "asset-thin",
                "YES",
                "Thin question",
                "thin-question",
                True,
                False,
                False,
                True,
                1000.0,
                2000.0,
                1,
            ),
        ]
        if include_flat_asset:
            rows.extend(
                [
                    ("market-flat", "asset-flat", 1_000, 0.40, 0.42, 0.02, 10.0, 10.0),
                    ("market-flat", "asset-flat", 61_000, 0.40, 0.42, 0.02, 10.0, 10.0),
                    ("market-flat", "asset-flat", 121_000, 0.40, 0.42, 0.02, 10.0, 10.0),
                ]
            )
            metadata.append(
                (
                    "market-flat",
                    "asset-flat",
                    "YES",
                    "Flat question",
                    "flat-question",
                    True,
                    False,
                    False,
                    True,
                    1000.0,
                    2000.0,
                    1,
                )
            )
        conn.executemany(
            "insert into orderbook_snapshots values (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.executemany(
            "insert into market_metadata values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            metadata,
        )
    return db_path
