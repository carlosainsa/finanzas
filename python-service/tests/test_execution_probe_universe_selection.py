from pathlib import Path
from typing import Any, cast

import duckdb

from src.research.execution_probe_universe_selection import (
    FILLABILITY_FALLBACK_REASON,
    MARKET_METADATA_FALLBACK_REASON,
    MARKET_OPPORTUNITY_FALLBACK_REASON,
    REPORT_VERSION,
    ExecutionProbeUniverseConfig,
    create_execution_probe_universe_selection,
)


def test_execution_probe_universe_selection_exports_ready_contract(
    tmp_path: Path,
) -> None:
    db_path = seed_universe_db(tmp_path, asset_count=6)

    report = create_execution_probe_universe_selection(
        db_path,
        tmp_path / "universe",
        ExecutionProbeUniverseConfig(limit=5, min_assets=5),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert report["decision_policy"] == "offline_multi_market_observation_universe_only"
    assert report["profile"] == "execution_probe_v5"
    assert report["status"] == "ready"
    assert report["market_asset_ids_count"] == 5
    assert len(str(report["market_asset_ids_sha256"])) == 64
    assert (tmp_path / "universe" / "execution_probe_universe_selection.parquet").exists()
    selected = cast(list[dict[str, Any]], report["selected"])
    assert selected[0]["asset_id"] == "asset-5"


def test_execution_probe_universe_selection_marks_insufficient_assets(
    tmp_path: Path,
) -> None:
    db_path = seed_universe_db(tmp_path, asset_count=3)

    report = create_execution_probe_universe_selection(
        db_path,
        tmp_path / "universe",
        ExecutionProbeUniverseConfig(limit=5, min_assets=5),
    )

    assert report["status"] == "insufficient_assets"
    assert report["market_asset_ids_count"] == 3
    assert "repeat_collection" in str(report["selection_reason"])


def test_execution_probe_universe_selection_supports_v6_profile(
    tmp_path: Path,
) -> None:
    db_path = seed_universe_db(tmp_path, asset_count=6)

    report = create_execution_probe_universe_selection(
        db_path,
        tmp_path / "universe",
        ExecutionProbeUniverseConfig(profile="execution_probe_v6", limit=5, min_assets=5),
    )

    assert report["profile"] == "execution_probe_v6"
    assert report["can_execute_trades"] is False
    assert report["status"] == "ready"


def test_execution_probe_universe_selection_supports_v7_profile(
    tmp_path: Path,
) -> None:
    db_path = seed_universe_db(tmp_path, asset_count=6)

    report = create_execution_probe_universe_selection(
        db_path,
        tmp_path / "universe",
        ExecutionProbeUniverseConfig(profile="execution_probe_v7", limit=5, min_assets=5),
    )

    assert report["profile"] == "execution_probe_v7"
    assert report["can_execute_trades"] is False
    assert report["status"] == "ready"


def test_execution_probe_universe_selection_supports_v8_profile(
    tmp_path: Path,
) -> None:
    db_path = seed_universe_db(tmp_path, asset_count=6)

    report = create_execution_probe_universe_selection(
        db_path,
        tmp_path / "universe",
        ExecutionProbeUniverseConfig(profile="execution_probe_v8", limit=5, min_assets=5),
    )

    assert report["profile"] == "execution_probe_v8"
    assert report["can_execute_trades"] is False
    assert report["status"] == "ready"


def test_execution_probe_universe_selection_supports_v10_executable_segments(
    tmp_path: Path,
) -> None:
    db_path = seed_executable_segment_universe_db(tmp_path)

    report = create_execution_probe_universe_selection(
        db_path,
        tmp_path / "universe",
        ExecutionProbeUniverseConfig(
            profile="execution_probe_v10",
            selection_source="executable_segments",
            limit=2,
            min_assets=1,
        ),
    )

    assert report["profile"] == "execution_probe_v10"
    assert report["source_report_version"] == "segment_opportunity_ranking_v1"
    assert report["status"] == "ready"
    assert report["market_asset_ids"] == ["asset-executable"]
    segment_filter = cast(dict[str, Any], report["segment_opportunity_filter"])
    assert segment_filter["enabled"] is True
    assert segment_filter["source_relation"] == "allowed_segment_candidates"
    assert segment_filter["selected_segments"] == 1
    assert segment_filter["allowed_segments"] == 1
    assert segment_filter["runtime_activity_backfill"] is True
    assert (
        tmp_path
        / "universe"
        / "segment_opportunity_ranking"
        / "allowed_segments.json"
    ).exists()


def test_execution_probe_universe_selection_backfills_runtime_active_segments(
    tmp_path: Path,
) -> None:
    db_path = seed_runtime_activity_segment_universe_db(tmp_path)

    report = create_execution_probe_universe_selection(
        db_path,
        tmp_path / "universe",
        ExecutionProbeUniverseConfig(
            profile="execution_probe_v10",
            selection_source="executable_segments",
            min_runtime_active_minutes=2,
            runtime_activity_backfill=True,
            runtime_backfill_min_opportunities=3,
            runtime_backfill_min_active_minutes=2,
            limit=3,
            min_assets=1,
        ),
    )

    assert report["status"] == "ready"
    assert report["market_asset_ids"] == ["asset-active-diagnostic"]
    assert report["market_asset_ids_count"] == 1
    segment_filter = cast(dict[str, Any], report["segment_opportunity_filter"])
    assert segment_filter["selected_segments"] == 0
    assert segment_filter["allowed_segments"] == 2
    selected = cast(list[dict[str, Any]], report["selected"])
    assert len(selected) == 2
    assert selected[0]["allowed_reason"] == "RUNTIME_ACTIVITY_BACKFILL"
    assert selected[0]["runtime_active_minutes"] == 3


def test_execution_probe_universe_selection_filters_by_future_touch_timing(
    tmp_path: Path,
) -> None:
    db_path = seed_universe_db(tmp_path, asset_count=6)
    seed_quote_execution_by_asset(db_path)

    report = create_execution_probe_universe_selection(
        db_path,
        tmp_path / "universe",
        ExecutionProbeUniverseConfig(
            profile="execution_probe_v7",
            limit=3,
            min_assets=2,
            market_timing_filter="future_touch",
            min_future_touch_rate=0.20,
            min_timing_signals=5,
        ),
    )

    assert report["status"] == "ready"
    assert report["market_asset_ids"] == ["asset-5", "asset-4", "asset-3"]
    assert "market_timing_filter=future_touch" in str(report["selection_reason"])
    selected = cast(list[dict[str, Any]], report["selected"])
    assert selected[0]["future_touch_rate"] == 0.5


def test_execution_probe_universe_selection_filters_candidate_adverse_market_side(
    tmp_path: Path,
) -> None:
    db_path = seed_universe_db(tmp_path, asset_count=6)
    seed_adverse_selection_by_strategy(
        db_path,
        [("market-5", "BUY", 12, 12, -0.01)],
    )

    report = create_execution_probe_universe_selection(
        db_path,
        tmp_path / "universe",
        ExecutionProbeUniverseConfig(
            profile="execution_probe_v8",
            limit=5,
            min_assets=5,
            adverse_selection_filter="market_side",
            max_adverse_30s_rate=0.50,
            min_adverse_filled_events=10,
        ),
    )

    assert report["status"] == "ready"
    assert "asset-5" not in cast(list[str], report["market_asset_ids"])
    assert "adverse_selection_filter=market_side" in str(report["selection_reason"])
    adverse = cast(dict[str, Any], report["adverse_selection_filter"])
    assert adverse["enabled"] is True
    assert adverse["filtered_count"] == 1
    assert (
        tmp_path
        / "universe"
        / "execution_probe_universe_adverse_exclusions.parquet"
    ).exists()


def test_execution_probe_universe_selection_filters_fillability_and_fallbacks(
    tmp_path: Path,
) -> None:
    db_path = seed_fillability_universe_db(tmp_path)
    seed_adverse_selection_by_strategy(
        db_path,
        [
            ("market-touch", "BUY", 20, 20, -0.01),
            ("market-fallback-0", "BUY", 20, 20, -0.01),
        ],
    )

    report = create_execution_probe_universe_selection(
        db_path,
        tmp_path / "universe",
        ExecutionProbeUniverseConfig(
            profile="execution_probe_v8",
            limit=3,
            min_assets=3,
            selection_source="fillability",
            min_future_touch_rate=0.05,
            min_timing_signals=1,
            min_avg_opportunity_spread=0.005,
            adverse_selection_filter="market_side",
        ),
    )

    asset_ids = cast(list[str], report["market_asset_ids"])
    assert "asset-touch" not in asset_ids
    assert "asset-fallback-0" not in asset_ids
    assert report["status"] == "insufficient_assets"
    adverse = cast(dict[str, Any], report["adverse_selection_filter"])
    assert adverse["filtered_count"] == 2
    selected = cast(list[dict[str, Any]], report["selected"])
    assert all(row["market_id"] not in {"market-touch", "market-fallback-0"} for row in selected)
    assert all(row["fallback_reason"] != MARKET_METADATA_FALLBACK_REASON for row in selected)


def test_execution_probe_universe_selection_adverse_filter_missing_evidence_is_safe(
    tmp_path: Path,
) -> None:
    db_path = seed_universe_db(tmp_path, asset_count=6)

    report = create_execution_probe_universe_selection(
        db_path,
        tmp_path / "universe",
        ExecutionProbeUniverseConfig(
            profile="execution_probe_v8",
            limit=5,
            min_assets=5,
            adverse_selection_filter="market_side",
        ),
    )

    assert report["status"] == "ready"
    adverse = cast(dict[str, Any], report["adverse_selection_filter"])
    assert adverse["enabled"] is True
    assert adverse["filtered_count"] == 0


def test_execution_probe_universe_selection_supports_fillability_source(
    tmp_path: Path,
) -> None:
    db_path = seed_fillability_universe_db(tmp_path)

    report = create_execution_probe_universe_selection(
        db_path,
        tmp_path / "universe",
        ExecutionProbeUniverseConfig(
            profile="execution_probe_v7",
            limit=1,
            min_assets=1,
            selection_source="fillability",
            min_future_touch_rate=0.05,
            min_timing_signals=1,
            min_avg_opportunity_spread=0.005,
        ),
    )

    assert report["status"] == "ready"
    assert report["source_report_version"] == "fillability_baseline_v1"
    assert report["market_asset_ids"] == ["asset-touch"]
    assert "selection_source=fillability" in str(report["selection_reason"])
    selected = cast(list[dict[str, Any]], report["selected"])
    assert selected[0]["recommendation"] == "PROMOTE_TO_OBSERVATION"
    assert selected[0]["selection_tier"] == "primary"
    assert selected[0]["fallback_reason"] is None
    fallback = cast(dict[str, Any], report["fallback"])
    assert fallback["assets_added"] == 0


def test_execution_probe_universe_selection_scores_toxicity_segments(
    tmp_path: Path,
) -> None:
    db_path = seed_fillability_universe_db(tmp_path)
    seed_fillability_toxic_fill(db_path)

    report = create_execution_probe_universe_selection(
        db_path,
        tmp_path / "universe",
        ExecutionProbeUniverseConfig(
            profile="execution_probe_v9",
            limit=1,
            min_assets=1,
            selection_source="fillability",
            min_future_touch_rate=0.05,
            min_timing_signals=1,
            min_avg_opportunity_spread=0.005,
            toxicity_filter="segment",
            min_toxicity_filled_events=1,
        ),
    )

    toxicity = cast(dict[str, Any], report["toxicity_filter"])
    assert toxicity["enabled"] is True
    assert toxicity["filtered_count"] == 1
    assert Path(str(toxicity["blocked_segments_path"])).exists()
    selected = cast(list[dict[str, Any]], report["selected"])
    assert selected[0]["asset_id"] == "asset-touch"
    assert selected[0]["rejected_toxicity_segments"] == 1
    assert selected[0]["quality_penalty"] > 0
    assert selected[0]["execution_quality_score"] < selected[0]["fillability_score"]


def test_execution_probe_universe_selection_backfills_fillability_min_assets(
    tmp_path: Path,
) -> None:
    db_path = seed_fillability_universe_db(tmp_path)

    report = create_execution_probe_universe_selection(
        db_path,
        tmp_path / "universe",
        ExecutionProbeUniverseConfig(
            profile="execution_probe_v7",
            limit=3,
            min_assets=3,
            selection_source="fillability",
            min_future_touch_rate=0.05,
            min_timing_signals=1,
            min_avg_opportunity_spread=0.005,
        ),
    )

    assert report["status"] == "ready"
    assert report["market_asset_ids_count"] == 3
    asset_ids = cast(list[str], report["market_asset_ids"])
    assert asset_ids[0] == "asset-touch"
    assert len(set(asset_ids)) == 3
    assert "fallback_fillability_backfill" in str(report["selection_reason"])
    fallback = cast(dict[str, Any], report["fallback"])
    assert fallback["used"] is True
    assert fallback["assets_added"] == 2
    selected = cast(list[dict[str, Any]], report["selected"])
    assert selected[0]["fallback_reason"] is None
    assert selected[1]["fallback_reason"] == FILLABILITY_FALLBACK_REASON
    assert selected[2]["fallback_reason"] == FILLABILITY_FALLBACK_REASON


def test_execution_probe_universe_selection_expands_strict_fillability_to_ready(
    tmp_path: Path,
) -> None:
    db_path = seed_fillability_universe_db(tmp_path)

    report = create_execution_probe_universe_selection(
        db_path,
        tmp_path / "universe",
        ExecutionProbeUniverseConfig(
            profile="execution_probe_v7",
            limit=4,
            min_assets=4,
            selection_source="fillability",
            min_future_touch_rate=0.05,
            min_timing_signals=1,
            min_avg_opportunity_spread=0.02,
            max_avg_opportunity_spread=0.08,
        ),
    )

    assert report["status"] == "ready"
    assert report["market_asset_ids_count"] == 4
    fallback = cast(dict[str, Any], report["fallback"])
    assert fallback["assets_added"] == 3


def test_execution_probe_universe_selection_expands_fillability_with_market_metadata(
    tmp_path: Path,
) -> None:
    db_path = seed_fillability_universe_db(tmp_path)

    report = create_execution_probe_universe_selection(
        db_path,
        tmp_path / "universe",
        ExecutionProbeUniverseConfig(
            profile="execution_probe_v7",
            limit=5,
            min_assets=5,
            selection_source="fillability",
            min_future_touch_rate=0.05,
            min_timing_signals=1,
            min_avg_opportunity_spread=0.005,
        ),
    )

    assert report["status"] == "ready"
    assert report["market_asset_ids_count"] == 5
    fallback = cast(dict[str, Any], report["fallback"])
    reasons = set(cast(list[str], fallback["fallback_reasons"]))
    assert FILLABILITY_FALLBACK_REASON in reasons
    assert MARKET_METADATA_FALLBACK_REASON in reasons
    selected = cast(list[dict[str, Any]], report["selected"])
    assert any(
        row["fallback_reason"] == MARKET_OPPORTUNITY_FALLBACK_REASON
        or row["fallback_reason"] == MARKET_METADATA_FALLBACK_REASON
        for row in selected
    )


def test_execution_probe_universe_selection_requires_timing_evidence_for_filter(
    tmp_path: Path,
) -> None:
    db_path = seed_universe_db(tmp_path, asset_count=6)

    try:
        create_execution_probe_universe_selection(
            db_path,
            tmp_path / "universe",
            ExecutionProbeUniverseConfig(
                market_timing_filter="future_touch",
            ),
        )
    except ValueError as exc:
        assert (
            "market_timing_filter=future_touch requires quote_execution_by_market_asset"
            in str(exc)
        )
    else:
        raise AssertionError("expected ValueError")


def test_execution_probe_universe_selection_rejects_invalid_profile() -> None:
    try:
        ExecutionProbeUniverseConfig(profile="live")
    except ValueError as exc:
        assert (
            "profile must be execution_probe_v5, execution_probe_v6, execution_probe_v7, execution_probe_v8, execution_probe_v9, or execution_probe_v10"
            in str(exc)
        )
    else:
        raise AssertionError("expected ValueError")


def test_execution_probe_universe_selection_rejects_invalid_selection_source() -> None:
    try:
        ExecutionProbeUniverseConfig(selection_source="manual")
    except ValueError as exc:
        assert (
            "selection_source must be candidate_market_ranking, fillability, or executable_segments"
            in str(exc)
        )
    else:
        raise AssertionError("expected ValueError")


def test_execution_probe_universe_selection_rejects_invalid_adverse_filter() -> None:
    try:
        ExecutionProbeUniverseConfig(adverse_selection_filter="freeform")
    except ValueError as exc:
        assert "adverse_selection_filter must be none or market_side" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def seed_universe_db(tmp_path: Path, asset_count: int) -> Path:
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
        snapshots = []
        metadata = []
        for asset_index in range(asset_count):
            asset_id = f"asset-{asset_index}"
            market_id = f"market-{asset_index}"
            for offset in range(10):
                snapshots.append(
                    (
                        market_id,
                        asset_id,
                        1_000 + offset * 1_000,
                        0.40,
                        0.45,
                        0.05,
                        10.0 + asset_index,
                        10.0 + asset_index,
                    )
                )
            metadata.append(
                (
                    market_id,
                    asset_id,
                    "YES",
                    f"Question {asset_index}",
                    f"question-{asset_index}",
                    True,
                    False,
                    False,
                    True,
                    1_000.0 + asset_index,
                    2_000.0 + asset_index,
                    1,
                )
            )
        conn.executemany(
            "insert into orderbook_snapshots values (?, ?, ?, ?, ?, ?, ?, ?)",
            snapshots,
        )
        conn.executemany(
            "insert into market_metadata values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            metadata,
        )
    return db_path


def seed_adverse_selection_by_strategy(
    db_path: Path,
    rows: list[tuple[str, str, int, int, float]],
) -> None:
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            create table adverse_selection_by_strategy (
                strategy varchar,
                market_id varchar,
                side varchar,
                filled_events bigint,
                avg_pnl_5s double,
                avg_pnl_30s double,
                avg_pnl_300s double,
                adverse_30s_count double,
                adverse_30s_rate double
            )
            """
        )
        conn.executemany(
            "insert into adverse_selection_by_strategy values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "probe",
                    market_id,
                    side,
                    filled_events,
                    avg_pnl_30s,
                    avg_pnl_30s,
                    avg_pnl_30s,
                    adverse_30s_count,
                    adverse_30s_count / filled_events,
                )
                for market_id, side, filled_events, adverse_30s_count, avg_pnl_30s in rows
            ],
        )


def seed_fillability_universe_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "research.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            create table signals (
                signal_id varchar,
                market_id varchar,
                asset_id varchar,
                side varchar,
                price double,
                size double,
                confidence double,
                strategy varchar,
                model_version varchar,
                data_version varchar,
                feature_version varchar,
                event_timestamp_ms bigint
            )
            """
        )
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
            create table execution_reports (
                signal_id varchar,
                order_id varchar,
                status varchar,
                filled_price double,
                filled_size double,
                cumulative_filled_size double,
                remaining_size double,
                error varchar,
                event_timestamp_ms bigint
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
        conn.executemany(
            "insert into signals values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "signal-touch",
                    "market-touch",
                    "asset-touch",
                    "BUY",
                    0.50,
                    1.0,
                    0.65,
                    "probe",
                    "model",
                    "data",
                    "feature",
                    1_000,
                ),
                (
                    "signal-stale",
                    "market-stale",
                    "asset-stale",
                    "BUY",
                    0.35,
                    1.0,
                    0.65,
                    "probe",
                    "model",
                    "data",
                    "feature",
                    1_000,
                ),
                (
                    "signal-fallback-0",
                    "market-fallback-0",
                    "asset-fallback-0",
                    "BUY",
                    0.34,
                    1.0,
                    0.65,
                    "probe",
                    "model",
                    "data",
                    "feature",
                    1_000,
                ),
                (
                    "signal-fallback-1",
                    "market-fallback-1",
                    "asset-fallback-1",
                    "BUY",
                    0.30,
                    1.0,
                    0.65,
                    "probe",
                    "model",
                    "data",
                    "feature",
                    1_000,
                ),
            ],
        )
        conn.executemany(
            "insert into orderbook_snapshots values (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("market-touch", "asset-touch", 900, 0.45, 0.50, 0.05, 10.0, 10.0),
                ("market-touch", "asset-touch", 1_100, 0.45, 0.50, 0.05, 10.0, 10.0),
                ("market-stale", "asset-stale", 900, 0.30, 0.40, 0.10, 10.0, 10.0),
                ("market-stale", "asset-stale", 1_100, 0.30, 0.40, 0.10, 10.0, 10.0),
                (
                    "market-fallback-0",
                    "asset-fallback-0",
                    900,
                    0.30,
                    0.36,
                    0.06,
                    100.0,
                    100.0,
                ),
                (
                    "market-fallback-0",
                    "asset-fallback-0",
                    1_100,
                    0.30,
                    0.36,
                    0.06,
                    100.0,
                    100.0,
                ),
                (
                    "market-fallback-1",
                    "asset-fallback-1",
                    900,
                    0.25,
                    0.35,
                    0.10,
                    50.0,
                    50.0,
                ),
                (
                    "market-fallback-1",
                    "asset-fallback-1",
                    1_100,
                    0.25,
                    0.35,
                    0.10,
                    50.0,
                    50.0,
                ),
            ],
        )
        conn.executemany(
            "insert into market_metadata values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "market-touch",
                    "asset-touch",
                    "Yes",
                    "Question touch",
                    "question-touch",
                    True,
                    False,
                    False,
                    True,
                    1_000.0,
                    2_000.0,
                    1,
                ),
                (
                    "market-stale",
                    "asset-stale",
                    "Yes",
                    "Question stale",
                    "question-stale",
                    True,
                    False,
                    False,
                    True,
                    1_000.0,
                    2_000.0,
                    1,
                ),
                (
                    "market-fallback-0",
                    "asset-fallback-0",
                    "Yes",
                    "Question fallback 0",
                    "question-fallback-0",
                    True,
                    False,
                    False,
                    True,
                    5_000.0,
                    6_000.0,
                    1,
                ),
                (
                    "market-fallback-1",
                    "asset-fallback-1",
                    "Yes",
                    "Question fallback 1",
                    "question-fallback-1",
                    True,
                    False,
                    False,
                    True,
                    3_000.0,
                    4_000.0,
                    1,
                ),
                (
                    "market-metadata-0",
                    "asset-metadata-0",
                    "Yes",
                    "Question metadata 0",
                    "question-metadata-0",
                    True,
                    False,
                    False,
                    True,
                    10_000.0,
                    12_000.0,
                    1,
                ),
                (
                    "market-metadata-1",
                    "asset-metadata-1",
                    "Yes",
                    "Question metadata 1",
                    "question-metadata-1",
                    True,
                    False,
                    False,
                    True,
                    9_000.0,
                    11_000.0,
                    1,
                ),
            ],
        )
    return db_path


def seed_quote_execution_by_asset(db_path: Path) -> None:
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            create table quote_execution_by_market_asset (
                market_id varchar,
                asset_id varchar,
                signals bigint,
                dry_run_filled_signals bigint,
                synthetic_filled_signals bigint
            )
            """
        )
        rows = []
        for asset_index in range(6):
            synthetic_fills = asset_index
            rows.append(
                (
                    f"market-{asset_index}",
                    f"asset-{asset_index}",
                    10,
                    0,
                    synthetic_fills,
                )
            )
        conn.executemany(
            "insert into quote_execution_by_market_asset values (?, ?, ?, ?, ?)",
            rows,
        )


def seed_executable_segment_universe_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "research.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            create table executable_opportunities (
                signal_id varchar,
                market_id varchar,
                asset_id varchar,
                side varchar,
                strategy varchar,
                model_version varchar,
                feature_version varchar,
                signal_timestamp_ms bigint,
                spread_bucket varchar,
                timing_bucket varchar,
                expected_edge double,
                available_depth double,
                observed_filled boolean,
                synthetic_filled boolean,
                observed_fill_rate double,
                synthetic_fill_rate double,
                pnl_30s double,
                adverse_30s double,
                executable_score double,
                is_executable boolean
            )
            """
        )
        conn.executemany(
            "insert into executable_opportunities values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "signal-1",
                    "market-executable",
                    "asset-executable",
                    "BUY",
                    "near_touch",
                    "research-model",
                    "features",
                    1_000,
                    "250_500bps",
                    "stable",
                    0.04,
                    50.0,
                    True,
                    True,
                    1.0,
                    1.0,
                    0.01,
                    0.0,
                    2.0,
                    True,
                ),
                (
                    "signal-2",
                    "market-executable",
                    "asset-executable",
                    "BUY",
                    "near_touch",
                    "research-model",
                    "features",
                    2_000,
                    "250_500bps",
                    "stable",
                    0.04,
                    50.0,
                    True,
                    True,
                    1.0,
                    1.0,
                    0.01,
                    0.0,
                    2.0,
                    True,
                ),
                (
                    "signal-3",
                    "market-executable",
                    "asset-executable",
                    "BUY",
                    "near_touch",
                    "research-model",
                    "features",
                    3_000,
                    "250_500bps",
                    "stable",
                    0.04,
                    50.0,
                    True,
                    True,
                    1.0,
                    1.0,
                    0.01,
                    0.0,
                    2.0,
                    True,
                ),
            ],
        )
    return db_path


def seed_runtime_activity_segment_universe_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "research.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            create table executable_opportunities (
                signal_id varchar,
                market_id varchar,
                asset_id varchar,
                side varchar,
                strategy varchar,
                model_version varchar,
                feature_version varchar,
                signal_timestamp_ms bigint,
                spread_bucket varchar,
                timing_bucket varchar,
                expected_edge double,
                available_depth double,
                observed_filled boolean,
                synthetic_filled boolean,
                observed_fill_rate double,
                synthetic_fill_rate double,
                pnl_30s double,
                adverse_30s double,
                executable_score double,
                is_executable boolean
            )
            """
        )
        conn.executemany(
            "insert into executable_opportunities values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "strict-1",
                    "market-strict",
                    "asset-strict-sparse",
                    "BUY",
                    "near_touch",
                    "research-model",
                    "features",
                    1_000,
                    "250_500bps",
                    "stable",
                    0.04,
                    50.0,
                    True,
                    True,
                    1.0,
                    1.0,
                    0.01,
                    0.0,
                    2.0,
                    True,
                ),
                (
                    "strict-2",
                    "market-strict",
                    "asset-strict-sparse",
                    "BUY",
                    "near_touch",
                    "research-model",
                    "features",
                    2_000,
                    "250_500bps",
                    "stable",
                    0.04,
                    50.0,
                    True,
                    True,
                    1.0,
                    1.0,
                    0.01,
                    0.0,
                    2.0,
                    True,
                ),
                (
                    "active-1",
                    "market-active",
                    "asset-active-diagnostic",
                    "BUY",
                    "near_touch",
                    "research-model",
                    "features",
                    1_000,
                    "250_500bps",
                    "stable",
                    0.005,
                    50.0,
                    True,
                    True,
                    1.0,
                    1.0,
                    0.01,
                    0.0,
                    2.0,
                    True,
                ),
                (
                    "active-2",
                    "market-active",
                    "asset-active-diagnostic",
                    "BUY",
                    "near_touch",
                    "research-model",
                    "features",
                    61_000,
                    "250_500bps",
                    "stable",
                    0.006,
                    50.0,
                    True,
                    True,
                    1.0,
                    1.0,
                    0.01,
                    0.0,
                    2.0,
                    True,
                ),
                (
                    "active-3",
                    "market-active",
                    "asset-active-diagnostic",
                    "BUY",
                    "near_touch",
                    "research-model",
                    "features",
                    121_000,
                    "250_500bps",
                    "stable",
                    0.007,
                    50.0,
                    True,
                    True,
                    1.0,
                    1.0,
                    0.01,
                    0.0,
                    2.0,
                    True,
                ),
                (
                    "active-4",
                    "market-active",
                    "asset-active-diagnostic",
                    "BUY",
                    "near_touch",
                    "research-model",
                    "features",
                    181_000,
                    "250_500bps",
                    "rotating",
                    0.007,
                    50.0,
                    True,
                    True,
                    1.0,
                    1.0,
                    0.01,
                    0.0,
                    2.0,
                    True,
                ),
                (
                    "active-5",
                    "market-active",
                    "asset-active-diagnostic",
                    "BUY",
                    "near_touch",
                    "research-model",
                    "features",
                    241_000,
                    "250_500bps",
                    "rotating",
                    0.006,
                    50.0,
                    True,
                    True,
                    1.0,
                    1.0,
                    0.01,
                    0.0,
                    2.0,
                    True,
                ),
                (
                    "active-6",
                    "market-active",
                    "asset-active-diagnostic",
                    "BUY",
                    "near_touch",
                    "research-model",
                    "features",
                    301_000,
                    "250_500bps",
                    "rotating",
                    0.005,
                    50.0,
                    True,
                    True,
                    1.0,
                    1.0,
                    0.01,
                    0.0,
                    2.0,
                    True,
                ),
            ],
        )
    return db_path


def seed_fillability_toxic_fill(db_path: Path) -> None:
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            insert into execution_reports values
            ('signal-touch', 'order-touch', 'MATCHED', 0.50, 1.0, 1.0, 0.0, null, 1100)
            """
        )
        conn.execute(
            """
            insert into orderbook_snapshots values
            ('market-touch', 'asset-touch', 32000, 0.43, 0.45, 0.02, 10.0, 10.0)
            """
        )
