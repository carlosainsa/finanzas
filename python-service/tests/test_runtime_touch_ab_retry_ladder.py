import json
from pathlib import Path
from typing import Any, cast

import duckdb

from src.research.runtime_touch_ab_retry_ladder import (
    REPORT_VERSION,
    RuntimeTouchAbRetryLadderConfig,
    create_runtime_touch_ab_retry_ladder,
    plan_payload,
)


def test_retry_ladder_selects_first_relaxed_signalable_attempt(
    tmp_path: Path,
) -> None:
    db_path = seed_retry_ladder_db(tmp_path, include_relaxed_asset=True)
    fresh_report_root = tmp_path / "reports" / "fresh"
    fresh_report_root.mkdir(parents=True)

    report = create_runtime_touch_ab_retry_ladder(
        db_path,
        tmp_path / "ladder",
        fresh_report_root,
        RuntimeTouchAbRetryLadderConfig(
            observation_seconds=1800,
            min_runtime_touch_snapshots=3,
        ),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert report["decision_policy"] == "offline_runtime_touch_ab_retry_ladder_only"
    assert report["recommendation"] == "RUN_RUNTIME_TOUCH_AB_WITH_SELECTED_ATTEMPT"
    selected = cast(dict[str, Any], report["selected_attempt"])
    assert selected["label"] == "lower_signalable_snapshots"
    assert selected["status"] == "ready"
    assert selected["market_asset_ids_count"] == 2
    attempts = cast(list[dict[str, Any]], report["attempts"])
    assert [attempt["label"] for attempt in attempts] == [
        "strict_signalable",
        "wider_universe",
        "lower_signalable_density",
        "lower_signalable_snapshots",
    ]
    assert attempts[0]["status"] == "insufficient_assets"
    next_command = str(report["next_command"])
    assert "scripts/run_runtime_touch_ab_cycle.sh" in next_command
    assert "EXECUTION_PROBE_MIN_RUNTIME_SIGNALABLE_SNAPSHOTS=1" in next_command
    assert "EXECUTION_PROBE_RUNTIME_SIGNAL_MIN_SPREAD=0.03" in next_command
    assert (tmp_path / "ladder" / "runtime_touch_ab_retry_ladder.json").exists()


def test_retry_ladder_keeps_live_blocked_when_no_attempt_is_ready(
    tmp_path: Path,
) -> None:
    db_path = seed_retry_ladder_db(tmp_path, include_relaxed_asset=False)

    report = create_runtime_touch_ab_retry_ladder(
        db_path,
        tmp_path / "ladder",
        None,
        RuntimeTouchAbRetryLadderConfig(
            observation_seconds=1800,
            min_runtime_touch_snapshots=3,
        ),
    )

    assert report["selected_attempt"] is None
    assert report["next_command"] is None
    assert (
        report["recommendation"]
        == "COLLECT_FRESH_RUNTIME_SAMPLE_OR_CHANGE_MARKET_TIMING"
    )
    attempts = cast(list[dict[str, Any]], report["attempts"])
    assert all(attempt["status"] == "insufficient_assets" for attempt in attempts)


def test_retry_ladder_plan_payload_is_research_only(tmp_path: Path) -> None:
    output_dir = tmp_path / "ladder"
    fresh_duckdb = tmp_path / "research.duckdb"

    plan = plan_payload(
        output_dir,
        fresh_duckdb,
        tmp_path / "reports" / "fresh",
        RuntimeTouchAbRetryLadderConfig(
            observation_seconds=1800,
            min_runtime_touch_snapshots=3,
        ),
    )

    assert plan["script"] == "scripts/run_runtime_touch_ab_retry_ladder.sh"
    assert plan["report_version"] == REPORT_VERSION
    assert plan["can_execute_trades"] is False
    assert "scripts/run_runtime_touch_ab_cycle.sh" in plan["delegates_to"]
    config = cast(dict[str, Any], plan["config"])
    attempts = cast(list[dict[str, Any]], config["attempts"])
    assert attempts[-1]["min_runtime_signalable_snapshots"] == 1
    assert config["runtime_signal_min_spread"] == 0.03
    assert plan["outputs"]["report"].endswith("runtime_touch_ab_retry_ladder.json")


def seed_retry_ladder_db(
    tmp_path: Path,
    *,
    include_relaxed_asset: bool,
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
            ("market-strong", "asset-strong", 1_000, 0.40, 0.45, 0.05, 10.0, 10.0),
            ("market-strong", "asset-strong", 61_000, 0.41, 0.45, 0.04, 10.0, 10.0),
            ("market-strong", "asset-strong", 121_000, 0.41, 0.44, 0.03, 10.0, 10.0),
        ]
        metadata = [
            (
                "market-strong",
                "asset-strong",
                "YES",
                "Strong runtime question",
                "strong-runtime-question",
                True,
                False,
                False,
                True,
                1000.0,
                2000.0,
                1,
            )
        ]
        if include_relaxed_asset:
            rows.extend(
                [
                    ("market-soft", "asset-soft", 1_000, 0.40, 0.42, 0.02, 10.0, 10.0),
                    ("market-soft", "asset-soft", 61_000, 0.41, 0.43, 0.02, 10.0, 10.0),
                    ("market-soft", "asset-soft", 121_000, 0.42, 0.45, 0.03, 10.0, 10.0),
                ]
            )
            metadata.append(
                (
                    "market-soft",
                    "asset-soft",
                    "YES",
                    "Soft runtime question",
                    "soft-runtime-question",
                    True,
                    False,
                    False,
                    True,
                    900.0,
                    1800.0,
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


def test_retry_ladder_json_serializes(tmp_path: Path) -> None:
    db_path = seed_retry_ladder_db(tmp_path, include_relaxed_asset=True)
    report = create_runtime_touch_ab_retry_ladder(
        db_path,
        tmp_path / "ladder",
        None,
        RuntimeTouchAbRetryLadderConfig(
            observation_seconds=1800,
            min_runtime_touch_snapshots=3,
        ),
    )

    json.dumps(report)
