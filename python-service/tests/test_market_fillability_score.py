from pathlib import Path

import duckdb

from src.research.market_fillability_score import (
    REPORT_VERSION,
    MarketFillabilityScoreConfig,
    create_market_fillability_score_report,
)


def test_market_fillability_score_promotes_observed_and_touchable_asset(
    tmp_path: Path,
) -> None:
    db_path = seed_fillability_db(tmp_path)

    report = create_market_fillability_score_report(
        db_path,
        tmp_path / "fillability",
        MarketFillabilityScoreConfig(min_signals=1, limit=10),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert report["can_promote_live"] is False
    assert report["decision_policy"] == "offline_market_fillability_scoring_only"
    assert report["market_asset_ids"][0] == "asset-good"
    first = report["selected"][0]
    assert first["recommendation"] == "PROMOTE_TO_DISCOVERY_BATCH"
    assert first["observed_fill_rate"] > 0
    assert (
        tmp_path / "fillability" / "market_fillability_score_ranking.parquet"
    ).exists()


def test_market_fillability_score_config_rejects_invalid_limit() -> None:
    try:
        MarketFillabilityScoreConfig(limit=0)
    except ValueError as exc:
        assert str(exc) == "limit must be positive"
    else:
        raise AssertionError("expected invalid limit to fail")


def seed_fillability_db(tmp_path: Path) -> Path:
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
        conn.executemany(
            "insert into signals values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "signal-good",
                    "market-good",
                    "asset-good",
                    "BUY",
                    0.42,
                    1.0,
                    0.60,
                    "probe",
                    "model",
                    "data",
                    "feature",
                    10_000,
                ),
                (
                    "signal-bad",
                    "market-bad",
                    "asset-bad",
                    "BUY",
                    0.30,
                    1.0,
                    0.60,
                    "probe",
                    "model",
                    "data",
                    "feature",
                    10_000,
                ),
            ],
        )
        conn.executemany(
            "insert into execution_reports values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "signal-good",
                    "dry-run-signal-good",
                    "MATCHED",
                    0.42,
                    1.0,
                    1.0,
                    0.0,
                    None,
                    20_000,
                ),
                (
                    "signal-bad",
                    "dry-run-signal-bad",
                    "UNMATCHED",
                    None,
                    0.0,
                    0.0,
                    1.0,
                    None,
                    20_000,
                ),
            ],
        )
        rows = []
        for asset_id, market_id, ask in (
            ("asset-good", "market-good", 0.42),
            ("asset-bad", "market-bad", 0.50),
        ):
            for index in range(10):
                rows.append(
                    (
                        market_id,
                        asset_id,
                        9_000 + index * 1_000,
                        0.39,
                        ask,
                        ask - 0.39,
                        10.0,
                        10.0,
                    )
                )
        conn.executemany(
            "insert into orderbook_snapshots values (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.executemany(
            "insert into market_metadata values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
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
                    10_000.0,
                    20_000.0,
                    1,
                ),
                (
                    "market-bad",
                    "asset-bad",
                    "YES",
                    "Bad question",
                    "bad-question",
                    True,
                    False,
                    False,
                    True,
                    10_000.0,
                    20_000.0,
                    1,
                ),
            ],
        )
    return db_path
