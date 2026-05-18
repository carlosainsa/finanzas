import json
from pathlib import Path

from src.research.market_family_memory import (
    REPORT_VERSION,
    MarketFamilyMemoryConfig,
    create_market_family_memory_report,
    market_family,
)


def test_market_family_memory_aggregates_batch_and_fillability_evidence(
    tmp_path: Path,
) -> None:
    discovery = write_discovery_batches(tmp_path)
    comparison = write_batch_comparison(tmp_path)
    diagnostics = write_batch_diagnostics(tmp_path)
    fillability = write_fillability_score(tmp_path)

    report = create_market_family_memory_report(
        tmp_path / "memory",
        discovery_batches_path=discovery,
        batch_comparison_path=comparison,
        batch_diagnostics_path=diagnostics,
        fillability_score_path=fillability,
        config=MarketFamilyMemoryConfig(min_observations=1),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert report["can_promote_live"] is False
    assert report["decision_policy"] == "offline_market_family_memory_only"
    politics = next(row for row in report["families"] if row["family_key"] == "tag:politics")
    assert politics["observations"] == 2
    assert politics["family_memory_decision"] == "EXPLOIT"
    assert politics["signalability_success_rate"] > 0
    assert politics["no_window_observations"] == 0
    assert (tmp_path / "memory" / "market_family_memory.parquet").exists()


def test_market_family_memory_consolidates_signalability_failures(
    tmp_path: Path,
) -> None:
    discovery = write_discovery_batches(tmp_path, duplicate_family_market=True)
    diagnostics = write_batch_diagnostics(
        tmp_path,
        readiness_status="DISCOVERY_EXPANSION_REQUIRED",
        primary_blocker="signalability_signalable_density",
        signalable_assets=1,
    )

    report = create_market_family_memory_report(
        tmp_path / "memory",
        discovery_batches_path=discovery,
        batch_diagnostics_path=diagnostics,
        config=MarketFamilyMemoryConfig(min_observations=1),
    )

    politics = next(row for row in report["families"] if row["family_key"] == "tag:politics")
    assert politics["observations"] == 1
    assert politics["signalability_failures"] == 1
    assert politics["signalability_failure_rate"] == 1.0
    assert politics["avg_signalable_assets"] == 1.0
    assert politics["family_memory_decision"] == "EXPLORE"


def test_market_family_prefers_tags_then_slug_then_question() -> None:
    assert market_family({"tags": ["Crypto"], "slug": "will-btc"}) == (
        "tag:crypto",
        "tag",
    )
    assert market_family({"slug": "will-btc-hit-100k"}) == (
        "slug:will-btc-hit",
        "slug",
    )
    assert market_family({"question": "Will CPI rise again?"}) == (
        "question:will-cpi-rise",
        "question",
    )


def test_market_family_memory_config_rejects_invalid_observations() -> None:
    try:
        MarketFamilyMemoryConfig(min_observations=0)
    except ValueError as exc:
        assert str(exc) == "min_observations must be positive"
    else:
        raise AssertionError("expected invalid min_observations to fail")


def write_discovery_batches(
    tmp_path: Path,
    *,
    duplicate_family_market: bool = False,
) -> Path:
    path = tmp_path / "discovery" / "runtime_touch_discovery_batches.json"
    path.parent.mkdir(parents=True)
    markets = [
        {
            "market_id": "market-a",
            "question": "Will A happen?",
            "slug": "will-a-happen",
            "tags": ["Politics"],
        }
    ]
    if duplicate_family_market:
        markets.append(
            {
                "market_id": "market-b",
                "question": "Will B happen?",
                "slug": "will-b-happen",
                "tags": ["Politics"],
            }
        )
    path.write_text(
        json.dumps(
            {
                "batches": [
                    {
                        "batch_id": "batch-01",
                        "markets": markets,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    return path


def write_batch_diagnostics(
    tmp_path: Path,
    *,
    readiness_status: str = "TIME_WINDOW_READY_ONLY",
    primary_blocker: str = "timing_window_candidate",
    signalable_assets: int = 2,
) -> Path:
    path = (
        tmp_path
        / "diagnostics"
        / "runtime_touch_discovery_batch_diagnostics.json"
    )
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "can_execute_trades": False,
                "can_promote_live": False,
                "batches": [
                    {
                        "batch_id": "batch-01",
                        "is_processed": True,
                        "readiness_status": readiness_status,
                        "primary_blocker": primary_blocker,
                        "signalable_assets_count": signalable_assets,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def write_batch_comparison(tmp_path: Path) -> Path:
    path = tmp_path / "comparison" / "runtime_touch_discovery_batch_comparison.json"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "batches": [
                    {
                        "batch_id": "batch-01",
                        "batch_decision": "READY",
                        "route_next_action": "CHANGE_TIME_WINDOW",
                        "selected_assets": 2,
                        "eligible_windows": 1,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    return path


def write_fillability_score(tmp_path: Path) -> Path:
    path = tmp_path / "fillability" / "market_fillability_score.json"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "selected": [
                    {
                        "market_id": "market-a",
                        "asset_id": "asset-a",
                        "question": "Will A happen?",
                        "slug": "will-a-happen",
                        "tags": ["Politics"],
                        "market_fillability_score": 12.0,
                        "recommendation": "PROMOTE_TO_DISCOVERY_BATCH",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    return path
