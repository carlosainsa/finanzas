from pathlib import Path
import json

from src.discovery.markets import MarketCandidate, score_market
from src.research.runtime_touch_discovery_batches import (
    REPORT_VERSION,
    RuntimeTouchDiscoveryBatchConfig,
    write_runtime_touch_discovery_batches,
)


def test_discovery_batches_groups_ranked_markets_without_execution(
    tmp_path: Path,
) -> None:
    markets = [
        score_market(candidate("market-a", score_hint=30_000)),
        score_market(candidate("market-b", score_hint=20_000)),
        score_market(candidate("market-c", score_hint=10_000)),
    ]

    report = write_runtime_touch_discovery_batches(
        tmp_path / "discovery",
        markets,
        RuntimeTouchDiscoveryBatchConfig(discovery_limit=3, batch_size=4),
    )

    assert report["report_version"] == REPORT_VERSION
    assert report["can_execute_trades"] is False
    assert report["can_promote_live"] is False
    assert report["decision_policy"] == "offline_runtime_touch_discovery_batching_only"
    assert report["counts"]["asset_ids"] == 6
    assert report["counts"]["batches"] == 2
    assert report["counts"]["markets"] == 3
    assert [batch["batch_id"] for batch in report["batches"]] == [
        "batch-01",
        "batch-02",
    ]
    assert report["batches"][0]["market_asset_ids"] == [
        "market-a-yes",
        "market-a-no",
        "market-b-yes",
        "market-b-no",
    ]
    assert (tmp_path / "discovery" / "runtime_touch_discovery_batches.json").exists()


def test_discovery_batches_use_fillability_and_family_memory(
    tmp_path: Path,
) -> None:
    fillability_path = tmp_path / "fillability.json"
    family_memory_path = tmp_path / "family_memory.json"
    fillability_path.write_text(
        json.dumps(
            {
                "selected": [
                    {
                        "asset_id": "market-b-yes",
                        "market_fillability_score": 50.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    family_memory_path.write_text(
        json.dumps(
            {
                "families": [
                    {
                        "family_key": "tag:politics",
                        "observations": 1,
                        "family_memory_score": 20.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    markets = [
        score_market(candidate("market-a", score_hint=30_000, tags=["Sports"])),
        score_market(candidate("market-b", score_hint=10_000, tags=["Politics"])),
    ]

    report = write_runtime_touch_discovery_batches(
        tmp_path / "discovery",
        markets,
        RuntimeTouchDiscoveryBatchConfig(
            discovery_limit=2,
            batch_size=4,
            market_fillability_score_path=str(fillability_path),
            market_family_memory_path=str(family_memory_path),
            exploration_rate=0.50,
        ),
    )

    assert (
        report["selection_policy"]
        == "epsilon_family_fillability_diversified_batches_v2"
    )
    assert report["markets"][0]["market_id"] == "market-b"
    assert report["markets"][0]["selection_mode"] == "exploit"
    assert report["markets"][1]["selection_mode"] == "explore"
    assert report["batches"][0]["families_count"] == 2
    assert report["batches"][0]["fillability_covered_assets_count"] == 1


def test_discovery_batches_diversify_families_within_batch(
    tmp_path: Path,
) -> None:
    markets = [
        score_market(candidate("market-a", score_hint=30_000, tags=["Politics"])),
        score_market(candidate("market-b", score_hint=29_000, tags=["Politics"])),
        score_market(candidate("market-c", score_hint=10_000, tags=["Sports"])),
    ]

    report = write_runtime_touch_discovery_batches(
        tmp_path / "discovery",
        markets,
        RuntimeTouchDiscoveryBatchConfig(discovery_limit=3, batch_size=4),
    )

    assert report["batches"][0]["market_ids"] == ["market-a", "market-c"]
    assert report["batches"][0]["families_count"] == 2
    assert report["batches"][1]["market_ids"] == ["market-b"]


def test_discovery_batches_can_disable_diversification(
    tmp_path: Path,
) -> None:
    markets = [
        score_market(candidate("market-a", score_hint=30_000, tags=["Politics"])),
        score_market(candidate("market-b", score_hint=29_000, tags=["Politics"])),
        score_market(candidate("market-c", score_hint=10_000, tags=["Sports"])),
    ]

    report = write_runtime_touch_discovery_batches(
        tmp_path / "discovery",
        markets,
        RuntimeTouchDiscoveryBatchConfig(
            discovery_limit=3,
            batch_size=4,
            diversify_batches=False,
        ),
    )

    assert report["batches"][0]["market_ids"] == ["market-a", "market-b"]
    assert report["batches"][0]["families_count"] == 1


def test_discovery_batch_config_rejects_invalid_batch_size() -> None:
    try:
        RuntimeTouchDiscoveryBatchConfig(batch_size=0)
    except ValueError as exc:
        assert str(exc) == "batch_size must be positive"
    else:
        raise AssertionError("expected invalid batch size to fail")


def candidate(
    market_id: str,
    score_hint: float,
    tags: list[str] | None = None,
) -> MarketCandidate:
    return MarketCandidate(
        market_id=market_id,
        question=f"Question {market_id}",
        active=True,
        closed=False,
        archived=False,
        enable_order_book=True,
        liquidity=score_hint,
        volume=score_hint * 2,
        outcomes=["Yes", "No"],
        outcome_prices=[0.45, 0.55],
        clob_token_ids=[f"{market_id}-yes", f"{market_id}-no"],
        description="A detailed market description with enough resolution context.",
        resolution_source="official source",
        tags=tags or ["Politics"],
    )
