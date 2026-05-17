from pathlib import Path

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
    assert report["counts"] == {"asset_ids": 6, "batches": 2, "markets": 3}
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


def test_discovery_batch_config_rejects_invalid_batch_size() -> None:
    try:
        RuntimeTouchDiscoveryBatchConfig(batch_size=0)
    except ValueError as exc:
        assert str(exc) == "batch_size must be positive"
    else:
        raise AssertionError("expected invalid batch size to fail")


def candidate(market_id: str, score_hint: float) -> MarketCandidate:
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
        tags=["Politics"],
    )
