from fastapi.testclient import TestClient

from src.api import app as api_app
from src.discovery.markets import (
    MarketCandidate,
    normalize_gamma_market,
    rank_markets,
)


def candidate(
    market_id: str,
    liquidity: float,
    volume: float,
    active: bool = True,
    enable_order_book: bool = True,
) -> MarketCandidate:
    return MarketCandidate(
        market_id=market_id,
        question=f"Question {market_id}",
        active=active,
        closed=False,
        archived=False,
        enable_order_book=enable_order_book,
        liquidity=liquidity,
        volume=volume,
        outcomes=["Yes", "No"],
        outcome_prices=[0.45, 0.55],
        clob_token_ids=[f"{market_id}-yes", f"{market_id}-no"],
        description="A detailed market description with enough resolution context for scoring.",
        resolution_source="official source",
        tags=["Politics"],
    )


def test_normalize_gamma_market_handles_json_encoded_fields() -> None:
    market = normalize_gamma_market(
        {
            "conditionId": "0xabc",
            "question": "Will this parse?",
            "active": "true",
            "closed": "false",
            "archived": False,
            "enableOrderBook": True,
            "liquidity": "1000.5",
            "volume": "2500",
            "outcomes": '["Yes","No"]',
            "outcomePrices": '["0.48","0.52"]',
            "clobTokenIds": '["1","2"]',
            "tags": [{"label": "Crypto"}],
        }
    )

    assert market.market_id == "0xabc"
    assert market.outcome_prices == [0.48, 0.52]
    assert market.clob_token_ids == ["1", "2"]
    assert market.tags == ["Crypto"]


def test_rank_markets_filters_non_tradable_and_sorts_by_score() -> None:
    ranked = rank_markets(
        [
            candidate("low", liquidity=500, volume=500),
            candidate("high", liquidity=50_000, volume=100_000),
            candidate("inactive", liquidity=100_000, volume=100_000, active=False),
            candidate("no-book", liquidity=100_000, volume=100_000, enable_order_book=False),
        ],
        min_liquidity=100,
        min_volume=100,
    )

    assert [item.market.market_id for item in ranked] == ["high", "low"]


def test_discovery_api_returns_ranked_markets(monkeypatch) -> None:
    async def fake_discover_markets(**_: object):
        return rank_markets([candidate("0xabc", 10_000, 50_000)], 100, 100)

    monkeypatch.setattr(api_app, "discover_markets", fake_discover_markets)
    client = TestClient(api_app.app)

    response = client.get("/markets/discover?limit=1")

    assert response.status_code == 200
    payload = response.json()
    assert payload["markets"][0]["market"]["market_id"] == "0xabc"


def test_discover_markets_cli_params(monkeypatch) -> None:
    from src import cli

    captured: dict[str, object] = {}

    def fake_request_json(client, method, path, json=None, params=None):
        captured["method"] = method
        captured["path"] = path
        captured["params"] = params
        return {"markets": []}

    monkeypatch.setattr(cli, "request_json", fake_request_json)
    args = cli.build_parser().parse_args(
        ["discover-markets", "--limit", "5", "--query", "bitcoin"]
    )

    result = cli.dispatch(args)

    assert result == {"markets": []}
    assert captured["path"] == "/markets/discover"
    assert captured["params"] == {"limit": 5, "query": "bitcoin"}
