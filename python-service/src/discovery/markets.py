import json
import math
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, Field

from src.config import settings


class MarketCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    market_id: str
    question: str
    slug: str | None = None
    active: bool
    closed: bool
    archived: bool
    enable_order_book: bool
    liquidity: float
    volume: float
    outcomes: list[str]
    outcome_prices: list[float]
    clob_token_ids: list[str]
    end_date: str | None = None
    tags: list[str] = Field(default_factory=list)
    description: str | None = None
    resolution_source: str | None = None

    @property
    def is_tradable(self) -> bool:
        return self.active and not self.closed and not self.archived and self.enable_order_book


class ScoredMarket(BaseModel):
    model_config = ConfigDict(extra="forbid")

    market: MarketCandidate
    score: float
    liquidity_score: float
    volume_score: float
    price_quality_score: float
    evidence_score: float
    reason: str


async def discover_markets(
    limit: int | None = None,
    query: str | None = None,
    min_liquidity: float | None = None,
    min_volume: float | None = None,
) -> list[ScoredMarket]:
    raw_markets = await fetch_gamma_markets(limit=limit or settings.discovery_limit, query=query)
    candidates = [normalize_gamma_market(item) for item in raw_markets]
    return rank_markets(
        candidates,
        min_liquidity=min_liquidity
        if min_liquidity is not None
        else settings.discovery_min_liquidity,
        min_volume=min_volume if min_volume is not None else settings.discovery_min_volume,
    )


async def fetch_gamma_markets(limit: int, query: str | None = None) -> list[dict[str, Any]]:
    params: dict[str, str | int | bool] = {
        "limit": limit,
        "active": True,
        "closed": False,
        "archived": False,
    }
    if query:
        params["q"] = query

    async with httpx.AsyncClient(base_url=settings.gamma_api_url, timeout=10.0) as client:
        response = await client.get("/markets", params=params)
        response.raise_for_status()
        payload = response.json()
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict) and isinstance(payload.get("markets"), list):
        return [item for item in payload["markets"] if isinstance(item, dict)]
    return []


def rank_markets(
    candidates: list[MarketCandidate],
    min_liquidity: float,
    min_volume: float,
) -> list[ScoredMarket]:
    scored = [
        score_market(candidate)
        for candidate in candidates
        if candidate.is_tradable
        and candidate.liquidity >= min_liquidity
        and candidate.volume >= min_volume
        and len(candidate.clob_token_ids) >= 2
    ]
    return sorted(scored, key=lambda item: item.score, reverse=True)


def score_market(candidate: MarketCandidate) -> ScoredMarket:
    liquidity_score = bounded_log_score(candidate.liquidity, scale=10_000.0)
    volume_score = bounded_log_score(candidate.volume, scale=100_000.0)
    price_quality_score = probability_quality(candidate.outcome_prices)
    evidence_score = evidence_quality(candidate)
    score = round(
        0.35 * liquidity_score
        + 0.30 * volume_score
        + 0.20 * price_quality_score
        + 0.15 * evidence_score,
        6,
    )
    return ScoredMarket(
        market=candidate,
        score=score,
        liquidity_score=liquidity_score,
        volume_score=volume_score,
        price_quality_score=price_quality_score,
        evidence_score=evidence_score,
        reason=score_reason(candidate, liquidity_score, volume_score, evidence_score),
    )


def normalize_gamma_market(item: dict[str, Any]) -> MarketCandidate:
    return MarketCandidate(
        market_id=str(
            first_present(item, "conditionId", "condition_id", "market_id", "id") or ""
        ),
        question=str(first_present(item, "question", "title") or ""),
        slug=optional_str(item.get("slug")),
        active=as_bool(item.get("active"), default=True),
        closed=as_bool(item.get("closed"), default=False),
        archived=as_bool(item.get("archived"), default=False),
        enable_order_book=as_bool(
            first_present(item, "enableOrderBook", "enable_order_book"), default=False
        ),
        liquidity=as_float(first_present(item, "liquidity", "liquidityNum", "liquidity_num")),
        volume=as_float(first_present(item, "volume", "volumeNum", "volume_num")),
        outcomes=parse_str_list(item.get("outcomes")),
        outcome_prices=parse_float_list(first_present(item, "outcomePrices", "outcome_prices")),
        clob_token_ids=parse_str_list(first_present(item, "clobTokenIds", "clob_token_ids")),
        end_date=optional_str(first_present(item, "endDate", "end_date")),
        tags=parse_tags(item.get("tags")),
        description=optional_str(item.get("description")),
        resolution_source=optional_str(
            first_present(item, "resolutionSource", "resolution_source")
        ),
    )


def bounded_log_score(value: float, scale: float) -> float:
    if value <= 0:
        return 0.0
    return min(1.0, math.log10(value + 1.0) / math.log10(scale + 1.0))


def probability_quality(prices: list[float]) -> float:
    valid_prices = [price for price in prices if 0.0 < price < 1.0]
    if len(valid_prices) < 2:
        return 0.0
    edge_distance = min(abs(price - 0.5) for price in valid_prices)
    return round(max(0.0, 1.0 - edge_distance * 2.0), 6)


def evidence_quality(candidate: MarketCandidate) -> float:
    score = 0.0
    if candidate.description and len(candidate.description) >= 80:
        score += 0.35
    if candidate.resolution_source:
        score += 0.35
    if candidate.tags:
        score += 0.20
    if candidate.end_date:
        score += 0.10
    return min(1.0, score)


def score_reason(
    candidate: MarketCandidate,
    liquidity_score: float,
    volume_score: float,
    evidence_score: float,
) -> str:
    reasons = []
    if liquidity_score >= 0.7:
        reasons.append("high liquidity")
    if volume_score >= 0.7:
        reasons.append("high volume")
    if evidence_score >= 0.7:
        reasons.append("strong metadata")
    if not reasons:
        reasons.append("passes deterministic filters")
    return f"{candidate.question}: {', '.join(reasons)}"


def first_present(item: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = item.get(key)
        if value is not None:
            return value
    return None


def optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes", "on"}
    return bool(value)


def as_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    return parsed if math.isfinite(parsed) else 0.0


def parse_str_list(value: Any) -> list[str]:
    parsed = parse_json_if_string(value)
    if isinstance(parsed, list):
        return [str(item) for item in parsed if item is not None]
    return []


def parse_float_list(value: Any) -> list[float]:
    parsed = parse_json_if_string(value)
    if isinstance(parsed, list):
        return [as_float(item) for item in parsed]
    return []


def parse_tags(value: Any) -> list[str]:
    parsed = parse_json_if_string(value)
    if not isinstance(parsed, list):
        return []
    tags: list[str] = []
    for item in parsed:
        if isinstance(item, dict):
            label = first_present(item, "label", "name", "slug")
            if label is not None:
                tags.append(str(label))
        elif item is not None:
            tags.append(str(item))
    return tags


def parse_json_if_string(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value
