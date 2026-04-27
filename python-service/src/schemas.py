from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator


class Level(BaseModel):
    model_config = ConfigDict(extra="forbid")

    price: float = Field(ge=0, le=1)
    size: float = Field(ge=0)


class OrderBook(BaseModel):
    model_config = ConfigDict(extra="forbid")

    market_id: str
    asset_id: str
    bids: list[Level]
    asks: list[Level]
    timestamp_ms: int

    @field_validator("bids")
    @classmethod
    def bids_descending(cls, bids: list[Level]) -> list[Level]:
        if any(left.price < right.price for left, right in zip(bids, bids[1:])):
            raise ValueError("bids must be sorted by descending price")
        return bids

    @field_validator("asks")
    @classmethod
    def asks_ascending(cls, asks: list[Level]) -> list[Level]:
        if any(left.price > right.price for left, right in zip(asks, asks[1:])):
            raise ValueError("asks must be sorted by ascending price")
        return asks

    @property
    def best_bid(self) -> Level | None:
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> Level | None:
        return self.asks[0] if self.asks else None


class TradeSignal(BaseModel):
    model_config = ConfigDict(extra="forbid")

    signal_id: str
    market_id: str
    asset_id: str
    side: Literal["BUY", "SELL"]
    price: float = Field(ge=0, le=1)
    size: float = Field(gt=0)
    confidence: float = Field(ge=0, le=1)
    timestamp_ms: int
    source_timestamp_ms: int | None = None
    strategy: str | None = None
    model_version: str | None = None
    data_version: str | None = None
    feature_version: str | None = None


class ExecutionReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    signal_id: str
    order_id: str
    status: Literal["MATCHED", "PARTIAL", "DELAYED", "UNMATCHED", "CANCELLED", "ERROR"]
    timestamp_ms: int
    filled_price: float | None = Field(default=None, ge=0, le=1)
    filled_size: float | None = Field(default=None, ge=0)
    cumulative_filled_size: float | None = Field(default=None, ge=0)
    remaining_size: float | None = Field(default=None, ge=0)
    error: str | None = None


class ExternalEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    evidence_id: str
    source: str
    source_type: Literal["news", "social", "search", "market_comment", "official", "other"]
    published_at_ms: int
    observed_at_ms: int
    available_at_ms: int
    market_id: str
    asset_id: str | None = None
    raw_reference_hash: str
    direction: Literal["YES", "NO", "NEUTRAL", "UNKNOWN"] = "UNKNOWN"
    sentiment_score: float = Field(default=0.0, ge=-1, le=1)
    source_quality: float = Field(default=0.5, ge=0, le=1)
    confidence: float = Field(default=0.5, ge=0, le=1)
    url: str | None = None
    title: str | None = None
    text_hash: str | None = None
    language: str | None = None
    data_version: str

    @field_validator("observed_at_ms")
    @classmethod
    def observed_after_publish(cls, observed_at_ms: int, info: ValidationInfo) -> int:
        published_at_ms = info.data.get("published_at_ms")
        if isinstance(published_at_ms, int) and observed_at_ms < published_at_ms:
            raise ValueError("observed_at_ms must be >= published_at_ms")
        return observed_at_ms

    @field_validator("available_at_ms")
    @classmethod
    def available_after_observation(cls, available_at_ms: int, info: ValidationInfo) -> int:
        observed_at_ms = info.data.get("observed_at_ms")
        if isinstance(observed_at_ms, int) and available_at_ms < observed_at_ms:
            raise ValueError("available_at_ms must be >= observed_at_ms")
        return available_at_ms


class SentimentFeature(BaseModel):
    model_config = ConfigDict(extra="forbid")

    feature_id: str
    evidence_id: str
    market_id: str
    asset_id: str | None = None
    observed_at_ms: int
    feature_timestamp_ms: int
    available_at_ms: int
    direction: Literal["YES", "NO", "NEUTRAL", "UNKNOWN"]
    sentiment_score: float = Field(ge=-1, le=1)
    net_sentiment: float = Field(ge=-1, le=1)
    lookback_ms: int = Field(gt=0)
    evidence_count: int = Field(ge=1)
    source_count: int = Field(ge=1)
    evidence_ids_hash: str
    sentiment_momentum: float | None = None
    sentiment_disagreement: float | None = Field(default=None, ge=0, le=1)
    source_quality: float = Field(ge=0, le=1)
    confidence: float = Field(ge=0, le=1)
    model_version: str
    data_version: str
    feature_version: str

    @field_validator("feature_timestamp_ms")
    @classmethod
    def feature_after_observation(cls, feature_timestamp_ms: int, info: ValidationInfo) -> int:
        observed_at_ms = info.data.get("observed_at_ms")
        if isinstance(observed_at_ms, int) and feature_timestamp_ms < observed_at_ms:
            raise ValueError("feature_timestamp_ms must be >= observed_at_ms")
        return feature_timestamp_ms

    @field_validator("available_at_ms")
    @classmethod
    def available_after_feature(cls, available_at_ms: int, info: ValidationInfo) -> int:
        feature_timestamp_ms = info.data.get("feature_timestamp_ms")
        if isinstance(feature_timestamp_ms, int) and available_at_ms < feature_timestamp_ms:
            raise ValueError("available_at_ms must be >= feature_timestamp_ms")
        return available_at_ms
