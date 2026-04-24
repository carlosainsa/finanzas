from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


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
    strategy: str | None = None


class ExecutionReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    signal_id: str
    order_id: str
    status: Literal["MATCHED", "DELAYED", "UNMATCHED", "CANCELLED", "ERROR"]
    timestamp_ms: int
    filled_price: float | None = Field(default=None, ge=0, le=1)
    filled_size: float | None = Field(default=None, ge=0)
    error: str | None = None
