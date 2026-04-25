from typing import Any

from pydantic import BaseModel, Field

from src.discovery.markets import ScoredMarket
from src.schemas import ExecutionReport


class PendingSummary(BaseModel):
    pending: int | None = None
    min: str | None = None
    max: str | None = None
    consumers: list[Any] | None = None
    raw: str | None = None


class StreamSummary(BaseModel):
    stream: str
    length: int
    consumer_group: str | None
    pending: PendingSummary | None


class PredictorSummary(BaseModel):
    min_spread: float
    order_size: float
    min_confidence: float


class StatusResponse(BaseModel):
    status: str
    kill_switch: bool
    streams: list[StreamSummary]
    predictor: PredictorSummary


class RiskLimits(BaseModel):
    max_order_size: float
    min_confidence: float
    signal_max_age_ms: int
    max_market_exposure: float
    max_daily_loss: float
    predictor_min_confidence: float
    predictor_order_size: float


class RiskResponse(BaseModel):
    kill_switch: bool
    source: str
    execution_mode: str
    limits: RiskLimits
    enforcement: str


class StreamsResponse(BaseModel):
    streams: list[StreamSummary]


class ControlCommand(BaseModel):
    type: str
    reason: str
    operator: str | None
    timestamp_ms: int
    enabled: bool | None = None
    command_id: str | None = None


class ControlResponse(BaseModel):
    kill_switch: bool | None = None
    accepted: bool | None = None
    command: ControlCommand


class CancelAllRequest(BaseModel):
    reason: str = Field(default="operator cancel all")
    operator: str | None = None


class OrdersOpenResponse(BaseModel):
    orders: list[ExecutionReport]
    source: str


class Position(BaseModel):
    market_id: str
    asset_id: str
    position: float


class PositionsResponse(BaseModel):
    positions: list[Position]
    source: str | list[str]


class ExecutionReportsResponse(BaseModel):
    reports: list[ExecutionReport]
    source: str


class StrategyMetricsResponse(BaseModel):
    sample_size: int
    matched: int
    open: int
    errors: int
    match_rate: float
    error_rate: float
    filled_size: float
    source: str


class MarketsDiscoverResponse(BaseModel):
    markets: list[ScoredMarket]
    source: str
