from typing import Any

from pydantic import BaseModel, Field

from src.discovery.markets import ScoredMarket
from src.schemas import ExecutionReport

JsonScalar = str | int | float | bool | None


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
    confirm: bool = False
    confirmation_phrase: str | None = None


class CancelBotOpenRequest(BaseModel):
    reason: str = Field(default="operator cancel bot open orders")
    operator: str | None = None


class ControlResult(BaseModel):
    type: str
    command_id: str
    status: str
    timestamp_ms: int
    error: str | None = None
    command_type: str | None = None
    canceled: list[str] | None = None
    canceled_count: int | None = None
    not_canceled: dict[str, str] | None = None
    divergences: list[str] | None = None
    operator: str | None = None
    reason: str | None = None
    command_created_at_ms: int | None = None
    completed_at_ms: int | None = None


class ControlResultsResponse(BaseModel):
    results: list[ControlResult]
    source: str


class ControlPreviewResponse(BaseModel):
    command_type: str
    scope: str
    affected_count: int
    affected_orders: list[ExecutionReport]
    source: str
    warnings: list[str]
    requires_confirmation: bool
    confirmation_phrase: str | None = None
    would_publish: bool = False


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
    latency_ms: float | None = None
    source: str


class RuntimeMetricsResponse(BaseModel):
    signals_received: int
    signals_rejected: int
    orders_submitted: int
    clob_errors: int
    clob_errors_by_type: dict[str, int]
    execution_reports: int
    execution_reports_by_status: dict[str, int]
    control_results: int
    control_results_by_type: dict[str, int]
    ws_to_report_latency_ms: float | None = None
    ws_to_signal_latency_ms: float | None = None
    signal_to_order_latency_ms: float | None = None
    order_to_report_latency_ms: float | None = None
    source: list[str]


class NIMBudgetResponse(BaseModel):
    status: str
    source: str
    run_id: str | None = None
    report_root: str | None = None
    enabled: bool | None = None
    nim_model: str | None = None
    annotations: int | None = None
    failures: int | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None
    latency_ms_avg: float | None = None
    estimated_cost: float | None = None
    budget_status: str | None = None
    budget_violations: list[str]
    can_execute_trades: bool
    updated_at: str | None = None


class ReconciliationEvent(BaseModel):
    event_id: str
    order_id: str | None = None
    signal_id: str | None = None
    event_type: str
    severity: str
    details: dict[str, JsonScalar]
    created_at: str


class ReconciliationStatusResponse(BaseModel):
    status: str
    source: str
    open_local_orders: int
    pending_cancel_requests: int
    diverged_cancel_requests: int
    stale_orders: int
    recent_event_count: int
    events_by_severity: dict[str, int]
    events_by_type: dict[str, int]
    recent_events: list[ReconciliationEvent]
    last_reconciled_at_ms: int | None = None


class MarketsDiscoverResponse(BaseModel):
    markets: list[ScoredMarket]
    source: str
