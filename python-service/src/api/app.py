from pathlib import Path
from typing import Annotated, Any, Awaitable, cast

from fastapi import APIRouter, Body, Depends, FastAPI, HTTPException, Response, status
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from src.config import settings
from src.config import validate_production_settings
from src.api.auth import require_control_auth, require_read_auth
from src.api.models import (
    CancelBotOpenRequest,
    CancelAllRequest,
    ControlResultsResponse,
    ControlResponse,
    ExecutionReportsResponse,
    MarketsDiscoverResponse,
    OrdersOpenResponse,
    PositionsResponse,
    RiskResponse,
    RuntimeMetricsResponse,
    StatusResponse,
    StrategyMetricsResponse,
    StreamsResponse,
)
from src.api.operator_service import (
    open_orders,
    positions,
    control_results,
    recent_execution_reports,
    request_cancel_bot_open,
    request_cancel_all,
    risk_summary,
    runtime_metrics,
    prometheus_metrics,
    set_kill_switch,
    strategy_metrics,
    status_summary,
    stream_summary,
    RedisLike,
)
from src.api.state_store import require_pool
from src.data.redis_client import get_redis
from src.discovery.markets import discover_markets

app = FastAPI(title="Polymarket Trading Control API")
validate_production_settings()
router = APIRouter()
ReadAuthDependency = Annotated[None, Depends(require_read_auth)]
ControlAuthDependency = Annotated[None, Depends(require_control_auth)]


class KillSwitchRequest(BaseModel):
    reason: str = Field(min_length=1)
    operator: str | None = None


class ResumeRequest(BaseModel):
    confirm: bool
    reason: str = Field(min_length=1)
    operator: str | None = None


@router.get("/health")
async def health() -> dict[str, str]:
    redis = await get_redis()
    await cast(Awaitable[Any], redis.ping())
    return {"status": "ok"}


@router.get("/status", response_model=StatusResponse)
async def get_status(_: ReadAuthDependency) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    return await status_summary(redis)


@router.get("/risk", response_model=RiskResponse)
async def risk(_: ReadAuthDependency) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    return await risk_summary(redis)


@router.get("/streams", response_model=StreamsResponse)
async def streams(_: ReadAuthDependency) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    return {"streams": await stream_summary(redis)}


@router.post("/control/kill-switch", response_model=ControlResponse)
async def enable_kill_switch(
    request: KillSwitchRequest, _: ControlAuthDependency
) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    return await set_kill_switch(
        redis, enabled=True, reason=request.reason, operator=request.operator
    )


@router.post("/control/resume", response_model=ControlResponse)
async def resume(request: ResumeRequest, _: ControlAuthDependency) -> dict[str, object]:
    if not request.confirm:
        raise HTTPException(status_code=400, detail="confirm=true is required to resume")
    redis = cast(RedisLike, await get_redis())
    return await set_kill_switch(
        redis, enabled=False, reason=request.reason, operator=request.operator
    )


@router.post(
    "/orders/cancel-all",
    response_model=ControlResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def cancel_all(
    _: ControlAuthDependency,
    request: Annotated[CancelAllRequest, Body()] = CancelAllRequest(),
) -> dict[str, object]:
    if not request.confirm or request.confirmation_phrase != "CANCEL ALL OPEN ORDERS":
        raise HTTPException(
            status_code=400,
            detail='cancel-all requires confirm=true and confirmation_phrase="CANCEL ALL OPEN ORDERS"',
        )
    redis = cast(RedisLike, await get_redis())
    return await request_cancel_all(
        redis,
        reason=request.reason,
        operator=request.operator,
        confirm=request.confirm,
        confirmation_phrase=request.confirmation_phrase,
    )


@router.post(
    "/orders/cancel-bot-open",
    response_model=ControlResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def cancel_bot_open(
    _: ControlAuthDependency,
    request: Annotated[CancelBotOpenRequest, Body()] = CancelBotOpenRequest(),
) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    return await request_cancel_bot_open(
        redis, reason=request.reason, operator=request.operator
    )


@router.get("/orders/open", response_model=OrdersOpenResponse)
async def orders_open(_: ReadAuthDependency) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    postgres_pool = await require_pool()
    return {
        "orders": await open_orders(redis, postgres_pool=postgres_pool),
        "source": "postgres" if postgres_pool is not None else settings.execution_reports_stream,
    }


@router.get("/positions", response_model=PositionsResponse)
async def get_positions(_: ReadAuthDependency) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    postgres_pool = await require_pool()
    return {
        "positions": await positions(redis, postgres_pool=postgres_pool),
        "source": "postgres"
        if postgres_pool is not None
        else [
            settings.execution_reports_stream,
            settings.signals_stream,
        ],
    }


@router.get("/execution-reports", response_model=ExecutionReportsResponse)
async def execution_reports(_: ReadAuthDependency, limit: int = 100) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    return {
        "reports": await recent_execution_reports(redis, count=max(1, min(limit, 500))),
        "source": settings.execution_reports_stream,
    }


@router.get("/strategy/metrics", response_model=StrategyMetricsResponse)
async def get_strategy_metrics(
    _: ReadAuthDependency, limit: int = 500
) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    return await strategy_metrics(redis, count=max(1, min(limit, 1000)))


@router.get("/control/results", response_model=ControlResultsResponse)
async def get_control_results(
    _: ReadAuthDependency, limit: int = 100
) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    return {
        "results": await control_results(redis, count=max(1, min(limit, 500))),
        "source": settings.operator_results_stream,
    }


@router.get("/metrics", response_model=RuntimeMetricsResponse)
async def get_runtime_metrics(_: ReadAuthDependency, limit: int = 500) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    return await runtime_metrics(redis, count=max(1, min(limit, 1000)))


@router.get("/metrics/prometheus", response_class=Response)
async def get_prometheus_metrics(_: ReadAuthDependency, limit: int = 500) -> Response:
    redis = cast(RedisLike, await get_redis())
    metrics = await runtime_metrics(redis, count=max(1, min(limit, 1000)))
    return Response(content=prometheus_metrics(metrics), media_type="text/plain")


@router.get("/markets/discover", response_model=MarketsDiscoverResponse)
async def markets_discover(
    _: ReadAuthDependency,
    limit: int | None = None,
    query: str | None = None,
    min_liquidity: float | None = None,
    min_volume: float | None = None,
) -> dict[str, object]:
    markets = await discover_markets(
        limit=limit,
        query=query,
        min_liquidity=min_liquidity,
        min_volume=min_volume,
    )
    return {
        "markets": markets,
        "source": settings.gamma_api_url,
    }


def frontend_dist_path() -> Path:
    return Path(__file__).resolve().parents[3] / "frontend" / "dist"


dist_path = frontend_dist_path()
app.include_router(router)
app.include_router(router, prefix="/api")
if (dist_path / "index.html").exists():
    app.mount("/", StaticFiles(directory=dist_path, html=True), name="dashboard")
