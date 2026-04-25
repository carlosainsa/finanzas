from pathlib import Path
from typing import Annotated, Any, Awaitable, cast

from fastapi import APIRouter, Body, Depends, FastAPI, HTTPException, status
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from src.config import settings
from src.api.auth import require_operator_auth
from src.api.models import (
    CancelAllRequest,
    ControlResponse,
    ExecutionReportsResponse,
    MarketsDiscoverResponse,
    OrdersOpenResponse,
    PositionsResponse,
    RiskResponse,
    StatusResponse,
    StrategyMetricsResponse,
    StreamsResponse,
)
from src.api.operator_service import (
    open_orders,
    positions,
    recent_execution_reports,
    request_cancel_all,
    risk_summary,
    set_kill_switch,
    strategy_metrics,
    status_summary,
    stream_summary,
    RedisLike,
)
from src.api.state_store import get_pool
from src.data.redis_client import get_redis
from src.discovery.markets import discover_markets

app = FastAPI(title="Polymarket Trading Control API")
router = APIRouter()
AuthDependency = Annotated[None, Depends(require_operator_auth)]


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
async def get_status(_: AuthDependency) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    return await status_summary(redis)


@router.get("/risk", response_model=RiskResponse)
async def risk(_: AuthDependency) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    return await risk_summary(redis)


@router.get("/streams", response_model=StreamsResponse)
async def streams(_: AuthDependency) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    return {"streams": await stream_summary(redis)}


@router.post("/control/kill-switch", response_model=ControlResponse)
async def enable_kill_switch(
    request: KillSwitchRequest, _: AuthDependency
) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    return await set_kill_switch(
        redis, enabled=True, reason=request.reason, operator=request.operator
    )


@router.post("/control/resume", response_model=ControlResponse)
async def resume(request: ResumeRequest, _: AuthDependency) -> dict[str, object]:
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
    _: AuthDependency,
    request: Annotated[CancelAllRequest, Body()] = CancelAllRequest(),
) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    return await request_cancel_all(redis, reason=request.reason, operator=request.operator)


@router.get("/orders/open", response_model=OrdersOpenResponse)
async def orders_open(_: AuthDependency) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    postgres_pool = await get_pool()
    return {
        "orders": await open_orders(redis, postgres_pool=postgres_pool),
        "source": "postgres" if postgres_pool is not None else settings.execution_reports_stream,
    }


@router.get("/positions", response_model=PositionsResponse)
async def get_positions(_: AuthDependency) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    postgres_pool = await get_pool()
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
async def execution_reports(_: AuthDependency, limit: int = 100) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    return {
        "reports": await recent_execution_reports(redis, count=max(1, min(limit, 500))),
        "source": settings.execution_reports_stream,
    }


@router.get("/strategy/metrics", response_model=StrategyMetricsResponse)
async def get_strategy_metrics(
    _: AuthDependency, limit: int = 500
) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    return await strategy_metrics(redis, count=max(1, min(limit, 1000)))


@router.get("/markets/discover", response_model=MarketsDiscoverResponse)
async def markets_discover(
    _: AuthDependency,
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
