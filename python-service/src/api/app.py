from typing import Any, Awaitable, cast

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from src.config import settings
from src.api.operator_service import (
    cancel_all_unavailable,
    open_orders,
    positions,
    risk_summary,
    set_kill_switch,
    status_summary,
    stream_summary,
    RedisLike,
)
from src.data.redis_client import get_redis

app = FastAPI(title="Polymarket Trading Control API")


class KillSwitchRequest(BaseModel):
    reason: str = Field(min_length=1)
    operator: str | None = None


class ResumeRequest(BaseModel):
    confirm: bool
    reason: str = Field(min_length=1)
    operator: str | None = None


@app.get("/health")
async def health() -> dict[str, str]:
    redis = await get_redis()
    await cast(Awaitable[Any], redis.ping())
    return {"status": "ok"}


@app.get("/status")
async def status() -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    return await status_summary(redis)


@app.get("/risk")
async def risk() -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    return await risk_summary(redis)


@app.get("/streams")
async def streams() -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    return {"streams": await stream_summary(redis)}


@app.post("/control/kill-switch")
async def enable_kill_switch(request: KillSwitchRequest) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    return await set_kill_switch(
        redis, enabled=True, reason=request.reason, operator=request.operator
    )


@app.post("/control/resume")
async def resume(request: ResumeRequest) -> dict[str, object]:
    if not request.confirm:
        raise HTTPException(status_code=400, detail="confirm=true is required to resume")
    redis = cast(RedisLike, await get_redis())
    return await set_kill_switch(
        redis, enabled=False, reason=request.reason, operator=request.operator
    )


@app.post("/orders/cancel-all")
async def cancel_all() -> dict[str, object]:
    cancel_all_unavailable()
    return {}


@app.get("/orders/open")
async def orders_open() -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    return {
        "orders": await open_orders(redis),
        "source": settings.execution_reports_stream,
    }


@app.get("/positions")
async def get_positions() -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    return {
        "positions": await positions(redis),
        "source": [
            settings.execution_reports_stream,
            settings.signals_stream,
        ],
    }
