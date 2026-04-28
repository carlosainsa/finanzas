from pathlib import Path
from typing import Annotated, Any, Awaitable, cast

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Response, status
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from src.config import settings
from src.config import validate_production_settings
from src.api.auth import require_control_auth, require_read_auth
from src.api.models import (
    CancelBotOpenRequest,
    CancelAllRequest,
    ControlPreviewResponse,
    ControlResultsResponse,
    ControlResponse,
    ExecutionReportsResponse,
    GoNoGoResponse,
    MarketsDiscoverResponse,
    OrdersOpenResponse,
    PositionsResponse,
    PreLiveReadinessResponse,
    ReconciliationStatusResponse,
    NIMBudgetResponse,
    RestrictedBlocklistHistoryResponse,
    RestrictedBlocklistRankingResponse,
    ResearchRunDetailResponse,
    ResearchRunsResponse,
    RiskResponse,
    RuntimeMetricsResponse,
    StatusResponse,
    StrategyMetricsResponse,
    StreamsResponse,
)
from src.api.research_service import (
    get_research_run,
    latest_go_no_go,
    latest_nim_budget,
    latest_pre_live_readiness,
    latest_restricted_blocklist_history,
    latest_restricted_blocklist_ranking,
    list_research_runs,
)
from src.api.operator_service import (
    open_orders,
    positions,
    control_results,
    preview_cancel_all,
    preview_cancel_bot_open,
    recent_execution_reports,
    reconciliation_status_fallback,
    request_cancel_bot_open,
    request_cancel_all,
    risk_summary,
    runtime_metrics,
    runtime_metrics_from_records,
    prometheus_metrics,
    set_kill_switch,
    strategy_metrics,
    strategy_metrics_from_records,
    signal_index_from_records,
    status_summary,
    stream_summary,
    RedisLike,
)
from src.api.state_store import (
    control_audit_summary_from_postgres,
    control_results_from_postgres,
    execution_reports_from_postgres,
    reconciliation_status_from_postgres,
    require_pool,
    trade_signals_from_postgres,
)
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
    await postgres_pool_or_503()
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
    postgres_pool = await postgres_pool_or_503()
    return await set_kill_switch(
        redis,
        enabled=True,
        reason=request.reason,
        operator=request.operator,
        postgres_pool=postgres_pool,
    )


@router.post("/control/resume", response_model=ControlResponse)
async def resume(request: ResumeRequest, _: ControlAuthDependency) -> dict[str, object]:
    if not request.confirm:
        raise HTTPException(status_code=400, detail="confirm=true is required to resume")
    redis = cast(RedisLike, await get_redis())
    postgres_pool = await postgres_pool_or_503()
    return await set_kill_switch(
        redis,
        enabled=False,
        reason=request.reason,
        operator=request.operator,
        postgres_pool=postgres_pool,
    )


@router.post(
    "/control/preview/cancel-all",
    response_model=ControlPreviewResponse,
)
async def preview_control_cancel_all(_: ControlAuthDependency) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    postgres_pool = await postgres_pool_or_503()
    return await preview_cancel_all(redis, postgres_pool=postgres_pool)


@router.post(
    "/control/preview/cancel-bot-open",
    response_model=ControlPreviewResponse,
)
async def preview_control_cancel_bot_open(_: ControlAuthDependency) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    postgres_pool = await postgres_pool_or_503()
    return await preview_cancel_bot_open(redis, postgres_pool=postgres_pool)


@router.post(
    "/orders/cancel-all",
    response_model=ControlResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def cancel_all(
    _: ControlAuthDependency,
    request: CancelAllRequest,
) -> dict[str, object]:
    if not request.confirm or request.confirmation_phrase != "CANCEL ALL OPEN ORDERS":
        raise HTTPException(
            status_code=400,
            detail='cancel-all requires confirm=true and confirmation_phrase="CANCEL ALL OPEN ORDERS"',
        )
    redis = cast(RedisLike, await get_redis())
    postgres_pool = await postgres_pool_or_503()
    return await request_cancel_all(
        redis,
        reason=request.reason,
        operator=request.operator,
        confirm=request.confirm,
        confirmation_phrase=request.confirmation_phrase,
        postgres_pool=postgres_pool,
    )


@router.post(
    "/orders/cancel-bot-open",
    response_model=ControlResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def cancel_bot_open(
    _: ControlAuthDependency,
    request: CancelBotOpenRequest,
) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    postgres_pool = await postgres_pool_or_503()
    return await request_cancel_bot_open(
        redis,
        reason=request.reason,
        operator=request.operator,
        postgres_pool=postgres_pool,
    )


@router.get("/orders/open", response_model=OrdersOpenResponse)
async def orders_open(_: ReadAuthDependency) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    postgres_pool = await postgres_pool_or_503()
    return {
        "orders": await open_orders(redis, postgres_pool=postgres_pool),
        "source": "postgres" if postgres_pool is not None else settings.execution_reports_stream,
    }


@router.get("/positions", response_model=PositionsResponse)
async def get_positions(_: ReadAuthDependency) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    postgres_pool = await postgres_pool_or_503()
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
    postgres_pool = await postgres_pool_or_503()
    bounded_limit = max(1, min(limit, 500))
    if postgres_pool is not None:
        return {
            "reports": await execution_reports_from_postgres(postgres_pool, bounded_limit),
            "source": "postgres",
        }
    return {
        "reports": await recent_execution_reports(redis, count=bounded_limit),
        "source": settings.execution_reports_stream,
    }


@router.get("/strategy/metrics", response_model=StrategyMetricsResponse)
async def get_strategy_metrics(
    _: ReadAuthDependency, limit: int = 500
) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    postgres_pool = await postgres_pool_or_503()
    bounded_limit = max(1, min(limit, 1000))
    if postgres_pool is not None:
        reports = await execution_reports_from_postgres(postgres_pool, bounded_limit)
        signals = signal_index_from_records(
            await trade_signals_from_postgres(postgres_pool, bounded_limit)
        )
        return strategy_metrics_from_records(reports, signals, source="postgres")
    return await strategy_metrics(redis, count=bounded_limit)


@router.get("/control/results", response_model=ControlResultsResponse)
async def get_control_results(
    _: ReadAuthDependency, limit: int = 100
) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    postgres_pool = await postgres_pool_or_503()
    bounded_limit = max(1, min(limit, 500))
    if postgres_pool is not None:
        return {
            "results": await control_results_from_postgres(postgres_pool, bounded_limit),
            "source": "postgres",
        }
    return {
        "results": await control_results(redis, count=bounded_limit),
        "source": settings.operator_results_stream,
    }


@router.get("/metrics", response_model=RuntimeMetricsResponse)
async def get_runtime_metrics(_: ReadAuthDependency, limit: int = 500) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    postgres_pool = await postgres_pool_or_503()
    bounded_limit = max(1, min(limit, 1000))
    if postgres_pool is not None:
        reports = await execution_reports_from_postgres(postgres_pool, bounded_limit)
        signals = signal_index_from_records(
            await trade_signals_from_postgres(postgres_pool, bounded_limit)
        )
        results = await control_results_from_postgres(postgres_pool, bounded_limit)
        return runtime_metrics_from_records(
            reports, signals, results, source=["postgres"]
        )
    return await runtime_metrics(redis, count=bounded_limit)


@router.get("/metrics/prometheus", response_class=Response)
async def get_prometheus_metrics(_: ReadAuthDependency, limit: int = 500) -> Response:
    redis = cast(RedisLike, await get_redis())
    postgres_pool = await postgres_pool_or_503()
    bounded_limit = max(1, min(limit, 1000))
    if postgres_pool is not None:
        reports = await execution_reports_from_postgres(postgres_pool, bounded_limit)
        signals = signal_index_from_records(
            await trade_signals_from_postgres(postgres_pool, bounded_limit)
        )
        results = await control_results_from_postgres(postgres_pool, bounded_limit)
        metrics = runtime_metrics_from_records(
            reports, signals, results, source=["postgres"]
        )
    else:
        metrics = await runtime_metrics(redis, count=bounded_limit)
    return Response(content=prometheus_metrics(metrics), media_type="text/plain")


@router.get("/research/nim-budget", response_model=NIMBudgetResponse)
async def research_nim_budget(_: ReadAuthDependency) -> dict[str, object]:
    return latest_nim_budget()


@router.get("/research/go-no-go", response_model=GoNoGoResponse)
async def research_go_no_go(_: ReadAuthDependency) -> dict[str, object]:
    return latest_go_no_go()


@router.get("/research/pre-live-readiness", response_model=PreLiveReadinessResponse)
async def research_pre_live_readiness(_: ReadAuthDependency) -> dict[str, object]:
    postgres_pool = await postgres_pool_or_503()
    audit_summary = None
    if postgres_pool is not None:
        try:
            audit_summary = await control_audit_summary_from_postgres(postgres_pool)
        except RuntimeError as exc:
            audit_summary = {"status": "error", "source": "postgres", "error": str(exc)}
    return latest_pre_live_readiness(audit_summary=audit_summary)


@router.get(
    "/research/restricted-blocklist-ranking",
    response_model=RestrictedBlocklistRankingResponse,
)
async def research_restricted_blocklist_ranking(
    _: ReadAuthDependency,
) -> dict[str, object]:
    return latest_restricted_blocklist_ranking()


@router.get(
    "/research/restricted-blocklist-history",
    response_model=RestrictedBlocklistHistoryResponse,
)
async def research_restricted_blocklist_history(
    _: ReadAuthDependency,
) -> dict[str, object]:
    return latest_restricted_blocklist_history()


@router.get("/research/runs", response_model=ResearchRunsResponse)
async def research_runs(_: ReadAuthDependency, limit: int = 20) -> dict[str, object]:
    return list_research_runs(limit=max(1, min(limit, 200)))


@router.get("/research/runs/{run_id}", response_model=ResearchRunDetailResponse)
async def research_run_detail(
    run_id: str, _: ReadAuthDependency
) -> dict[str, object]:
    return get_research_run(run_id)


@router.get("/reconciliation/status", response_model=ReconciliationStatusResponse)
async def reconciliation_status(
    _: ReadAuthDependency, limit: int = 100
) -> dict[str, object]:
    redis = cast(RedisLike, await get_redis())
    postgres_pool = await postgres_pool_or_503()
    bounded_limit = max(1, min(limit, 500))
    if postgres_pool is not None:
        return await reconciliation_status_from_postgres(postgres_pool, bounded_limit)
    return await reconciliation_status_fallback(redis, count=bounded_limit)


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


async def postgres_pool_or_503() -> object | None:
    try:
        return await require_pool()
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc


dist_path = frontend_dist_path()
app.include_router(router)
app.include_router(router, prefix="/api")
if (dist_path / "index.html").exists():
    app.mount("/", StaticFiles(directory=dist_path, html=True), name="dashboard")
