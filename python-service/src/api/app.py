from fastapi import FastAPI

from src.config import settings
from src.data.redis_client import get_redis

app = FastAPI(title="Polymarket Trading Control API")


@app.get("/health")
async def health() -> dict[str, str]:
    redis = await get_redis()
    await redis.ping()
    return {"status": "ok"}


@app.get("/status")
async def status() -> dict[str, object]:
    redis = await get_redis()
    streams = [
        settings.orderbook_stream,
        settings.signals_stream,
        settings.execution_reports_stream,
        settings.orderbook_deadletter_stream,
    ]
    lengths = {}
    for stream in streams:
        try:
            lengths[stream] = await redis.xlen(stream)
        except Exception:
            lengths[stream] = 0
    return {
        "status": "ok",
        "streams": lengths,
        "predictor": {
            "min_spread": settings.predictor_min_spread,
            "order_size": settings.predictor_order_size,
            "min_confidence": settings.predictor_min_confidence,
        },
    }


@app.get("/risk")
async def risk() -> dict[str, object]:
    return {
        "status": "python-service-readonly",
        "message": "Risk enforcement is performed by rust-engine before execution.",
    }
