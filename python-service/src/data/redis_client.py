import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from src.config import settings

_redis: aioredis.Redis | None = None


async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(settings.redis_url, decode_responses=True)
    return _redis


async def ensure_stream_group(redis: aioredis.Redis, stream: str, group: str) -> None:
    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def publish_json(redis: aioredis.Redis, stream: str, payload: str) -> str:
    return await redis.xadd(stream, {"payload": payload})
