import json
import asyncio
import logging

from pydantic import ValidationError

from src.data.redis_client import get_redis
from src.ml.predictor import Predictor
from src.schemas import OrderBook, TradeSignal

predictor = Predictor()
logger = logging.getLogger(__name__)


async def run():
    redis = await get_redis()
    pubsub = redis.pubsub()
    await pubsub.subscribe("orderbook:raw")

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue

        try:
            data = json.loads(message["data"])
            orderbook = OrderBook.model_validate(data)
        except (json.JSONDecodeError, ValidationError) as exc:
            logger.warning("invalid orderbook payload: %s", exc)
            continue

        signal = predictor.predict(orderbook)

        if signal:
            validated_signal = TradeSignal.model_validate(signal.model_dump())
            await redis.publish("signals:trade", validated_signal.model_dump_json())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())
