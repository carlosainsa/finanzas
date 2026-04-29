import json
import asyncio
import logging

from pydantic import ValidationError

from src.config import settings
from src.data.redis_client import ensure_stream_group, get_redis, publish_json
from src.ml.predictor import Predictor
from src.schemas import OrderBook, TradeSignal

predictor = Predictor()
logger = logging.getLogger(__name__)


async def run():
    redis = await get_redis()
    await ensure_stream_group(
        redis, settings.orderbook_stream, settings.orderbook_consumer_group
    )

    while True:
        messages = await redis.xreadgroup(
            settings.orderbook_consumer_group,
            settings.orderbook_consumer_name,
            streams={settings.orderbook_stream: ">"},
            count=1,
            block=5000,
        )
        if not messages:
            continue

        _, entries = messages[0]
        message_id, fields = entries[0]
        payload = fields.get("payload")

        try:
            if payload is None:
                raise ValueError("stream entry missing payload field")
            data = json.loads(payload)
            orderbook = OrderBook.model_validate(data)
        except (json.JSONDecodeError, ValidationError, ValueError) as exc:
            logger.warning("invalid orderbook payload: %s", exc)
            await publish_json(
                redis,
                settings.orderbook_deadletter_stream,
                json.dumps(
                    {
                        "stream_id": message_id,
                        "error": str(exc),
                        "payload": payload,
                    }
                ),
            )
            await redis.xack(
                settings.orderbook_stream,
                settings.orderbook_consumer_group,
                message_id,
            )
            continue

        decision = predictor.evaluate(orderbook)

        if decision.signal:
            validated_signal = TradeSignal.model_validate(decision.signal.model_dump())
            await publish_json(
                redis, settings.signals_stream, validated_signal.model_dump_json()
            )

        await redis.xack(
            settings.orderbook_stream,
            settings.orderbook_consumer_group,
            message_id,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())
