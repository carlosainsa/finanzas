import json
import asyncio
import logging
import time

from pydantic import ValidationError

from src.config import settings
from src.data.redis_client import ensure_stream_group, get_redis, publish_json
from src.ml.predictor import DATA_VERSION, PredictionDecision, Predictor
from src.schemas import OrderBook, PredictorDecisionTrace, TradeSignal

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
        trace = build_predictor_decision_trace(message_id, orderbook, decision)
        await publish_json(
            redis,
            settings.predictor_decisions_stream,
            trace.model_dump_json(),
        )

        await redis.xack(
            settings.orderbook_stream,
            settings.orderbook_consumer_group,
            message_id,
        )


def build_predictor_decision_trace(
    stream_id: str,
    orderbook: OrderBook,
    decision: PredictionDecision,
) -> PredictorDecisionTrace:
    signal = decision.signal
    best_bid = orderbook.best_bid
    best_ask = orderbook.best_ask
    return PredictorDecisionTrace(
        stream_id=stream_id,
        market_id=orderbook.market_id,
        asset_id=orderbook.asset_id,
        accepted=decision.accepted,
        rejection_reason=decision.rejection_reason,
        strategy_profile=decision.strategy_profile,
        timestamp_ms=int(time.time() * 1000),
        source_timestamp_ms=orderbook.timestamp_ms,
        side=signal.side if signal else None,
        signal_id=signal.signal_id if signal else None,
        price=signal.price if signal else None,
        size=signal.size if signal else None,
        confidence=decision.confidence,
        spread=decision.spread,
        best_bid=best_bid.price if best_bid else None,
        best_ask=best_ask.price if best_ask else None,
        bid_depth=best_bid.size if best_bid else None,
        ask_depth=best_ask.size if best_ask else None,
        top_change_count=decision.top_change_count,
        model_version=decision.model_version,
        data_version=signal.data_version if signal else DATA_VERSION,
        feature_version=decision.feature_version,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())
