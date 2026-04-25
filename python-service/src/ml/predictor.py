import time
import uuid

from src.config import settings
from src.schemas import OrderBook, TradeSignal


class Predictor:
    """
    Estrategia base conservadora.

    Emite una orden pasiva de compra solo cuando el spread observado supera
    el umbral configurado. El modelo ML real puede reemplazar esta clase sin
    cambiar el contrato Redis.
    """

    def predict(self, orderbook: OrderBook) -> TradeSignal | None:
        best_bid = orderbook.best_bid
        best_ask = orderbook.best_ask
        if best_bid is None or best_ask is None:
            return None

        spread = best_ask.price - best_bid.price
        if spread < settings.predictor_min_spread:
            return None

        confidence = min(0.99, 0.5 + spread * 5)
        if confidence < settings.predictor_min_confidence:
            return None

        return TradeSignal(
            signal_id=str(uuid.uuid4()),
            market_id=orderbook.market_id,
            asset_id=orderbook.asset_id,
            side="BUY",
            price=best_bid.price,
            size=min(settings.predictor_order_size, best_bid.size),
            confidence=confidence,
            timestamp_ms=int(time.time() * 1000),
            source_timestamp_ms=orderbook.timestamp_ms,
            strategy="passive_spread_capture_v1",
        )
