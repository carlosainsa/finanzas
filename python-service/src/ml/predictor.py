import time
import uuid

from src.config import settings
from src.schemas import OrderBook, TradeSignal

MODEL_VERSION = "passive_spread_capture_v1"
FEATURE_VERSION = "orderbook_top_of_book_v1"
DATA_VERSION = "redis_orderbook_v1"
NEAR_TOUCH_MODEL_VERSION = "passive_spread_capture_near_touch_v1"
NEAR_TOUCH_FEATURE_VERSION = "orderbook_top_of_book_near_touch_v1"


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

        quote_price, model_version, feature_version = quote_price_for_buy(
            best_bid.price,
            best_ask.price,
        )
        quote_depth = best_ask.size if model_version == NEAR_TOUCH_MODEL_VERSION else best_bid.size

        return TradeSignal(
            signal_id=str(uuid.uuid4()),
            market_id=orderbook.market_id,
            asset_id=orderbook.asset_id,
            side="BUY",
            price=quote_price,
            size=min(settings.predictor_order_size, quote_depth),
            confidence=confidence,
            timestamp_ms=int(time.time() * 1000),
            source_timestamp_ms=orderbook.timestamp_ms,
            strategy=model_version,
            model_version=model_version,
            data_version=DATA_VERSION,
            feature_version=feature_version,
        )


def quote_price_for_buy(best_bid: float, best_ask: float) -> tuple[float, str, str]:
    placement = settings.predictor_quote_placement.lower()
    if placement == "passive_bid":
        return best_bid, MODEL_VERSION, FEATURE_VERSION
    if placement != "near_touch":
        raise ValueError(f"unsupported predictor quote placement: {placement}")
    validate_near_touch_allowed()
    spread = best_ask - best_bid
    if spread <= 0:
        return best_bid, NEAR_TOUCH_MODEL_VERSION, NEAR_TOUCH_FEATURE_VERSION
    tick_size = settings.predictor_near_touch_tick_size
    offset = settings.predictor_near_touch_offset_ticks * tick_size
    cap = best_ask - offset
    fractional_price = best_bid + (
        spread * settings.predictor_near_touch_max_spread_fraction
    )
    price = max(best_bid, min(cap, fractional_price))
    return round(price, 6), NEAR_TOUCH_MODEL_VERSION, NEAR_TOUCH_FEATURE_VERSION


def validate_near_touch_allowed() -> None:
    execution_mode = settings.execution_mode.lower()
    app_env = settings.app_env.lower()
    if settings.predictor_near_touch_research_only and (
        execution_mode != "dry_run" or app_env == "production"
    ):
        raise RuntimeError(
            "near-touch predictor quote placement is only allowed for dry_run research"
        )
    if settings.predictor_near_touch_tick_size < 0:
        raise ValueError("predictor near-touch tick size must be non-negative")
    if settings.predictor_near_touch_offset_ticks < 0:
        raise ValueError("predictor near-touch offset ticks must be non-negative")
    if not 0 <= settings.predictor_near_touch_max_spread_fraction <= 1:
        raise ValueError(
            "predictor near-touch max spread fraction must be between 0 and 1"
        )
