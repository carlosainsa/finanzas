import time
import uuid
from dataclasses import dataclass
from typing import Literal

from src.config import settings
from src.ml.execution_probe_selection import (
    load_execution_probe_v5_fraction_selection,
    load_execution_probe_v6_fraction_selection,
    load_execution_probe_v7_fraction_selection,
)
from src.ml.segment_blocklist import SegmentBlocklist
from src.schemas import OrderBook, TradeSignal

MODEL_VERSION = "passive_spread_capture_v1"
FEATURE_VERSION = "orderbook_top_of_book_v1"
DATA_VERSION = "redis_orderbook_v1"
NEAR_TOUCH_MODEL_VERSION = "passive_spread_capture_near_touch_v1"
NEAR_TOUCH_FEATURE_VERSION = "orderbook_top_of_book_near_touch_v1"
CONSERVATIVE_MODEL_VERSION = "passive_spread_capture_conservative_v1"
CONSERVATIVE_FEATURE_VERSION = "orderbook_top_of_book_conservative_v1"
CONSERVATIVE_NEAR_TOUCH_MODEL_VERSION = "passive_spread_capture_conservative_near_touch_v1"
CONSERVATIVE_NEAR_TOUCH_FEATURE_VERSION = (
    "orderbook_top_of_book_conservative_near_touch_v1"
)
BALANCED_MODEL_VERSION = "passive_spread_capture_balanced_v1"
BALANCED_FEATURE_VERSION = "orderbook_top_of_book_balanced_v1"
BALANCED_NEAR_TOUCH_MODEL_VERSION = "passive_spread_capture_balanced_near_touch_v1"
BALANCED_NEAR_TOUCH_FEATURE_VERSION = "orderbook_top_of_book_balanced_near_touch_v1"
EXECUTION_PROBE_MODEL_VERSION = "passive_spread_capture_execution_probe_v1"
EXECUTION_PROBE_FEATURE_VERSION = "orderbook_top_of_book_execution_probe_v1"
EXECUTION_PROBE_NEAR_TOUCH_MODEL_VERSION = (
    "passive_spread_capture_execution_probe_near_touch_v1"
)
EXECUTION_PROBE_NEAR_TOUCH_FEATURE_VERSION = (
    "orderbook_top_of_book_execution_probe_near_touch_v1"
)
EXECUTION_PROBE_V2_MODEL_VERSION = "passive_spread_capture_execution_probe_v2"
EXECUTION_PROBE_V2_FEATURE_VERSION = "orderbook_top_of_book_execution_probe_v2"
EXECUTION_PROBE_V2_NEAR_TOUCH_MODEL_VERSION = (
    "passive_spread_capture_execution_probe_near_touch_v2"
)
EXECUTION_PROBE_V2_NEAR_TOUCH_FEATURE_VERSION = (
    "orderbook_top_of_book_execution_probe_near_touch_v2"
)
EXECUTION_PROBE_V3_MODEL_VERSION = "passive_spread_capture_execution_probe_v3"
EXECUTION_PROBE_V3_FEATURE_VERSION = "orderbook_top_of_book_execution_probe_v3"
EXECUTION_PROBE_V3_NEAR_TOUCH_MODEL_VERSION = (
    "passive_spread_capture_execution_probe_near_touch_v3"
)
EXECUTION_PROBE_V3_NEAR_TOUCH_FEATURE_VERSION = (
    "orderbook_top_of_book_execution_probe_near_touch_v3"
)
EXECUTION_PROBE_V4_MODEL_VERSION = "passive_spread_capture_execution_probe_v4"
EXECUTION_PROBE_V4_FEATURE_VERSION = "orderbook_top_of_book_execution_probe_v4"
EXECUTION_PROBE_V4_NEAR_TOUCH_MODEL_VERSION = (
    "passive_spread_capture_execution_probe_near_touch_v4"
)
EXECUTION_PROBE_V4_NEAR_TOUCH_FEATURE_VERSION = (
    "orderbook_top_of_book_execution_probe_near_touch_v4"
)
EXECUTION_PROBE_V5_MODEL_VERSION = "passive_spread_capture_execution_probe_v5"
EXECUTION_PROBE_V5_FEATURE_VERSION = "orderbook_top_of_book_execution_probe_v5"
EXECUTION_PROBE_V5_NEAR_TOUCH_MODEL_VERSION = (
    "passive_spread_capture_execution_probe_near_touch_v5"
)
EXECUTION_PROBE_V5_NEAR_TOUCH_FEATURE_VERSION = (
    "orderbook_top_of_book_execution_probe_near_touch_v5"
)
EXECUTION_PROBE_V6_MODEL_VERSION = "passive_spread_capture_execution_probe_v6"
EXECUTION_PROBE_V6_FEATURE_VERSION = "orderbook_top_of_book_execution_probe_v6"
EXECUTION_PROBE_V6_NEAR_TOUCH_MODEL_VERSION = (
    "passive_spread_capture_execution_probe_near_touch_v6"
)
EXECUTION_PROBE_V6_NEAR_TOUCH_FEATURE_VERSION = (
    "orderbook_top_of_book_execution_probe_near_touch_v6"
)
EXECUTION_PROBE_V7_MODEL_VERSION = "passive_spread_capture_execution_probe_v7"
EXECUTION_PROBE_V7_FEATURE_VERSION = "orderbook_top_of_book_execution_probe_v7"
EXECUTION_PROBE_V7_NEAR_TOUCH_MODEL_VERSION = (
    "passive_spread_capture_execution_probe_near_touch_v7"
)
EXECUTION_PROBE_V7_NEAR_TOUCH_FEATURE_VERSION = (
    "orderbook_top_of_book_execution_probe_near_touch_v7"
)

TOP_CHANGE_EPSILON = 1e-9
RejectionReason = Literal[
    "accepted",
    "missing_top_of_book",
    "low_spread",
    "low_depth",
    "top_rotation",
    "rate_limited",
    "low_confidence",
    "blocked_segment",
]


@dataclass(frozen=True)
class PredictionDecision:
    signal: TradeSignal | None
    rejection_reason: RejectionReason
    strategy_profile: str
    model_version: str | None = None
    feature_version: str | None = None
    confidence: float | None = None
    spread: float | None = None
    top_change_count: int | None = None

    @property
    def accepted(self) -> bool:
        return self.signal is not None


class Predictor:
    """
    Estrategia base conservadora.

    Emite una orden pasiva de compra solo cuando el spread observado supera
    el umbral configurado. El modelo ML real puede reemplazar esta clase sin
    cambiar el contrato Redis.
    """

    def __init__(self, blocklist: SegmentBlocklist | None = None) -> None:
        self.blocklist = blocklist or SegmentBlocklist.from_file(
            settings.predictor_blocked_segments_path
        )
        self._top_of_book_history: dict[
            tuple[str, str], list[tuple[int, float, float]]
        ] = {}
        self._last_signal_timestamp_by_key: dict[tuple[str, str], int] = {}

    def predict(self, orderbook: OrderBook) -> TradeSignal | None:
        return self.evaluate(orderbook).signal

    def evaluate(self, orderbook: OrderBook) -> PredictionDecision:
        profile = strategy_profile()
        best_bid = orderbook.best_bid
        best_ask = orderbook.best_ask
        if best_bid is None or best_ask is None:
            return PredictionDecision(
                signal=None,
                rejection_reason="missing_top_of_book",
                strategy_profile=profile.name,
            )

        spread = best_ask.price - best_bid.price
        if spread < settings.predictor_min_spread:
            return PredictionDecision(
                signal=None,
                rejection_reason="low_spread",
                strategy_profile=profile.name,
                spread=spread,
            )
        if profile.risk_filters_enabled:
            if (
                min(best_bid.size, best_ask.size)
                < profile.min_depth
            ):
                return PredictionDecision(
                    signal=None,
                    rejection_reason="low_depth",
                    strategy_profile=profile.name,
                    spread=spread,
                )
            top_change_count = self._top_of_book_change_count(orderbook)
            if top_change_count > profile.max_top_changes:
                return PredictionDecision(
                    signal=None,
                    rejection_reason="top_rotation",
                    strategy_profile=profile.name,
                    spread=spread,
                    top_change_count=top_change_count,
                )
            last_signal_timestamp_ms = self._last_signal_timestamp_by_key.get(
                (orderbook.market_id, orderbook.asset_id)
            )
            if (
                last_signal_timestamp_ms is not None
                and profile.min_signal_interval_ms > 0
                and orderbook.timestamp_ms - last_signal_timestamp_ms
                < profile.min_signal_interval_ms
            ):
                return PredictionDecision(
                    signal=None,
                    rejection_reason="rate_limited",
                    strategy_profile=profile.name,
                    spread=spread,
                    top_change_count=top_change_count,
                )
        else:
            top_change_count = None

        confidence = min(0.99, 0.5 + spread * 5)
        min_confidence = effective_min_confidence()
        if confidence < min_confidence:
            return PredictionDecision(
                signal=None,
                rejection_reason="low_confidence",
                strategy_profile=profile.name,
                confidence=confidence,
                spread=spread,
                top_change_count=top_change_count,
            )

        quote_price, model_version, feature_version = quote_price_for_buy(
            best_bid.price,
            best_ask.price,
        )
        if self.blocklist.is_blocked(
            orderbook.market_id,
            orderbook.asset_id,
            "BUY",
            model_version,
        ):
            return PredictionDecision(
                signal=None,
                rejection_reason="blocked_segment",
                strategy_profile=profile.name,
                model_version=model_version,
                feature_version=feature_version,
                confidence=confidence,
                spread=spread,
                top_change_count=top_change_count,
            )
        quote_depth = best_ask.size if near_touch_model(model_version) else best_bid.size

        signal = TradeSignal(
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
        if profile.min_signal_interval_ms > 0:
            self._last_signal_timestamp_by_key[
                (orderbook.market_id, orderbook.asset_id)
            ] = orderbook.timestamp_ms
        return PredictionDecision(
            signal=signal,
            rejection_reason="accepted",
            strategy_profile=profile.name,
            model_version=model_version,
            feature_version=feature_version,
            confidence=confidence,
            spread=spread,
            top_change_count=top_change_count,
        )

    def _top_of_book_change_count(self, orderbook: OrderBook) -> int:
        best_bid = orderbook.best_bid
        best_ask = orderbook.best_ask
        if best_bid is None or best_ask is None:
            return 0
        key = (orderbook.market_id, orderbook.asset_id)
        timestamp_ms = orderbook.timestamp_ms
        profile = strategy_profile()
        window_start = timestamp_ms - profile.top_change_window_ms
        history = [
            item
            for item in self._top_of_book_history.get(key, [])
            if item[0] >= window_start
        ]
        current = (timestamp_ms, best_bid.price, best_ask.price)
        if not history:
            history.append(current)
            self._top_of_book_history[key] = history
            return 0
        changed = (
            abs(history[-1][1] - best_bid.price) > TOP_CHANGE_EPSILON
            or abs(history[-1][2] - best_ask.price) > TOP_CHANGE_EPSILON
        )
        if changed:
            history.append(current)
        self._top_of_book_history[key] = history
        return max(0, len(history) - 1)


def quote_price_for_buy(best_bid: float, best_ask: float) -> tuple[float, str, str]:
    placement = settings.predictor_quote_placement.lower()
    profile = strategy_profile()
    if placement == "passive_bid":
        if profile.name == "conservative_v1":
            return best_bid, CONSERVATIVE_MODEL_VERSION, CONSERVATIVE_FEATURE_VERSION
        if profile.name == "balanced_v1":
            return best_bid, BALANCED_MODEL_VERSION, BALANCED_FEATURE_VERSION
        if profile.name == "execution_probe_v1":
            return (
                best_bid,
                EXECUTION_PROBE_MODEL_VERSION,
                EXECUTION_PROBE_FEATURE_VERSION,
            )
        if profile.name == "execution_probe_v2":
            return (
                best_bid,
                EXECUTION_PROBE_V2_MODEL_VERSION,
                EXECUTION_PROBE_V2_FEATURE_VERSION,
            )
        if profile.name == "execution_probe_v3":
            return (
                best_bid,
                EXECUTION_PROBE_V3_MODEL_VERSION,
                EXECUTION_PROBE_V3_FEATURE_VERSION,
            )
        if profile.name == "execution_probe_v4":
            return (
                best_bid,
                EXECUTION_PROBE_V4_MODEL_VERSION,
                EXECUTION_PROBE_V4_FEATURE_VERSION,
            )
        if profile.name == "execution_probe_v5":
            return (
                best_bid,
                EXECUTION_PROBE_V5_MODEL_VERSION,
                EXECUTION_PROBE_V5_FEATURE_VERSION,
            )
        if profile.name == "execution_probe_v6":
            return (
                best_bid,
                EXECUTION_PROBE_V6_MODEL_VERSION,
                EXECUTION_PROBE_V6_FEATURE_VERSION,
            )
        if profile.name == "execution_probe_v7":
            return (
                best_bid,
                EXECUTION_PROBE_V7_MODEL_VERSION,
                EXECUTION_PROBE_V7_FEATURE_VERSION,
            )
        return best_bid, MODEL_VERSION, FEATURE_VERSION
    if placement != "near_touch":
        raise ValueError(f"unsupported predictor quote placement: {placement}")
    validate_near_touch_allowed()
    spread = best_ask - best_bid
    if spread <= 0:
        return best_bid, NEAR_TOUCH_MODEL_VERSION, NEAR_TOUCH_FEATURE_VERSION
    tick_size = settings.predictor_near_touch_tick_size
    offset = settings.predictor_near_touch_offset_ticks * tick_size
    if profile.name == "execution_probe_v6":
        offset = settings.predictor_execution_probe_v6_offset_ticks * tick_size
    if profile.name == "execution_probe_v7":
        offset = settings.predictor_execution_probe_v7_offset_ticks * tick_size
    cap = best_ask - offset
    max_spread_fraction = profile.near_touch_max_spread_fraction
    fractional_price = best_bid + (spread * max_spread_fraction)
    price = max(best_bid, min(cap, fractional_price))
    if profile.name == "conservative_v1":
        return (
            round(price, 6),
            CONSERVATIVE_NEAR_TOUCH_MODEL_VERSION,
            CONSERVATIVE_NEAR_TOUCH_FEATURE_VERSION,
        )
    if profile.name == "balanced_v1":
        return (
            round(price, 6),
            BALANCED_NEAR_TOUCH_MODEL_VERSION,
            BALANCED_NEAR_TOUCH_FEATURE_VERSION,
        )
    if profile.name == "execution_probe_v1":
        return (
            round(price, 6),
            EXECUTION_PROBE_NEAR_TOUCH_MODEL_VERSION,
            EXECUTION_PROBE_NEAR_TOUCH_FEATURE_VERSION,
        )
    if profile.name == "execution_probe_v2":
        return (
            round(price, 6),
            EXECUTION_PROBE_V2_NEAR_TOUCH_MODEL_VERSION,
            EXECUTION_PROBE_V2_NEAR_TOUCH_FEATURE_VERSION,
        )
    if profile.name == "execution_probe_v3":
        return (
            round(price, 6),
            EXECUTION_PROBE_V3_NEAR_TOUCH_MODEL_VERSION,
            EXECUTION_PROBE_V3_NEAR_TOUCH_FEATURE_VERSION,
        )
    if profile.name == "execution_probe_v4":
        return (
            round(price, 6),
            EXECUTION_PROBE_V4_NEAR_TOUCH_MODEL_VERSION,
            EXECUTION_PROBE_V4_NEAR_TOUCH_FEATURE_VERSION,
        )
    if profile.name == "execution_probe_v5":
        return (
            round(price, 6),
            EXECUTION_PROBE_V5_NEAR_TOUCH_MODEL_VERSION,
            EXECUTION_PROBE_V5_NEAR_TOUCH_FEATURE_VERSION,
        )
    if profile.name == "execution_probe_v6":
        return (
            round(price, 6),
            EXECUTION_PROBE_V6_NEAR_TOUCH_MODEL_VERSION,
            EXECUTION_PROBE_V6_NEAR_TOUCH_FEATURE_VERSION,
        )
    if profile.name == "execution_probe_v7":
        return (
            round(price, 6),
            EXECUTION_PROBE_V7_NEAR_TOUCH_MODEL_VERSION,
            EXECUTION_PROBE_V7_NEAR_TOUCH_FEATURE_VERSION,
        )
    return round(price, 6), NEAR_TOUCH_MODEL_VERSION, NEAR_TOUCH_FEATURE_VERSION


def conservative_profile_enabled() -> bool:
    return strategy_profile().name == "conservative_v1"


class StrategyProfile:
    def __init__(
        self,
        *,
        name: str,
        min_confidence: float,
        near_touch_max_spread_fraction: float,
        min_depth: float,
        max_top_changes: int,
        top_change_window_ms: int,
        risk_filters_enabled: bool,
        min_signal_interval_ms: int = 0,
    ) -> None:
        self.name = name
        self.min_confidence = min_confidence
        self.near_touch_max_spread_fraction = near_touch_max_spread_fraction
        self.min_depth = min_depth
        self.max_top_changes = max_top_changes
        self.top_change_window_ms = top_change_window_ms
        self.risk_filters_enabled = risk_filters_enabled
        self.min_signal_interval_ms = min_signal_interval_ms


def strategy_profile() -> StrategyProfile:
    profile = settings.predictor_strategy_profile.lower()
    if profile in {"baseline", "default"}:
        return StrategyProfile(
            name="baseline",
            min_confidence=settings.predictor_min_confidence,
            near_touch_max_spread_fraction=settings.predictor_near_touch_max_spread_fraction,
            min_depth=0.0,
            max_top_changes=0,
            top_change_window_ms=0,
            risk_filters_enabled=False,
        )
    if profile == "balanced_v1":
        return StrategyProfile(
            name=profile,
            min_confidence=max(
                settings.predictor_min_confidence,
                settings.predictor_balanced_min_confidence,
            ),
            near_touch_max_spread_fraction=(
                settings.predictor_balanced_near_touch_max_spread_fraction
            ),
            min_depth=settings.predictor_balanced_min_depth,
            max_top_changes=settings.predictor_balanced_max_top_changes,
            top_change_window_ms=settings.predictor_balanced_top_change_window_ms,
            risk_filters_enabled=True,
        )
    if profile == "execution_probe_v1":
        validate_execution_probe_allowed()
        return StrategyProfile(
            name=profile,
            min_confidence=max(
                settings.predictor_min_confidence,
                settings.predictor_execution_probe_min_confidence,
            ),
            near_touch_max_spread_fraction=(
                settings.predictor_execution_probe_near_touch_max_spread_fraction
            ),
            min_depth=settings.predictor_execution_probe_min_depth,
            max_top_changes=settings.predictor_execution_probe_max_top_changes,
            top_change_window_ms=settings.predictor_execution_probe_top_change_window_ms,
            risk_filters_enabled=True,
            min_signal_interval_ms=(
                settings.predictor_execution_probe_min_signal_interval_ms
            ),
        )
    if profile == "execution_probe_v2":
        validate_execution_probe_allowed()
        return StrategyProfile(
            name=profile,
            min_confidence=max(
                settings.predictor_min_confidence,
                settings.predictor_execution_probe_v2_min_confidence,
            ),
            near_touch_max_spread_fraction=(
                settings.predictor_execution_probe_v2_near_touch_max_spread_fraction
            ),
            min_depth=settings.predictor_execution_probe_v2_min_depth,
            max_top_changes=settings.predictor_execution_probe_v2_max_top_changes,
            top_change_window_ms=settings.predictor_execution_probe_v2_top_change_window_ms,
            risk_filters_enabled=True,
            min_signal_interval_ms=(
                settings.predictor_execution_probe_v2_min_signal_interval_ms
            ),
        )
    if profile == "execution_probe_v3":
        validate_execution_probe_allowed()
        return StrategyProfile(
            name=profile,
            min_confidence=max(
                settings.predictor_min_confidence,
                settings.predictor_execution_probe_v3_min_confidence,
            ),
            near_touch_max_spread_fraction=(
                settings.predictor_execution_probe_v3_near_touch_max_spread_fraction
            ),
            min_depth=settings.predictor_execution_probe_v3_min_depth,
            max_top_changes=settings.predictor_execution_probe_v3_max_top_changes,
            top_change_window_ms=settings.predictor_execution_probe_v3_top_change_window_ms,
            risk_filters_enabled=True,
            min_signal_interval_ms=(
                settings.predictor_execution_probe_v3_min_signal_interval_ms
            ),
        )
    if profile == "execution_probe_v4":
        validate_execution_probe_allowed()
        return StrategyProfile(
            name=profile,
            min_confidence=max(
                settings.predictor_min_confidence,
                settings.predictor_execution_probe_v4_min_confidence,
            ),
            near_touch_max_spread_fraction=(
                settings.predictor_execution_probe_v4_near_touch_max_spread_fraction
            ),
            min_depth=settings.predictor_execution_probe_v4_min_depth,
            max_top_changes=settings.predictor_execution_probe_v4_max_top_changes,
            top_change_window_ms=settings.predictor_execution_probe_v4_top_change_window_ms,
            risk_filters_enabled=True,
            min_signal_interval_ms=(
                settings.predictor_execution_probe_v4_min_signal_interval_ms
            ),
        )
    if profile == "execution_probe_v5":
        validate_execution_probe_allowed()
        selection = load_execution_probe_v5_fraction_selection(
            settings.predictor_execution_probe_v5_fraction_selection_path,
            default_fraction=(
                settings.predictor_execution_probe_v5_near_touch_max_spread_fraction
            ),
        )
        return StrategyProfile(
            name=profile,
            min_confidence=max(
                settings.predictor_min_confidence,
                settings.predictor_execution_probe_v5_min_confidence,
            ),
            near_touch_max_spread_fraction=(
                selection.near_touch_max_spread_fraction
            ),
            min_depth=settings.predictor_execution_probe_v5_min_depth,
            max_top_changes=settings.predictor_execution_probe_v5_max_top_changes,
            top_change_window_ms=settings.predictor_execution_probe_v5_top_change_window_ms,
            risk_filters_enabled=True,
            min_signal_interval_ms=(
                settings.predictor_execution_probe_v5_min_signal_interval_ms
            ),
        )
    if profile == "execution_probe_v6":
        validate_execution_probe_allowed()
        selection = load_execution_probe_v6_fraction_selection(
            settings.predictor_execution_probe_v6_fraction_selection_path,
            default_fraction=(
                settings.predictor_execution_probe_v6_near_touch_max_spread_fraction
            ),
        )
        return StrategyProfile(
            name=profile,
            min_confidence=max(
                settings.predictor_min_confidence,
                settings.predictor_execution_probe_v6_min_confidence,
            ),
            near_touch_max_spread_fraction=(
                selection.near_touch_max_spread_fraction
            ),
            min_depth=settings.predictor_execution_probe_v6_min_depth,
            max_top_changes=settings.predictor_execution_probe_v6_max_top_changes,
            top_change_window_ms=settings.predictor_execution_probe_v6_top_change_window_ms,
            risk_filters_enabled=True,
            min_signal_interval_ms=(
                settings.predictor_execution_probe_v6_min_signal_interval_ms
            ),
        )
    if profile == "execution_probe_v7":
        validate_execution_probe_allowed()
        selection = load_execution_probe_v7_fraction_selection(
            settings.predictor_execution_probe_v7_fraction_selection_path,
            default_fraction=(
                settings.predictor_execution_probe_v7_near_touch_max_spread_fraction
            ),
        )
        return StrategyProfile(
            name=profile,
            min_confidence=max(
                settings.predictor_min_confidence,
                settings.predictor_execution_probe_v7_min_confidence,
            ),
            near_touch_max_spread_fraction=(
                selection.near_touch_max_spread_fraction
            ),
            min_depth=settings.predictor_execution_probe_v7_min_depth,
            max_top_changes=settings.predictor_execution_probe_v7_max_top_changes,
            top_change_window_ms=settings.predictor_execution_probe_v7_top_change_window_ms,
            risk_filters_enabled=True,
            min_signal_interval_ms=(
                settings.predictor_execution_probe_v7_min_signal_interval_ms
            ),
        )
    if profile == "conservative_v1":
        return StrategyProfile(
            name=profile,
            min_confidence=max(
                settings.predictor_min_confidence,
                settings.predictor_conservative_min_confidence,
            ),
            near_touch_max_spread_fraction=(
                settings.predictor_conservative_near_touch_max_spread_fraction
            ),
            min_depth=settings.predictor_conservative_min_depth,
            max_top_changes=settings.predictor_conservative_max_top_changes,
            top_change_window_ms=settings.predictor_conservative_top_change_window_ms,
            risk_filters_enabled=True,
        )
    raise ValueError(f"unsupported predictor strategy profile: {profile}")


def effective_min_confidence() -> float:
    return strategy_profile().min_confidence


def near_touch_model(model_version: str) -> bool:
    return model_version in {
        NEAR_TOUCH_MODEL_VERSION,
        CONSERVATIVE_NEAR_TOUCH_MODEL_VERSION,
        BALANCED_NEAR_TOUCH_MODEL_VERSION,
        EXECUTION_PROBE_NEAR_TOUCH_MODEL_VERSION,
        EXECUTION_PROBE_V2_NEAR_TOUCH_MODEL_VERSION,
        EXECUTION_PROBE_V3_NEAR_TOUCH_MODEL_VERSION,
        EXECUTION_PROBE_V4_NEAR_TOUCH_MODEL_VERSION,
        EXECUTION_PROBE_V5_NEAR_TOUCH_MODEL_VERSION,
        EXECUTION_PROBE_V6_NEAR_TOUCH_MODEL_VERSION,
        EXECUTION_PROBE_V7_NEAR_TOUCH_MODEL_VERSION,
    }


def validate_execution_probe_allowed() -> None:
    execution_mode = settings.execution_mode.lower()
    app_env = settings.app_env.lower()
    if execution_mode != "dry_run" or app_env == "production":
        raise RuntimeError(
            "execution probe predictor profiles are only allowed for dry_run research"
        )


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
    if not 0 <= settings.predictor_conservative_near_touch_max_spread_fraction <= 1:
        raise ValueError(
            "predictor conservative near-touch max spread fraction must be between 0 and 1"
        )
    if not 0 <= settings.predictor_balanced_near_touch_max_spread_fraction <= 1:
        raise ValueError(
            "predictor balanced near-touch max spread fraction must be between 0 and 1"
        )
    if (
        not 0
        <= settings.predictor_execution_probe_near_touch_max_spread_fraction
        <= 1
    ):
        raise ValueError(
            "predictor execution probe near-touch max spread fraction must be between 0 and 1"
        )
    if (
        not 0
        <= settings.predictor_execution_probe_v2_near_touch_max_spread_fraction
        <= 1
    ):
        raise ValueError(
            "predictor execution probe v2 near-touch max spread fraction must be between 0 and 1"
        )
    if (
        not 0
        <= settings.predictor_execution_probe_v3_near_touch_max_spread_fraction
        <= 1
    ):
        raise ValueError(
            "predictor execution probe v3 near-touch max spread fraction must be between 0 and 1"
        )
    if (
        not 0
        <= settings.predictor_execution_probe_v4_near_touch_max_spread_fraction
        <= 1
    ):
        raise ValueError(
            "predictor execution probe v4 near-touch max spread fraction must be between 0 and 1"
        )
    if (
        not 0
        <= settings.predictor_execution_probe_v5_near_touch_max_spread_fraction
        <= 1
    ):
        raise ValueError(
            "predictor execution probe v5 near-touch max spread fraction must be between 0 and 1"
        )
    if (
        not 0
        <= settings.predictor_execution_probe_v6_near_touch_max_spread_fraction
        <= 1
    ):
        raise ValueError(
            "predictor execution probe v6 near-touch max spread fraction must be between 0 and 1"
        )
    if settings.predictor_execution_probe_v6_offset_ticks < 0:
        raise ValueError("predictor execution probe v6 offset ticks must be non-negative")
    if (
        not 0
        <= settings.predictor_execution_probe_v7_near_touch_max_spread_fraction
        <= 1
    ):
        raise ValueError(
            "predictor execution probe v7 near-touch max spread fraction must be between 0 and 1"
        )
    if settings.predictor_execution_probe_v7_offset_ticks < 0:
        raise ValueError("predictor execution probe v7 offset ticks must be non-negative")
