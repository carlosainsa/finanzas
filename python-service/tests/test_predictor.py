import pytest

from src.config import settings
from src.ml.predictor import Predictor
from src.schemas import OrderBook


def make_book(bid: float, ask: float) -> OrderBook:
    return OrderBook.model_validate(
        {
            "market_id": "0xabc",
            "asset_id": "123",
            "bids": [{"price": bid, "size": 3.0}],
            "asks": [{"price": ask, "size": 5.0}],
            "timestamp_ms": 1760000000000,
        }
    )


def test_predictor_returns_none_for_low_spread() -> None:
    assert Predictor().predict(make_book(0.45, 0.46)) is None


def test_predictor_returns_valid_signal_for_wide_spread() -> None:
    signal = Predictor().predict(make_book(0.45, 0.50))

    assert signal is not None
    assert signal.side == "BUY"
    assert signal.price == 0.45
    assert signal.source_timestamp_ms == 1760000000000
    assert signal.strategy == "passive_spread_capture_v1"
    assert signal.model_version == "passive_spread_capture_v1"
    assert signal.data_version == "redis_orderbook_v1"
    assert signal.feature_version == "orderbook_top_of_book_v1"


def test_predictor_near_touch_quote_is_dry_run_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "predictor_quote_placement", "near_touch")
    monkeypatch.setattr(settings, "execution_mode", "dry_run")
    monkeypatch.setattr(settings, "app_env", "development")
    monkeypatch.setattr(settings, "predictor_near_touch_tick_size", 0.01)
    monkeypatch.setattr(settings, "predictor_near_touch_offset_ticks", 0)
    monkeypatch.setattr(settings, "predictor_near_touch_max_spread_fraction", 1.0)

    signal = Predictor().predict(make_book(0.45, 0.50))

    assert signal is not None
    assert signal.price == 0.50
    assert signal.strategy == "passive_spread_capture_near_touch_v1"
    assert signal.feature_version == "orderbook_top_of_book_near_touch_v1"


def test_predictor_rejects_near_touch_in_live_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "predictor_quote_placement", "near_touch")
    monkeypatch.setattr(settings, "execution_mode", "live")

    with pytest.raises(RuntimeError, match="only allowed for dry_run research"):
        Predictor().predict(make_book(0.45, 0.50))


def test_predictor_returns_none_without_liquidity() -> None:
    book = OrderBook.model_validate(
        {
            "market_id": "0xabc",
            "asset_id": "123",
            "bids": [],
            "asks": [],
            "timestamp_ms": 1760000000000,
        }
    )

    assert Predictor().predict(book) is None
