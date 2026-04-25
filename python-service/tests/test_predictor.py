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
