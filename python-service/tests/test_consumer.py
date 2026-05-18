from src.data.consumer import build_predictor_decision_trace
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


def test_build_predictor_decision_trace_records_rejection_reason() -> None:
    orderbook = make_book(0.45, 0.46)
    decision = Predictor().evaluate(orderbook)

    trace = build_predictor_decision_trace("1-0", orderbook, decision)

    assert trace.stream_id == "1-0"
    assert trace.accepted is False
    assert trace.rejection_reason == "low_spread"
    assert trace.market_id == "0xabc"
    assert trace.asset_id == "123"
    assert trace.best_bid == 0.45
    assert trace.best_ask == 0.46
    assert trace.bid_depth == 3.0
    assert trace.ask_depth == 5.0
    assert trace.data_version == "redis_orderbook_v1"


def test_build_predictor_decision_trace_records_signal_id_when_accepted() -> None:
    orderbook = make_book(0.45, 0.50)
    decision = Predictor().evaluate(orderbook)

    trace = build_predictor_decision_trace("2-0", orderbook, decision)

    assert trace.accepted is True
    assert trace.rejection_reason == "accepted"
    assert trace.signal_id is not None
    assert trace.side == "BUY"
    assert trace.price == 0.45
    assert trace.size == 1.0
