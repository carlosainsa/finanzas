import pytest
from pydantic import ValidationError

from src.schemas import ExecutionReport, OrderBook, TradeSignal


def test_orderbook_rejects_extra_fields() -> None:
    with pytest.raises(ValidationError):
        OrderBook.model_validate(
            {
                "market_id": "0xabc",
                "asset_id": "123",
                "bids": [],
                "asks": [],
                "timestamp_ms": 1760000000000,
                "extra": True,
            }
        )


def test_orderbook_rejects_unsorted_bids() -> None:
    with pytest.raises(ValidationError):
        OrderBook.model_validate(
            {
                "market_id": "0xabc",
                "asset_id": "123",
                "bids": [{"price": 0.40, "size": 1}, {"price": 0.41, "size": 1}],
                "asks": [{"price": 0.50, "size": 1}],
                "timestamp_ms": 1760000000000,
            }
        )


def test_trade_signal_contract() -> None:
    signal = TradeSignal.model_validate(
        {
            "signal_id": "signal-1",
            "market_id": "0xabc",
            "asset_id": "123",
            "side": "BUY",
            "price": 0.45,
            "size": 1.0,
            "confidence": 0.8,
            "timestamp_ms": 1760000000000,
            "source_timestamp_ms": 1759999999990,
        }
    )

    assert signal.side == "BUY"
    assert signal.source_timestamp_ms == 1759999999990


def test_execution_report_contract() -> None:
    report = ExecutionReport.model_validate(
        {
            "signal_id": "signal-1",
            "order_id": "order-1",
            "status": "PARTIAL",
            "filled_size": 2.0,
            "cumulative_filled_size": 2.0,
            "remaining_size": 8.0,
            "timestamp_ms": 1760000000000,
        }
    )

    assert report.status == "PARTIAL"
    assert report.remaining_size == 8.0
