import pytest
from pydantic import ValidationError

from src.schemas import (
    ExecutionReport,
    ExternalEvidence,
    OrderBook,
    SentimentFeature,
    TradeSignal,
)


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
    assert signal.model_version is None


def test_trade_signal_accepts_optional_versions() -> None:
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
            "model_version": "model-v1",
            "data_version": "data-v1",
            "feature_version": "features-v1",
        }
    )

    assert signal.model_version == "model-v1"
    assert signal.data_version == "data-v1"
    assert signal.feature_version == "features-v1"


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


def test_external_evidence_rejects_future_observation_leakage() -> None:
    with pytest.raises(ValidationError):
        ExternalEvidence.model_validate(
            {
                "evidence_id": "evidence-1",
                "source": "newswire",
                "source_type": "news",
                "published_at_ms": 2_000,
                "observed_at_ms": 1_000,
                "market_id": "market-1",
                "raw_reference_hash": "sha256:abc",
                "data_version": "external_evidence_v1",
            }
        )


def test_sentiment_feature_contract() -> None:
    feature = SentimentFeature.model_validate(
        {
            "feature_id": "sentiment-1",
            "evidence_id": "evidence-1",
            "market_id": "market-1",
            "asset_id": "asset-yes",
            "observed_at_ms": 1_000,
            "feature_timestamp_ms": 1_100,
            "direction": "YES",
            "sentiment_score": 0.5,
            "source_quality": 0.8,
            "confidence": 0.7,
            "model_version": "sentiment_baseline_v1",
            "data_version": "external_evidence_v1",
            "feature_version": "sentiment_features_v1",
        }
    )

    assert feature.direction == "YES"
    assert feature.sentiment_score == 0.5
