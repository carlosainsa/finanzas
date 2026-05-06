import pytest
from pathlib import Path

from src.config import settings
from src.ml.predictor import Predictor
from src.ml.segment_blocklist import BlockedSegment, SegmentBlocklist
from src.schemas import OrderBook


def make_book(bid: float, ask: float, *, timestamp_ms: int = 1760000000000) -> OrderBook:
    return OrderBook.model_validate(
        {
            "market_id": "0xabc",
            "asset_id": "123",
            "bids": [{"price": bid, "size": 3.0}],
            "asks": [{"price": ask, "size": 5.0}],
            "timestamp_ms": timestamp_ms,
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


def test_predictor_skips_blocked_segment() -> None:
    blocklist = SegmentBlocklist(
        [
            BlockedSegment(
                market_id="0xabc",
                asset_id="123",
                side="BUY",
                model_version="passive_spread_capture_v1",
                reason="bounded_drawdown",
            )
        ]
    )

    assert Predictor(blocklist=blocklist).predict(make_book(0.45, 0.50)) is None


def test_predictor_loads_blocked_segments_file(tmp_path: Path) -> None:
    blocklist_path = tmp_path / "blocked_segments.json"
    blocklist_path.write_text(
        """
        {
          "version": "blocked_segments_v1",
          "segments": [
            {
              "market_id": "0xabc",
              "asset_id": "123",
              "side": "BUY",
              "model_version": "passive_spread_capture_v1",
              "reason": "positive_realized_edge"
            }
          ]
        }
        """,
        encoding="utf-8",
    )

    predictor = Predictor(blocklist=SegmentBlocklist.from_file(blocklist_path))

    assert predictor.predict(make_book(0.45, 0.50)) is None


def test_blocked_segments_rejects_unknown_version(tmp_path: Path) -> None:
    blocklist_path = tmp_path / "blocked_segments.json"
    blocklist_path.write_text(
        '{"version": "blocked_segments_v0", "segments": []}',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unsupported blocked segments version"):
        SegmentBlocklist.from_file(blocklist_path)


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


def test_conservative_predictor_requires_higher_confidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "predictor_strategy_profile", "conservative_v1")
    monkeypatch.setattr(settings, "predictor_conservative_min_confidence", 0.80)
    monkeypatch.setattr(settings, "predictor_conservative_min_depth", 1.0)

    assert Predictor().predict(make_book(0.45, 0.50)) is None
    assert Predictor().predict(make_book(0.45, 0.52)) is not None


def test_conservative_predictor_uses_less_aggressive_near_touch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "predictor_strategy_profile", "conservative_v1")
    monkeypatch.setattr(settings, "predictor_quote_placement", "near_touch")
    monkeypatch.setattr(settings, "execution_mode", "dry_run")
    monkeypatch.setattr(settings, "app_env", "development")
    monkeypatch.setattr(settings, "predictor_conservative_min_confidence", 0.55)
    monkeypatch.setattr(settings, "predictor_conservative_min_depth", 1.0)
    monkeypatch.setattr(settings, "predictor_near_touch_tick_size", 0.01)
    monkeypatch.setattr(settings, "predictor_near_touch_offset_ticks", 0)
    monkeypatch.setattr(settings, "predictor_conservative_near_touch_max_spread_fraction", 0.5)

    signal = Predictor().predict(make_book(0.45, 0.50))

    assert signal is not None
    assert signal.price == 0.475
    assert signal.strategy == "passive_spread_capture_conservative_near_touch_v1"
    assert signal.feature_version == "orderbook_top_of_book_conservative_near_touch_v1"


def test_balanced_predictor_uses_intermediate_near_touch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "predictor_strategy_profile", "balanced_v1")
    monkeypatch.setattr(settings, "predictor_quote_placement", "near_touch")
    monkeypatch.setattr(settings, "execution_mode", "dry_run")
    monkeypatch.setattr(settings, "app_env", "development")
    monkeypatch.setattr(settings, "predictor_balanced_min_confidence", 0.55)
    monkeypatch.setattr(settings, "predictor_balanced_min_depth", 1.0)
    monkeypatch.setattr(settings, "predictor_near_touch_tick_size", 0.01)
    monkeypatch.setattr(settings, "predictor_near_touch_offset_ticks", 0)
    monkeypatch.setattr(settings, "predictor_balanced_near_touch_max_spread_fraction", 0.75)

    signal = Predictor().predict(make_book(0.45, 0.50))

    assert signal is not None
    assert signal.price == 0.4875
    assert signal.size == 1.0
    assert signal.strategy == "passive_spread_capture_balanced_near_touch_v1"
    assert signal.feature_version == "orderbook_top_of_book_balanced_near_touch_v1"


def test_balanced_predictor_keeps_some_rotation_tolerance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "predictor_strategy_profile", "balanced_v1")
    monkeypatch.setattr(settings, "predictor_balanced_min_confidence", 0.55)
    monkeypatch.setattr(settings, "predictor_balanced_min_depth", 1.0)
    monkeypatch.setattr(settings, "predictor_balanced_max_top_changes", 2)
    monkeypatch.setattr(settings, "predictor_balanced_top_change_window_ms", 60_000)
    predictor = Predictor()

    assert predictor.predict(make_book(0.45, 0.50, timestamp_ms=1_000)) is not None
    assert predictor.predict(make_book(0.44, 0.50, timestamp_ms=2_000)) is not None
    assert predictor.predict(make_book(0.43, 0.50, timestamp_ms=3_000)) is not None
    assert predictor.predict(make_book(0.42, 0.50, timestamp_ms=4_000)) is None


def test_execution_probe_near_touch_generates_versioned_fill_probe_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "predictor_strategy_profile", "execution_probe_v1")
    monkeypatch.setattr(settings, "predictor_quote_placement", "near_touch")
    monkeypatch.setattr(settings, "execution_mode", "dry_run")
    monkeypatch.setattr(settings, "app_env", "development")
    monkeypatch.setattr(settings, "predictor_min_confidence", 0.50)
    monkeypatch.setattr(settings, "predictor_execution_probe_min_confidence", 0.50)
    monkeypatch.setattr(settings, "predictor_execution_probe_min_depth", 0.25)
    monkeypatch.setattr(settings, "predictor_near_touch_tick_size", 0.01)
    monkeypatch.setattr(settings, "predictor_near_touch_offset_ticks", 0)
    monkeypatch.setattr(settings, "predictor_execution_probe_near_touch_max_spread_fraction", 1.0)

    signal = Predictor().predict(make_book(0.45, 0.50))

    assert signal is not None
    assert signal.price == 0.50
    assert signal.size == 1.0
    assert signal.strategy == "passive_spread_capture_execution_probe_near_touch_v1"
    assert signal.model_version == "passive_spread_capture_execution_probe_near_touch_v1"
    assert signal.feature_version == "orderbook_top_of_book_execution_probe_near_touch_v1"


def test_execution_probe_rejects_live_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "predictor_strategy_profile", "execution_probe_v1")
    monkeypatch.setattr(settings, "execution_mode", "live")
    monkeypatch.setattr(settings, "app_env", "development")

    with pytest.raises(RuntimeError, match="only allowed for dry_run research"):
        Predictor().predict(make_book(0.45, 0.50))


def test_execution_probe_rejects_production_even_in_dry_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "predictor_strategy_profile", "execution_probe_v1")
    monkeypatch.setattr(settings, "execution_mode", "dry_run")
    monkeypatch.setattr(settings, "app_env", "production")

    with pytest.raises(RuntimeError, match="only allowed for dry_run research"):
        Predictor().predict(make_book(0.45, 0.50))


def test_execution_probe_uses_own_rotation_tolerance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "predictor_strategy_profile", "execution_probe_v1")
    monkeypatch.setattr(settings, "execution_mode", "dry_run")
    monkeypatch.setattr(settings, "app_env", "development")
    monkeypatch.setattr(settings, "predictor_min_confidence", 0.50)
    monkeypatch.setattr(settings, "predictor_execution_probe_min_confidence", 0.50)
    monkeypatch.setattr(settings, "predictor_execution_probe_min_depth", 0.25)
    monkeypatch.setattr(settings, "predictor_execution_probe_max_top_changes", 2)
    monkeypatch.setattr(settings, "predictor_execution_probe_top_change_window_ms", 60_000)
    predictor = Predictor()

    assert predictor.predict(make_book(0.45, 0.50, timestamp_ms=1_000)) is not None
    assert predictor.predict(make_book(0.44, 0.50, timestamp_ms=2_000)) is not None
    assert predictor.predict(make_book(0.43, 0.50, timestamp_ms=3_000)) is not None
    assert predictor.predict(make_book(0.42, 0.50, timestamp_ms=4_000)) is None


def test_execution_probe_blocklist_uses_probe_model_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "predictor_strategy_profile", "execution_probe_v1")
    monkeypatch.setattr(settings, "predictor_quote_placement", "near_touch")
    monkeypatch.setattr(settings, "execution_mode", "dry_run")
    monkeypatch.setattr(settings, "app_env", "development")
    monkeypatch.setattr(settings, "predictor_min_confidence", 0.50)
    monkeypatch.setattr(settings, "predictor_execution_probe_min_confidence", 0.50)
    monkeypatch.setattr(settings, "predictor_execution_probe_min_depth", 0.25)
    blocklist = SegmentBlocklist(
        [
            BlockedSegment(
                market_id="0xabc",
                asset_id="123",
                side="BUY",
                model_version="passive_spread_capture_execution_probe_near_touch_v1",
                reason="execution_probe_failed",
            )
        ]
    )

    assert Predictor(blocklist=blocklist).predict(make_book(0.45, 0.50)) is None


def test_execution_probe_v2_near_touch_is_less_aggressive_and_versioned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "predictor_strategy_profile", "execution_probe_v2")
    monkeypatch.setattr(settings, "predictor_quote_placement", "near_touch")
    monkeypatch.setattr(settings, "execution_mode", "dry_run")
    monkeypatch.setattr(settings, "app_env", "development")
    monkeypatch.setattr(settings, "predictor_min_confidence", 0.50)
    monkeypatch.setattr(settings, "predictor_execution_probe_v2_min_confidence", 0.50)
    monkeypatch.setattr(settings, "predictor_execution_probe_v2_min_depth", 2.0)
    monkeypatch.setattr(
        settings,
        "predictor_execution_probe_v2_near_touch_max_spread_fraction",
        0.75,
    )

    signal = Predictor().predict(make_book(0.45, 0.50))

    assert signal is not None
    assert signal.price == 0.4875
    assert signal.strategy == "passive_spread_capture_execution_probe_near_touch_v2"
    assert (
        signal.model_version == "passive_spread_capture_execution_probe_near_touch_v2"
    )
    assert signal.feature_version == (
        "orderbook_top_of_book_execution_probe_near_touch_v2"
    )


def test_execution_probe_v2_rate_limits_repeated_asset_signals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "predictor_strategy_profile", "execution_probe_v2")
    monkeypatch.setattr(settings, "execution_mode", "dry_run")
    monkeypatch.setattr(settings, "app_env", "development")
    monkeypatch.setattr(settings, "predictor_min_confidence", 0.50)
    monkeypatch.setattr(settings, "predictor_execution_probe_v2_min_confidence", 0.50)
    monkeypatch.setattr(settings, "predictor_execution_probe_v2_min_depth", 2.0)
    monkeypatch.setattr(
        settings,
        "predictor_execution_probe_v2_min_signal_interval_ms",
        10_000,
    )
    predictor = Predictor()

    assert predictor.evaluate(make_book(0.45, 0.50, timestamp_ms=1_000)).accepted
    decision = predictor.evaluate(make_book(0.45, 0.50, timestamp_ms=5_000))
    assert decision.signal is None
    assert decision.rejection_reason == "rate_limited"
    assert predictor.evaluate(make_book(0.45, 0.50, timestamp_ms=12_000)).accepted


def test_execution_probe_v2_rejects_live_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "predictor_strategy_profile", "execution_probe_v2")
    monkeypatch.setattr(settings, "execution_mode", "live")
    monkeypatch.setattr(settings, "app_env", "development")

    with pytest.raises(RuntimeError, match="only allowed for dry_run research"):
        Predictor().predict(make_book(0.45, 0.50))


def test_execution_probe_v2_blocklist_uses_v2_model_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "predictor_strategy_profile", "execution_probe_v2")
    monkeypatch.setattr(settings, "predictor_quote_placement", "near_touch")
    monkeypatch.setattr(settings, "execution_mode", "dry_run")
    monkeypatch.setattr(settings, "app_env", "development")
    monkeypatch.setattr(settings, "predictor_min_confidence", 0.50)
    monkeypatch.setattr(settings, "predictor_execution_probe_v2_min_confidence", 0.50)
    monkeypatch.setattr(settings, "predictor_execution_probe_v2_min_depth", 2.0)
    blocklist = SegmentBlocklist(
        [
            BlockedSegment(
                market_id="0xabc",
                asset_id="123",
                side="BUY",
                model_version="passive_spread_capture_execution_probe_near_touch_v2",
                reason="observed_synthetic_gap",
            )
        ]
    )

    assert Predictor(blocklist=blocklist).predict(make_book(0.45, 0.50)) is None


def test_execution_probe_v3_near_touch_is_intermediate_and_versioned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "predictor_strategy_profile", "execution_probe_v3")
    monkeypatch.setattr(settings, "predictor_quote_placement", "near_touch")
    monkeypatch.setattr(settings, "execution_mode", "dry_run")
    monkeypatch.setattr(settings, "app_env", "development")
    monkeypatch.setattr(settings, "predictor_min_confidence", 0.50)
    monkeypatch.setattr(settings, "predictor_execution_probe_v3_min_confidence", 0.50)
    monkeypatch.setattr(settings, "predictor_execution_probe_v3_min_depth", 1.0)
    monkeypatch.setattr(
        settings,
        "predictor_execution_probe_v3_near_touch_max_spread_fraction",
        0.85,
    )

    signal = Predictor().predict(make_book(0.45, 0.50))

    assert signal is not None
    assert signal.price == 0.4925
    assert signal.strategy == "passive_spread_capture_execution_probe_near_touch_v3"
    assert (
        signal.model_version == "passive_spread_capture_execution_probe_near_touch_v3"
    )
    assert signal.feature_version == (
        "orderbook_top_of_book_execution_probe_near_touch_v3"
    )


def test_execution_probe_v3_rate_limits_repeated_asset_signals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "predictor_strategy_profile", "execution_probe_v3")
    monkeypatch.setattr(settings, "execution_mode", "dry_run")
    monkeypatch.setattr(settings, "app_env", "development")
    monkeypatch.setattr(settings, "predictor_min_confidence", 0.50)
    monkeypatch.setattr(settings, "predictor_execution_probe_v3_min_confidence", 0.50)
    monkeypatch.setattr(settings, "predictor_execution_probe_v3_min_depth", 1.0)
    monkeypatch.setattr(
        settings,
        "predictor_execution_probe_v3_min_signal_interval_ms",
        5_000,
    )
    predictor = Predictor()

    assert predictor.evaluate(make_book(0.45, 0.50, timestamp_ms=1_000)).accepted
    decision = predictor.evaluate(make_book(0.45, 0.50, timestamp_ms=4_000))
    assert decision.signal is None
    assert decision.rejection_reason == "rate_limited"
    assert predictor.evaluate(make_book(0.45, 0.50, timestamp_ms=6_000)).accepted


def test_execution_probe_v3_rejects_live_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "predictor_strategy_profile", "execution_probe_v3")
    monkeypatch.setattr(settings, "execution_mode", "live")
    monkeypatch.setattr(settings, "app_env", "development")

    with pytest.raises(RuntimeError, match="only allowed for dry_run research"):
        Predictor().predict(make_book(0.45, 0.50))


def test_execution_probe_v3_blocklist_uses_v3_model_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "predictor_strategy_profile", "execution_probe_v3")
    monkeypatch.setattr(settings, "predictor_quote_placement", "near_touch")
    monkeypatch.setattr(settings, "execution_mode", "dry_run")
    monkeypatch.setattr(settings, "app_env", "development")
    monkeypatch.setattr(settings, "predictor_min_confidence", 0.50)
    monkeypatch.setattr(settings, "predictor_execution_probe_v3_min_confidence", 0.50)
    monkeypatch.setattr(settings, "predictor_execution_probe_v3_min_depth", 1.0)
    blocklist = SegmentBlocklist(
        [
            BlockedSegment(
                market_id="0xabc",
                asset_id="123",
                side="BUY",
                model_version="passive_spread_capture_execution_probe_near_touch_v3",
                reason="observed_synthetic_gap",
            )
        ]
    )

    assert Predictor(blocklist=blocklist).predict(make_book(0.45, 0.50)) is None


def test_execution_probe_v4_near_touch_is_less_aggressive_and_versioned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "predictor_strategy_profile", "execution_probe_v4")
    monkeypatch.setattr(settings, "predictor_quote_placement", "near_touch")
    monkeypatch.setattr(settings, "execution_mode", "dry_run")
    monkeypatch.setattr(settings, "app_env", "development")
    monkeypatch.setattr(settings, "predictor_min_confidence", 0.50)
    monkeypatch.setattr(settings, "predictor_execution_probe_v4_min_confidence", 0.50)
    monkeypatch.setattr(settings, "predictor_execution_probe_v4_min_depth", 1.0)
    monkeypatch.setattr(
        settings,
        "predictor_execution_probe_v4_near_touch_max_spread_fraction",
        0.60,
    )

    signal = Predictor().predict(make_book(0.45, 0.50))

    assert signal is not None
    assert signal.price == 0.48
    assert signal.strategy == "passive_spread_capture_execution_probe_near_touch_v4"
    assert (
        signal.model_version == "passive_spread_capture_execution_probe_near_touch_v4"
    )
    assert signal.feature_version == (
        "orderbook_top_of_book_execution_probe_near_touch_v4"
    )


def test_execution_probe_v4_rate_limits_repeated_asset_signals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "predictor_strategy_profile", "execution_probe_v4")
    monkeypatch.setattr(settings, "execution_mode", "dry_run")
    monkeypatch.setattr(settings, "app_env", "development")
    monkeypatch.setattr(settings, "predictor_min_confidence", 0.50)
    monkeypatch.setattr(settings, "predictor_execution_probe_v4_min_confidence", 0.50)
    monkeypatch.setattr(settings, "predictor_execution_probe_v4_min_depth", 1.0)
    monkeypatch.setattr(
        settings,
        "predictor_execution_probe_v4_min_signal_interval_ms",
        5_000,
    )
    predictor = Predictor()

    assert predictor.evaluate(make_book(0.45, 0.50, timestamp_ms=1_000)).accepted
    decision = predictor.evaluate(make_book(0.45, 0.50, timestamp_ms=4_000))
    assert decision.signal is None
    assert decision.rejection_reason == "rate_limited"
    assert predictor.evaluate(make_book(0.45, 0.50, timestamp_ms=7_000)).accepted


def test_execution_probe_v4_rejects_live_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "predictor_strategy_profile", "execution_probe_v4")
    monkeypatch.setattr(settings, "execution_mode", "live")
    monkeypatch.setattr(settings, "app_env", "development")

    with pytest.raises(RuntimeError, match="only allowed for dry_run research"):
        Predictor().predict(make_book(0.45, 0.50))


def test_execution_probe_v4_blocklist_uses_v4_model_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "predictor_strategy_profile", "execution_probe_v4")
    monkeypatch.setattr(settings, "predictor_quote_placement", "near_touch")
    monkeypatch.setattr(settings, "execution_mode", "dry_run")
    monkeypatch.setattr(settings, "app_env", "development")
    monkeypatch.setattr(settings, "predictor_min_confidence", 0.50)
    monkeypatch.setattr(settings, "predictor_execution_probe_v4_min_confidence", 0.50)
    monkeypatch.setattr(settings, "predictor_execution_probe_v4_min_depth", 1.0)
    blocklist = SegmentBlocklist(
        [
            BlockedSegment(
                market_id="0xabc",
                asset_id="123",
                side="BUY",
                model_version="passive_spread_capture_execution_probe_near_touch_v4",
                reason="observed_synthetic_gap",
            )
        ]
    )

    assert Predictor(blocklist=blocklist).predict(make_book(0.45, 0.50)) is None


def test_conservative_predictor_rejects_high_top_of_book_rotation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "predictor_strategy_profile", "conservative_v1")
    monkeypatch.setattr(settings, "predictor_conservative_min_confidence", 0.55)
    monkeypatch.setattr(settings, "predictor_conservative_min_depth", 1.0)
    monkeypatch.setattr(settings, "predictor_conservative_max_top_changes", 1)
    monkeypatch.setattr(settings, "predictor_conservative_top_change_window_ms", 60_000)
    predictor = Predictor()

    assert predictor.predict(make_book(0.45, 0.50, timestamp_ms=1_000)) is not None
    assert predictor.predict(make_book(0.44, 0.50, timestamp_ms=2_000)) is not None
    assert predictor.predict(make_book(0.43, 0.50, timestamp_ms=3_000)) is None
