import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class BlockedSegment:
    market_id: str
    asset_id: str
    side: str | None = None
    strategy: str | None = None
    model_version: str | None = None
    spread_bucket: str | None = None
    timing_bucket: str | None = None
    reason: str | None = None


class SegmentBlocklist:
    def __init__(self, segments: list[BlockedSegment] | None = None) -> None:
        self._segments = segments or []

    @classmethod
    def from_file(cls, path: str | Path | None) -> "SegmentBlocklist":
        if path is None:
            return cls()
        source = Path(path)
        data = json.loads(source.read_text(encoding="utf-8"))
        if isinstance(data, dict) and data.get("version") != "blocked_segments_v1":
            raise ValueError("unsupported blocked segments version")
        rows = data.get("segments") if isinstance(data, dict) else data
        if not isinstance(rows, list):
            raise ValueError("blocked segments file must contain a list")
        return cls([blocked_segment_from_dict(row) for row in rows])

    def is_blocked(
        self,
        market_id: str,
        asset_id: str,
        side: str | None = None,
        model_version: str | None = None,
        *,
        strategy: str | None = None,
        spread_bucket: str | None = None,
        timing_bucket: str | None = None,
    ) -> bool:
        return any(
            segment_matches(
                segment,
                market_id,
                asset_id,
                side,
                model_version,
                strategy=strategy,
                spread_bucket=spread_bucket,
                timing_bucket=timing_bucket,
            )
            for segment in self._segments
        )


def blocked_segment_from_dict(value: object) -> BlockedSegment:
    if not isinstance(value, dict):
        raise ValueError("blocked segment entries must be objects")
    market_id = required_string(value, "market_id")
    asset_id = required_string(value, "asset_id")
    return BlockedSegment(
        market_id=market_id,
        asset_id=asset_id,
        side=optional_string(value, "side"),
        strategy=optional_string(value, "strategy"),
        model_version=optional_string(value, "model_version"),
        spread_bucket=optional_string(value, "spread_bucket"),
        timing_bucket=optional_string(value, "timing_bucket"),
        reason=optional_string(value, "reason") or optional_string(value, "block_reason"),
    )


def segment_matches(
    segment: BlockedSegment,
    market_id: str,
    asset_id: str,
    side: str | None,
    model_version: str | None,
    *,
    strategy: str | None = None,
    spread_bucket: str | None = None,
    timing_bucket: str | None = None,
) -> bool:
    if segment.market_id != market_id or segment.asset_id != asset_id:
        return False
    if segment.side is not None and segment.side != side:
        return False
    if segment.strategy is not None and segment.strategy != strategy:
        return False
    if segment.model_version is not None and segment.model_version != model_version:
        return False
    if segment.spread_bucket is not None and segment.spread_bucket != spread_bucket:
        return False
    if segment.timing_bucket is not None and segment.timing_bucket != timing_bucket:
        return False
    return True


def spread_bucket_for_value(spread: float | None) -> str:
    if spread is None:
        return "unknown"
    epsilon = 1e-9
    if spread <= 0.005 + epsilon:
        return "000_050bps"
    if spread <= 0.010 + epsilon:
        return "050_100bps"
    if spread <= 0.025 + epsilon:
        return "100_250bps"
    if spread <= 0.050 + epsilon:
        return "250_500bps"
    return "500bps_plus"


def timing_bucket_for_top_change_count(top_change_count: int | None) -> str:
    if top_change_count is None:
        return "unknown"
    if top_change_count <= 0:
        return "stable"
    if top_change_count <= 2:
        return "rotating"
    return "volatile"


def required_string(value: dict[str, Any], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ValueError(f"blocked segment missing {key}")
    return item


def optional_string(value: dict[str, Any], key: str) -> str | None:
    item = value.get(key)
    if item is None:
        return None
    if not isinstance(item, str):
        raise ValueError(f"blocked segment {key} must be a string")
    return item or None
