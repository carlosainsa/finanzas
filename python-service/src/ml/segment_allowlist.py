import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

@dataclass(frozen=True)
class AllowedSegment:
    market_id: str
    asset_id: str
    side: str | None = None
    strategy: str | None = None
    model_version: str | None = None
    spread_bucket: str | None = None
    timing_bucket: str | None = None
    reason: str | None = None


class SegmentAllowlist:
    def __init__(self, segments: list[AllowedSegment] | None = None) -> None:
        self._segments = segments or []

    @classmethod
    def from_file(cls, path: str | Path | None) -> "SegmentAllowlist":
        if path is None:
            return cls()
        source = Path(path)
        data = json.loads(source.read_text(encoding="utf-8"))
        if isinstance(data, dict) and data.get("version") != "allowed_segments_v1":
            raise ValueError("unsupported allowed segments version")
        rows = data.get("segments") if isinstance(data, dict) else data
        if not isinstance(rows, list):
            raise ValueError("allowed segments file must contain a list")
        return cls([allowed_segment_from_dict(row) for row in rows])

    @property
    def enabled(self) -> bool:
        return bool(self._segments)

    def is_allowed(
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
        if not self._segments:
            return True
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


def allowed_segment_from_dict(value: object) -> AllowedSegment:
    if not isinstance(value, dict):
        raise ValueError("allowed segment entries must be objects")
    market_id = required_string(value, "market_id")
    asset_id = required_string(value, "asset_id")
    return AllowedSegment(
        market_id=market_id,
        asset_id=asset_id,
        side=optional_string(value, "side"),
        strategy=optional_string(value, "strategy"),
        model_version=optional_string(value, "model_version"),
        spread_bucket=optional_string(value, "spread_bucket"),
        timing_bucket=optional_string(value, "timing_bucket"),
        reason=optional_string(value, "reason"),
    )


def required_string(value: dict[str, Any], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ValueError(f"allowed segment missing {key}")
    return item


def optional_string(value: dict[str, Any], key: str) -> str | None:
    item = value.get(key)
    if item is None:
        return None
    if not isinstance(item, str):
        raise ValueError(f"allowed segment {key} must be a string")
    return item or None


def segment_matches(
    segment: AllowedSegment,
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
