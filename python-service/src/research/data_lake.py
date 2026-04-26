import asyncio
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol, cast

import duckdb
import pandas as pd  # type: ignore[import-untyped]
from pydantic import ValidationError

from src.config import settings
from src.data.redis_client import get_redis
from src.discovery.markets import MarketCandidate, fetch_gamma_markets, normalize_gamma_market
from src.schemas import ExecutionReport, OrderBook, TradeSignal


class RedisRangeReader(Protocol):
    async def xrange(
        self,
        name: str,
        min: str = "-",
        max: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, str]]]: ...


@dataclass(frozen=True)
class StreamDataset:
    stream: str
    dataset: str
    schema_name: str


@dataclass(frozen=True)
class StreamExportResult:
    rows: list[dict[str, object]]
    derived_rows: dict[str, list[dict[str, object]]]
    last_stream_id: str | None


STREAM_DATASETS: tuple[StreamDataset, ...] = (
    StreamDataset(settings.orderbook_stream, "orderbook_snapshots", "orderbook"),
    StreamDataset(settings.signals_stream, "signals", "trade_signal"),
    StreamDataset(settings.execution_reports_stream, "execution_reports", "execution_report"),
    StreamDataset(settings.orderbook_deadletter_stream, "orderbook_deadletter", "deadletter"),
    StreamDataset(settings.signals_deadletter_stream, "signals_deadletter", "deadletter"),
    StreamDataset(settings.operator_commands_stream, "operator_commands", "operator_command"),
)
DERIVED_DATASETS = ("orderbook_levels", "market_metadata")
DATASET_NAMES = tuple(dataset.dataset for dataset in STREAM_DATASETS) + DERIVED_DATASETS


async def export_data_lake(
    redis: RedisRangeReader,
    root: Path,
    count: int,
    datasets: tuple[StreamDataset, ...] = STREAM_DATASETS,
    incremental: bool = False,
) -> dict[str, int]:
    exported: dict[str, int] = {}
    derived_rows: dict[str, list[dict[str, object]]] = {"orderbook_levels": []}
    state = load_export_state(root) if incremental else {}
    next_state = dict(state)
    for dataset in datasets:
        min_id = exclusive_min_stream_id(state.get(dataset.dataset)) if incremental else "-"
        result = await read_stream_rows(redis, dataset, count=count, min_id=min_id)
        exported[dataset.dataset] = write_dataset(root, dataset, result.rows)
        if result.last_stream_id is not None:
            next_state[dataset.dataset] = result.last_stream_id
        for name, dataset_rows in result.derived_rows.items():
            derived_rows.setdefault(name, []).extend(dataset_rows)
    for name, rows in derived_rows.items():
        exported[name] = write_named_dataset(root, name, rows)
    if incremental:
        save_export_state(root, next_state)
    return exported


def export_market_metadata(root: Path, markets: list[MarketCandidate]) -> int:
    return write_named_dataset(root, "market_metadata", market_metadata_rows(markets))


def market_metadata_rows(markets: list[MarketCandidate]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    ingested_at_ms = now_ms()
    for market in markets:
        for index, asset_id in enumerate(market.clob_token_ids):
            rows.append(
                {
                    "market_id": market.market_id,
                    "asset_id": asset_id,
                    "outcome": value_at(market.outcomes, index),
                    "outcome_index": index,
                    "question": market.question,
                    "slug": market.slug,
                    "active": market.active,
                    "closed": market.closed,
                    "archived": market.archived,
                    "enable_order_book": market.enable_order_book,
                    "liquidity": market.liquidity,
                    "volume": market.volume,
                    "outcome_price": numeric_value_at(market.outcome_prices, index),
                    "end_date": market.end_date,
                    "tags_json": json.dumps(market.tags, sort_keys=True),
                    "ingested_at_ms": ingested_at_ms,
                }
            )
    return rows


async def read_stream_rows(
    redis: RedisRangeReader, dataset: StreamDataset, count: int, min_id: str = "-"
) -> StreamExportResult:
    entries = await redis.xrange(dataset.stream, min=min_id, count=count)
    rows: list[dict[str, object]] = []
    derived: dict[str, list[dict[str, object]]] = {"orderbook_levels": []}
    last_stream_id: str | None = None
    for stream_id, fields in entries:
        payload = parse_payload(fields.get("payload"))
        if payload is None:
            continue
        normalized = normalize_payload(dataset.schema_name, payload)
        rows.append(normalize_row(dataset, stream_id, normalized))
        if dataset.schema_name == "orderbook":
            derived["orderbook_levels"].extend(orderbook_level_rows(stream_id, normalized))
        last_stream_id = stream_id
    return StreamExportResult(rows=rows, derived_rows=derived, last_stream_id=last_stream_id)


def write_dataset(root: Path, dataset: StreamDataset, rows: list[dict[str, object]]) -> int:
    return write_named_dataset(root, dataset.dataset, rows)


def write_named_dataset(root: Path, dataset_name: str, rows: list[dict[str, object]]) -> int:
    if not rows:
        return 0

    partition = datetime.now(timezone.utc).strftime("date=%Y-%m-%d")
    target_dir = root / dataset_name / partition
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / f"part-{now_ms()}-{uuid.uuid4().hex}.parquet"
    pd.DataFrame(rows).to_parquet(target_file, index=False)
    return len(rows)


def export_state_path(root: Path) -> Path:
    return root / "_export_state.json"


def load_export_state(root: Path) -> dict[str, str]:
    path = export_state_path(root)
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(value, dict):
        return {}
    state: dict[str, str] = {}
    for key, stream_id in value.items():
        if isinstance(key, str) and isinstance(stream_id, str):
            state[key] = stream_id
    return state


def save_export_state(root: Path, state: dict[str, str]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    export_state_path(root).write_text(
        json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def exclusive_min_stream_id(stream_id: str | None) -> str:
    return f"({stream_id}" if stream_id else "-"


def create_duckdb_views(root: Path, db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
        for dataset_name in DATASET_NAMES:
            if not any((root / dataset_name).glob("**/*.parquet")):
                continue
            parquet_glob = (root / dataset_name / "**" / "*.parquet").as_posix()
            conn.execute(
                f"""
                create or replace view {dataset_name} as
                select * from read_parquet('{duckdb_literal(parquet_glob)}')
                """
            )


def parse_payload(payload: str | None) -> dict[str, object] | None:
    if payload is None:
        return None
    try:
        value = json.loads(payload)
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def duckdb_literal(value: str) -> str:
    return value.replace("'", "''")


def normalize_payload(schema_name: str, payload: dict[str, object]) -> dict[str, object]:
    if schema_name == "orderbook":
        return OrderBook.model_validate(payload).model_dump(mode="json")
    if schema_name == "trade_signal":
        return TradeSignal.model_validate(payload).model_dump(mode="json")
    if schema_name == "execution_report":
        return ExecutionReport.model_validate(payload).model_dump(mode="json")
    return payload


def normalize_row(
    dataset: StreamDataset, stream_id: str, payload: dict[str, object]
) -> dict[str, object]:
    base = base_row(dataset, stream_id, payload)
    if dataset.schema_name == "orderbook":
        bids = payload.get("bids", [])
        asks = payload.get("asks", [])
        best_bid = price_at(bids, 0)
        best_ask = price_at(asks, 0)
        return {
            **base,
            "market_id": payload.get("market_id"),
            "asset_id": payload.get("asset_id"),
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": best_ask - best_bid if best_bid is not None and best_ask is not None else None,
            "bid_depth": depth(bids),
            "ask_depth": depth(asks),
        }
    if dataset.schema_name == "trade_signal":
        return {
            **base,
            "signal_id": payload.get("signal_id"),
            "market_id": payload.get("market_id"),
            "asset_id": payload.get("asset_id"),
            "side": payload.get("side"),
            "price": payload.get("price"),
            "size": payload.get("size"),
            "confidence": payload.get("confidence"),
            "strategy": payload.get("strategy"),
            "model_version": payload.get("model_version"),
            "data_version": payload.get("data_version"),
            "feature_version": payload.get("feature_version"),
        }
    if dataset.schema_name == "execution_report":
        return {
            **base,
            "signal_id": payload.get("signal_id"),
            "order_id": payload.get("order_id"),
            "status": payload.get("status"),
            "filled_price": payload.get("filled_price"),
            "filled_size": payload.get("filled_size"),
            "cumulative_filled_size": payload.get("cumulative_filled_size"),
            "remaining_size": payload.get("remaining_size"),
            "error": payload.get("error"),
        }
    return base


def base_row(
    dataset: StreamDataset, stream_id: str, payload: dict[str, object]
) -> dict[str, object]:
    return {
        "stream": dataset.stream,
        "stream_id": stream_id,
        "schema_name": dataset.schema_name,
        "event_timestamp_ms": event_timestamp_ms(payload),
        "ingested_at_ms": now_ms(),
        "payload_json": json.dumps(payload, sort_keys=True),
    }


def orderbook_level_rows(
    stream_id: str, payload: dict[str, object]
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for side, levels in (("bid", payload.get("bids", [])), ("ask", payload.get("asks", []))):
        if not isinstance(levels, list):
            continue
        for index, level in enumerate(levels):
            if not isinstance(level, dict):
                continue
            rows.append(
                {
                    "stream_id": stream_id,
                    "market_id": payload.get("market_id"),
                    "asset_id": payload.get("asset_id"),
                    "timestamp_ms": event_timestamp_ms(payload),
                    "side": side,
                    "level_index": index,
                    "price": level.get("price"),
                    "size": level.get("size"),
                }
            )
    return rows


def price_at(levels: object, index: int) -> float | None:
    if not isinstance(levels, list) or len(levels) <= index:
        return None
    level = levels[index]
    if not isinstance(level, dict):
        return None
    value = level.get("price")
    return float(value) if isinstance(value, (int, float)) else None


def depth(levels: object) -> float:
    if not isinstance(levels, list):
        return 0.0
    total = 0.0
    for level in levels:
        if isinstance(level, dict) and isinstance(level.get("size"), (int, float)):
            total += float(level["size"])
    return total


def value_at(values: list[str], index: int) -> str | None:
    return values[index] if len(values) > index else None


def numeric_value_at(values: list[float], index: int) -> float | None:
    return values[index] if len(values) > index else None


def event_timestamp_ms(payload: dict[str, object]) -> int | None:
    value = payload.get("timestamp_ms")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return None


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


async def run_export(
    root: Path,
    db_path: Path,
    count: int,
    incremental: bool = True,
    include_market_metadata: bool = False,
    metadata_limit: int | None = None,
) -> dict[str, int]:
    redis = cast(RedisRangeReader, await get_redis())
    exported = await export_data_lake(redis, root=root, count=count, incremental=incremental)
    if include_market_metadata:
        raw_markets = await fetch_gamma_markets(limit=metadata_limit or settings.discovery_limit)
        exported["market_metadata"] = export_market_metadata(
            root, [normalize_gamma_market(item) for item in raw_markets]
        )
    create_duckdb_views(root=root, db_path=db_path)
    return exported


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="research-data-lake")
    parser.add_argument("--root", default=settings.data_lake_root)
    parser.add_argument("--duckdb", default=settings.data_lake_duckdb_path)
    parser.add_argument("--count", type=int, default=settings.data_lake_export_count)
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="ignore saved Redis Stream IDs and export from the beginning",
    )
    parser.add_argument(
        "--include-market-metadata",
        action="store_true",
        help="fetch Gamma market metadata and export asset/outcome mapping snapshots",
    )
    parser.add_argument("--metadata-limit", type=int, default=settings.discovery_limit)
    args = parser.parse_args()

    try:
        exported = asyncio.run(
            run_export(
                root=Path(args.root),
                db_path=Path(args.duckdb),
                count=args.count,
                incremental=not args.full_refresh,
                include_market_metadata=args.include_market_metadata,
                metadata_limit=args.metadata_limit,
            )
        )
    except ValidationError as exc:
        print(f"schema validation failed: {exc}")
        return 1

    print(json.dumps(exported, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
