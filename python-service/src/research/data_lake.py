import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol, cast

import duckdb
import pandas as pd  # type: ignore[import-untyped]
from pydantic import ValidationError

from src.config import settings
from src.data.redis_client import get_redis
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


STREAM_DATASETS: tuple[StreamDataset, ...] = (
    StreamDataset(settings.orderbook_stream, "orderbook_snapshots", "orderbook"),
    StreamDataset(settings.signals_stream, "signals", "trade_signal"),
    StreamDataset(settings.execution_reports_stream, "execution_reports", "execution_report"),
    StreamDataset(settings.orderbook_deadletter_stream, "orderbook_deadletter", "deadletter"),
    StreamDataset(settings.signals_deadletter_stream, "signals_deadletter", "deadletter"),
    StreamDataset(settings.operator_commands_stream, "operator_commands", "operator_command"),
)
DERIVED_DATASETS = ("orderbook_levels",)
DATASET_NAMES = tuple(dataset.dataset for dataset in STREAM_DATASETS) + DERIVED_DATASETS


async def export_data_lake(
    redis: RedisRangeReader,
    root: Path,
    count: int,
    datasets: tuple[StreamDataset, ...] = STREAM_DATASETS,
) -> dict[str, int]:
    exported: dict[str, int] = {}
    derived_rows: dict[str, list[dict[str, object]]] = {"orderbook_levels": []}
    for dataset in datasets:
        rows, derived = await read_stream_rows(redis, dataset, count=count)
        exported[dataset.dataset] = write_dataset(root, dataset, rows)
        for name, dataset_rows in derived.items():
            derived_rows.setdefault(name, []).extend(dataset_rows)
    for name, rows in derived_rows.items():
        exported[name] = write_named_dataset(root, name, rows)
    return exported


async def read_stream_rows(
    redis: RedisRangeReader, dataset: StreamDataset, count: int
) -> tuple[list[dict[str, object]], dict[str, list[dict[str, object]]]]:
    entries = await redis.xrange(dataset.stream, count=count)
    rows: list[dict[str, object]] = []
    derived: dict[str, list[dict[str, object]]] = {"orderbook_levels": []}
    for stream_id, fields in entries:
        payload = parse_payload(fields.get("payload"))
        if payload is None:
            continue
        normalized = normalize_payload(dataset.schema_name, payload)
        rows.append(normalize_row(dataset, stream_id, normalized))
        if dataset.schema_name == "orderbook":
            derived["orderbook_levels"].extend(orderbook_level_rows(stream_id, normalized))
    return rows, derived


def write_dataset(root: Path, dataset: StreamDataset, rows: list[dict[str, object]]) -> int:
    return write_named_dataset(root, dataset.dataset, rows)


def write_named_dataset(root: Path, dataset_name: str, rows: list[dict[str, object]]) -> int:
    if not rows:
        return 0

    partition = datetime.now(timezone.utc).strftime("date=%Y-%m-%d")
    target_dir = root / dataset_name / partition
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / "part-000.parquet"
    pd.DataFrame(rows).to_parquet(target_file, index=False)
    return len(rows)


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


def event_timestamp_ms(payload: dict[str, object]) -> int | None:
    value = payload.get("timestamp_ms")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return None


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


async def run_export(root: Path, db_path: Path, count: int) -> dict[str, int]:
    redis = cast(RedisRangeReader, await get_redis())
    exported = await export_data_lake(redis, root=root, count=count)
    create_duckdb_views(root=root, db_path=db_path)
    return exported


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="research-data-lake")
    parser.add_argument("--root", default=settings.data_lake_root)
    parser.add_argument("--duckdb", default=settings.data_lake_duckdb_path)
    parser.add_argument("--count", type=int, default=settings.data_lake_export_count)
    args = parser.parse_args()

    try:
        exported = asyncio.run(
            run_export(root=Path(args.root), db_path=Path(args.duckdb), count=args.count)
        )
    except ValidationError as exc:
        print(f"schema validation failed: {exc}")
        return 1

    print(json.dumps(exported, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
