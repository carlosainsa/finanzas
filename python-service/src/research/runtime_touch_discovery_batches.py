import argparse
import asyncio
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.discovery.markets import ScoredMarket, discover_markets
from src.research.runtime_touch_ranking import normalize_records


REPORT_VERSION = "runtime_touch_discovery_batches_v1"


@dataclass(frozen=True)
class RuntimeTouchDiscoveryBatchConfig:
    discovery_limit: int = 50
    batch_size: int = 20
    min_liquidity: float = 100.0
    min_volume: float = 100.0
    query: str | None = None

    def __post_init__(self) -> None:
        if self.discovery_limit <= 0:
            raise ValueError("discovery_limit must be positive")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.min_liquidity < 0:
            raise ValueError("min_liquidity must be non-negative")
        if self.min_volume < 0:
            raise ValueError("min_volume must be non-negative")


async def create_runtime_touch_discovery_batches(
    output_dir: Path,
    config: RuntimeTouchDiscoveryBatchConfig = RuntimeTouchDiscoveryBatchConfig(),
) -> dict[str, Any]:
    markets = await discover_markets(
        limit=config.discovery_limit,
        query=config.query,
        min_liquidity=config.min_liquidity,
        min_volume=config.min_volume,
    )
    return write_runtime_touch_discovery_batches(output_dir, markets, config)


def write_runtime_touch_discovery_batches(
    output_dir: Path,
    markets: list[ScoredMarket],
    config: RuntimeTouchDiscoveryBatchConfig,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    market_rows = [market_to_record(scored) for scored in markets]
    batches = build_batches(market_rows, config.batch_size)
    payload: dict[str, Any] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "can_promote_live": False,
        "decision_policy": "offline_runtime_touch_discovery_batching_only",
        "selection_source": "gamma_discovery_ranked_batches",
        "risk_contract": {
            "execution_mode": "dry_run",
            "does_not_modify_quote_policy": True,
            "does_not_modify_risk_limits": True,
            "does_not_publish_signals": True,
        },
        "config": asdict(config),
        "counts": {
            "markets": len(market_rows),
            "batches": len(batches),
            "asset_ids": len(unique_asset_ids(market_rows)),
        },
        "markets": normalize_records(market_rows),
        "batches": batches,
        "outputs": [
            "runtime_touch_discovery_batches.json",
        ],
    }
    (output_dir / "runtime_touch_discovery_batches.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload


def build_batches(
    market_rows: list[dict[str, Any]],
    batch_size: int,
) -> list[dict[str, Any]]:
    batches: list[dict[str, Any]] = []
    batch_markets: list[dict[str, Any]] = []
    batch_assets: list[str] = []
    seen_assets: set[str] = set()
    for market in market_rows:
        token_ids = [str(item) for item in market.get("clob_token_ids", [])]
        next_unique = [asset_id for asset_id in token_ids if asset_id not in seen_assets]
        if batch_markets and len(batch_assets) + len(next_unique) > batch_size:
            batches.append(batch_record(len(batches) + 1, batch_markets, batch_assets))
            batch_markets = []
            batch_assets = []
        batch_markets.append(market)
        for asset_id in next_unique:
            seen_assets.add(asset_id)
            batch_assets.append(asset_id)
    if batch_markets:
        batches.append(batch_record(len(batches) + 1, batch_markets, batch_assets))
    return batches


def batch_record(
    index: int,
    markets: list[dict[str, Any]],
    asset_ids: list[str],
) -> dict[str, Any]:
    batch_id = f"batch-{index:02d}"
    asset_csv = ",".join(asset_ids)
    return {
        "batch_index": index,
        "batch_id": batch_id,
        "markets_count": len(markets),
        "asset_ids_count": len(asset_ids),
        "market_ids": [str(market["market_id"]) for market in markets],
        "market_asset_ids": asset_ids,
        "market_asset_ids_csv": asset_csv,
        "market_asset_ids_sha256": hashlib.sha256(asset_csv.encode("utf-8")).hexdigest(),
        "markets": markets,
    }


def market_to_record(scored: ScoredMarket) -> dict[str, Any]:
    market = scored.market
    return {
        "market_id": market.market_id,
        "question": market.question,
        "slug": market.slug,
        "score": scored.score,
        "liquidity": market.liquidity,
        "volume": market.volume,
        "liquidity_score": scored.liquidity_score,
        "volume_score": scored.volume_score,
        "price_quality_score": scored.price_quality_score,
        "evidence_score": scored.evidence_score,
        "reason": scored.reason,
        "clob_token_ids": market.clob_token_ids,
        "outcomes": market.outcomes,
    }


def unique_asset_ids(markets: list[dict[str, Any]]) -> list[str]:
    seen: set[str] = set()
    ids: list[str] = []
    for market in markets:
        for asset_id in market.get("clob_token_ids", []):
            if str(asset_id) in seen:
                continue
            seen.add(str(asset_id))
            ids.append(str(asset_id))
    return ids


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--discovery-limit", type=int, default=50)
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--min-liquidity", type=float, default=100.0)
    parser.add_argument("--min-volume", type=float, default=100.0)
    parser.add_argument("--query")
    args = parser.parse_args()
    report = asyncio.run(
        create_runtime_touch_discovery_batches(
            Path(args.output_dir),
            RuntimeTouchDiscoveryBatchConfig(
                discovery_limit=args.discovery_limit,
                batch_size=args.batch_size,
                min_liquidity=args.min_liquidity,
                min_volume=args.min_volume,
                query=args.query,
            ),
        )
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
