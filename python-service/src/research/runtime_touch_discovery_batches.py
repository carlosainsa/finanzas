import argparse
import asyncio
import hashlib
import json
import math
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.discovery.markets import ScoredMarket, discover_markets
from src.research.market_family_memory import load_family_memory_map, market_family
from src.research.market_fillability_score import load_asset_score_map
from src.research.runtime_touch_ranking import normalize_records


REPORT_VERSION = "runtime_touch_discovery_batches_v1"


@dataclass(frozen=True)
class RuntimeTouchDiscoveryBatchConfig:
    discovery_limit: int = 50
    batch_size: int = 20
    min_liquidity: float = 100.0
    min_volume: float = 100.0
    query: str | None = None
    market_fillability_score_path: str | None = None
    market_family_memory_path: str | None = None
    exploration_rate: float = 0.20
    fillability_weight: float = 1.0
    family_memory_weight: float = 1.0
    min_family_observations_for_exploit: int = 1

    def __post_init__(self) -> None:
        if self.discovery_limit <= 0:
            raise ValueError("discovery_limit must be positive")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.min_liquidity < 0:
            raise ValueError("min_liquidity must be non-negative")
        if self.min_volume < 0:
            raise ValueError("min_volume must be non-negative")
        if not 0 <= self.exploration_rate <= 1:
            raise ValueError("exploration_rate must be between 0 and 1")
        if self.fillability_weight < 0:
            raise ValueError("fillability_weight must be non-negative")
        if self.family_memory_weight < 0:
            raise ValueError("family_memory_weight must be non-negative")
        if self.min_family_observations_for_exploit <= 0:
            raise ValueError("min_family_observations_for_exploit must be positive")


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
    market_rows = apply_explore_exploit_policy(market_rows, config)
    batches = build_batches(market_rows, config.batch_size)
    payload: dict[str, Any] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "can_promote_live": False,
        "decision_policy": "offline_runtime_touch_discovery_batching_only",
        "selection_source": "gamma_discovery_ranked_batches",
        "selection_policy": "epsilon_family_fillability_explore_exploit_v1",
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
            "exploit_markets": sum(
                1 for row in market_rows if row["selection_mode"] == "exploit"
            ),
            "explore_markets": sum(
                1 for row in market_rows if row["selection_mode"] == "explore"
            ),
        },
        "memory_inputs": {
            "market_fillability_score": config.market_fillability_score_path,
            "market_family_memory": config.market_family_memory_path,
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


def apply_explore_exploit_policy(
    market_rows: list[dict[str, Any]],
    config: RuntimeTouchDiscoveryBatchConfig,
) -> list[dict[str, Any]]:
    fillability_scores = load_asset_score_map(
        Path(config.market_fillability_score_path)
        if config.market_fillability_score_path
        else None
    )
    family_memory = load_family_memory_map(
        Path(config.market_family_memory_path)
        if config.market_family_memory_path
        else None
    )
    enriched: list[dict[str, Any]] = []
    for original_rank, market in enumerate(market_rows, start=1):
        row = dict(market)
        family_key, family_source = market_family(row)
        family = family_memory.get(family_key, {})
        asset_scores = [
            fillability_scores.get(str(asset_id), 0.0)
            for asset_id in row.get("clob_token_ids", [])
        ]
        best_fillability_score = max(asset_scores) if asset_scores else 0.0
        family_memory_score = float(family.get("family_memory_score") or 0.0)
        family_observations = int(family.get("observations") or 0)
        has_exploit_memory = (
            best_fillability_score > 0
            or family_observations >= config.min_family_observations_for_exploit
        )
        row["original_rank"] = original_rank
        row["market_family"] = family_key
        row["market_family_source"] = family_source
        row["family_memory_score"] = family_memory_score
        row["family_observations"] = family_observations
        row["best_asset_fillability_score"] = best_fillability_score
        row["combined_discovery_score"] = (
            float(row.get("score") or 0.0) * 100
            + best_fillability_score * config.fillability_weight
            + family_memory_score * config.family_memory_weight
        )
        row["selection_mode"] = "exploit" if has_exploit_memory else "explore"
        enriched.append(row)

    exploit = sorted(
        [row for row in enriched if row["selection_mode"] == "exploit"],
        key=lambda row: (
            row["combined_discovery_score"],
            row["best_asset_fillability_score"],
            row["family_memory_score"],
            -row["original_rank"],
        ),
        reverse=True,
    )
    explore = sorted(
        [row for row in enriched if row["selection_mode"] == "explore"],
        key=lambda row: (float(row.get("score") or 0.0), -row["original_rank"]),
        reverse=True,
    )
    exploration_slots = min(
        len(explore),
        math.ceil(len(enriched) * config.exploration_rate),
    )
    ordered = exploit + explore[:exploration_slots] + explore[exploration_slots:]
    for policy_rank, row in enumerate(ordered, start=1):
        row["policy_rank"] = policy_rank
    return ordered


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
        "tags": market.tags,
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
    parser.add_argument("--market-fillability-score")
    parser.add_argument("--market-family-memory")
    parser.add_argument("--exploration-rate", type=float, default=0.20)
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
                market_fillability_score_path=args.market_fillability_score,
                market_family_memory_path=args.market_family_memory,
                exploration_rate=args.exploration_rate,
            ),
        )
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
