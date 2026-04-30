import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd  # type: ignore[import-untyped]

from src.config import settings
from src.research.game_theory import relation_exists


REPORT_VERSION = "market_opportunity_selector_v1"


@dataclass(frozen=True)
class MarketOpportunityConfig:
    min_spread: float = settings.predictor_min_spread
    max_spread: float = 0.30
    min_snapshots: int = 10
    min_opportunity_density: float = 0.005
    min_liquidity: float = 0.0
    max_stale_rate: float = 0.05
    stale_gap_ms: int = 60_000
    limit: int = 20

    def __post_init__(self) -> None:
        if self.min_spread < 0 or self.max_spread < 0:
            raise ValueError("spread bounds must be non-negative")
        if self.min_spread > self.max_spread:
            raise ValueError("min_spread must be less than or equal to max_spread")
        if self.min_snapshots <= 0:
            raise ValueError("min_snapshots must be positive")
        if not 0 <= self.min_opportunity_density <= 1:
            raise ValueError("min_opportunity_density must be between 0 and 1")
        if self.min_liquidity < 0:
            raise ValueError("min_liquidity must be non-negative")
        if not 0 <= self.max_stale_rate <= 1:
            raise ValueError("max_stale_rate must be between 0 and 1")
        if self.stale_gap_ms <= 0:
            raise ValueError("stale_gap_ms must be positive")
        if self.limit <= 0:
            raise ValueError("limit must be positive")


def create_market_opportunity_report(
    db_path: Path,
    output_dir: Path,
    config: MarketOpportunityConfig = MarketOpportunityConfig(),
) -> dict[str, object]:
    create_market_opportunity_views(db_path, config)
    output_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
        ranking = conn.execute(
            """
            select *
            from market_opportunity_ranking
            order by rank
            """
        ).fetch_df()
        selected = ranking.head(config.limit)
        ranking.to_parquet(output_dir / "market_opportunity_ranking.parquet", index=False)
        selected.to_parquet(output_dir / "selected_market_opportunities.parquet", index=False)
    selected_rows = normalize_records(selected.to_dict(orient="records"))
    selected_asset_ids = [str(row["asset_id"]) for row in selected_rows]
    report: dict[str, object] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_market_selection_only",
        "config": asdict(config),
        "counts": {
            "ranked_markets": len(ranking.index),
            "selected_markets": len(selected.index),
        },
        "selected_market_asset_ids": selected_asset_ids,
        "selected_market_asset_ids_csv": ",".join(selected_asset_ids),
        "selected": selected_rows,
        "outputs": [
            "market_opportunity_ranking.parquet",
            "selected_market_opportunities.parquet",
            "market_opportunity_selector.json",
        ],
    }
    (output_dir / "market_opportunity_selector.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return report


def create_market_opportunity_views(
    db_path: Path,
    config: MarketOpportunityConfig = MarketOpportunityConfig(),
) -> None:
    with duckdb.connect(str(db_path)) as conn:
        ensure_orderbook_snapshots(conn)
        ensure_market_metadata(conn)
        conn.execute(
            f"""
            create or replace view market_opportunity_snapshots as
            select
                market_id,
                asset_id,
                event_timestamp_ms,
                best_bid,
                best_ask,
                spread,
                bid_depth,
                ask_depth,
                event_timestamp_ms - lag(event_timestamp_ms) over (
                    partition by market_id, asset_id
                    order by event_timestamp_ms
                ) as snapshot_gap_ms,
                spread >= {config.min_spread}
                    and spread <= {config.max_spread}
                    and best_bid is not null
                    and best_ask is not null
                    and bid_depth > 0
                    and ask_depth > 0 as is_spread_opportunity
            from orderbook_snapshots
            where event_timestamp_ms is not null
            """
        )
        conn.execute(
            f"""
            create or replace view market_opportunity_summary as
            select
                market_id,
                asset_id,
                count(*) as snapshots,
                sum(case when is_spread_opportunity then 1 else 0 end) as opportunity_snapshots,
                avg(case when is_spread_opportunity then spread else null end) as avg_opportunity_spread,
                max(case when is_spread_opportunity then spread else null end) as max_opportunity_spread,
                avg(spread) as avg_spread,
                avg(bid_depth + ask_depth) as avg_total_depth,
                coalesce(
                    avg(case when coalesce(snapshot_gap_ms, 0) > {config.stale_gap_ms} then 1.0 else 0.0 end),
                    0
                ) as stale_rate,
                case
                    when count(*) > 0 then
                        sum(case when is_spread_opportunity then 1 else 0 end)::double / count(*)
                    else 0
                end as spread_opportunity_density
            from market_opportunity_snapshots
            group by market_id, asset_id
            """
        )
        conn.execute(
            f"""
            create or replace view market_opportunity_ranking as
            with latest_metadata as (
                select *
                from (
                    select
                        *,
                        row_number() over (
                            partition by market_id, asset_id
                            order by ingested_at_ms desc nulls last
                        ) as rn
                    from market_metadata
                )
                where rn = 1
            ),
            eligible as (
                select
                    summary.*,
                    metadata.outcome,
                    metadata.question,
                    metadata.slug,
                    coalesce(metadata.liquidity, 0) as liquidity,
                    coalesce(metadata.volume, 0) as volume,
                    metadata.active,
                    metadata.closed,
                    metadata.archived,
                    metadata.enable_order_book,
                    (
                        summary.spread_opportunity_density * 100
                        + least(coalesce(metadata.liquidity, 0), 100000) / 100000
                        + least(coalesce(metadata.volume, 0), 100000) / 200000
                        - summary.stale_rate * 10
                    ) as opportunity_score
                from market_opportunity_summary summary
                left join latest_metadata metadata
                  on metadata.market_id = summary.market_id
                 and metadata.asset_id = summary.asset_id
                where summary.snapshots >= {config.min_snapshots}
                  and summary.spread_opportunity_density >= {config.min_opportunity_density}
                  and coalesce(metadata.liquidity, 0) >= {config.min_liquidity}
                  and summary.stale_rate <= {config.max_stale_rate}
                  and coalesce(metadata.active, true)
                  and not coalesce(metadata.closed, false)
                  and not coalesce(metadata.archived, false)
                  and coalesce(metadata.enable_order_book, true)
            )
            select
                row_number() over (
                    order by opportunity_score desc, spread_opportunity_density desc, snapshots desc, asset_id
                ) as rank,
                *
            from eligible
            """
        )


def ensure_orderbook_snapshots(conn: duckdb.DuckDBPyConnection) -> None:
    if relation_exists(conn, "orderbook_snapshots"):
        return
    conn.execute(
        """
        create or replace view orderbook_snapshots as
        select
            cast(null as varchar) as market_id,
            cast(null as varchar) as asset_id,
            cast(null as bigint) as event_timestamp_ms,
            cast(null as double) as best_bid,
            cast(null as double) as best_ask,
            cast(null as double) as spread,
            cast(null as double) as bid_depth,
            cast(null as double) as ask_depth
        where false
        """
    )


def ensure_market_metadata(conn: duckdb.DuckDBPyConnection) -> None:
    if relation_exists(conn, "market_metadata"):
        return
    conn.execute(
        """
        create or replace view market_metadata as
        select
            cast(null as varchar) as market_id,
            cast(null as varchar) as asset_id,
            cast(null as varchar) as outcome,
            cast(null as varchar) as question,
            cast(null as varchar) as slug,
            cast(null as boolean) as active,
            cast(null as boolean) as closed,
            cast(null as boolean) as archived,
            cast(null as boolean) as enable_order_book,
            cast(null as double) as liquidity,
            cast(null as double) as volume,
            cast(null as bigint) as ingested_at_ms
        where false
        """
    )


def normalize_records(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for row in rows:
        normalized.append({key: normalize_value(value) for key, value in row.items()})
    return normalized


def normalize_value(value: object) -> object:
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        return value.item()  # type: ignore[no-any-return]
    return value


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="market-opportunity-selector")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--min-spread", type=float, default=MarketOpportunityConfig.min_spread)
    parser.add_argument("--max-spread", type=float, default=MarketOpportunityConfig.max_spread)
    parser.add_argument("--min-snapshots", type=int, default=MarketOpportunityConfig.min_snapshots)
    parser.add_argument(
        "--min-opportunity-density",
        type=float,
        default=MarketOpportunityConfig.min_opportunity_density,
    )
    parser.add_argument("--min-liquidity", type=float, default=MarketOpportunityConfig.min_liquidity)
    parser.add_argument("--max-stale-rate", type=float, default=MarketOpportunityConfig.max_stale_rate)
    parser.add_argument("--stale-gap-ms", type=int, default=MarketOpportunityConfig.stale_gap_ms)
    parser.add_argument("--limit", type=int, default=MarketOpportunityConfig.limit)
    args = parser.parse_args()

    report = create_market_opportunity_report(
        Path(args.duckdb),
        Path(args.output_dir),
        MarketOpportunityConfig(
            min_spread=args.min_spread,
            max_spread=args.max_spread,
            min_snapshots=args.min_snapshots,
            min_opportunity_density=args.min_opportunity_density,
            min_liquidity=args.min_liquidity,
            max_stale_rate=args.max_stale_rate,
            stale_gap_ms=args.stale_gap_ms,
            limit=args.limit,
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
