import argparse
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd  # type: ignore[import-untyped]

from src.research.backtest import duckdb_literal
from src.research.market_opportunity_selector import ensure_market_metadata
from src.research.runtime_touch_ranking import ensure_orderbook_snapshots, normalize_records


REPORT_VERSION = "runtime_touch_candidate_expansion_v1"


@dataclass(frozen=True)
class RuntimeTouchCandidateExpansionConfig:
    lookback_ms: int = 900_000
    min_assets: int = 2
    limit: int = 20
    signal_min_spread: float = 0.03
    signal_min_depth: float = 1.5
    min_signalable_snapshots: int = 1
    min_signalable_density: float = 0.01
    min_snapshots: int = 3
    min_active_minutes: int = 1
    max_stale_rate: float = 0.15
    min_liquidity: float = 0.0
    stale_gap_ms: int = 60_000
    excluded_asset_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.lookback_ms <= 0:
            raise ValueError("lookback_ms must be positive")
        if self.min_assets <= 0:
            raise ValueError("min_assets must be positive")
        if self.limit < self.min_assets:
            raise ValueError("limit must be >= min_assets")
        if self.signal_min_spread < 0:
            raise ValueError("signal_min_spread must be non-negative")
        if self.signal_min_depth < 0:
            raise ValueError("signal_min_depth must be non-negative")
        if self.min_signalable_snapshots < 0:
            raise ValueError("min_signalable_snapshots must be non-negative")
        if not 0 <= self.min_signalable_density <= 1:
            raise ValueError("min_signalable_density must be between 0 and 1")
        if self.min_snapshots <= 0:
            raise ValueError("min_snapshots must be positive")
        if self.min_active_minutes <= 0:
            raise ValueError("min_active_minutes must be positive")
        if not 0 <= self.max_stale_rate <= 1:
            raise ValueError("max_stale_rate must be between 0 and 1")
        if self.min_liquidity < 0:
            raise ValueError("min_liquidity must be non-negative")
        if self.stale_gap_ms <= 0:
            raise ValueError("stale_gap_ms must be positive")


def create_runtime_touch_candidate_expansion(
    db_path: Path,
    output_dir: Path,
    config: RuntimeTouchCandidateExpansionConfig = (
        RuntimeTouchCandidateExpansionConfig()
    ),
) -> dict[str, Any]:
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB does not exist: {db_path}")
    create_candidate_expansion_views(db_path, config)
    output_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
        candidates = conn.execute(
            """
            select *
            from runtime_touch_candidate_expansion
            order by rank
            """
        ).fetch_df()
        selected = conn.execute(
            f"""
            select *
            from runtime_touch_candidate_expansion
            where candidate_decision = 'SELECT_FOR_SELECTION_PROBE'
            order by rank
            limit {config.limit}
            """
        ).fetch_df()

    candidates.to_parquet(
        output_dir / "runtime_touch_candidate_expansion.parquet",
        index=False,
    )
    selected.to_parquet(
        output_dir / "selected_runtime_touch_expansion.parquet",
        index=False,
    )
    selected_rows = normalize_records(selected.to_dict(orient="records"))
    selected_asset_ids = [str(row["asset_id"]) for row in selected_rows]
    status = "ready" if len(selected_asset_ids) >= config.min_assets else "insufficient_assets"
    payload: dict[str, Any] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_runtime_touch_selection_only",
        "source_duckdb": str(db_path),
        "selection_source": "runtime_touch_candidate_expansion",
        "risk_contract": {
            "execution_mode": "dry_run",
            "does_not_modify_quote_policy": True,
            "does_not_modify_risk_limits": True,
            "runtime_signal_min_spread": config.signal_min_spread,
            "runtime_signal_min_depth": config.signal_min_depth,
            "min_assets": config.min_assets,
        },
        "config": asdict(config),
        "status": status,
        "selection_reason": selection_reason(status, selected_asset_ids, config),
        "counts": {
            "ranked_assets": len(candidates.index),
            "selected_assets": len(selected.index),
        },
        "market_asset_ids": selected_asset_ids,
        "market_asset_ids_count": len(selected_asset_ids),
        "market_asset_ids_csv": ",".join(selected_asset_ids),
        "market_asset_ids_sha256": hashlib.sha256(
            ",".join(selected_asset_ids).encode("utf-8")
        ).hexdigest(),
        "selected": selected_rows,
        "outputs": [
            "runtime_touch_candidate_expansion.parquet",
            "selected_runtime_touch_expansion.parquet",
            "runtime_touch_candidate_expansion.json",
        ],
    }
    (output_dir / "runtime_touch_candidate_expansion.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload


def create_candidate_expansion_views(
    db_path: Path,
    config: RuntimeTouchCandidateExpansionConfig,
) -> None:
    excluded_sql = excluded_assets_sql(config.excluded_asset_ids)
    with duckdb.connect(str(db_path)) as conn:
        ensure_orderbook_snapshots(conn)
        ensure_market_metadata(conn)
        max_ts = conn.execute(
            "select max(event_timestamp_ms) from orderbook_snapshots"
        ).fetchone()
        max_timestamp_ms = int(max_ts[0]) if max_ts and max_ts[0] is not None else 0
        min_timestamp_ms = max(0, max_timestamp_ms - config.lookback_ms)
        conn.execute(
            f"""
            create or replace view runtime_touch_expansion_recent_books as
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
                ) as snapshot_gap_ms
            from orderbook_snapshots
            where event_timestamp_ms >= {min_timestamp_ms}
              and event_timestamp_ms <= {max_timestamp_ms}
              and best_bid is not null
              and best_ask is not null
              and best_ask > best_bid
              and bid_depth > 0
              and ask_depth > 0
            """
        )
        conn.execute(
            f"""
            create or replace view runtime_touch_candidate_expansion as
            with grouped as (
                select
                    market_id,
                    asset_id,
                    count(*) as snapshots,
                    count(distinct floor(event_timestamp_ms / 60000)) as active_minutes,
                    avg(spread) as avg_spread,
                    min(spread) as min_spread,
                    max(spread) as max_spread,
                    avg(bid_depth + ask_depth) as avg_total_depth,
                    avg(least(bid_depth, ask_depth)) as avg_min_side_depth,
                    sum(
                        case
                            when spread >= {config.signal_min_spread}
                             and least(bid_depth, ask_depth) >= {config.signal_min_depth}
                            then 1 else 0
                        end
                    ) as signalable_snapshots,
                    avg(
                        case
                            when spread >= {config.signal_min_spread}
                             and least(bid_depth, ask_depth) >= {config.signal_min_depth}
                            then 1.0 else 0.0
                        end
                    ) as signalable_density,
                    coalesce(
                        avg(
                            case
                                when coalesce(snapshot_gap_ms, 0) > {config.stale_gap_ms}
                                then 1.0 else 0.0
                            end
                        ),
                        0
                    ) as stale_rate
                from runtime_touch_expansion_recent_books
                group by market_id, asset_id
            ),
            latest_metadata as (
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
            scored as (
                select
                    grouped.*,
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
                        coalesce(signalable_density, 0) * 100
                        + coalesce(signalable_snapshots, 0)
                        + least(coalesce(metadata.liquidity, 0), 100000) / 25000
                        + least(coalesce(avg_total_depth, 0), 100000) / 50000
                        - coalesce(stale_rate, 0) * 25
                    ) as expansion_score,
                    case
                        when snapshots >= {config.min_snapshots}
                         and active_minutes >= {config.min_active_minutes}
                         and signalable_snapshots >= {config.min_signalable_snapshots}
                         and signalable_density >= {config.min_signalable_density}
                         and stale_rate <= {config.max_stale_rate}
                         and coalesce(metadata.liquidity, 0) >= {config.min_liquidity}
                         and coalesce(metadata.active, true)
                         and not coalesce(metadata.closed, false)
                         and not coalesce(metadata.archived, false)
                         and coalesce(metadata.enable_order_book, true)
                        then 'SELECT_FOR_SELECTION_PROBE'
                        else 'KEEP_DIAGNOSTIC'
                    end as candidate_decision
                from grouped
                left join latest_metadata metadata
                  on metadata.market_id = grouped.market_id
                 and metadata.asset_id = grouped.asset_id
                where true
                  {excluded_sql}
            )
            select
                row_number() over (
                    order by
                        case when candidate_decision = 'SELECT_FOR_SELECTION_PROBE' then 0 else 1 end,
                        expansion_score desc,
                        signalable_density desc,
                        signalable_snapshots desc,
                        liquidity desc,
                        asset_id
                ) as rank,
                *
            from scored
            """
        )


def excluded_assets_sql(asset_ids: tuple[str, ...]) -> str:
    if not asset_ids:
        return ""
    values = ",".join(f"'{duckdb_literal(asset_id)}'" for asset_id in asset_ids)
    return f"and grouped.asset_id not in ({values})"


def selection_reason(
    status: str,
    selected_asset_ids: list[str],
    config: RuntimeTouchCandidateExpansionConfig,
) -> str:
    details = (
        f"min_signalable_snapshots={config.min_signalable_snapshots};"
        f"min_signalable_density={config.min_signalable_density};"
        f"signal_min_spread={config.signal_min_spread};"
        f"signal_min_depth={config.signal_min_depth}"
    )
    if status == "ready":
        return f"expanded_signalable_universe_meets_minimum_asset_coverage;{details}"
    return (
        f"only_{len(selected_asset_ids)}_assets_available_below_minimum_{config.min_assets};"
        f"change_market_timing_or_collect_fresh_selection_probe;{details}"
    )


def parse_excluded_assets(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(item.strip() for item in value.split(",") if item.strip())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--min-assets", type=int, default=2)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--signal-min-spread", type=float, default=0.03)
    parser.add_argument("--signal-min-depth", type=float, default=1.5)
    parser.add_argument("--min-signalable-snapshots", type=int, default=1)
    parser.add_argument("--min-signalable-density", type=float, default=0.01)
    parser.add_argument("--min-snapshots", type=int, default=3)
    parser.add_argument("--min-active-minutes", type=int, default=1)
    parser.add_argument("--max-stale-rate", type=float, default=0.15)
    parser.add_argument("--min-liquidity", type=float, default=0.0)
    parser.add_argument("--excluded-asset-ids", default="")
    args = parser.parse_args()
    report = create_runtime_touch_candidate_expansion(
        Path(args.duckdb),
        Path(args.output_dir),
        RuntimeTouchCandidateExpansionConfig(
            min_assets=args.min_assets,
            limit=args.limit,
            signal_min_spread=args.signal_min_spread,
            signal_min_depth=args.signal_min_depth,
            min_signalable_snapshots=args.min_signalable_snapshots,
            min_signalable_density=args.min_signalable_density,
            min_snapshots=args.min_snapshots,
            min_active_minutes=args.min_active_minutes,
            max_stale_rate=args.max_stale_rate,
            min_liquidity=args.min_liquidity,
            excluded_asset_ids=parse_excluded_assets(args.excluded_asset_ids),
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
