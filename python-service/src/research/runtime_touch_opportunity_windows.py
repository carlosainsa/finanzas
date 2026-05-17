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


REPORT_VERSION = "runtime_touch_opportunity_windows_v1"


@dataclass(frozen=True)
class RuntimeTouchOpportunityWindowConfig:
    lookback_ms: int = 900_000
    window_ms: int = 300_000
    min_assets: int = 2
    limit: int = 20
    signal_min_spread: float = 0.03
    signal_min_depth: float = 1.5
    min_window_snapshots: int = 2
    min_window_signalable_snapshots: int = 1
    min_window_signalable_density: float = 0.10
    min_asset_windows: int = 1
    max_window_stale_rate: float = 0.20
    min_liquidity: float = 0.0
    stale_gap_ms: int = 60_000
    excluded_asset_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.lookback_ms <= 0:
            raise ValueError("lookback_ms must be positive")
        if self.window_ms <= 0:
            raise ValueError("window_ms must be positive")
        if self.min_assets <= 0:
            raise ValueError("min_assets must be positive")
        if self.limit < self.min_assets:
            raise ValueError("limit must be >= min_assets")
        if self.signal_min_spread < 0:
            raise ValueError("signal_min_spread must be non-negative")
        if self.signal_min_depth < 0:
            raise ValueError("signal_min_depth must be non-negative")
        if self.min_window_snapshots <= 0:
            raise ValueError("min_window_snapshots must be positive")
        if self.min_window_signalable_snapshots <= 0:
            raise ValueError("min_window_signalable_snapshots must be positive")
        if not 0 <= self.min_window_signalable_density <= 1:
            raise ValueError("min_window_signalable_density must be between 0 and 1")
        if self.min_asset_windows <= 0:
            raise ValueError("min_asset_windows must be positive")
        if not 0 <= self.max_window_stale_rate <= 1:
            raise ValueError("max_window_stale_rate must be between 0 and 1")
        if self.min_liquidity < 0:
            raise ValueError("min_liquidity must be non-negative")
        if self.stale_gap_ms <= 0:
            raise ValueError("stale_gap_ms must be positive")


def create_runtime_touch_opportunity_windows(
    db_path: Path,
    output_dir: Path,
    config: RuntimeTouchOpportunityWindowConfig = (
        RuntimeTouchOpportunityWindowConfig()
    ),
) -> dict[str, Any]:
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB does not exist: {db_path}")
    create_opportunity_window_views(db_path, config)
    output_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
        windows = conn.execute(
            """
            select *
            from runtime_touch_opportunity_windows
            order by asset_id, window_start_ms
            """
        ).fetch_df()
        assets = conn.execute(
            """
            select *
            from runtime_touch_opportunity_window_assets
            order by rank
            """
        ).fetch_df()
        selected = conn.execute(
            f"""
            select *
            from runtime_touch_opportunity_window_assets
            where candidate_decision = 'SELECT_FOR_SELECTION_PROBE'
            order by rank
            limit {config.limit}
            """
        ).fetch_df()

    windows.to_parquet(
        output_dir / "runtime_touch_opportunity_windows.parquet",
        index=False,
    )
    assets.to_parquet(
        output_dir / "runtime_touch_opportunity_window_assets.parquet",
        index=False,
    )
    selected.to_parquet(
        output_dir / "selected_runtime_touch_opportunity_window_assets.parquet",
        index=False,
    )
    selected_rows = normalize_records(selected.to_dict(orient="records"))
    selected_asset_ids = [str(row["asset_id"]) for row in selected_rows]
    status = "ready" if len(selected_asset_ids) >= config.min_assets else "insufficient_assets"
    payload: dict[str, Any] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_runtime_touch_opportunity_window_selection_only",
        "source_duckdb": str(db_path),
        "selection_source": "runtime_touch_opportunity_windows",
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
            "windows": len(windows.index),
            "ranked_assets": len(assets.index),
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
            "runtime_touch_opportunity_windows.parquet",
            "runtime_touch_opportunity_window_assets.parquet",
            "selected_runtime_touch_opportunity_window_assets.parquet",
            "runtime_touch_opportunity_windows.json",
        ],
    }
    (output_dir / "runtime_touch_opportunity_windows.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload


def create_opportunity_window_views(
    db_path: Path,
    config: RuntimeTouchOpportunityWindowConfig,
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
            create or replace view runtime_touch_window_books as
            select
                market_id,
                asset_id,
                floor(event_timestamp_ms / {config.window_ms})::bigint
                    * {config.window_ms} as window_start_ms,
                event_timestamp_ms,
                spread,
                bid_depth,
                ask_depth,
                event_timestamp_ms - lag(event_timestamp_ms) over (
                    partition by market_id, asset_id
                    order by event_timestamp_ms
                ) as snapshot_gap_ms,
                case
                    when spread >= {config.signal_min_spread}
                     and least(bid_depth, ask_depth) >= {config.signal_min_depth}
                    then 1 else 0
                end as is_signalable
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
            create or replace view runtime_touch_opportunity_windows as
            select
                market_id,
                asset_id,
                window_start_ms,
                count(*) as snapshots,
                sum(is_signalable) as signalable_snapshots,
                avg(is_signalable::double) as signalable_density,
                avg(spread) as avg_spread,
                avg(bid_depth + ask_depth) as avg_total_depth,
                avg(least(bid_depth, ask_depth)) as avg_min_side_depth,
                coalesce(
                    avg(
                        case
                            when coalesce(snapshot_gap_ms, 0) > {config.stale_gap_ms}
                            then 1.0 else 0.0
                        end
                    ),
                    0
                ) as stale_rate,
                case
                    when count(*) >= {config.min_window_snapshots}
                     and sum(is_signalable) >= {config.min_window_signalable_snapshots}
                     and avg(is_signalable::double) >= {config.min_window_signalable_density}
                     and coalesce(
                        avg(
                            case
                                when coalesce(snapshot_gap_ms, 0) > {config.stale_gap_ms}
                                then 1.0 else 0.0
                            end
                        ),
                        0
                     ) <= {config.max_window_stale_rate}
                    then true else false
                end as is_opportunity_window
            from runtime_touch_window_books
            group by market_id, asset_id, window_start_ms
            """
        )
        conn.execute(
            f"""
            create or replace view runtime_touch_opportunity_window_assets as
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
            asset_summary as (
                select
                    market_id,
                    asset_id,
                    count(*) as windows_seen,
                    sum(case when is_opportunity_window then 1 else 0 end)
                        as opportunity_windows,
                    sum(signalable_snapshots) as signalable_snapshots,
                    max(signalable_density) as best_window_signalable_density,
                    avg(signalable_density) as avg_window_signalable_density,
                    avg(stale_rate) as avg_window_stale_rate,
                    avg(avg_spread) as avg_spread,
                    avg(avg_total_depth) as avg_total_depth,
                    avg(avg_min_side_depth) as avg_min_side_depth
                from runtime_touch_opportunity_windows
                group by market_id, asset_id
            ),
            scored as (
                select
                    asset_summary.*,
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
                        coalesce(opportunity_windows, 0) * 100
                        + coalesce(best_window_signalable_density, 0) * 50
                        + least(coalesce(metadata.liquidity, 0), 100000) / 25000
                        + least(coalesce(avg_total_depth, 0), 100000) / 50000
                        - coalesce(avg_window_stale_rate, 0) * 25
                    ) as opportunity_window_score,
                    case
                        when opportunity_windows >= {config.min_asset_windows}
                         and coalesce(metadata.liquidity, 0) >= {config.min_liquidity}
                         and coalesce(metadata.active, true)
                         and not coalesce(metadata.closed, false)
                         and not coalesce(metadata.archived, false)
                         and coalesce(metadata.enable_order_book, true)
                        then 'SELECT_FOR_SELECTION_PROBE'
                        else 'KEEP_DIAGNOSTIC'
                    end as candidate_decision
                from asset_summary
                left join latest_metadata metadata
                  on metadata.market_id = asset_summary.market_id
                 and metadata.asset_id = asset_summary.asset_id
                where true
                  {excluded_sql}
            )
            select
                row_number() over (
                    order by
                        case when candidate_decision = 'SELECT_FOR_SELECTION_PROBE' then 0 else 1 end,
                        opportunity_window_score desc,
                        opportunity_windows desc,
                        best_window_signalable_density desc,
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
    return f"and asset_summary.asset_id not in ({values})"


def selection_reason(
    status: str,
    selected_asset_ids: list[str],
    config: RuntimeTouchOpportunityWindowConfig,
) -> str:
    details = (
        f"window_ms={config.window_ms};"
        f"min_window_signalable_snapshots={config.min_window_signalable_snapshots};"
        f"min_window_signalable_density={config.min_window_signalable_density};"
        f"signal_min_spread={config.signal_min_spread};"
        f"signal_min_depth={config.signal_min_depth}"
    )
    if status == "ready":
        return f"opportunity_window_universe_meets_minimum_asset_coverage;{details}"
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
    parser.add_argument("--window-ms", type=int, default=300_000)
    parser.add_argument("--min-assets", type=int, default=2)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--signal-min-spread", type=float, default=0.03)
    parser.add_argument("--signal-min-depth", type=float, default=1.5)
    parser.add_argument("--min-window-snapshots", type=int, default=2)
    parser.add_argument("--min-window-signalable-snapshots", type=int, default=1)
    parser.add_argument("--min-window-signalable-density", type=float, default=0.10)
    parser.add_argument("--min-asset-windows", type=int, default=1)
    parser.add_argument("--max-window-stale-rate", type=float, default=0.20)
    parser.add_argument("--min-liquidity", type=float, default=0.0)
    parser.add_argument("--excluded-asset-ids", default="")
    args = parser.parse_args()
    report = create_runtime_touch_opportunity_windows(
        Path(args.duckdb),
        Path(args.output_dir),
        RuntimeTouchOpportunityWindowConfig(
            window_ms=args.window_ms,
            min_assets=args.min_assets,
            limit=args.limit,
            signal_min_spread=args.signal_min_spread,
            signal_min_depth=args.signal_min_depth,
            min_window_snapshots=args.min_window_snapshots,
            min_window_signalable_snapshots=args.min_window_signalable_snapshots,
            min_window_signalable_density=args.min_window_signalable_density,
            min_asset_windows=args.min_asset_windows,
            max_window_stale_rate=args.max_window_stale_rate,
            min_liquidity=args.min_liquidity,
            excluded_asset_ids=parse_excluded_assets(args.excluded_asset_ids),
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
