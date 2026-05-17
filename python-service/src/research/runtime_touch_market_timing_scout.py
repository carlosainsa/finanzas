import argparse
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from src.research.market_opportunity_selector import ensure_market_metadata
from src.research.runtime_touch_ranking import ensure_orderbook_snapshots, normalize_records


REPORT_VERSION = "runtime_touch_market_timing_scout_v1"


@dataclass(frozen=True)
class RuntimeTouchMarketTimingScoutConfig:
    lookback_ms: int = 3_600_000
    window_ms: int = 900_000
    min_assets: int = 2
    limit: int = 20
    signal_min_spread: float = 0.03
    signal_min_depth: float = 1.5
    min_window_snapshots: int = 2
    min_signalable_snapshots: int = 1
    min_signalable_density: float = 0.10
    min_touch_change_rate: float = 0.0
    max_stale_rate: float = 0.25
    stale_gap_ms: int = 60_000
    min_liquidity: float = 0.0

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
        if self.min_signalable_snapshots <= 0:
            raise ValueError("min_signalable_snapshots must be positive")
        if not 0 <= self.min_signalable_density <= 1:
            raise ValueError("min_signalable_density must be between 0 and 1")
        if not 0 <= self.min_touch_change_rate <= 1:
            raise ValueError("min_touch_change_rate must be between 0 and 1")
        if not 0 <= self.max_stale_rate <= 1:
            raise ValueError("max_stale_rate must be between 0 and 1")
        if self.stale_gap_ms <= 0:
            raise ValueError("stale_gap_ms must be positive")
        if self.min_liquidity < 0:
            raise ValueError("min_liquidity must be non-negative")


def create_runtime_touch_market_timing_scout(
    db_path: Path,
    output_dir: Path,
    config: RuntimeTouchMarketTimingScoutConfig = (
        RuntimeTouchMarketTimingScoutConfig()
    ),
) -> dict[str, Any]:
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB does not exist: {db_path}")
    create_market_timing_scout_views(db_path, config)
    output_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
        asset_windows = conn.execute(
            """
            select *
            from runtime_touch_market_timing_asset_windows
            order by window_start_ms, asset_rank
            """
        ).fetch_df()
        windows = conn.execute(
            """
            select *
            from runtime_touch_market_timing_windows
            order by window_rank
            """
        ).fetch_df()
        selected_assets = conn.execute(
            """
            select *
            from selected_runtime_touch_market_timing_assets
            order by asset_rank
            """
        ).fetch_df()

    asset_windows.to_parquet(
        output_dir / "runtime_touch_market_timing_asset_windows.parquet",
        index=False,
    )
    windows.to_parquet(
        output_dir / "runtime_touch_market_timing_windows.parquet",
        index=False,
    )
    selected_assets.to_parquet(
        output_dir / "selected_runtime_touch_market_timing_assets.parquet",
        index=False,
    )
    selected_rows = normalize_records(selected_assets.to_dict(orient="records"))
    all_window_rows = normalize_records(windows.to_dict(orient="records"))
    window_rows = all_window_rows[:10]
    selected_asset_ids = [str(row["asset_id"]) for row in selected_rows]
    eligible_windows = sum(
        1 for row in all_window_rows if row.get("window_decision") == "SCOUT_WINDOW_READY"
    )
    status = "ready" if len(selected_asset_ids) >= config.min_assets else "insufficient_assets"
    payload: dict[str, Any] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_runtime_touch_market_timing_scout_only",
        "source_duckdb": str(db_path),
        "selection_source": "runtime_touch_market_timing_scout",
        "risk_contract": {
            "execution_mode": "dry_run",
            "does_not_modify_quote_policy": True,
            "does_not_modify_risk_limits": True,
            "does_not_publish_signals": True,
            "min_assets": config.min_assets,
        },
        "config": asdict(config),
        "status": status,
        "selection_reason": selection_reason(status, selected_asset_ids, config),
        "counts": {
            "asset_windows": len(asset_windows.index),
            "windows": len(windows.index),
            "eligible_windows": eligible_windows,
            "selected_assets": len(selected_assets.index),
        },
        "best_windows": window_rows,
        "market_asset_ids": selected_asset_ids,
        "market_asset_ids_count": len(selected_asset_ids),
        "market_asset_ids_csv": ",".join(selected_asset_ids),
        "market_asset_ids_sha256": hashlib.sha256(
            ",".join(selected_asset_ids).encode("utf-8")
        ).hexdigest(),
        "selected": selected_rows,
        "outputs": [
            "runtime_touch_market_timing_asset_windows.parquet",
            "runtime_touch_market_timing_windows.parquet",
            "selected_runtime_touch_market_timing_assets.parquet",
            "runtime_touch_market_timing_scout.json",
        ],
    }
    (output_dir / "runtime_touch_market_timing_scout.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload


def create_market_timing_scout_views(
    db_path: Path,
    config: RuntimeTouchMarketTimingScoutConfig,
) -> None:
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
            create or replace view runtime_touch_market_timing_books as
            select
                market_id,
                asset_id,
                floor(event_timestamp_ms / {config.window_ms})::bigint
                    * {config.window_ms} as window_start_ms,
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
                case
                    when lag(best_bid) over (
                        partition by market_id, asset_id
                        order by event_timestamp_ms
                    ) is not null
                    and abs(
                        best_bid - lag(best_bid) over (
                            partition by market_id, asset_id
                            order by event_timestamp_ms
                        )
                    ) > 0.000000001
                    then 1 else 0
                end as best_bid_changed,
                case
                    when lag(best_ask) over (
                        partition by market_id, asset_id
                        order by event_timestamp_ms
                    ) is not null
                    and abs(
                        best_ask - lag(best_ask) over (
                            partition by market_id, asset_id
                            order by event_timestamp_ms
                        )
                    ) > 0.000000001
                    then 1 else 0
                end as best_ask_changed,
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
            create or replace view runtime_touch_market_timing_asset_windows as
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
            grouped as (
                select
                    market_id,
                    asset_id,
                    window_start_ms,
                    window_start_ms + {config.window_ms} as window_end_ms,
                    count(*) as snapshots,
                    count(distinct floor(event_timestamp_ms / 60000)) as active_minutes,
                    sum(is_signalable) as signalable_snapshots,
                    avg(is_signalable::double) as signalable_density,
                    avg(spread) as avg_spread,
                    avg(bid_depth + ask_depth) as avg_total_depth,
                    avg(least(bid_depth, ask_depth)) as avg_min_side_depth,
                    case
                        when count(*) > 0
                        then (sum(best_bid_changed) + sum(best_ask_changed))::double
                           / count(*)
                        else 0
                    end as touch_change_rate,
                    coalesce(
                        avg(
                            case
                                when coalesce(snapshot_gap_ms, 0) > {config.stale_gap_ms}
                                then 1.0 else 0.0
                            end
                        ),
                        0
                    ) as stale_rate
                from runtime_touch_market_timing_books
                group by market_id, asset_id, window_start_ms
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
                        + coalesce(signalable_snapshots, 0) * 4
                        + coalesce(touch_change_rate, 0) * 50
                        + least(coalesce(avg_total_depth, 0), 100000) / 50000
                        + least(coalesce(metadata.liquidity, 0), 100000) / 25000
                        - coalesce(stale_rate, 0) * 25
                    ) as scout_asset_score,
                    case
                        when snapshots >= {config.min_window_snapshots}
                         and signalable_snapshots >= {config.min_signalable_snapshots}
                         and signalable_density >= {config.min_signalable_density}
                         and touch_change_rate >= {config.min_touch_change_rate}
                         and stale_rate <= {config.max_stale_rate}
                         and coalesce(metadata.liquidity, 0) >= {config.min_liquidity}
                         and coalesce(metadata.active, true)
                         and not coalesce(metadata.closed, false)
                         and not coalesce(metadata.archived, false)
                         and coalesce(metadata.enable_order_book, true)
                        then 'SELECT_FOR_TIMING_WINDOW'
                        else 'KEEP_DIAGNOSTIC'
                    end as asset_window_decision
                from grouped
                left join latest_metadata metadata
                  on metadata.market_id = grouped.market_id
                 and metadata.asset_id = grouped.asset_id
            )
            select
                row_number() over (
                    partition by window_start_ms
                    order by
                        case when asset_window_decision = 'SELECT_FOR_TIMING_WINDOW'
                            then 0 else 1 end,
                        scout_asset_score desc,
                        signalable_density desc,
                        liquidity desc,
                        asset_id
                ) as asset_rank,
                *
            from scored
            """
        )
        conn.execute(
            f"""
            create or replace view runtime_touch_market_timing_windows as
            with grouped as (
                select
                    window_start_ms,
                    max(window_end_ms) as window_end_ms,
                    count(*) as asset_windows,
                    sum(
                        case when asset_window_decision = 'SELECT_FOR_TIMING_WINDOW'
                        then 1 else 0 end
                    ) as selected_asset_windows,
                    count(distinct asset_id) as assets_seen,
                    count(
                        distinct case
                            when asset_window_decision = 'SELECT_FOR_TIMING_WINDOW'
                            then asset_id else null
                        end
                    ) as selected_assets,
                    sum(signalable_snapshots) as signalable_snapshots,
                    avg(signalable_density) as avg_signalable_density,
                    avg(touch_change_rate) as avg_touch_change_rate,
                    avg(stale_rate) as avg_stale_rate,
                    avg(scout_asset_score) as avg_scout_asset_score,
                    max(scout_asset_score) as best_scout_asset_score
                from runtime_touch_market_timing_asset_windows
                group by window_start_ms
            ),
            scored as (
                select
                    *,
                    (
                        coalesce(selected_assets, 0) * 100
                        + coalesce(avg_signalable_density, 0) * 50
                        + coalesce(avg_touch_change_rate, 0) * 25
                        + coalesce(best_scout_asset_score, 0)
                        - coalesce(avg_stale_rate, 0) * 25
                    ) as scout_window_score,
                    case
                        when selected_assets >= {config.min_assets}
                        then 'SCOUT_WINDOW_READY'
                        else 'INSUFFICIENT_ASSETS'
                    end as window_decision
                from grouped
            )
            select
                row_number() over (
                    order by
                        case when window_decision = 'SCOUT_WINDOW_READY' then 0 else 1 end,
                        scout_window_score desc,
                        selected_assets desc,
                        avg_signalable_density desc,
                        window_start_ms desc
                ) as window_rank,
                *
            from scored
            """
        )
        conn.execute(
            f"""
            create or replace view selected_runtime_touch_market_timing_assets as
            with best_window as (
                select window_start_ms
                from runtime_touch_market_timing_windows
                where window_decision = 'SCOUT_WINDOW_READY'
                order by window_rank
                limit 1
            )
            select
                assets.*
            from runtime_touch_market_timing_asset_windows assets
            join best_window using (window_start_ms)
            where asset_window_decision = 'SELECT_FOR_TIMING_WINDOW'
            order by asset_rank
            limit {config.limit}
            """
        )


def selection_reason(
    status: str,
    selected_asset_ids: list[str],
    config: RuntimeTouchMarketTimingScoutConfig,
) -> str:
    details = (
        f"window_ms={config.window_ms};"
        f"min_signalable_snapshots={config.min_signalable_snapshots};"
        f"min_signalable_density={config.min_signalable_density};"
        f"signal_min_spread={config.signal_min_spread};"
        f"signal_min_depth={config.signal_min_depth}"
    )
    if status == "ready":
        return f"market_timing_window_meets_minimum_asset_coverage;{details}"
    return (
        f"only_{len(selected_asset_ids)}_assets_available_below_minimum_{config.min_assets};"
        f"expand_market_discovery_or_change_capture_window;{details}"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--lookback-ms", type=int, default=3_600_000)
    parser.add_argument("--window-ms", type=int, default=900_000)
    parser.add_argument("--min-assets", type=int, default=2)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--signal-min-spread", type=float, default=0.03)
    parser.add_argument("--signal-min-depth", type=float, default=1.5)
    parser.add_argument("--min-window-snapshots", type=int, default=2)
    parser.add_argument("--min-signalable-snapshots", type=int, default=1)
    parser.add_argument("--min-signalable-density", type=float, default=0.10)
    parser.add_argument("--min-touch-change-rate", type=float, default=0.0)
    parser.add_argument("--max-stale-rate", type=float, default=0.25)
    parser.add_argument("--min-liquidity", type=float, default=0.0)
    args = parser.parse_args()
    report = create_runtime_touch_market_timing_scout(
        Path(args.duckdb),
        Path(args.output_dir),
        RuntimeTouchMarketTimingScoutConfig(
            lookback_ms=args.lookback_ms,
            window_ms=args.window_ms,
            min_assets=args.min_assets,
            limit=args.limit,
            signal_min_spread=args.signal_min_spread,
            signal_min_depth=args.signal_min_depth,
            min_window_snapshots=args.min_window_snapshots,
            min_signalable_snapshots=args.min_signalable_snapshots,
            min_signalable_density=args.min_signalable_density,
            min_touch_change_rate=args.min_touch_change_rate,
            max_stale_rate=args.max_stale_rate,
            min_liquidity=args.min_liquidity,
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
