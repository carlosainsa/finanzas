import json
from dataclasses import asdict, dataclass
from pathlib import Path

import duckdb

from src.research.backtest import duckdb_literal
from src.research.game_theory import ensure_base_views

BASELINE_STRATEGY = "deterministic_microstructure_baseline_v1"
BASELINE_MODEL_VERSION = "deterministic_microstructure_baseline_v1"
BASELINE_FEATURE_VERSION = "microstructure_features_v1"
BASELINE_DATA_VERSION = "research_orderbook_snapshots_v1"
NEAR_TOUCH_BASELINE_STRATEGY = "deterministic_microstructure_baseline_near_touch_v1"
NEAR_TOUCH_BASELINE_MODEL_VERSION = "deterministic_microstructure_baseline_near_touch_v1"
NEAR_TOUCH_BASELINE_FEATURE_VERSION = "microstructure_features_near_touch_v1"


@dataclass(frozen=True)
class BaselineConfig:
    min_spread: float = 0.03
    max_spread: float = 0.30
    min_depth: float = 1.0
    max_abs_imbalance: float = 0.80
    max_abs_momentum: float = 0.10
    momentum_window_ms: int = 30_000
    max_stale_gap_ms: int = 60_000
    max_adverse_30s_rate: float = 0.50
    order_size: float = 1.0
    quote_placement: str = "passive_bid"
    near_touch_tick_size: float = 0.01
    near_touch_offset_ticks: int = 0
    near_touch_max_spread_fraction: float = 1.0
    max_snapshots_per_asset: int | None = None

    def __post_init__(self) -> None:
        placement = self.quote_placement.lower()
        if placement not in {"passive_bid", "near_touch"}:
            raise ValueError("quote_placement must be passive_bid or near_touch")
        if self.near_touch_tick_size < 0:
            raise ValueError("near_touch_tick_size must be non-negative")
        if self.near_touch_offset_ticks < 0:
            raise ValueError("near_touch_offset_ticks must be non-negative")
        if not 0 <= self.near_touch_max_spread_fraction <= 1:
            raise ValueError("near_touch_max_spread_fraction must be between 0 and 1")
        if self.max_snapshots_per_asset is not None and self.max_snapshots_per_asset <= 0:
            raise ValueError("max_snapshots_per_asset must be positive")


def create_baseline_views(db_path: Path, config: BaselineConfig = BaselineConfig()) -> None:
    strategy = baseline_strategy(config)
    model_version = baseline_model_version(config)
    feature_version = baseline_feature_version(config)
    price_expression = baseline_buy_price_expression(config)
    size_depth_expression = baseline_buy_size_depth_expression(config)
    snapshot_limit_filter = ""
    if config.max_snapshots_per_asset is not None:
        snapshot_limit_filter = (
            "qualify row_number() over ("
            "partition by book.market_id, book.asset_id "
            "order by book.event_timestamp_ms desc"
            f") <= {config.max_snapshots_per_asset}"
        )
    with duckdb.connect(str(db_path)) as conn:
        ensure_base_views(conn)
        ensure_adverse_selection_view(conn)
        conn.execute(
            f"""
            create or replace view baseline_market_features as
            select
                book.market_id,
                book.asset_id,
                book.event_timestamp_ms,
                book.best_bid,
                book.best_ask,
                book.spread,
                book.bid_depth,
                book.ask_depth,
                book.bid_depth + book.ask_depth as total_depth,
                case
                    when book.best_bid is not null and book.best_ask is not null
                    then (book.best_bid + book.best_ask) / 2
                    else null
                end as mid_price,
                case
                    when book.bid_depth + book.ask_depth > 0
                    then (book.bid_depth - book.ask_depth) / (book.bid_depth + book.ask_depth)
                    else null
                end as imbalance,
                book.event_timestamp_ms - lag(book.event_timestamp_ms) over (
                    partition by book.market_id, book.asset_id
                    order by book.event_timestamp_ms
                ) as snapshot_gap_ms,
                (
                    case
                        when book.best_bid is not null and book.best_ask is not null
                        then (book.best_bid + book.best_ask) / 2
                        else null
                    end
                    -
                    (
                        select
                            case
                                when previous.best_bid is not null and previous.best_ask is not null
                                then (previous.best_bid + previous.best_ask) / 2
                                else null
                            end
                        from orderbook_snapshots previous
                        where previous.market_id = book.market_id
                          and previous.asset_id = book.asset_id
                          and previous.event_timestamp_ms <= book.event_timestamp_ms - {config.momentum_window_ms}
                        order by previous.event_timestamp_ms desc
                        limit 1
                    )
                ) as momentum,
                adverse.adverse_30s_rate
            from orderbook_snapshots book
            left join adverse_selection_by_strategy adverse
              on adverse.market_id = book.market_id
             and adverse.side = 'BUY'
             and adverse.strategy = '{duckdb_literal(strategy)}'
            where book.event_timestamp_ms is not null
            {snapshot_limit_filter}
            """
        )
        conn.execute(
            f"""
            create or replace view baseline_filter_decisions as
            select
                *,
                spread >= {config.min_spread} and spread <= {config.max_spread} as passes_spread,
                bid_depth >= {config.min_depth} and ask_depth >= {config.min_depth} as passes_depth,
                abs(coalesce(imbalance, 0)) <= {config.max_abs_imbalance} as passes_imbalance,
                momentum is not null and abs(momentum) <= {config.max_abs_momentum} as passes_momentum,
                coalesce(snapshot_gap_ms, 0) <= {config.max_stale_gap_ms} as passes_stale,
                adverse_30s_rate is null or adverse_30s_rate <= {config.max_adverse_30s_rate} as passes_adverse_selection
            from baseline_market_features
            """
        )
        conn.execute(
            f"""
            create or replace view baseline_signals as
            select
                md5('{duckdb_literal(strategy)}' || ':' || market_id || ':' || asset_id || ':' || event_timestamp_ms::varchar || ':BUY') as signal_id,
                market_id,
                asset_id,
                'BUY' as side,
                {price_expression} as price,
                least({config.order_size}, {size_depth_expression}) as size,
                least(
                    0.99,
                    greatest(
                        0.01,
                        0.50 + spread * 3 + least(total_depth, 100) / 1000 - abs(coalesce(momentum, 0))
                    )
                ) as confidence,
                event_timestamp_ms as timestamp_ms,
                event_timestamp_ms as source_timestamp_ms,
                '{duckdb_literal(strategy)}' as strategy,
                '{duckdb_literal(model_version)}' as model_version,
                '{BASELINE_DATA_VERSION}' as data_version,
                '{duckdb_literal(feature_version)}' as feature_version
            from baseline_filter_decisions
            where passes_spread
              and passes_depth
              and passes_imbalance
              and passes_momentum
              and passes_stale
              and passes_adverse_selection
              and best_bid is not null
            order by market_id, asset_id, event_timestamp_ms
            """
        )
        conn.execute(
            """
            create or replace view baseline_summary as
            select
                count(*) as snapshots,
                sum(case when passes_spread then 1 else 0 end) as spread_passes,
                sum(case when passes_depth then 1 else 0 end) as depth_passes,
                sum(case when passes_imbalance then 1 else 0 end) as imbalance_passes,
                sum(case when passes_momentum then 1 else 0 end) as momentum_passes,
                sum(case when passes_stale then 1 else 0 end) as stale_passes,
                sum(case when passes_adverse_selection then 1 else 0 end) as adverse_selection_passes,
                (select count(*) from baseline_signals) as signals
            from baseline_filter_decisions
            """
        )


def export_baseline_report(
    db_path: Path, output_dir: Path, config: BaselineConfig = BaselineConfig()
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    create_baseline_views(db_path, config)
    outputs = ("baseline_market_features", "baseline_filter_decisions", "baseline_signals", "baseline_summary")
    counts: dict[str, int] = {}
    with duckdb.connect(str(db_path)) as conn:
        for view_name in outputs:
            target = output_dir / f"{view_name}.parquet"
            conn.execute(
                f"copy (select * from {view_name}) to '{duckdb_literal(target.as_posix())}' (format parquet)"
            )
            row = conn.execute(f"select count(*) from {view_name}").fetchone()
            counts[view_name] = int(row[0]) if row else 0
    report: dict[str, object] = {
        "counts": counts,
        "config": asdict(config),
        "strategy": baseline_strategy(config),
        "model_version": baseline_model_version(config),
        "data_version": BASELINE_DATA_VERSION,
        "feature_version": baseline_feature_version(config),
    }
    (output_dir / "baseline_summary.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return report


def ensure_adverse_selection_view(conn: duckdb.DuckDBPyConnection) -> None:
    row = conn.execute(
        """
        select count(*)
        from information_schema.tables
        where table_name = 'adverse_selection_by_strategy'
        """
    ).fetchone()
    if row and int(row[0]) > 0:
        return
    conn.execute(
        """
        create or replace view adverse_selection_by_strategy as
        select
            cast(null as varchar) as strategy,
            cast(null as varchar) as market_id,
            cast(null as varchar) as side,
            cast(null as double) as adverse_30s_rate
        where false
        """
    )


def baseline_strategy(config: BaselineConfig) -> str:
    return (
        NEAR_TOUCH_BASELINE_STRATEGY
        if config.quote_placement.lower() == "near_touch"
        else BASELINE_STRATEGY
    )


def baseline_model_version(config: BaselineConfig) -> str:
    return (
        NEAR_TOUCH_BASELINE_MODEL_VERSION
        if config.quote_placement.lower() == "near_touch"
        else BASELINE_MODEL_VERSION
    )


def baseline_feature_version(config: BaselineConfig) -> str:
    return (
        NEAR_TOUCH_BASELINE_FEATURE_VERSION
        if config.quote_placement.lower() == "near_touch"
        else BASELINE_FEATURE_VERSION
    )


def baseline_buy_price_expression(config: BaselineConfig) -> str:
    if config.quote_placement.lower() == "passive_bid":
        return "best_bid"
    offset = config.near_touch_offset_ticks * config.near_touch_tick_size
    return (
        "greatest(best_bid, least("
        f"best_ask - {offset}, "
        f"best_bid + spread * {config.near_touch_max_spread_fraction}"
        "))"
    )


def baseline_buy_size_depth_expression(config: BaselineConfig) -> str:
    if config.quote_placement.lower() == "near_touch":
        return "ask_depth"
    return "bid_depth"


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="research-deterministic-baseline")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--min-spread", type=float, default=BaselineConfig.min_spread)
    parser.add_argument("--max-spread", type=float, default=BaselineConfig.max_spread)
    parser.add_argument("--min-depth", type=float, default=BaselineConfig.min_depth)
    parser.add_argument("--max-abs-imbalance", type=float, default=BaselineConfig.max_abs_imbalance)
    parser.add_argument("--max-abs-momentum", type=float, default=BaselineConfig.max_abs_momentum)
    parser.add_argument("--momentum-window-ms", type=int, default=BaselineConfig.momentum_window_ms)
    parser.add_argument("--max-stale-gap-ms", type=int, default=BaselineConfig.max_stale_gap_ms)
    parser.add_argument("--max-adverse-30s-rate", type=float, default=BaselineConfig.max_adverse_30s_rate)
    parser.add_argument("--order-size", type=float, default=BaselineConfig.order_size)
    parser.add_argument(
        "--quote-placement",
        choices=("passive_bid", "near_touch"),
        default=BaselineConfig.quote_placement,
    )
    parser.add_argument("--near-touch-tick-size", type=float, default=BaselineConfig.near_touch_tick_size)
    parser.add_argument("--near-touch-offset-ticks", type=int, default=BaselineConfig.near_touch_offset_ticks)
    parser.add_argument(
        "--near-touch-max-spread-fraction",
        type=float,
        default=BaselineConfig.near_touch_max_spread_fraction,
    )
    parser.add_argument("--max-snapshots-per-asset", type=int)
    args = parser.parse_args()

    report = export_baseline_report(
        Path(args.duckdb),
        Path(args.output_dir),
        BaselineConfig(
            min_spread=args.min_spread,
            max_spread=args.max_spread,
            min_depth=args.min_depth,
            max_abs_imbalance=args.max_abs_imbalance,
            max_abs_momentum=args.max_abs_momentum,
            momentum_window_ms=args.momentum_window_ms,
            max_stale_gap_ms=args.max_stale_gap_ms,
            max_adverse_30s_rate=args.max_adverse_30s_rate,
            order_size=args.order_size,
            quote_placement=args.quote_placement,
            near_touch_tick_size=args.near_touch_tick_size,
            near_touch_offset_ticks=args.near_touch_offset_ticks,
            near_touch_max_spread_fraction=args.near_touch_max_spread_fraction,
            max_snapshots_per_asset=args.max_snapshots_per_asset,
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
