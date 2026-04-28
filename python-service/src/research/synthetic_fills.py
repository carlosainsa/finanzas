import json
from dataclasses import asdict, dataclass
from pathlib import Path

import duckdb

from src.research.backtest import duckdb_literal
from src.research.game_theory import ensure_base_views, relation_exists


SYNTHETIC_FILL_MODEL_VERSION = "conservative_orderbook_fill_v1"
SYNTHETIC_FILL_DATA_VERSION = "orderbook_snapshots_v1"
SYNTHETIC_FILL_FEATURE_VERSION = "limit_touch_after_signal_v1"


@dataclass(frozen=True)
class SyntheticFillConfig:
    max_fill_delay_ms: int = 300_000
    min_fill_size: float = 0.000001
    max_fill_fraction: float = 1.0
    min_confidence: float = 0.55


def create_synthetic_fill_views(
    db_path: Path, config: SyntheticFillConfig = SyntheticFillConfig()
) -> None:
    with duckdb.connect(str(db_path)) as conn:
        ensure_base_views(conn)
        conn.execute(
            f"""
            create or replace view synthetic_fill_candidates as
            select
                s.signal_id,
                s.market_id,
                s.asset_id,
                s.side,
                coalesce(s.strategy, 'unknown') as strategy,
                s.model_version::varchar as model_version,
                s.data_version::varchar as data_version,
                s.feature_version::varchar as feature_version,
                s.price as signal_price,
                s.size as signal_size,
                s.confidence,
                s.event_timestamp_ms as signal_timestamp_ms,
                book.event_timestamp_ms as fill_timestamp_ms,
                case
                    when s.side = 'BUY' then book.best_ask
                    when s.side = 'SELL' then book.best_bid
                    else null
                end as touch_price,
                case
                    when s.side = 'BUY' then book.ask_depth
                    when s.side = 'SELL' then book.bid_depth
                    else 0
                end as touch_depth,
                row_number() over (
                    partition by s.signal_id
                    order by book.event_timestamp_ms asc
                ) as candidate_rank
            from signals s
            join orderbook_snapshots book
              on book.market_id = s.market_id
             and book.asset_id = s.asset_id
             and book.event_timestamp_ms > s.event_timestamp_ms
             and book.event_timestamp_ms <= s.event_timestamp_ms + {config.max_fill_delay_ms}
            where s.signal_id is not null
              and s.event_timestamp_ms is not null
              and s.price is not null
              and s.size is not null
              and s.confidence >= {config.min_confidence}
              and (
                (s.side = 'BUY' and book.best_ask is not null and book.best_ask <= s.price)
                or
                (s.side = 'SELL' and book.best_bid is not null and book.best_bid >= s.price)
              )
            """
        )
        conn.execute(
            f"""
            create or replace view synthetic_execution_reports as
            select
                signal_id,
                'synthetic-fill-' || signal_id as order_id,
                case
                    when filled_size >= signal_size then 'MATCHED'
                    else 'PARTIAL'
                end as status,
                signal_price as filled_price,
                filled_size,
                filled_size as cumulative_filled_size,
                greatest(signal_size - filled_size, 0) as remaining_size,
                cast(null as varchar) as error,
                fill_timestamp_ms as event_timestamp_ms,
                '{SYNTHETIC_FILL_MODEL_VERSION}' as synthetic_model_version,
                '{SYNTHETIC_FILL_DATA_VERSION}' as synthetic_data_version,
                '{SYNTHETIC_FILL_FEATURE_VERSION}' as synthetic_feature_version
            from (
                select
                    *,
                    least(
                        signal_size * {config.max_fill_fraction},
                        greatest(touch_depth, 0)
                    ) as filled_size
                from synthetic_fill_candidates
                where candidate_rank = 1
            )
            where filled_size >= {config.min_fill_size}
            """
        )
        conn.execute(
            """
            create or replace view synthetic_fill_summary as
            select
                count(*) as synthetic_reports,
                sum(case when status = 'MATCHED' then 1 else 0 end) as matched_reports,
                sum(case when status = 'PARTIAL' then 1 else 0 end) as partial_reports,
                avg(case when remaining_size = 0 then 1.0 else 0.0 end) as full_fill_rate,
                avg(filled_size) as avg_filled_size
            from synthetic_execution_reports
            """
        )


def export_synthetic_fill_report(
    db_path: Path,
    output_dir: Path,
    config: SyntheticFillConfig = SyntheticFillConfig(),
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    create_synthetic_fill_views(db_path, config)
    counts: dict[str, int] = {}
    with duckdb.connect(str(db_path)) as conn:
        for view_name in (
            "synthetic_fill_candidates",
            "synthetic_execution_reports",
            "synthetic_fill_summary",
        ):
            target = output_dir / f"{view_name}.parquet"
            conn.execute(
                f"copy (select * from {view_name}) to '{duckdb_literal(target.as_posix())}' (format parquet)"
            )
            row = conn.execute(f"select count(*) from {view_name}").fetchone()
            counts[view_name] = int(row[0]) if row else 0
    report: dict[str, object] = {
        "model_version": SYNTHETIC_FILL_MODEL_VERSION,
        "data_version": SYNTHETIC_FILL_DATA_VERSION,
        "feature_version": SYNTHETIC_FILL_FEATURE_VERSION,
        "config": asdict(config),
        "counts": counts,
    }
    (output_dir / "synthetic_fills.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return report


def synthetic_fills_available(conn: duckdb.DuckDBPyConnection) -> bool:
    return relation_exists(conn, "synthetic_execution_reports")


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="research-synthetic-fills")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--max-fill-delay-ms", type=int, default=SyntheticFillConfig.max_fill_delay_ms)
    parser.add_argument("--min-fill-size", type=float, default=SyntheticFillConfig.min_fill_size)
    parser.add_argument(
        "--max-fill-fraction",
        type=float,
        default=SyntheticFillConfig.max_fill_fraction,
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=SyntheticFillConfig.min_confidence,
    )
    args = parser.parse_args()

    report = export_synthetic_fill_report(
        Path(args.duckdb),
        Path(args.output_dir),
        SyntheticFillConfig(
            max_fill_delay_ms=args.max_fill_delay_ms,
            min_fill_size=args.min_fill_size,
            max_fill_fraction=args.max_fill_fraction,
            min_confidence=args.min_confidence,
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
