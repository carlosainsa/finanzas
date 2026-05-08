import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd  # type: ignore[import-untyped]

from src.research.backtest import duckdb_literal
from src.research.market_opportunity_selector import (
    MarketOpportunityConfig,
    create_market_opportunity_views,
)
from src.research.quote_execution_diagnostics import (
    QuoteExecutionDiagnosticsConfig,
    create_quote_execution_diagnostics_views,
)


REPORT_VERSION = "fillability_baseline_v1"


@dataclass(frozen=True)
class FillabilityBaselineConfig:
    min_signals: int = 5
    min_future_touch_rate: float = 0.05
    max_stale_rate: float = 0.10
    limit: int = 20

    def __post_init__(self) -> None:
        if self.min_signals <= 0:
            raise ValueError("min_signals must be positive")
        if not 0 <= self.min_future_touch_rate <= 1:
            raise ValueError("min_future_touch_rate must be between 0 and 1")
        if not 0 <= self.max_stale_rate <= 1:
            raise ValueError("max_stale_rate must be between 0 and 1")
        if self.limit <= 0:
            raise ValueError("limit must be positive")


def create_fillability_baseline_report(
    db_path: Path,
    output_dir: Path,
    config: FillabilityBaselineConfig = FillabilityBaselineConfig(),
) -> dict[str, object]:
    create_fillability_baseline_views(db_path, config)
    output_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
        counts = copy_views(
            conn,
            output_dir,
            (
                "fillability_market_features",
                "fillability_market_ranking",
                "selected_fillability_markets",
            ),
        )
        selected = normalize_records(
            conn.execute(
                """
                select *
                from selected_fillability_markets
                order by rank
                """
            )
            .fetch_df()
            .to_dict(orient="records")
        )
    report: dict[str, object] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_fillability_ranking_only",
        "config": asdict(config),
        "counts": counts,
        "selected_market_asset_ids": [str(row["asset_id"]) for row in selected],
        "selected": selected,
        "outputs": [
            "fillability_market_features.parquet",
            "fillability_market_ranking.parquet",
            "selected_fillability_markets.parquet",
            "fillability_baseline.json",
        ],
    }
    (output_dir / "fillability_baseline.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return report


def create_fillability_baseline_views(
    db_path: Path,
    config: FillabilityBaselineConfig = FillabilityBaselineConfig(),
) -> None:
    create_quote_execution_diagnostics_views(db_path, QuoteExecutionDiagnosticsConfig())
    create_market_opportunity_views(db_path, MarketOpportunityConfig(limit=config.limit))
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            f"""
            create or replace view fillability_market_features as
            select
                q.market_id,
                q.asset_id,
                q.side,
                q.strategy,
                q.model_version,
                q.signals,
                q.dry_run_signal_lifecycles,
                q.dry_run_filled_signals,
                q.synthetic_filled_signals,
                q.neither_signals,
                q.future_touch_rate,
                q.inside_spread_rate,
                q.behind_touch_rate,
                q.avg_spread_at_signal,
                q.avg_distance_to_mid,
                q.avg_distance_to_touch,
                q.avg_required_quote_move,
                q.avg_book_age_ms,
                coalesce(m.stale_rate, 0.0) as stale_rate,
                coalesce(m.spread_opportunity_density, 0.0) as spread_opportunity_density,
                coalesce(m.avg_total_depth, 0.0) as avg_total_depth,
                coalesce(m.liquidity, 0.0) as liquidity,
                coalesce(m.volume, 0.0) as volume,
                m.question,
                m.slug,
                m.outcome,
                (
                    coalesce(q.future_touch_rate, 0) * 100
                    + coalesce(q.inside_spread_rate, 0) * 20
                    + coalesce(m.spread_opportunity_density, 0) * 10
                    + least(coalesce(m.avg_total_depth, 0), 1000000) / 100000
                    + least(coalesce(m.liquidity, 0), 100000) / 10000
                    - coalesce(q.behind_touch_rate, 0) * 15
                    - coalesce(m.stale_rate, 0.0) * 20
                    - coalesce(q.avg_required_quote_move, 0) * 100
                ) as fillability_score
            from quote_execution_by_market_asset q
            left join market_opportunity_ranking m
              on m.market_id = q.market_id
             and m.asset_id = q.asset_id
            where q.signals >= {config.min_signals}
            """
        )
        conn.execute(
            f"""
            create or replace view fillability_market_ranking as
            select
                row_number() over (
                    order by
                        fillability_score desc,
                        future_touch_rate desc nulls last,
                        signals desc,
                        asset_id
                ) as rank,
                *,
                case
                    when future_touch_rate >= {config.min_future_touch_rate}
                     and stale_rate <= {config.max_stale_rate}
                    then 'PROMOTE_TO_OBSERVATION'
                    when future_touch_rate is null then 'NEEDS_TOUCH_EVIDENCE'
                    when future_touch_rate < {config.min_future_touch_rate}
                    then 'KEEP_DIAGNOSTIC'
                    else 'KEEP_DIAGNOSTIC'
                end as recommendation
            from fillability_market_features
            """
        )
        conn.execute(
            f"""
            create or replace view selected_fillability_markets as
            select *
            from fillability_market_ranking
            where recommendation = 'PROMOTE_TO_OBSERVATION'
            order by rank
            limit {config.limit}
            """
        )


def copy_views(
    conn: duckdb.DuckDBPyConnection,
    output_dir: Path,
    view_names: tuple[str, ...],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for view_name in view_names:
        target = output_dir / f"{view_name}.parquet"
        conn.execute(
            f"copy (select * from {view_name}) to '{duckdb_literal(target.as_posix())}' (format parquet)"
        )
        row = conn.execute(f"select count(*) from {view_name}").fetchone()
        counts[view_name] = int(row[0]) if row else 0
    return counts


def normalize_records(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return [{key: normalize_value(value) for key, value in row.items()} for row in rows]


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
    parser = argparse.ArgumentParser(description="Rank markets by offline fillability")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--min-signals", type=int, default=FillabilityBaselineConfig.min_signals)
    parser.add_argument(
        "--min-future-touch-rate",
        type=float,
        default=FillabilityBaselineConfig.min_future_touch_rate,
    )
    parser.add_argument(
        "--max-stale-rate",
        type=float,
        default=FillabilityBaselineConfig.max_stale_rate,
    )
    parser.add_argument("--limit", type=int, default=FillabilityBaselineConfig.limit)
    args = parser.parse_args()
    report = create_fillability_baseline_report(
        Path(args.duckdb),
        Path(args.output_dir),
        FillabilityBaselineConfig(
            min_signals=args.min_signals,
            min_future_touch_rate=args.min_future_touch_rate,
            max_stale_rate=args.max_stale_rate,
            limit=args.limit,
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
