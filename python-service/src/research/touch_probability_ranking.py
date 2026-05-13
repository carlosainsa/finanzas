import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd  # type: ignore[import-untyped]

from src.research.backtest import duckdb_literal
from src.research.game_theory import relation_exists
from src.research.market_opportunity_selector import (
    MarketOpportunityConfig,
    create_market_opportunity_views,
)
from src.research.quote_execution_diagnostics import (
    QuoteExecutionDiagnosticsConfig,
    create_quote_execution_diagnostics_views,
)


REPORT_VERSION = "touch_probability_ranking_v1"


@dataclass(frozen=True)
class TouchProbabilityRankingConfig:
    min_signals: int = 5
    min_future_touch_rate: float = 0.00625
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


def create_touch_probability_ranking_report(
    db_path: Path,
    output_dir: Path,
    config: TouchProbabilityRankingConfig = TouchProbabilityRankingConfig(),
) -> dict[str, object]:
    create_touch_probability_ranking_views(db_path, config)
    output_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
        counts = copy_views(
            conn,
            output_dir,
            (
                "touch_probability_features",
                "touch_probability_ranking",
                "selected_touch_probability_markets",
            ),
        )
        selected = normalize_records(
            conn.execute(
                """
                select *
                from selected_touch_probability_markets
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
        "decision_policy": "offline_touch_probability_ranking_only",
        "config": asdict(config),
        "counts": counts,
        "selected_market_asset_ids": [str(row["asset_id"]) for row in selected],
        "selected": selected,
        "outputs": [
            "touch_probability_features.parquet",
            "touch_probability_ranking.parquet",
            "selected_touch_probability_markets.parquet",
            "touch_probability_ranking.json",
        ],
    }
    (output_dir / "touch_probability_ranking.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return report


def create_touch_probability_ranking_views(
    db_path: Path,
    config: TouchProbabilityRankingConfig = TouchProbabilityRankingConfig(),
) -> None:
    create_market_opportunity_views(db_path, MarketOpportunityConfig(limit=config.limit))
    with duckdb.connect(str(db_path)) as conn:
        if relation_exists(conn, "segment_opportunity_base"):
            create_touch_probability_views_from_segment_base(conn, config)
            return
    create_quote_execution_diagnostics_views(db_path, QuoteExecutionDiagnosticsConfig())
    with duckdb.connect(str(db_path)) as conn:
        create_touch_probability_views_from_quote_execution(conn, config)


def create_touch_probability_views_from_segment_base(
    conn: duckdb.DuckDBPyConnection,
    config: TouchProbabilityRankingConfig,
) -> None:
    conn.execute(
        f"""
        create or replace view touch_probability_features as
        with asset_segments as (
            select
                market_id,
                asset_id,
                coalesce(nullif(side, ''), 'BUY') as side,
                coalesce(strategy, 'unknown') as strategy,
                coalesce(model_version, 'unknown') as model_version,
                sum(coalesce(opportunities, 0)) as signals,
                sum(coalesce(opportunities, 0)) as dry_run_signal_lifecycles,
                sum(coalesce(observed_fills, 0)) as dry_run_filled_signals,
                sum(coalesce(synthetic_fills, 0)) as synthetic_filled_signals,
                greatest(
                    sum(coalesce(synthetic_fills, 0))
                    - sum(coalesce(observed_fills, 0)),
                    0
                ) as synthetic_only_signals,
                greatest(
                    sum(coalesce(opportunities, 0))
                    - greatest(
                        sum(coalesce(synthetic_fills, 0)),
                        sum(coalesce(observed_fills, 0))
                    ),
                    0
                ) as neither_signals,
                case
                    when sum(coalesce(opportunities, 0)) > 0
                    then sum(coalesce(observed_fills, 0))::double
                       / sum(coalesce(opportunities, 0))
                    else null
                end as observed_fill_rate,
                case
                    when sum(coalesce(opportunities, 0)) > 0
                    then sum(coalesce(synthetic_fills, 0))::double
                       / sum(coalesce(opportunities, 0))
                    else null
                end as synthetic_fill_rate,
                case
                    when sum(coalesce(opportunities, 0)) > 0
                    then sum(coalesce(synthetic_fills, 0))::double
                       / sum(coalesce(opportunities, 0))
                    else null
                end as future_touch_rate,
                cast(null as double) as at_touch_rate,
                cast(null as double) as inside_spread_rate,
                cast(null as double) as behind_touch_rate,
                avg(coalesce(executable_rate, 0)) as executable_rate,
                avg(coalesce(avg_expected_edge, 0)) as avg_expected_edge,
                avg(coalesce(avg_available_depth, 0)) as avg_available_depth,
                avg(coalesce(avg_executable_score, 0)) as avg_executable_score,
                avg(coalesce(runtime_active_minutes, 0)) as runtime_active_minutes,
                sum(coalesce(runtime_opportunities, 0)) as runtime_opportunities
            from segment_opportunity_base
            group by
                market_id,
                asset_id,
                coalesce(nullif(side, ''), 'BUY'),
                coalesce(strategy, 'unknown'),
                coalesce(model_version, 'unknown')
        )
        select
            s.market_id,
            s.asset_id,
            s.side,
            s.strategy,
            s.model_version,
            s.signals,
            s.dry_run_signal_lifecycles,
            s.dry_run_filled_signals,
            s.synthetic_filled_signals,
            s.synthetic_only_signals,
            s.neither_signals,
            s.observed_fill_rate,
            s.synthetic_fill_rate,
            s.future_touch_rate,
            s.at_touch_rate,
            s.inside_spread_rate,
            s.behind_touch_rate,
            coalesce(m.avg_opportunity_spread, m.avg_spread, 0.0) as avg_spread_at_signal,
            cast(null as double) as avg_distance_to_mid,
            cast(null as double) as avg_distance_to_touch,
            cast(null as double) as avg_required_quote_move,
            cast(null as double) as avg_book_age_ms,
            cast(null as double) as avg_dry_run_terminal_latency_ms,
            cast(null as double) as avg_synthetic_touch_latency_ms,
            coalesce(m.stale_rate, 0.0) as stale_rate,
            coalesce(m.spread_opportunity_density, 0.0) as spread_opportunity_density,
            greatest(coalesce(m.avg_total_depth, 0.0), coalesce(s.avg_available_depth, 0.0)) as avg_total_depth,
            coalesce(m.liquidity, 0.0) as liquidity,
            coalesce(m.volume, 0.0) as volume,
            m.question,
            m.slug,
            m.outcome,
            (
                coalesce(s.future_touch_rate, 0) * 120
                + coalesce(s.observed_fill_rate, 0) * 80
                + coalesce(s.executable_rate, 0) * 20
                + least(greatest(coalesce(m.avg_total_depth, 0), coalesce(s.avg_available_depth, 0)), 1000000) / 200000
                + least(coalesce(m.liquidity, 0), 100000) / 20000
                + coalesce(m.spread_opportunity_density, 0) * 5
                + coalesce(s.avg_executable_score, 0) * 2
                - coalesce(m.stale_rate, 0.0) * 25
                - greatest(coalesce(s.synthetic_fill_rate, 0) - coalesce(s.observed_fill_rate, 0), 0) * 40
            ) as touch_probability_score
        from asset_segments s
        left join market_opportunity_ranking m
          on m.market_id = s.market_id
         and m.asset_id = s.asset_id
        where s.signals >= {config.min_signals}
        """
    )
    create_touch_probability_ranking_selection_views(conn, config)


def create_touch_probability_views_from_quote_execution(
    conn: duckdb.DuckDBPyConnection,
    config: TouchProbabilityRankingConfig,
) -> None:
        conn.execute(
            f"""
            create or replace view touch_probability_features as
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
                q.synthetic_only_signals,
                q.neither_signals,
                case
                    when q.signals > 0
                    then q.dry_run_filled_signals::double / q.signals
                    else null
                end as observed_fill_rate,
                case
                    when q.signals > 0
                    then q.synthetic_filled_signals::double / q.signals
                    else null
                end as synthetic_fill_rate,
                q.future_touch_rate,
                q.at_touch_rate,
                q.inside_spread_rate,
                q.behind_touch_rate,
                q.avg_spread_at_signal,
                q.avg_distance_to_mid,
                q.avg_distance_to_touch,
                q.avg_required_quote_move,
                q.avg_book_age_ms,
                q.avg_dry_run_terminal_latency_ms,
                q.avg_synthetic_touch_latency_ms,
                coalesce(m.stale_rate, 0.0) as stale_rate,
                coalesce(m.spread_opportunity_density, 0.0) as spread_opportunity_density,
                coalesce(m.avg_total_depth, 0.0) as avg_total_depth,
                coalesce(m.liquidity, 0.0) as liquidity,
                coalesce(m.volume, 0.0) as volume,
                m.question,
                m.slug,
                m.outcome,
                (
                    coalesce(q.future_touch_rate, 0) * 120
                    + coalesce(q.dry_run_filled_signals::double / nullif(q.signals, 0), 0) * 80
                    + coalesce(q.inside_spread_rate, 0) * 15
                    + coalesce(q.at_touch_rate, 0) * 10
                    + least(coalesce(m.avg_total_depth, 0), 1000000) / 200000
                    + least(coalesce(m.liquidity, 0), 100000) / 20000
                    + coalesce(m.spread_opportunity_density, 0) * 5
                    - coalesce(q.behind_touch_rate, 0) * 15
                    - coalesce(m.stale_rate, 0.0) * 25
                    - coalesce(q.avg_required_quote_move, 0) * 200
                    - greatest(
                        coalesce(q.synthetic_filled_signals::double / nullif(q.signals, 0), 0)
                        - coalesce(q.dry_run_filled_signals::double / nullif(q.signals, 0), 0),
                        0
                    ) * 40
                ) as touch_probability_score
            from quote_execution_by_market_asset q
            left join market_opportunity_ranking m
              on m.market_id = q.market_id
             and m.asset_id = q.asset_id
            where q.signals >= {config.min_signals}
            """
        )
        create_touch_probability_ranking_selection_views(conn, config)


def create_touch_probability_ranking_selection_views(
    conn: duckdb.DuckDBPyConnection,
    config: TouchProbabilityRankingConfig,
) -> None:
    conn.execute(
        f"""
        create or replace view touch_probability_ranking as
        select
            row_number() over (
                order by
                    touch_probability_score desc,
                    future_touch_rate desc nulls last,
                    observed_fill_rate desc nulls last,
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
        from touch_probability_features
        """
    )
    conn.execute(
        f"""
        create or replace view selected_touch_probability_markets as
        select * exclude (asset_rank)
        from (
            select
                *,
                row_number() over (
                    partition by asset_id
                    order by rank
                ) as asset_rank
            from touch_probability_ranking
            where recommendation = 'PROMOTE_TO_OBSERVATION'
        )
        where asset_rank = 1
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
    parser = argparse.ArgumentParser(
        description="Rank markets by observed/future touch probability"
    )
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--min-signals", type=int, default=TouchProbabilityRankingConfig.min_signals
    )
    parser.add_argument(
        "--min-future-touch-rate",
        type=float,
        default=TouchProbabilityRankingConfig.min_future_touch_rate,
    )
    parser.add_argument(
        "--max-stale-rate",
        type=float,
        default=TouchProbabilityRankingConfig.max_stale_rate,
    )
    parser.add_argument("--limit", type=int, default=TouchProbabilityRankingConfig.limit)
    args = parser.parse_args()
    report = create_touch_probability_ranking_report(
        Path(args.duckdb),
        Path(args.output_dir),
        TouchProbabilityRankingConfig(
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
