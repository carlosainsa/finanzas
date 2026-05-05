import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd  # type: ignore[import-untyped]

from src.research.backtest import duckdb_literal
from src.research.execution_quality import (
    ExecutionQualityConfig,
    create_execution_quality_views,
)
from src.research.game_theory import relation_exists
from src.research.market_opportunity_selector import (
    MarketOpportunityConfig,
    create_market_opportunity_views,
)


REPORT_VERSION = "candidate_market_ranking_v1"


@dataclass(frozen=True)
class CandidateMarketRankingConfig:
    opportunity_weight: float = 0.45
    execution_weight: float = 0.55
    min_combined_score: float = 0.0
    min_execution_fill_rate: float = 0.01
    max_unfilled_rate: float = 0.95
    max_stale_rate: float = 0.10
    limit: int = 20

    def __post_init__(self) -> None:
        if self.opportunity_weight < 0 or self.execution_weight < 0:
            raise ValueError("weights must be non-negative")
        if self.opportunity_weight + self.execution_weight <= 0:
            raise ValueError("at least one weight must be positive")
        if not 0 <= self.min_execution_fill_rate <= 1:
            raise ValueError("min_execution_fill_rate must be between 0 and 1")
        if not 0 <= self.max_unfilled_rate <= 1:
            raise ValueError("max_unfilled_rate must be between 0 and 1")
        if not 0 <= self.max_stale_rate <= 1:
            raise ValueError("max_stale_rate must be between 0 and 1")
        if self.limit <= 0:
            raise ValueError("limit must be positive")


def create_candidate_market_ranking_report(
    db_path: Path,
    output_dir: Path,
    config: CandidateMarketRankingConfig = CandidateMarketRankingConfig(),
) -> dict[str, object]:
    create_candidate_market_ranking_views(db_path, config)
    output_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
        counts = copy_views(
            conn,
            output_dir,
            (
                "candidate_market_ranking",
                "selected_candidate_markets",
            ),
        )
        selected = conn.execute(
            """
            select *
            from selected_candidate_markets
            order by rank
            """
        ).fetch_df()
        recommendation_counts = conn.execute(
            """
            select recommendation, count(*) as count
            from candidate_market_ranking
            group by recommendation
            order by recommendation
            """
        ).fetchall()

    selected_rows = normalize_records(selected.to_dict(orient="records"))
    report: dict[str, object] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_combined_market_ranking_only",
        "config": asdict(config),
        "counts": {
            **counts,
            "recommendations": {
                str(row[0]): int(row[1]) for row in recommendation_counts
            },
        },
        "selected_market_asset_ids": [str(row["asset_id"]) for row in selected_rows],
        "selected": selected_rows,
        "outputs": [
            "candidate_market_ranking.parquet",
            "selected_candidate_markets.parquet",
            "candidate_market_ranking.json",
        ],
    }
    (output_dir / "candidate_market_ranking.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return report


def create_candidate_market_ranking_views(
    db_path: Path,
    config: CandidateMarketRankingConfig = CandidateMarketRankingConfig(),
) -> None:
    with duckdb.connect(str(db_path)) as conn:
        if not relation_exists(conn, "market_opportunity_ranking"):
            create_market_opportunity_views(db_path, MarketOpportunityConfig())
        if not relation_exists(conn, "execution_quality_by_asset"):
            create_execution_quality_views(db_path, ExecutionQualityConfig())

    with duckdb.connect(str(db_path)) as conn:
        ensure_execution_quality_ranking(conn)
        conn.execute(
            f"""
            create or replace view candidate_market_ranking as
            with joined as (
                select
                    coalesce(opportunity.market_id, quality.market_id) as market_id,
                    coalesce(opportunity.asset_id, quality.asset_id) as asset_id,
                    opportunity.outcome,
                    opportunity.question,
                    opportunity.slug,
                    opportunity.snapshots,
                    opportunity.opportunity_snapshots,
                    opportunity.spread_opportunity_density,
                    opportunity.avg_opportunity_spread,
                    opportunity.avg_spread,
                    opportunity.avg_total_depth,
                    opportunity.stale_rate,
                    opportunity.liquidity,
                    opportunity.volume,
                    opportunity.opportunity_score,
                    quality.signals,
                    quality.observed_reports,
                    quality.filled_signals,
                    quality.observed_fill_rate,
                    quality.matched_rate,
                    quality.partial_rate,
                    quality.error_rate,
                    quality.unfilled_rate,
                    quality.avg_abs_slippage,
                    quality.avg_report_latency_ms,
                    quality.avg_distance_to_mid,
                    quality.avg_touch_side_depth,
                    quality.execution_quality_score
                from market_opportunity_ranking opportunity
                full outer join execution_quality_ranking quality
                  on quality.market_id = opportunity.market_id
                 and quality.asset_id = opportunity.asset_id
            ),
            scored as (
                select
                    *,
                    (
                        coalesce(opportunity_score, 0) * {config.opportunity_weight}
                        + coalesce(execution_quality_score, 0) * {config.execution_weight}
                    ) as combined_score,
                    case
                        when asset_id is null then 'REJECT'
                        when opportunity_score is null then 'NEEDS_SPREAD_EVIDENCE'
                        when execution_quality_score is null then 'NEEDS_EXECUTION_EVIDENCE'
                        when coalesce(stale_rate, 1) > {config.max_stale_rate} then 'KEEP_DIAGNOSTIC'
                        when coalesce(observed_fill_rate, 0) < {config.min_execution_fill_rate} then 'KEEP_DIAGNOSTIC'
                        when coalesce(unfilled_rate, 1) > {config.max_unfilled_rate} then 'KEEP_DIAGNOSTIC'
                        else 'PROMOTE_TO_OBSERVATION'
                    end as recommendation
                from joined
                where asset_id is not null
            )
            select
                row_number() over (
                    order by
                        case recommendation
                            when 'PROMOTE_TO_OBSERVATION' then 1
                            when 'KEEP_DIAGNOSTIC' then 2
                            when 'NEEDS_EXECUTION_EVIDENCE' then 3
                            when 'NEEDS_SPREAD_EVIDENCE' then 4
                            else 5
                        end,
                        combined_score desc,
                        coalesce(observed_fill_rate, 0) desc,
                        coalesce(spread_opportunity_density, 0) desc,
                        asset_id
                ) as rank,
                *
            from scored
            where combined_score >= {config.min_combined_score}
            """
        )
        conn.execute(
            f"""
            create or replace view selected_candidate_markets as
            select *
            from candidate_market_ranking
            where recommendation = 'PROMOTE_TO_OBSERVATION'
            order by rank
            limit {config.limit}
            """
        )


def ensure_execution_quality_ranking(conn: duckdb.DuckDBPyConnection) -> None:
    if relation_exists(conn, "execution_quality_ranking"):
        return
    conn.execute(
        """
        create or replace view execution_quality_ranking as
        select
            cast(null as bigint) as rank,
            cast(null as varchar) as market_id,
            cast(null as varchar) as asset_id,
            cast(0 as bigint) as signals,
            cast(0 as bigint) as observed_reports,
            cast(0 as bigint) as filled_signals,
            cast(0 as bigint) as matched_reports,
            cast(0 as bigint) as partial_reports,
            cast(0 as bigint) as error_reports,
            cast(null as double) as avg_fill_fraction,
            cast(null as double) as observed_fill_rate,
            cast(null as double) as matched_rate,
            cast(null as double) as partial_rate,
            cast(null as double) as error_rate,
            cast(null as double) as unfilled_rate,
            cast(null as double) as avg_abs_slippage,
            cast(null as double) as avg_report_latency_ms,
            cast(null as double) as avg_spread_at_signal,
            cast(null as double) as avg_distance_to_mid,
            cast(null as double) as avg_touch_side_depth,
            cast(null as double) as execution_quality_score
        where false
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
    parser = argparse.ArgumentParser(description="Export combined offline candidate market ranking")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--opportunity-weight",
        type=float,
        default=CandidateMarketRankingConfig.opportunity_weight,
    )
    parser.add_argument(
        "--execution-weight",
        type=float,
        default=CandidateMarketRankingConfig.execution_weight,
    )
    parser.add_argument(
        "--min-combined-score",
        type=float,
        default=CandidateMarketRankingConfig.min_combined_score,
    )
    parser.add_argument(
        "--min-execution-fill-rate",
        type=float,
        default=CandidateMarketRankingConfig.min_execution_fill_rate,
    )
    parser.add_argument(
        "--max-unfilled-rate",
        type=float,
        default=CandidateMarketRankingConfig.max_unfilled_rate,
    )
    parser.add_argument(
        "--max-stale-rate",
        type=float,
        default=CandidateMarketRankingConfig.max_stale_rate,
    )
    parser.add_argument("--limit", type=int, default=CandidateMarketRankingConfig.limit)
    args = parser.parse_args()

    report = create_candidate_market_ranking_report(
        Path(args.duckdb),
        Path(args.output_dir),
        CandidateMarketRankingConfig(
            opportunity_weight=args.opportunity_weight,
            execution_weight=args.execution_weight,
            min_combined_score=args.min_combined_score,
            min_execution_fill_rate=args.min_execution_fill_rate,
            max_unfilled_rate=args.max_unfilled_rate,
            max_stale_rate=args.max_stale_rate,
            limit=args.limit,
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
