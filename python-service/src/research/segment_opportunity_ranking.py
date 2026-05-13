import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd  # type: ignore[import-untyped]

from src.research.backtest import duckdb_literal
from src.research.executable_opportunities import (
    ExecutableOpportunitiesConfig,
    create_executable_opportunities_report,
)
from src.research.game_theory import relation_exists


REPORT_VERSION = "segment_opportunity_ranking_v1"
ALLOWLIST_VERSION = "allowed_segments_v1"


@dataclass(frozen=True)
class SegmentOpportunityRankingConfig:
    min_executable_opportunities: int = 3
    min_avg_expected_edge: float = 0.01
    min_avg_available_depth: float = 1.0
    min_runtime_active_minutes: int = 1
    max_adverse_30s_rate: float = 0.50
    max_synthetic_observed_gap: float = 0.05
    runtime_activity_backfill: bool = False
    runtime_backfill_min_opportunities: int = 3
    runtime_backfill_min_active_minutes: int = 2
    limit: int = 25

    def __post_init__(self) -> None:
        if self.min_executable_opportunities <= 0:
            raise ValueError("min_executable_opportunities must be positive")
        if self.min_runtime_active_minutes <= 0:
            raise ValueError("min_runtime_active_minutes must be positive")
        if self.min_avg_available_depth < 0:
            raise ValueError("min_avg_available_depth must be non-negative")
        if self.runtime_backfill_min_opportunities <= 0:
            raise ValueError("runtime_backfill_min_opportunities must be positive")
        if self.runtime_backfill_min_active_minutes <= 0:
            raise ValueError("runtime_backfill_min_active_minutes must be positive")
        if not 0 <= self.max_adverse_30s_rate <= 1:
            raise ValueError("max_adverse_30s_rate must be between 0 and 1")
        if not 0 <= self.max_synthetic_observed_gap <= 1:
            raise ValueError("max_synthetic_observed_gap must be between 0 and 1")
        if self.limit <= 0:
            raise ValueError("limit must be positive")


def create_segment_opportunity_ranking_report(
    db_path: Path,
    output_dir: Path,
    config: SegmentOpportunityRankingConfig = SegmentOpportunityRankingConfig(),
) -> dict[str, object]:
    create_segment_opportunity_ranking_views(db_path, config)
    output_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
        counts = copy_views(
            conn,
            output_dir,
            (
                "segment_opportunity_ranking",
                "selected_segment_opportunities",
                "allowed_segment_candidates",
            ),
        )
        selected = normalize_records(
            conn.execute(
                """
                select
                    rank,
                    market_id,
                    asset_id,
                    side,
                    strategy,
                    model_version,
                    spread_bucket,
                    timing_bucket,
                    executable_opportunities,
                    avg_expected_edge,
                    avg_available_depth,
                    runtime_opportunities,
                    runtime_active_minutes,
                    runtime_opportunity_density,
                    observed_fill_rate,
                    synthetic_fill_rate,
                    synthetic_observed_gap,
                    adverse_30s_rate,
                    avg_pnl_30s,
                    recommendation
                from selected_segment_opportunities
                order by rank
                """
            )
            .fetch_df()
            .to_dict(orient="records")
        )
        allowed = normalize_records(
            conn.execute(
                """
                select
                    rank,
                    market_id,
                    asset_id,
                    side,
                    strategy,
                    model_version,
                    spread_bucket,
                    timing_bucket,
                    executable_opportunities,
                    runtime_opportunities,
                    runtime_active_minutes,
                    runtime_opportunity_density,
                    observed_fill_rate,
                    synthetic_fill_rate,
                    synthetic_observed_gap,
                    adverse_30s_rate,
                    avg_pnl_30s,
                    recommendation,
                    allowed_reason
                from allowed_segment_candidates
                order by rank
                """
            )
            .fetch_df()
            .to_dict(orient="records")
        )
        top_segments = normalize_records(
            conn.execute(
                """
                select *
                from segment_opportunity_ranking
                order by rank
                limit 25
                """
            )
            .fetch_df()
            .to_dict(orient="records")
        )
    allowed_payload = allowed_segments_payload(allowed, output_dir)
    allowed_path = output_dir / "allowed_segments.json"
    allowed_path.write_text(
        json.dumps(allowed_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    report: dict[str, object] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_segment_opportunity_ranking_only",
        "config": asdict(config),
        "counts": {
            **counts,
            "ranked_segments": counts["segment_opportunity_ranking"],
            "selected_segments": counts["selected_segment_opportunities"],
            "allowed_segments": counts["allowed_segment_candidates"],
        },
        "selected": selected,
        "selected_segment_keys": [segment_key(row) for row in selected],
        "allowed": allowed,
        "allowed_segment_keys": [segment_key(row) for row in allowed],
        "top_segments": top_segments,
        "allowed_segments_path": str(allowed_path),
        "outputs": [
            "segment_opportunity_ranking.parquet",
            "selected_segment_opportunities.parquet",
            "allowed_segment_candidates.parquet",
            "allowed_segments.json",
            "segment_opportunity_ranking.json",
        ],
    }
    (output_dir / "segment_opportunity_ranking.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return report


def create_segment_opportunity_ranking_views(
    db_path: Path,
    config: SegmentOpportunityRankingConfig = SegmentOpportunityRankingConfig(),
) -> None:
    with duckdb.connect(str(db_path)) as conn:
        has_executable_opportunities = relation_exists(conn, "executable_opportunities")
    if not has_executable_opportunities:
        create_executable_opportunities_report(
            db_path,
            Path(str(db_path.parent / "executable_opportunities")),
            ExecutableOpportunitiesConfig(),
        )
    with duckdb.connect(str(db_path)) as conn:
        timestamp_expr = executable_opportunity_timestamp_expr(conn)
        conn.execute(
            f"""
            create or replace view segment_opportunity_base as
            select
                market_id,
                asset_id,
                coalesce(nullif(side, ''), 'BUY') as side,
                coalesce(strategy, 'unknown') as strategy,
                coalesce(model_version, 'unknown') as model_version,
                coalesce(spread_bucket, 'unknown') as spread_bucket,
                coalesce(timing_bucket, 'unknown') as timing_bucket,
                count(*) as opportunities,
                count(*) as runtime_opportunities,
                count(distinct cast(floor(({timestamp_expr}) / 60000) as bigint))
                    as runtime_active_minutes,
                case
                    when count(distinct cast(floor(({timestamp_expr}) / 60000) as bigint)) > 0
                    then count(*)::double
                        / count(distinct cast(floor(({timestamp_expr}) / 60000) as bigint))
                    else 0
                end as runtime_opportunity_density,
                min({timestamp_expr}) as first_signal_timestamp_ms,
                max({timestamp_expr}) as last_signal_timestamp_ms,
                cast(sum(case when is_executable then 1 else 0 end) as bigint)
                    as executable_opportunities,
                avg(case when is_executable then 1.0 else 0.0 end) as executable_rate,
                cast(sum(case when observed_filled then 1 else 0 end) as bigint)
                    as observed_fills,
                avg(case when observed_filled then 1.0 else 0.0 end) as observed_fill_rate,
                cast(sum(case when synthetic_filled then 1 else 0 end) as bigint)
                    as synthetic_fills,
                avg(case when synthetic_filled then 1.0 else 0.0 end) as synthetic_fill_rate,
                avg(expected_edge) as avg_expected_edge,
                avg(available_depth) as avg_available_depth,
                avg(adverse_30s) as adverse_30s_rate,
                avg(pnl_30s) as avg_pnl_30s,
                avg(executable_score) as avg_executable_score
            from executable_opportunities
            group by
                market_id,
                asset_id,
                coalesce(nullif(side, ''), 'BUY'),
                coalesce(strategy, 'unknown'),
                coalesce(model_version, 'unknown'),
                coalesce(spread_bucket, 'unknown'),
                coalesce(timing_bucket, 'unknown')
            """
        )
        conn.execute(
            f"""
            create or replace view segment_opportunity_ranking as
            select
                row_number() over (
                    order by
                        case recommendation
                            when 'PROMOTE_TO_OBSERVATION' then 1
                            when 'KEEP_DIAGNOSTIC' then 2
                            when 'REJECT_SEGMENT' then 3
                            else 4
                        end,
                        opportunity_score desc,
                        executable_opportunities desc,
                        asset_id
                ) as rank,
                *
            from (
                select
                    *,
                    coalesce(synthetic_fill_rate, 0) - coalesce(observed_fill_rate, 0)
                        as synthetic_observed_gap,
                    (
                        coalesce(observed_fill_rate, 0) * 4.0
                        + coalesce(executable_rate, 0) * 1.5
                        + least(greatest(coalesce(avg_expected_edge, 0), 0) * 10.0, 1.0)
                        + least(coalesce(avg_available_depth, 0) / 100.0, 1.0) * 0.25
                        + least(coalesce(runtime_active_minutes, 0) / 10.0, 1.0) * 0.75
                        + least(coalesce(runtime_opportunity_density, 0) / 10.0, 1.0) * 0.25
                        + coalesce(avg_pnl_30s, 0) * 5.0
                        - greatest(coalesce(synthetic_fill_rate, 0) - coalesce(observed_fill_rate, 0), 0) * 2.0
                        - coalesce(adverse_30s_rate, 0) * 2.0
                    ) as opportunity_score,
                    case
                        when executable_opportunities < {config.min_executable_opportunities}
                        then 'KEEP_DIAGNOSTIC'
                        when runtime_active_minutes < {config.min_runtime_active_minutes}
                        then 'KEEP_DIAGNOSTIC'
                        when coalesce(avg_available_depth, 0) < {config.min_avg_available_depth}
                        then 'KEEP_DIAGNOSTIC'
                        when coalesce(avg_expected_edge, 0) < {config.min_avg_expected_edge}
                        then 'KEEP_DIAGNOSTIC'
                        when adverse_30s_rate is not null
                          and adverse_30s_rate > {config.max_adverse_30s_rate}
                        then 'REJECT_SEGMENT'
                        when coalesce(synthetic_fill_rate, 0) - coalesce(observed_fill_rate, 0)
                          > {config.max_synthetic_observed_gap}
                        then 'KEEP_DIAGNOSTIC'
                        else 'PROMOTE_TO_OBSERVATION'
                    end as recommendation
                from segment_opportunity_base
            )
            """
        )
        backfill_sql = ""
        if config.runtime_activity_backfill:
            backfill_sql = f"""
                union all
                select
                    *,
                    'RUNTIME_ACTIVITY_BACKFILL' as allowed_reason
                from ranked_with_key
                where recommendation = 'KEEP_DIAGNOSTIC'
                  and runtime_opportunities >= {config.runtime_backfill_min_opportunities}
                  and runtime_active_minutes >= {config.runtime_backfill_min_active_minutes}
                  and segment_key not in (
                    select segment_key
                    from promoted_segments
                  )
            """
        conn.execute(
            f"""
            create or replace view selected_segment_opportunities as
            select *
            from segment_opportunity_ranking
            where recommendation = 'PROMOTE_TO_OBSERVATION'
            order by rank
            limit {config.limit}
            """
        )
        conn.execute(
            f"""
            create or replace view allowed_segment_candidates as
            with ranked_with_key as (
                select
                    *,
                    market_id || '|' || asset_id || '|' || side || '|' || strategy || '|' || model_version
                        as segment_key
                from segment_opportunity_ranking
            ),
            promoted_segments as (
                select
                    *,
                    'PROMOTE_TO_OBSERVATION' as allowed_reason
                from ranked_with_key
                where recommendation = 'PROMOTE_TO_OBSERVATION'
            ),
            candidate_segments as (
                select *
                from promoted_segments
                {backfill_sql}
            )
            select *
            from candidate_segments
            order by
                case allowed_reason
                    when 'PROMOTE_TO_OBSERVATION' then 1
                    when 'RUNTIME_ACTIVITY_BACKFILL' then 2
                    else 3
                end,
                opportunity_score desc,
                runtime_active_minutes desc,
                runtime_opportunities desc,
                rank
            limit {config.limit}
            """
        )


def allowed_segments_payload(
    selected: list[dict[str, object]],
    output_dir: Path,
) -> dict[str, object]:
    segments: list[dict[str, object]] = []
    for row in selected:
        segments.append(
            {
                "market_id": row["market_id"],
                "asset_id": row["asset_id"],
                "side": row["side"],
                "strategy": None,
                "model_version": None,
                "spread_bucket": row.get("spread_bucket"),
                "timing_bucket": row.get("timing_bucket"),
                "reason": row.get("allowed_reason") or "executable_opportunity_segment",
            }
        )
    return {
        "version": ALLOWLIST_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "source_report_version": REPORT_VERSION,
        "decision_policy": "offline_allowed_segment_candidate_only",
        "source_report": str(output_dir / "segment_opportunity_ranking.json"),
        "segments": segments,
    }


def segment_key(row: dict[str, object]) -> str:
    return "|".join(
        str(row.get(key) or "")
        for key in ("market_id", "asset_id", "side", "strategy", "model_version")
    )


def executable_opportunity_timestamp_expr(conn: duckdb.DuckDBPyConnection) -> str:
    columns = {
        str(row[1])
        for row in conn.execute("pragma table_info('executable_opportunities')").fetchall()
    }
    if "signal_timestamp_ms" in columns:
        return "signal_timestamp_ms"
    if "event_timestamp_ms" in columns:
        return "event_timestamp_ms"
    return "0"


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
    parser = argparse.ArgumentParser(description="Rank executable opportunity segments")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--min-executable-opportunities",
        type=int,
        default=SegmentOpportunityRankingConfig.min_executable_opportunities,
    )
    parser.add_argument(
        "--min-avg-expected-edge",
        type=float,
        default=SegmentOpportunityRankingConfig.min_avg_expected_edge,
    )
    parser.add_argument(
        "--min-avg-available-depth",
        type=float,
        default=SegmentOpportunityRankingConfig.min_avg_available_depth,
    )
    parser.add_argument(
        "--max-adverse-30s-rate",
        type=float,
        default=SegmentOpportunityRankingConfig.max_adverse_30s_rate,
    )
    parser.add_argument(
        "--max-synthetic-observed-gap",
        type=float,
        default=SegmentOpportunityRankingConfig.max_synthetic_observed_gap,
    )
    parser.add_argument(
        "--min-runtime-active-minutes",
        type=int,
        default=SegmentOpportunityRankingConfig.min_runtime_active_minutes,
    )
    parser.add_argument(
        "--runtime-activity-backfill",
        action="store_true",
    )
    parser.add_argument(
        "--runtime-backfill-min-opportunities",
        type=int,
        default=SegmentOpportunityRankingConfig.runtime_backfill_min_opportunities,
    )
    parser.add_argument(
        "--runtime-backfill-min-active-minutes",
        type=int,
        default=SegmentOpportunityRankingConfig.runtime_backfill_min_active_minutes,
    )
    parser.add_argument("--limit", type=int, default=SegmentOpportunityRankingConfig.limit)
    args = parser.parse_args()
    report = create_segment_opportunity_ranking_report(
        Path(args.duckdb),
        Path(args.output_dir),
        SegmentOpportunityRankingConfig(
            min_executable_opportunities=args.min_executable_opportunities,
            min_avg_expected_edge=args.min_avg_expected_edge,
            min_avg_available_depth=args.min_avg_available_depth,
            min_runtime_active_minutes=args.min_runtime_active_minutes,
            max_adverse_30s_rate=args.max_adverse_30s_rate,
            max_synthetic_observed_gap=args.max_synthetic_observed_gap,
            runtime_activity_backfill=args.runtime_activity_backfill,
            runtime_backfill_min_opportunities=args.runtime_backfill_min_opportunities,
            runtime_backfill_min_active_minutes=args.runtime_backfill_min_active_minutes,
            limit=args.limit,
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
