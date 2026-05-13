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
from src.research.candidate_market_ranking import (
    CandidateMarketRankingConfig,
    create_candidate_market_ranking_views,
)
from src.research.fillability_baseline import (
    FillabilityBaselineConfig,
    create_fillability_baseline_views,
)
from src.research.fill_toxicity import FillToxicityConfig, create_fill_toxicity_report
from src.research.game_theory import relation_exists
from src.research.segment_opportunity_ranking import (
    SegmentOpportunityRankingConfig,
    create_segment_opportunity_ranking_report,
)


REPORT_VERSION = "execution_probe_universe_selection_v1"
FILLABILITY_FALLBACK_REASON = (
    "fillability_min_assets_backfill_keep_diagnostic_liquidity_spread"
)
MARKET_OPPORTUNITY_FALLBACK_REASON = (
    "fillability_min_assets_backfill_market_opportunity_liquidity"
)
MARKET_METADATA_FALLBACK_REASON = (
    "fillability_min_assets_backfill_market_metadata_liquidity"
)
DEFAULT_RECOMMENDATIONS = (
    "PROMOTE_TO_OBSERVATION",
    "KEEP_DIAGNOSTIC",
    "NEEDS_EXECUTION_EVIDENCE",
)
MARKET_TIMING_FILTERS = ("none", "future_touch")
SELECTION_SOURCES = ("candidate_market_ranking", "fillability", "executable_segments")
ADVERSE_SELECTION_FILTERS = ("none", "market_side")
TOXICITY_FILTERS = ("none", "segment")


@dataclass(frozen=True)
class ExecutionProbeUniverseConfig:
    profile: str = "execution_probe_v5"
    limit: int = 10
    min_assets: int = 5
    recommendations: tuple[str, ...] = DEFAULT_RECOMMENDATIONS
    market_timing_filter: str = "none"
    min_future_touch_rate: float = 0.10
    min_timing_signals: int = 5
    min_avg_opportunity_spread: float | None = None
    max_avg_opportunity_spread: float | None = None
    selection_source: str = "candidate_market_ranking"
    adverse_selection_filter: str = "none"
    max_adverse_30s_rate: float = 0.50
    min_adverse_filled_events: int = 10
    toxicity_filter: str = "none"
    min_toxicity_filled_events: int = 3
    min_runtime_active_minutes: int = 1
    runtime_activity_backfill: bool = False
    runtime_backfill_min_opportunities: int = 3
    runtime_backfill_min_active_minutes: int = 2

    def __post_init__(self) -> None:
        if self.profile not in {
            "execution_probe_v5",
            "execution_probe_v6",
            "execution_probe_v7",
            "execution_probe_v8",
            "execution_probe_v9",
            "execution_probe_v10",
        }:
            raise ValueError(
                "profile must be execution_probe_v5, execution_probe_v6, execution_probe_v7, execution_probe_v8, execution_probe_v9, or execution_probe_v10"
            )
        if self.limit <= 0:
            raise ValueError("limit must be positive")
        if self.min_assets <= 0:
            raise ValueError("min_assets must be positive")
        if self.min_assets > self.limit:
            raise ValueError("min_assets cannot exceed limit")
        if not self.recommendations:
            raise ValueError("recommendations cannot be empty")
        if self.market_timing_filter not in MARKET_TIMING_FILTERS:
            raise ValueError("market_timing_filter must be none or future_touch")
        if self.selection_source not in SELECTION_SOURCES:
            raise ValueError(
                "selection_source must be candidate_market_ranking, fillability, or executable_segments"
            )
        if self.adverse_selection_filter not in ADVERSE_SELECTION_FILTERS:
            raise ValueError("adverse_selection_filter must be none or market_side")
        if self.toxicity_filter not in TOXICITY_FILTERS:
            raise ValueError("toxicity_filter must be none or segment")
        if not 0 <= self.min_future_touch_rate <= 1:
            raise ValueError("min_future_touch_rate must be between 0 and 1")
        if not 0 <= self.max_adverse_30s_rate <= 1:
            raise ValueError("max_adverse_30s_rate must be between 0 and 1")
        if self.min_timing_signals <= 0:
            raise ValueError("min_timing_signals must be positive")
        if self.min_adverse_filled_events <= 0:
            raise ValueError("min_adverse_filled_events must be positive")
        if self.min_toxicity_filled_events <= 0:
            raise ValueError("min_toxicity_filled_events must be positive")
        if self.min_runtime_active_minutes <= 0:
            raise ValueError("min_runtime_active_minutes must be positive")
        if self.runtime_backfill_min_opportunities <= 0:
            raise ValueError("runtime_backfill_min_opportunities must be positive")
        if self.runtime_backfill_min_active_minutes <= 0:
            raise ValueError("runtime_backfill_min_active_minutes must be positive")
        if (
            self.min_avg_opportunity_spread is not None
            and self.min_avg_opportunity_spread < 0
        ):
            raise ValueError("min_avg_opportunity_spread must be non-negative")
        if (
            self.max_avg_opportunity_spread is not None
            and self.max_avg_opportunity_spread < 0
        ):
            raise ValueError("max_avg_opportunity_spread must be non-negative")
        if (
            self.min_avg_opportunity_spread is not None
            and self.max_avg_opportunity_spread is not None
            and self.min_avg_opportunity_spread > self.max_avg_opportunity_spread
        ):
            raise ValueError(
                "min_avg_opportunity_spread must be less than or equal to max_avg_opportunity_spread"
            )


def create_execution_probe_universe_selection(
    db_path: Path,
    output_dir: Path,
    config: ExecutionProbeUniverseConfig = ExecutionProbeUniverseConfig(),
) -> dict[str, object]:
    if config.selection_source == "executable_segments":
        pass
    elif config.selection_source == "fillability":
        create_fillability_baseline_views(
            db_path,
            FillabilityBaselineConfig(
                min_signals=config.min_timing_signals,
                min_future_touch_rate=config.min_future_touch_rate,
                limit=max(config.limit, config.min_assets),
            ),
        )
    else:
        create_candidate_market_ranking_views(
            db_path, CandidateMarketRankingConfig(limit=max(config.limit, config.min_assets))
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    segment_opportunity_report = create_segment_opportunity_inputs(
        db_path, output_dir, config
    )
    toxicity_report = create_toxicity_inputs(db_path, output_dir, config)
    with duckdb.connect(str(db_path)) as conn:
        has_quote_execution_by_asset = relation_exists(conn, "quote_execution_by_market_asset")
        adverse_exclusions = create_adverse_selection_filter_view(conn, config)
        toxicity_quality = create_toxicity_quality_view(conn, config)
        if (
            config.market_timing_filter == "future_touch"
            and not has_quote_execution_by_asset
        ):
            raise ValueError(
                "market_timing_filter=future_touch requires quote_execution_by_market_asset"
            )
        if config.selection_source == "executable_segments":
            frame = select_executable_segment_universe(conn, config)
        elif config.selection_source == "fillability":
            frame = select_fillability_universe(conn, config)
        else:
            spread_filters = build_candidate_spread_filters(config)
            recommendation_list = ",".join(
                f"'{duckdb_literal(item)}'" for item in config.recommendations
            )
            frame = select_candidate_universe(
                conn,
                config,
                has_quote_execution_by_asset,
                recommendation_list,
                spread_filters,
            )

    selected = normalize_records(frame.to_dict(orient="records"))
    asset_ids = [str(row["asset_id"]) for row in selected if row.get("asset_id")]
    status = "ready" if len(asset_ids) >= config.min_assets else "insufficient_assets"
    fallback = fillability_fallback_summary(selected, config)
    output_parquet = output_dir / "execution_probe_universe_selection.parquet"
    adverse_exclusions_parquet = output_dir / "execution_probe_universe_adverse_exclusions.parquet"
    pd.DataFrame(selected).to_parquet(output_parquet, index=False)
    adverse_exclusions.to_parquet(adverse_exclusions_parquet, index=False)
    toxicity_quality_parquet = output_dir / "execution_probe_universe_toxicity_quality.parquet"
    toxicity_quality.to_parquet(toxicity_quality_parquet, index=False)
    adverse_payload = adverse_selection_filter_payload(adverse_exclusions, config)
    toxicity_payload = toxicity_filter_payload(toxicity_quality, config, toxicity_report)
    payload: dict[str, object] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_multi_market_observation_universe_only",
        "profile": config.profile,
        "config": asdict(config),
        "source_duckdb": str(db_path),
        "source_report_version": source_report_version(config),
        "status": status,
        "selection_reason": selection_reason(status, asset_ids, config, fallback),
        "fallback": fallback,
        "adverse_selection_filter": adverse_payload,
        "toxicity_filter": toxicity_payload,
        "segment_opportunity_filter": segment_opportunity_payload(
            segment_opportunity_report
        ),
        "market_asset_ids": asset_ids,
        "market_asset_ids_count": len(asset_ids),
        "market_asset_ids_csv": ",".join(asset_ids),
        "market_asset_ids_sha256": hashlib.sha256(
            ",".join(asset_ids).encode("utf-8")
        ).hexdigest(),
        "selected": selected,
        "outputs": [
            "execution_probe_universe_selection.parquet",
            "execution_probe_universe_selection.json",
            "execution_probe_universe_adverse_exclusions.parquet",
            "execution_probe_universe_toxicity_quality.parquet",
            "segment_opportunity_ranking/segment_opportunity_ranking.json",
            "segment_opportunity_ranking/allowed_segments.json",
        ],
    }
    (output_dir / "execution_probe_universe_selection.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return payload


def build_candidate_spread_filters(config: ExecutionProbeUniverseConfig) -> list[str]:
    filters: list[str] = []
    if config.min_avg_opportunity_spread is not None:
        filters.append(
            f"coalesce(candidate.avg_opportunity_spread, candidate.avg_spread) >= {config.min_avg_opportunity_spread}"
        )
    if config.max_avg_opportunity_spread is not None:
        filters.append(
            f"coalesce(candidate.avg_opportunity_spread, candidate.avg_spread) <= {config.max_avg_opportunity_spread}"
        )
    return filters


def create_adverse_selection_filter_view(
    conn: duckdb.DuckDBPyConnection,
    config: ExecutionProbeUniverseConfig,
) -> pd.DataFrame:
    if config.adverse_selection_filter == "none" or not relation_exists(
        conn, "adverse_selection_by_strategy"
    ):
        conn.execute(
            """
            create or replace temp table execution_probe_adverse_market_side_exclusions (
                market_id varchar,
                side varchar,
                adverse_filled_events bigint,
                adverse_30s_rate double,
                avg_pnl_30s double,
                reason varchar
            )
            """
        )
        return conn.execute(
            "select * from execution_probe_adverse_market_side_exclusions"
        ).fetch_df()
    conn.execute(
        f"""
        create or replace temp table execution_probe_adverse_market_side_exclusions as
        with adverse as (
            select
                market_id,
                coalesce(nullif(side, ''), 'BUY') as side,
                sum(coalesce(filled_events, 0)) as adverse_filled_events,
                case
                    when sum(coalesce(filled_events, 0)) > 0
                    then sum(coalesce(adverse_30s_count, 0))::double
                       / sum(coalesce(filled_events, 0))
                    else null
                end as adverse_30s_rate,
                case
                    when sum(coalesce(filled_events, 0)) > 0
                    then sum(coalesce(avg_pnl_30s, 0) * coalesce(filled_events, 0))::double
                       / sum(coalesce(filled_events, 0))
                    else null
                end as avg_pnl_30s
            from adverse_selection_by_strategy
            group by market_id, coalesce(nullif(side, ''), 'BUY')
        )
        select
            market_id,
            side,
            adverse_filled_events,
            adverse_30s_rate,
            avg_pnl_30s,
            'market_side_adverse_selection_above_threshold' as reason
        from adverse
        where adverse_filled_events >= {config.min_adverse_filled_events}
          and adverse_30s_rate > {config.max_adverse_30s_rate}
        """
    )
    return conn.execute(
        "select * from execution_probe_adverse_market_side_exclusions"
    ).fetch_df()


def adverse_join_sql(alias: str, side_expression: str = "'BUY'") -> str:
    return f"""
        left join execution_probe_adverse_market_side_exclusions adverse
          on adverse.market_id = {alias}.market_id
         and adverse.side = coalesce(nullif({side_expression}, ''), 'BUY')
    """


def adverse_filter_sql() -> str:
    return "and adverse.market_id is null"


def adverse_selection_filter_payload(
    exclusions: pd.DataFrame,
    config: ExecutionProbeUniverseConfig,
) -> dict[str, object]:
    return {
        "enabled": config.adverse_selection_filter != "none",
        "mode": config.adverse_selection_filter,
        "source_relation": "adverse_selection_by_strategy",
        "max_adverse_30s_rate": config.max_adverse_30s_rate,
        "min_adverse_filled_events": config.min_adverse_filled_events,
        "filtered_count": int(len(exclusions)),
        "excluded": normalize_records(exclusions.to_dict(orient="records")),
    }


def create_toxicity_inputs(
    db_path: Path,
    output_dir: Path,
    config: ExecutionProbeUniverseConfig,
) -> dict[str, object] | None:
    if config.toxicity_filter == "none":
        return None
    return create_fill_toxicity_report(
        db_path,
        output_dir / "fill_toxicity",
        FillToxicityConfig(min_filled_events=config.min_toxicity_filled_events),
    )


def create_toxicity_quality_view(
    conn: duckdb.DuckDBPyConnection,
    config: ExecutionProbeUniverseConfig,
) -> pd.DataFrame:
    if config.toxicity_filter == "none" or not relation_exists(
        conn, "fill_toxicity_by_segment"
    ):
        conn.execute(
            """
            create or replace temp table execution_probe_toxicity_quality (
                market_id varchar,
                asset_id varchar,
                side varchar,
                toxicity_segments bigint,
                toxicity_filled_events bigint,
                rejected_toxicity_segments bigint,
                adverse_30s_rate double,
                avg_pnl_30s double,
                max_toxicity_score double,
                quality_penalty double
            )
            """
        )
        return conn.execute("select * from execution_probe_toxicity_quality").fetch_df()
    conn.execute(
        f"""
        create or replace temp table execution_probe_toxicity_quality as
        with segment_scores as (
            select
                market_id,
                asset_id,
                coalesce(nullif(side, ''), 'BUY') as side,
                count(*) as toxicity_segments,
                sum(coalesce(filled_events, 0)) as toxicity_filled_events,
                sum(case when decision = 'REJECT_TOXICITY' then 1 else 0 end)
                    as rejected_toxicity_segments,
                max(coalesce(toxicity_score, 0)) as max_toxicity_score,
                sum(coalesce(adverse_30s_rate, 0) * coalesce(filled_events, 0))
                    as adverse_weighted_sum,
                sum(coalesce(avg_pnl_30s, 0) * coalesce(filled_events, 0))
                    as pnl_weighted_sum
            from fill_toxicity_by_segment
            group by market_id, asset_id, coalesce(nullif(side, ''), 'BUY')
        )
        select
            market_id,
            asset_id,
            side,
            toxicity_segments,
            toxicity_filled_events,
            rejected_toxicity_segments,
            case
                when toxicity_filled_events > 0
                then adverse_weighted_sum::double / toxicity_filled_events
                else null
            end as adverse_30s_rate,
            case
                when toxicity_filled_events > 0
                then pnl_weighted_sum::double / toxicity_filled_events
                else null
            end as avg_pnl_30s,
            max_toxicity_score,
            case
                when toxicity_filled_events < {config.min_toxicity_filled_events}
                then 0.0
                else
                    least(1.0, coalesce(max_toxicity_score, 0) / 100.0)
                    + case when rejected_toxicity_segments > 0 then 0.50 else 0.0 end
            end as quality_penalty
        from segment_scores
        """
    )
    return conn.execute("select * from execution_probe_toxicity_quality").fetch_df()


def toxicity_join_sql(alias: str, side_expression: str = "'BUY'") -> str:
    return f"""
        left join execution_probe_toxicity_quality toxicity_quality
          on toxicity_quality.market_id = {alias}.market_id
         and toxicity_quality.asset_id = {alias}.asset_id
         and toxicity_quality.side = coalesce(nullif({side_expression}, ''), 'BUY')
    """


def fillability_quality_score_sql() -> str:
    return """
        (
            coalesce(fillability.fillability_score, 0)
            + coalesce(fillability.future_touch_rate, 0)
            + least(coalesce(fillability.liquidity, 0) / 10000.0, 1.0) * 0.10
            - coalesce(toxicity_quality.quality_penalty, 0)
        )
    """


def toxicity_select_columns_sql() -> str:
    return """
            toxicity_quality.toxicity_segments,
            toxicity_quality.toxicity_filled_events,
            toxicity_quality.rejected_toxicity_segments,
            toxicity_quality.adverse_30s_rate as toxicity_adverse_30s_rate,
            toxicity_quality.avg_pnl_30s as toxicity_avg_pnl_30s,
            toxicity_quality.max_toxicity_score,
            toxicity_quality.quality_penalty
    """


def toxicity_filter_payload(
    quality: pd.DataFrame,
    config: ExecutionProbeUniverseConfig,
    toxicity_report: dict[str, object] | None,
) -> dict[str, object]:
    filtered_count = int(
        quality.get("rejected_toxicity_segments", pd.Series(dtype=int))
        .fillna(0)
        .gt(0)
        .sum()
    )
    return {
        "enabled": config.toxicity_filter != "none",
        "mode": config.toxicity_filter,
        "source_relation": "fill_toxicity_by_segment",
        "min_toxicity_filled_events": config.min_toxicity_filled_events,
        "filtered_count": filtered_count,
        "blocked_segments_path": (
            toxicity_report.get("blocked_segments_path")
            if isinstance(toxicity_report, dict)
            else None
        ),
        "quality_rows": int(len(quality)),
    }


def select_candidate_universe(
    conn: duckdb.DuckDBPyConnection,
    config: ExecutionProbeUniverseConfig,
    has_quote_execution_by_asset: bool,
    recommendation_list: str,
    spread_filters: list[str],
) -> pd.DataFrame:
    spread_filter_sql = (
        "\n              and " + "\n              and ".join(spread_filters)
        if spread_filters
        else ""
    )
    timing_join_sql = ""
    timing_filter_sql = ""
    select_columns_sql = "candidate.*"
    timing_order_sql = "(0 + 0)"
    if has_quote_execution_by_asset:
        timing_join_sql = """
            left join (
                select
                    market_id,
                    asset_id,
                    sum(signals) as timing_signals,
                    case
                        when sum(signals) > 0 then
                            sum(coalesce(synthetic_filled_signals, dry_run_filled_signals, 0))::double
                            / sum(signals)
                        else null
                    end as future_touch_rate
                from quote_execution_by_market_asset
                group by market_id, asset_id
            ) timing
              on timing.market_id = candidate.market_id
             and timing.asset_id = candidate.asset_id
            """
        select_columns_sql = """
                candidate.*,
                timing.timing_signals,
                timing.future_touch_rate
            """
        timing_order_sql = "coalesce(timing.future_touch_rate, 0)"
    if config.market_timing_filter == "future_touch":
        timing_filter_sql = f"""
              and coalesce(timing.timing_signals, 0) >= {config.min_timing_signals}
              and coalesce(timing.future_touch_rate, 0) >= {config.min_future_touch_rate}
            """
    return conn.execute(
        f"""
            select {select_columns_sql}
            from candidate_market_ranking candidate
            {timing_join_sql}
            {adverse_join_sql("candidate")}
            where candidate.recommendation in ({recommendation_list})
              {adverse_filter_sql()}
              {spread_filter_sql}
              {timing_filter_sql}
            order by
                case candidate.recommendation
                    when 'PROMOTE_TO_OBSERVATION' then 1
                    when 'KEEP_DIAGNOSTIC' then 2
                    when 'NEEDS_EXECUTION_EVIDENCE' then 3
                    when 'NEEDS_SPREAD_EVIDENCE' then 4
                    else 5
                end,
                candidate.combined_score desc,
                {timing_order_sql} desc,
                coalesce(candidate.observed_fill_rate, 0) desc,
                coalesce(candidate.spread_opportunity_density, 0) desc,
                candidate.asset_id
            limit {config.limit}
            """
    ).fetch_df()


def select_fillability_universe(
    conn: duckdb.DuckDBPyConnection,
    config: ExecutionProbeUniverseConfig,
) -> pd.DataFrame:
    spread_filters = []
    if config.min_avg_opportunity_spread is not None:
        spread_filters.append(
            f"coalesce(fillability.avg_spread_at_signal, 0) >= {config.min_avg_opportunity_spread}"
        )
    if config.max_avg_opportunity_spread is not None:
        spread_filters.append(
            f"coalesce(fillability.avg_spread_at_signal, 0) <= {config.max_avg_opportunity_spread}"
        )
    spread_filter_sql = (
        "\n          and " + "\n          and ".join(spread_filters)
        if spread_filters
        else ""
    )
    primary_limit = config.limit
    primary = conn.execute(
        f"""
        select
            fillability.*,
            {fillability_quality_score_sql()} as execution_quality_score,
            {toxicity_select_columns_sql()},
            fillability.signals as timing_signals,
            'primary' as selection_tier,
            null::varchar as fallback_reason
        from fillability_market_ranking fillability
        {adverse_join_sql("fillability", "fillability.side")}
        {toxicity_join_sql("fillability", "fillability.side")}
        where fillability.recommendation = 'PROMOTE_TO_OBSERVATION'
          {adverse_filter_sql()}
          and fillability.signals >= {config.min_timing_signals}
          and coalesce(fillability.future_touch_rate, 0) >= {config.min_future_touch_rate}
          {spread_filter_sql}
        order by
            execution_quality_score desc,
            fillability.fillability_score desc,
            coalesce(fillability.future_touch_rate, 0) desc,
            fillability.signals desc,
            fillability.asset_id
        limit {primary_limit}
        """
    ).fetch_df()
    if len(primary) >= config.min_assets:
        return primary.head(config.limit)
    missing_assets = min(config.min_assets - len(primary), config.limit - len(primary))
    if missing_assets <= 0:
        return primary.head(config.limit)
    selected_assets = asset_ids_from_frame(primary)
    selected_filter_sql = excluded_assets_sql(selected_assets, "fillability")
    fallback = conn.execute(
        f"""
        select
            fillability.*,
            {fillability_quality_score_sql()} as execution_quality_score,
            {toxicity_select_columns_sql()},
            fillability.signals as timing_signals,
            'fallback' as selection_tier,
            '{FILLABILITY_FALLBACK_REASON}' as fallback_reason
        from fillability_market_ranking fillability
        {adverse_join_sql("fillability", "fillability.side")}
        {toxicity_join_sql("fillability", "fillability.side")}
        where fillability.recommendation = 'KEEP_DIAGNOSTIC'
          {adverse_filter_sql()}
          and fillability.signals >= {config.min_timing_signals}
          {selected_filter_sql}
          {spread_filter_sql}
        order by
            execution_quality_score desc,
            coalesce(fillability.liquidity, 0) desc,
            coalesce(fillability.spread_opportunity_density, 0) desc,
            coalesce(fillability.avg_spread_at_signal, 0) desc,
            fillability.fillability_score desc,
            fillability.signals desc,
            fillability.asset_id
        limit {missing_assets}
        """
    ).fetch_df()
    selected = pd.concat([primary, fallback], ignore_index=True)
    if len(selected) >= config.min_assets:
        return selected.head(config.limit)
    selected_assets = asset_ids_from_frame(selected)
    missing_assets = min(config.min_assets - len(selected), config.limit - len(selected))
    if relation_exists(conn, "market_opportunity_ranking"):
        opportunity = select_market_opportunity_fallback(
            conn,
            selected_assets,
            missing_assets,
            config,
        )
    else:
        opportunity = pd.DataFrame()
    selected = pd.concat([selected, opportunity], ignore_index=True)
    if len(selected) >= config.min_assets:
        return selected.head(config.limit)
    if config.adverse_selection_filter != "none":
        return selected.head(config.limit)
    selected_assets = asset_ids_from_frame(selected)
    missing_assets = min(config.min_assets - len(selected), config.limit - len(selected))
    metadata = select_market_metadata_fallback(conn, selected_assets, missing_assets)
    selected = pd.concat([selected, metadata], ignore_index=True)
    return selected.head(config.limit)


def select_market_opportunity_fallback(
    conn: duckdb.DuckDBPyConnection,
    selected_assets: list[str],
    limit: int,
    config: ExecutionProbeUniverseConfig,
) -> pd.DataFrame:
    if limit <= 0:
        return pd.DataFrame()
    spread_filters = []
    if config.min_avg_opportunity_spread is not None:
        spread_filters.append(
            f"coalesce(opportunity.avg_opportunity_spread, opportunity.avg_spread, 0) >= {config.min_avg_opportunity_spread}"
        )
    if config.max_avg_opportunity_spread is not None:
        spread_filters.append(
            f"coalesce(opportunity.avg_opportunity_spread, opportunity.avg_spread, 0) <= {config.max_avg_opportunity_spread}"
        )
    spread_filter_sql = (
        "\n          and " + "\n          and ".join(spread_filters)
        if spread_filters
        else ""
    )
    return conn.execute(
        f"""
        select
            opportunity.rank,
            opportunity.market_id,
            opportunity.asset_id,
            cast(null as varchar) as side,
            cast(null as varchar) as strategy,
            cast(null as varchar) as model_version,
            cast(0 as bigint) as signals,
            cast(0 as double) as dry_run_signal_lifecycles,
            cast(0 as double) as dry_run_filled_signals,
            cast(0 as double) as synthetic_filled_signals,
            cast(0 as double) as neither_signals,
            cast(null as double) as future_touch_rate,
            cast(null as double) as inside_spread_rate,
            cast(null as double) as behind_touch_rate,
            coalesce(opportunity.avg_opportunity_spread, opportunity.avg_spread) as avg_spread_at_signal,
            cast(null as double) as avg_distance_to_mid,
            cast(null as double) as avg_distance_to_touch,
            cast(null as double) as avg_required_quote_move,
            cast(null as double) as avg_book_age_ms,
            opportunity.stale_rate,
            opportunity.spread_opportunity_density,
            opportunity.avg_total_depth,
            opportunity.liquidity,
            opportunity.volume,
            opportunity.question,
            opportunity.slug,
            opportunity.outcome,
            opportunity.opportunity_score as fillability_score,
            'MARKET_OPPORTUNITY_BACKFILL' as recommendation,
            cast(null as bigint) as timing_signals,
            'market_opportunity_fallback' as selection_tier,
            '{MARKET_OPPORTUNITY_FALLBACK_REASON}' as fallback_reason
        from market_opportunity_ranking opportunity
        {adverse_join_sql("opportunity")}
        where true
          {adverse_filter_sql()}
          {excluded_assets_sql(selected_assets, "opportunity")}
          {spread_filter_sql}
        order by
            opportunity.opportunity_score desc,
            opportunity.spread_opportunity_density desc,
            coalesce(opportunity.liquidity, 0) desc,
            opportunity.asset_id
        limit {limit}
        """
    ).fetch_df()


def select_market_metadata_fallback(
    conn: duckdb.DuckDBPyConnection,
    selected_assets: list[str],
    limit: int,
) -> pd.DataFrame:
    if limit <= 0:
        return pd.DataFrame()
    return conn.execute(
        f"""
        with latest_metadata as (
            select *
            from (
                select
                    *,
                    row_number() over (
                        partition by market_id, asset_id
                        order by ingested_at_ms desc nulls last
                    ) as metadata_rank
                from market_metadata
            )
            where metadata_rank = 1
        )
        select
            row_number() over (
                order by coalesce(latest_metadata.liquidity, 0) desc,
                    coalesce(latest_metadata.volume, 0) desc,
                    latest_metadata.asset_id
            ) as rank,
            latest_metadata.market_id,
            latest_metadata.asset_id,
            cast(null as varchar) as side,
            cast(null as varchar) as strategy,
            cast(null as varchar) as model_version,
            cast(0 as bigint) as signals,
            cast(0 as double) as dry_run_signal_lifecycles,
            cast(0 as double) as dry_run_filled_signals,
            cast(0 as double) as synthetic_filled_signals,
            cast(0 as double) as neither_signals,
            cast(null as double) as future_touch_rate,
            cast(null as double) as inside_spread_rate,
            cast(null as double) as behind_touch_rate,
            cast(null as double) as avg_spread_at_signal,
            cast(null as double) as avg_distance_to_mid,
            cast(null as double) as avg_distance_to_touch,
            cast(null as double) as avg_required_quote_move,
            cast(null as double) as avg_book_age_ms,
            cast(null as double) as stale_rate,
            cast(null as double) as spread_opportunity_density,
            cast(null as double) as avg_total_depth,
            coalesce(latest_metadata.liquidity, 0) as liquidity,
            coalesce(latest_metadata.volume, 0) as volume,
            latest_metadata.question,
            latest_metadata.slug,
            latest_metadata.outcome,
            coalesce(latest_metadata.liquidity, 0) / 1000 + coalesce(latest_metadata.volume, 0) / 10000 as fillability_score,
            'MARKET_METADATA_BACKFILL' as recommendation,
            cast(null as bigint) as timing_signals,
            'market_metadata_fallback' as selection_tier,
            '{MARKET_METADATA_FALLBACK_REASON}' as fallback_reason
        from latest_metadata
        {adverse_join_sql("latest_metadata")}
        where coalesce(latest_metadata.active, true)
          {adverse_filter_sql()}
          and not coalesce(latest_metadata.closed, false)
          and not coalesce(latest_metadata.archived, false)
          and coalesce(latest_metadata.enable_order_book, true)
          {excluded_assets_sql(selected_assets, "latest_metadata")}
        order by coalesce(latest_metadata.liquidity, 0) desc,
            coalesce(latest_metadata.volume, 0) desc,
            latest_metadata.asset_id
        limit {limit}
        """
    ).fetch_df()


def create_segment_opportunity_inputs(
    db_path: Path,
    output_dir: Path,
    config: ExecutionProbeUniverseConfig,
) -> dict[str, object] | None:
    if config.selection_source != "executable_segments":
        return None
    return create_segment_opportunity_ranking_report(
        db_path,
        output_dir / "segment_opportunity_ranking",
        SegmentOpportunityRankingConfig(
            min_runtime_active_minutes=config.min_runtime_active_minutes,
            runtime_activity_backfill=(
                config.runtime_activity_backfill
                or config.profile == "execution_probe_v10"
            ),
            runtime_backfill_min_opportunities=config.runtime_backfill_min_opportunities,
            runtime_backfill_min_active_minutes=config.runtime_backfill_min_active_minutes,
            limit=max(config.limit, config.min_assets),
        ),
    )


def select_executable_segment_universe(
    conn: duckdb.DuckDBPyConnection,
    config: ExecutionProbeUniverseConfig,
) -> pd.DataFrame:
    if not relation_exists(conn, "allowed_segment_candidates"):
        return pd.DataFrame()
    return conn.execute(
        f"""
        select
            rank,
            market_id,
            asset_id,
            side,
            strategy,
            model_version,
            executable_opportunities as signals,
            observed_fill_rate,
            synthetic_fill_rate,
            synthetic_observed_gap,
            avg_expected_edge,
            avg_available_depth,
            runtime_opportunities,
            runtime_active_minutes,
            runtime_opportunity_density,
            adverse_30s_rate,
            avg_pnl_30s,
            opportunity_score as execution_quality_score,
            spread_bucket,
            timing_bucket,
            recommendation,
            allowed_reason,
            runtime_opportunities as timing_signals,
            'primary' as selection_tier,
            null::varchar as fallback_reason
        from allowed_segment_candidates
        order by rank
        limit {config.limit}
        """
    ).fetch_df()


def asset_ids_from_frame(frame: pd.DataFrame) -> list[str]:
    return [
        str(asset_id)
        for asset_id in frame.get("asset_id", pd.Series(dtype=str)).tolist()
        if str(asset_id)
    ]


def excluded_assets_sql(asset_ids: list[str], alias: str) -> str:
    if not asset_ids:
        return ""
    selected_list = ",".join(f"'{duckdb_literal(asset_id)}'" for asset_id in asset_ids)
    return f"and {alias}.asset_id not in ({selected_list})"


def source_report_version(config: ExecutionProbeUniverseConfig) -> str:
    if config.selection_source == "executable_segments":
        return "segment_opportunity_ranking_v1"
    if config.selection_source == "fillability":
        return "fillability_baseline_v1"
    return "candidate_market_ranking_v1"


def fillability_fallback_summary(
    selected: list[dict[str, object]],
    config: ExecutionProbeUniverseConfig,
) -> dict[str, object]:
    fallback_assets = [
        row
        for row in selected
        if row.get("fallback_reason")
        in {
            FILLABILITY_FALLBACK_REASON,
            MARKET_OPPORTUNITY_FALLBACK_REASON,
            MARKET_METADATA_FALLBACK_REASON,
        }
    ]
    primary_assets = [
        row
        for row in selected
        if row.get("selection_tier") == "primary"
    ]
    return {
        "enabled": config.selection_source == "fillability",
        "reason": "fillability_min_assets_backfill",
        "assets_added": len(fallback_assets),
        "primary_assets": len(primary_assets),
        "fallback_assets": len(fallback_assets),
        "fallback_reasons": sorted(
            {
                str(row.get("fallback_reason"))
                for row in fallback_assets
                if row.get("fallback_reason")
            }
        ),
        "used": len(fallback_assets) > 0,
    }


def segment_opportunity_payload(
    report: dict[str, object] | None,
) -> dict[str, object]:
    return {
        "enabled": report is not None,
        "mode": "executable_segments" if report is not None else "none",
        "source_relation": "allowed_segment_candidates",
        "selected_segments": (
            typed_count(report.get("counts"), "selected_segments")
            if isinstance(report, dict)
            else 0
        ),
        "allowed_segments": (
            typed_count(report.get("counts"), "allowed_segments")
            if isinstance(report, dict)
            else 0
        ),
        "runtime_activity_backfill": (
            typed_config_bool(report.get("config"), "runtime_activity_backfill")
            if isinstance(report, dict)
            else False
        ),
        "runtime_backfill_min_opportunities": (
            typed_config_int(report.get("config"), "runtime_backfill_min_opportunities")
            if isinstance(report, dict)
            else 0
        ),
        "runtime_backfill_min_active_minutes": (
            typed_config_int(report.get("config"), "runtime_backfill_min_active_minutes")
            if isinstance(report, dict)
            else 0
        ),
        "allowed_segments_path": (
            report.get("allowed_segments_path")
            if isinstance(report, dict)
            else None
        ),
    }


def typed_count(value: object, key: str) -> int:
    if not isinstance(value, dict):
        return 0
    item = value.get(key)
    return int(item) if isinstance(item, (int, float)) else 0


def typed_config_int(value: object, key: str) -> int:
    if not isinstance(value, dict):
        return 0
    item = value.get(key)
    return int(item) if isinstance(item, (int, float)) else 0


def typed_config_bool(value: object, key: str) -> bool:
    if not isinstance(value, dict):
        return False
    return value.get(key) is True


def selection_reason(
    status: str,
    asset_ids: list[str],
    config: ExecutionProbeUniverseConfig,
    fallback: dict[str, object],
) -> str:
    filter_note = ""
    filter_note += f";selection_source={config.selection_source}"
    if config.market_timing_filter != "none":
        filter_note += (
            f";market_timing_filter={config.market_timing_filter}"
            f";min_future_touch_rate={config.min_future_touch_rate}"
            f";min_timing_signals={config.min_timing_signals}"
        )
    if config.min_avg_opportunity_spread is not None:
        filter_note += f";min_avg_opportunity_spread={config.min_avg_opportunity_spread}"
    if config.max_avg_opportunity_spread is not None:
        filter_note += f";max_avg_opportunity_spread={config.max_avg_opportunity_spread}"
    if config.adverse_selection_filter != "none":
        filter_note += (
            f";adverse_selection_filter={config.adverse_selection_filter}"
            f";max_adverse_30s_rate={config.max_adverse_30s_rate}"
            f";min_adverse_filled_events={config.min_adverse_filled_events}"
        )
    if config.toxicity_filter != "none":
        filter_note += (
            f";toxicity_filter={config.toxicity_filter}"
            f";min_toxicity_filled_events={config.min_toxicity_filled_events}"
        )
    if fallback.get("used"):
        reasons = fallback.get("fallback_reasons")
        if isinstance(reasons, list) and reasons:
            reason_text = ",".join(str(reason) for reason in reasons)
        else:
            reason_text = "unknown"
        filter_note += f";fallback_fillability_backfill={reason_text}"
    if status == "ready":
        return "ranked_multi_market_universe_meets_minimum_asset_coverage" + filter_note
    return (
        f"only_{len(asset_ids)}_assets_available_below_minimum_{config.min_assets};"
        "repeat_collection_or_relax_offline_universe_filters"
        + filter_note
    )


def normalize_records(rows: list[dict[str, Any]]) -> list[dict[str, object]]:
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


def parse_recommendations(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


def main() -> int:
    parser = argparse.ArgumentParser(description="Select multi-market universe for execution probes")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--profile", default=ExecutionProbeUniverseConfig.profile)
    parser.add_argument("--limit", type=int, default=ExecutionProbeUniverseConfig.limit)
    parser.add_argument("--min-assets", type=int, default=ExecutionProbeUniverseConfig.min_assets)
    parser.add_argument(
        "--market-timing-filter",
        choices=MARKET_TIMING_FILTERS,
        default=ExecutionProbeUniverseConfig.market_timing_filter,
    )
    parser.add_argument(
        "--min-future-touch-rate",
        type=float,
        default=ExecutionProbeUniverseConfig.min_future_touch_rate,
    )
    parser.add_argument(
        "--min-timing-signals",
        type=int,
        default=ExecutionProbeUniverseConfig.min_timing_signals,
    )
    parser.add_argument("--min-avg-opportunity-spread", type=float, default=None)
    parser.add_argument("--max-avg-opportunity-spread", type=float, default=None)
    parser.add_argument(
        "--selection-source",
        choices=SELECTION_SOURCES,
        default=ExecutionProbeUniverseConfig.selection_source,
    )
    parser.add_argument(
        "--adverse-selection-filter",
        choices=ADVERSE_SELECTION_FILTERS,
        default=ExecutionProbeUniverseConfig.adverse_selection_filter,
    )
    parser.add_argument(
        "--max-adverse-30s-rate",
        type=float,
        default=ExecutionProbeUniverseConfig.max_adverse_30s_rate,
    )
    parser.add_argument(
        "--min-adverse-filled-events",
        type=int,
        default=ExecutionProbeUniverseConfig.min_adverse_filled_events,
    )
    parser.add_argument(
        "--toxicity-filter",
        choices=TOXICITY_FILTERS,
        default=ExecutionProbeUniverseConfig.toxicity_filter,
    )
    parser.add_argument(
        "--min-toxicity-filled-events",
        type=int,
        default=ExecutionProbeUniverseConfig.min_toxicity_filled_events,
    )
    parser.add_argument(
        "--min-runtime-active-minutes",
        type=int,
        default=ExecutionProbeUniverseConfig.min_runtime_active_minutes,
    )
    parser.add_argument("--runtime-activity-backfill", action="store_true")
    parser.add_argument(
        "--runtime-backfill-min-opportunities",
        type=int,
        default=ExecutionProbeUniverseConfig.runtime_backfill_min_opportunities,
    )
    parser.add_argument(
        "--runtime-backfill-min-active-minutes",
        type=int,
        default=ExecutionProbeUniverseConfig.runtime_backfill_min_active_minutes,
    )
    parser.add_argument(
        "--recommendations",
        default=",".join(DEFAULT_RECOMMENDATIONS),
        help="Comma-separated candidate_market_ranking recommendations to include.",
    )
    args = parser.parse_args()
    report = create_execution_probe_universe_selection(
        Path(args.duckdb),
        Path(args.output_dir),
        ExecutionProbeUniverseConfig(
            profile=args.profile,
            limit=args.limit,
            min_assets=args.min_assets,
            recommendations=parse_recommendations(args.recommendations),
            market_timing_filter=args.market_timing_filter,
            min_future_touch_rate=args.min_future_touch_rate,
            min_timing_signals=args.min_timing_signals,
            min_avg_opportunity_spread=args.min_avg_opportunity_spread,
            max_avg_opportunity_spread=args.max_avg_opportunity_spread,
            selection_source=args.selection_source,
            adverse_selection_filter=args.adverse_selection_filter,
            max_adverse_30s_rate=args.max_adverse_30s_rate,
            min_adverse_filled_events=args.min_adverse_filled_events,
            toxicity_filter=args.toxicity_filter,
            min_toxicity_filled_events=args.min_toxicity_filled_events,
            min_runtime_active_minutes=args.min_runtime_active_minutes,
            runtime_activity_backfill=args.runtime_activity_backfill,
            runtime_backfill_min_opportunities=args.runtime_backfill_min_opportunities,
            runtime_backfill_min_active_minutes=args.runtime_backfill_min_active_minutes,
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
