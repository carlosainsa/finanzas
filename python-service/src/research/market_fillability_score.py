import argparse
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from src.research.backtest import duckdb_literal
from src.research.market_opportunity_selector import (
    MarketOpportunityConfig,
    create_market_opportunity_views,
)
from src.research.quote_execution_diagnostics import (
    QuoteExecutionDiagnosticsConfig,
    create_quote_execution_diagnostics_views,
)
from src.research.runtime_touch_ranking import normalize_records


REPORT_VERSION = "market_fillability_score_v1"


@dataclass(frozen=True)
class MarketFillabilityScoreConfig:
    min_signals: int = 1
    min_fillability_score: float = 0.0
    max_synthetic_gap: float = 0.50
    limit: int = 100

    def __post_init__(self) -> None:
        if self.min_signals < 0:
            raise ValueError("min_signals must be non-negative")
        if self.max_synthetic_gap < 0:
            raise ValueError("max_synthetic_gap must be non-negative")
        if self.limit <= 0:
            raise ValueError("limit must be positive")


def create_market_fillability_score_report(
    db_path: Path,
    output_dir: Path,
    config: MarketFillabilityScoreConfig = MarketFillabilityScoreConfig(),
) -> dict[str, Any]:
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB does not exist: {db_path}")
    create_market_fillability_score_views(db_path, config)
    output_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
        scores = conn.execute(
            """
            select *
            from market_fillability_score_ranking
            order by rank
            """
        ).fetch_df()
        selected = conn.execute(
            f"""
            select *
            from market_fillability_score_ranking
            where recommendation = 'PROMOTE_TO_DISCOVERY_BATCH'
            order by rank
            limit {config.limit}
            """
        ).fetch_df()

    scores.to_parquet(output_dir / "market_fillability_score_ranking.parquet", index=False)
    selected.to_parquet(
        output_dir / "selected_market_fillability_scores.parquet",
        index=False,
    )
    selected_rows = normalize_records(selected.to_dict(orient="records"))
    selected_asset_ids = [str(row["asset_id"]) for row in selected_rows]
    payload: dict[str, Any] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "can_promote_live": False,
        "decision_policy": "offline_market_fillability_scoring_only",
        "selection_source": "market_fillability_score",
        "risk_contract": {
            "execution_mode": "dry_run",
            "does_not_modify_quote_policy": True,
            "does_not_modify_risk_limits": True,
            "does_not_publish_signals": True,
        },
        "config": asdict(config),
        "counts": {
            "ranked_assets": len(scores.index),
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
            "market_fillability_score_ranking.parquet",
            "selected_market_fillability_scores.parquet",
            "market_fillability_score.json",
        ],
    }
    (output_dir / "market_fillability_score.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload


def create_market_fillability_score_views(
    db_path: Path,
    config: MarketFillabilityScoreConfig = MarketFillabilityScoreConfig(),
) -> None:
    create_quote_execution_diagnostics_views(db_path, QuoteExecutionDiagnosticsConfig())
    create_market_opportunity_views(db_path, MarketOpportunityConfig(limit=config.limit))
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            create or replace view market_fillability_signal_features as
            select
                market_id,
                asset_id,
                sum(signals) as signals,
                sum(dry_run_signal_lifecycles) as dry_run_signal_lifecycles,
                sum(dry_run_filled_signals) as dry_run_filled_signals,
                sum(synthetic_filled_signals) as synthetic_filled_signals,
                sum(synthetic_only_signals) as synthetic_only_signals,
                sum(neither_signals) as neither_signals,
                case
                    when sum(signals) > 0
                    then sum(dry_run_filled_signals)::double / sum(signals)
                    else 0
                end as observed_fill_rate,
                case
                    when sum(signals) > 0
                    then sum(synthetic_filled_signals)::double / sum(signals)
                    else 0
                end as synthetic_fill_rate,
                case
                    when sum(signals) > 0
                    then sum(synthetic_only_signals)::double / sum(signals)
                    else 0
                end as synthetic_only_rate,
                avg(future_touch_rate) as future_touch_rate,
                avg(inside_spread_rate) as inside_spread_rate,
                avg(behind_touch_rate) as behind_touch_rate,
                avg(avg_spread_at_signal) as avg_spread_at_signal,
                avg(avg_distance_to_touch) as avg_distance_to_touch,
                avg(avg_required_quote_move) as avg_required_quote_move,
                avg(avg_book_age_ms) as avg_book_age_ms
            from quote_execution_by_market_asset
            group by market_id, asset_id
            """
        )
        conn.execute(
            f"""
            create or replace view market_fillability_score_features as
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
            )
            select
                coalesce(signals.market_id, opportunity.market_id) as market_id,
                coalesce(signals.asset_id, opportunity.asset_id) as asset_id,
                metadata.outcome,
                metadata.question,
                metadata.slug,
                coalesce(metadata.liquidity, opportunity.liquidity, 0) as liquidity,
                coalesce(metadata.volume, opportunity.volume, 0) as volume,
                coalesce(signals.signals, 0) as signals,
                coalesce(signals.dry_run_signal_lifecycles, 0) as dry_run_signal_lifecycles,
                coalesce(signals.dry_run_filled_signals, 0) as dry_run_filled_signals,
                coalesce(signals.synthetic_filled_signals, 0) as synthetic_filled_signals,
                coalesce(signals.synthetic_only_signals, 0) as synthetic_only_signals,
                coalesce(signals.neither_signals, 0) as neither_signals,
                coalesce(signals.observed_fill_rate, 0) as observed_fill_rate,
                coalesce(signals.synthetic_fill_rate, 0) as synthetic_fill_rate,
                coalesce(signals.synthetic_only_rate, 0) as synthetic_only_rate,
                greatest(
                    coalesce(signals.synthetic_fill_rate, 0)
                    - coalesce(signals.observed_fill_rate, 0),
                    0
                ) as synthetic_gap,
                coalesce(signals.future_touch_rate, 0) as future_touch_rate,
                coalesce(signals.inside_spread_rate, 0) as inside_spread_rate,
                coalesce(signals.behind_touch_rate, 0) as behind_touch_rate,
                coalesce(signals.avg_spread_at_signal, opportunity.avg_spread, 0)
                    as avg_spread,
                coalesce(signals.avg_distance_to_touch, 0) as avg_distance_to_touch,
                coalesce(signals.avg_required_quote_move, 0) as avg_required_quote_move,
                coalesce(signals.avg_book_age_ms, 0) as avg_book_age_ms,
                coalesce(opportunity.spread_opportunity_density, 0)
                    as spread_opportunity_density,
                coalesce(opportunity.avg_total_depth, 0) as avg_total_depth,
                coalesce(opportunity.stale_rate, 0) as stale_rate,
                (
                    coalesce(signals.observed_fill_rate, 0) * 120
                    + coalesce(signals.future_touch_rate, 0) * 55
                    + coalesce(opportunity.spread_opportunity_density, 0) * 35
                    + coalesce(signals.inside_spread_rate, 0) * 15
                    + least(coalesce(opportunity.avg_total_depth, 0), 100000) / 50000
                    + least(coalesce(metadata.liquidity, opportunity.liquidity, 0), 100000) / 25000
                    - greatest(
                        coalesce(signals.synthetic_fill_rate, 0)
                        - coalesce(signals.observed_fill_rate, 0),
                        0
                    ) * 75
                    - coalesce(signals.behind_touch_rate, 0) * 25
                    - coalesce(opportunity.stale_rate, 0) * 35
                    - coalesce(signals.avg_required_quote_move, 0) * 100
                ) as market_fillability_score
            from market_fillability_signal_features signals
            full outer join market_opportunity_ranking opportunity
              on opportunity.market_id = signals.market_id
             and opportunity.asset_id = signals.asset_id
            left join latest_metadata metadata
              on metadata.market_id = coalesce(signals.market_id, opportunity.market_id)
             and metadata.asset_id = coalesce(signals.asset_id, opportunity.asset_id)
            where coalesce(signals.market_id, opportunity.market_id) is not null
              and coalesce(signals.asset_id, opportunity.asset_id) is not null
            """
        )
        conn.execute(
            f"""
            create or replace view market_fillability_score_ranking as
            select
                row_number() over (
                    order by
                        case
                            when recommendation = 'PROMOTE_TO_DISCOVERY_BATCH'
                            then 0 else 1
                        end,
                        market_fillability_score desc,
                        observed_fill_rate desc,
                        future_touch_rate desc,
                        liquidity desc,
                        asset_id
                ) as rank,
                *
            from (
                select
                    *,
                    case
                        when signals >= {config.min_signals}
                         and market_fillability_score >= {config.min_fillability_score}
                         and synthetic_gap <= {config.max_synthetic_gap}
                        then 'PROMOTE_TO_DISCOVERY_BATCH'
                        when signals < {config.min_signals} then 'NEEDS_RUNTIME_EVIDENCE'
                        when synthetic_gap > {config.max_synthetic_gap}
                        then 'KEEP_DIAGNOSTIC_SYNTHETIC_OPTIMISM'
                        else 'KEEP_DIAGNOSTIC'
                    end as recommendation
                from market_fillability_score_features
            )
            """
        )


def load_asset_score_map(path: Path | None) -> dict[str, float]:
    if path is None or not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("selected") if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return {}
    scores: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        asset_id = str(row.get("asset_id") or "")
        if not asset_id:
            continue
        scores[asset_id] = float(row.get("market_fillability_score") or 0.0)
    return scores


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--min-signals", type=int, default=1)
    parser.add_argument("--min-fillability-score", type=float, default=0.0)
    parser.add_argument("--max-synthetic-gap", type=float, default=0.50)
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    report = create_market_fillability_score_report(
        Path(args.duckdb),
        Path(args.output_dir),
        MarketFillabilityScoreConfig(
            min_signals=args.min_signals,
            min_fillability_score=args.min_fillability_score,
            max_synthetic_gap=args.max_synthetic_gap,
            limit=args.limit,
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
