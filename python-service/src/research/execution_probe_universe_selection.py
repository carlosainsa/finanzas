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
from src.research.game_theory import relation_exists


REPORT_VERSION = "execution_probe_universe_selection_v1"
DEFAULT_RECOMMENDATIONS = (
    "PROMOTE_TO_OBSERVATION",
    "KEEP_DIAGNOSTIC",
    "NEEDS_EXECUTION_EVIDENCE",
)
MARKET_TIMING_FILTERS = ("none", "future_touch")


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

    def __post_init__(self) -> None:
        if self.profile not in {
            "execution_probe_v5",
            "execution_probe_v6",
            "execution_probe_v7",
        }:
            raise ValueError(
                "profile must be execution_probe_v5, execution_probe_v6, or execution_probe_v7"
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
        if not 0 <= self.min_future_touch_rate <= 1:
            raise ValueError("min_future_touch_rate must be between 0 and 1")
        if self.min_timing_signals <= 0:
            raise ValueError("min_timing_signals must be positive")
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
    create_candidate_market_ranking_views(
        db_path, CandidateMarketRankingConfig(limit=max(config.limit, config.min_assets))
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
        has_quote_execution_by_asset = relation_exists(conn, "quote_execution_by_market_asset")
        if (
            config.market_timing_filter == "future_touch"
            and not has_quote_execution_by_asset
        ):
            raise ValueError(
                "market_timing_filter=future_touch requires quote_execution_by_market_asset"
            )
        recommendation_list = ",".join(
            f"'{duckdb_literal(item)}'" for item in config.recommendations
        )
        spread_filters = []
        if config.min_avg_opportunity_spread is not None:
            spread_filters.append(
                f"coalesce(candidate.avg_opportunity_spread, candidate.avg_spread) >= {config.min_avg_opportunity_spread}"
            )
        if config.max_avg_opportunity_spread is not None:
            spread_filters.append(
                f"coalesce(candidate.avg_opportunity_spread, candidate.avg_spread) <= {config.max_avg_opportunity_spread}"
            )
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
        frame = conn.execute(
            f"""
            select {select_columns_sql}
            from candidate_market_ranking candidate
            {timing_join_sql}
            where candidate.recommendation in ({recommendation_list})
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

    selected = normalize_records(frame.to_dict(orient="records"))
    asset_ids = [str(row["asset_id"]) for row in selected if row.get("asset_id")]
    status = "ready" if len(asset_ids) >= config.min_assets else "insufficient_assets"
    output_parquet = output_dir / "execution_probe_universe_selection.parquet"
    pd.DataFrame(selected).to_parquet(output_parquet, index=False)
    payload: dict[str, object] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_multi_market_observation_universe_only",
        "profile": config.profile,
        "config": asdict(config),
        "source_duckdb": str(db_path),
        "source_report_version": "candidate_market_ranking_v1",
        "status": status,
        "selection_reason": selection_reason(status, asset_ids, config),
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
        ],
    }
    (output_dir / "execution_probe_universe_selection.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return payload


def selection_reason(
    status: str, asset_ids: list[str], config: ExecutionProbeUniverseConfig
) -> str:
    filter_note = ""
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
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
