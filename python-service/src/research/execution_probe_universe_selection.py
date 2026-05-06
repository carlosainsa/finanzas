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


REPORT_VERSION = "execution_probe_universe_selection_v1"
DEFAULT_RECOMMENDATIONS = (
    "PROMOTE_TO_OBSERVATION",
    "KEEP_DIAGNOSTIC",
    "NEEDS_EXECUTION_EVIDENCE",
)


@dataclass(frozen=True)
class ExecutionProbeUniverseConfig:
    profile: str = "execution_probe_v5"
    limit: int = 10
    min_assets: int = 5
    recommendations: tuple[str, ...] = DEFAULT_RECOMMENDATIONS

    def __post_init__(self) -> None:
        if self.profile not in {"execution_probe_v5", "execution_probe_v6"}:
            raise ValueError("profile must be execution_probe_v5 or execution_probe_v6")
        if self.limit <= 0:
            raise ValueError("limit must be positive")
        if self.min_assets <= 0:
            raise ValueError("min_assets must be positive")
        if self.min_assets > self.limit:
            raise ValueError("min_assets cannot exceed limit")
        if not self.recommendations:
            raise ValueError("recommendations cannot be empty")


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
        recommendation_list = ",".join(
            f"'{duckdb_literal(item)}'" for item in config.recommendations
        )
        frame = conn.execute(
            f"""
            select *
            from candidate_market_ranking
            where recommendation in ({recommendation_list})
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
    if status == "ready":
        return "ranked_multi_market_universe_meets_minimum_asset_coverage"
    return (
        f"only_{len(asset_ids)}_assets_available_below_minimum_{config.min_assets};"
        "repeat_collection_or_relax_offline_universe_filters"
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
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
