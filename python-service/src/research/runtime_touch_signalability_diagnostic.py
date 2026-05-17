import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd  # type: ignore[import-untyped]

from src.research.runtime_touch_ranking import (
    RuntimeTouchRankingConfig,
    create_runtime_touch_ranking_views,
    normalize_records,
)


REPORT_VERSION = "runtime_touch_signalability_diagnostic_v1"


@dataclass(frozen=True)
class RuntimeTouchSignalabilityDiagnosticConfig:
    lookback_ms: int = 900_000
    min_snapshots: int = 10
    min_active_minutes: int = 2
    min_touch_change_rate: float = 0.01
    min_spread: float = 0.000625
    signal_min_spread: float = 0.03
    signal_min_depth: float = 1.5
    min_signalable_snapshots: int = 3
    min_signalable_density: float = 0.05
    max_stale_rate: float = 0.10
    min_assets: int = 2
    limit: int = 50

    def __post_init__(self) -> None:
        if self.lookback_ms <= 0:
            raise ValueError("lookback_ms must be positive")
        if self.min_snapshots <= 0:
            raise ValueError("min_snapshots must be positive")
        if self.min_active_minutes <= 0:
            raise ValueError("min_active_minutes must be positive")
        if not 0 <= self.min_touch_change_rate <= 1:
            raise ValueError("min_touch_change_rate must be between 0 and 1")
        if self.min_spread < 0:
            raise ValueError("min_spread must be non-negative")
        if self.signal_min_spread < 0:
            raise ValueError("signal_min_spread must be non-negative")
        if self.signal_min_depth < 0:
            raise ValueError("signal_min_depth must be non-negative")
        if self.min_signalable_snapshots < 0:
            raise ValueError("min_signalable_snapshots must be non-negative")
        if not 0 <= self.min_signalable_density <= 1:
            raise ValueError("min_signalable_density must be between 0 and 1")
        if not 0 <= self.max_stale_rate <= 1:
            raise ValueError("max_stale_rate must be between 0 and 1")
        if self.min_assets <= 0:
            raise ValueError("min_assets must be positive")
        if self.limit <= 0:
            raise ValueError("limit must be positive")


def create_runtime_touch_signalability_diagnostic(
    db_path: Path,
    output_dir: Path,
    config: RuntimeTouchSignalabilityDiagnosticConfig = (
        RuntimeTouchSignalabilityDiagnosticConfig()
    ),
) -> dict[str, Any]:
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB does not exist: {db_path}")
    create_runtime_touch_ranking_views(
        db_path,
        RuntimeTouchRankingConfig(
            lookback_ms=config.lookback_ms,
            min_snapshots=config.min_snapshots,
            min_active_minutes=config.min_active_minutes,
            min_touch_change_rate=config.min_touch_change_rate,
            min_spread=config.min_spread,
            min_signalable_snapshots=config.min_signalable_snapshots,
            min_signalable_density=config.min_signalable_density,
            signal_min_spread=config.signal_min_spread,
            signal_min_depth=config.signal_min_depth,
            max_stale_rate=config.max_stale_rate,
            limit=config.limit,
        ),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
        per_asset = conn.execute(
            f"""
            with diagnostic as (
                select
                    rank,
                    market_id,
                    asset_id,
                    outcome,
                    question,
                    slug,
                    snapshots,
                    active_minutes,
                    touch_change_rate,
                    avg_spread,
                    avg_total_depth,
                    signalable_snapshots,
                    signalable_density,
                    stale_rate,
                    liquidity,
                    volume,
                    active,
                    closed,
                    archived,
                    enable_order_book,
                    recommendation,
                    snapshots >= {config.min_snapshots} as passes_snapshots,
                    active_minutes >= {config.min_active_minutes} as passes_active_minutes,
                    touch_change_rate >= {config.min_touch_change_rate} as passes_touch_change_rate,
                    avg_spread >= {config.min_spread} as passes_avg_spread,
                    signalable_snapshots >= {config.min_signalable_snapshots} as passes_signalable_snapshots,
                    signalable_density >= {config.min_signalable_density} as passes_signalable_density,
                    stale_rate <= {config.max_stale_rate} as passes_stale_rate,
                    coalesce(active, true)
                        and not coalesce(closed, false)
                        and not coalesce(archived, false)
                        and coalesce(enable_order_book, true) as passes_metadata,
                    (
                        snapshots >= {config.min_snapshots}
                        and active_minutes >= {config.min_active_minutes}
                        and touch_change_rate >= {config.min_touch_change_rate}
                        and avg_spread >= {config.min_spread}
                        and signalable_snapshots >= {config.min_signalable_snapshots}
                        and signalable_density >= {config.min_signalable_density}
                        and stale_rate <= {config.max_stale_rate}
                        and coalesce(active, true)
                        and not coalesce(closed, false)
                        and not coalesce(archived, false)
                        and coalesce(enable_order_book, true)
                    ) as signalable_for_ab
                from runtime_touch_ranking
            )
            select
                *,
                array_to_string(
                    list_filter([
                        case when not passes_snapshots then 'snapshots' else null end,
                        case when not passes_active_minutes then 'active_minutes' else null end,
                        case when not passes_touch_change_rate then 'touch_change_rate' else null end,
                        case when not passes_avg_spread then 'avg_spread' else null end,
                        case when not passes_signalable_snapshots then 'signalable_snapshots' else null end,
                        case when not passes_signalable_density then 'signalable_density' else null end,
                        case when not passes_stale_rate then 'stale_rate' else null end,
                        case when not passes_metadata then 'metadata' else null end
                    ], item -> item is not null),
                    ','
                ) as blocker_reasons
            from diagnostic
            order by
                signalable_for_ab desc,
                signalable_density desc,
                signalable_snapshots desc,
                touch_change_rate desc,
                liquidity desc,
                rank
            """
        ).fetch_df()

    per_asset.to_parquet(output_dir / "runtime_touch_signalability_assets.parquet", index=False)
    rows = normalize_records(per_asset.head(config.limit).to_dict(orient="records"))
    blocker_counts = count_blockers(rows)
    signalable_count = sum(1 for row in rows if row.get("signalable_for_ab") is True)
    status = "ready" if signalable_count >= config.min_assets else "insufficient_signalable_assets"
    payload: dict[str, Any] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_runtime_touch_signalability_diagnostic_only",
        "source_duckdb": str(db_path),
        "config": asdict(config),
        "status": status,
        "assets_analyzed": len(rows),
        "signalable_assets_count": signalable_count,
        "min_assets": config.min_assets,
        "blocker_counts": blocker_counts,
        "top_candidates": rows[: min(config.limit, 20)],
        "outputs": [
            "runtime_touch_signalability_assets.parquet",
            "runtime_touch_signalability_diagnostic.json",
        ],
    }
    (output_dir / "runtime_touch_signalability_diagnostic.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload


def count_blockers(rows: list[dict[str, object]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        reasons = str(row.get("blocker_reasons") or "")
        if not reasons:
            continue
        for reason in reasons.split(","):
            counts[reason] = counts.get(reason, 0) + 1
    return dict(sorted(counts.items()))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--min-assets", type=int, default=2)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--min-snapshots", type=int, default=10)
    parser.add_argument("--min-active-minutes", type=int, default=2)
    parser.add_argument("--min-touch-change-rate", type=float, default=0.01)
    parser.add_argument("--signal-min-spread", type=float, default=0.03)
    parser.add_argument("--signal-min-depth", type=float, default=1.5)
    parser.add_argument("--min-signalable-snapshots", type=int, default=3)
    parser.add_argument("--min-signalable-density", type=float, default=0.05)
    args = parser.parse_args()
    report = create_runtime_touch_signalability_diagnostic(
        Path(args.duckdb),
        Path(args.output_dir),
        RuntimeTouchSignalabilityDiagnosticConfig(
            min_assets=args.min_assets,
            limit=args.limit,
            min_snapshots=args.min_snapshots,
            min_active_minutes=args.min_active_minutes,
            min_touch_change_rate=args.min_touch_change_rate,
            signal_min_spread=args.signal_min_spread,
            signal_min_depth=args.signal_min_depth,
            min_signalable_snapshots=args.min_signalable_snapshots,
            min_signalable_density=args.min_signalable_density,
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
