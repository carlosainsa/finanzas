import argparse
import json
from pathlib import Path

import duckdb

from src.research.backtest import duckdb_literal
from src.research.market_regime import create_market_regime_views
from src.research.sentiment_lift import create_sentiment_lift_views


FEATURE_BLOCKLIST_REPORT_VERSION = "feature_blocklist_candidates_v1"
FEATURE_BLOCKLIST_OUTPUTS = (
    "research_feature_bucket_performance",
    "research_feature_blocklist_candidates",
)


def create_feature_blocklist_candidate_views(
    db_path: Path,
    lookback_ms: int = 86_400_000,
    min_samples: int = 10,
    max_realized_edge: float = 0.0,
    min_adverse_edge_rate: float = 0.5,
    max_drawdown: float = 0.10,
    max_sentiment_disagreement: float = 0.50,
    min_fill_rate: float = 0.25,
) -> None:
    if lookback_ms <= 0:
        raise ValueError("lookback_ms must be positive")
    if min_samples <= 0:
        raise ValueError("min_samples must be positive")
    if not 0.0 <= min_adverse_edge_rate <= 1.0:
        raise ValueError("min_adverse_edge_rate must be between 0 and 1")
    if max_drawdown < 0:
        raise ValueError("max_drawdown must be non-negative")
    if not 0.0 <= max_sentiment_disagreement <= 1.0:
        raise ValueError("max_sentiment_disagreement must be between 0 and 1")
    if not 0.0 <= min_fill_rate <= 1.0:
        raise ValueError("min_fill_rate must be between 0 and 1")

    create_market_regime_views(db_path)
    create_sentiment_lift_views(db_path, lookback_ms=lookback_ms)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            create or replace view research_feature_bucket_performance as
            select
                'regime' as feature_family,
                bucket_type as feature_name,
                bucket,
                coalesce(strategy, 'unknown') as strategy,
                side,
                signals,
                filled_signals,
                avg_fill_rate,
                avg_realized_edge_after_slippage,
                realized_edge_pnl,
                adverse_edge_rate,
                avg_slippage,
                max_drawdown,
                pnl_per_signal,
                avg_whale_pressure_score,
                avg_realized_volatility,
                avg_mid_drawdown,
                cast(null as double) as avg_sentiment_disagreement,
                cast(null as double) as fill_rate_lift,
                cast(null as double) as realized_edge_lift,
                cast(null as double) as adverse_edge_rate_lift
            from market_regime_bucket_performance
            union all
            select
                'sentiment' as feature_family,
                'sentiment_alignment' as feature_name,
                signal_sentiment_alignment || ':' || sentiment_bucket as bucket,
                coalesce(strategy, 'unknown') as strategy,
                side,
                signals,
                filled_signals,
                avg_fill_rate,
                avg_realized_edge_after_slippage,
                realized_edge_pnl,
                adverse_edge_rate,
                avg_slippage,
                max_drawdown,
                pnl_per_signal,
                cast(null as double) as avg_whale_pressure_score,
                cast(null as double) as avg_realized_volatility,
                cast(null as double) as avg_mid_drawdown,
                avg_sentiment_disagreement,
                fill_rate_lift,
                realized_edge_lift,
                adverse_edge_rate_lift
            from sentiment_lift_summary
            """
        )
        conn.execute(
            f"""
            create or replace view research_feature_blocklist_candidates as
            select
                md5(
                    feature_family || ':' || feature_name || ':' || bucket || ':' ||
                    strategy || ':' || coalesce(side, 'unknown')
                ) as candidate_id,
                feature_family,
                feature_name,
                bucket,
                strategy,
                side,
                signals,
                filled_signals,
                avg_fill_rate,
                avg_realized_edge_after_slippage,
                realized_edge_pnl,
                adverse_edge_rate,
                avg_slippage,
                max_drawdown,
                pnl_per_signal,
                avg_whale_pressure_score,
                avg_realized_volatility,
                avg_mid_drawdown,
                avg_sentiment_disagreement,
                fill_rate_lift,
                realized_edge_lift,
                adverse_edge_rate_lift,
                case
                    when signals < {min_samples} then 'insufficient_samples'
                    when feature_family = 'regime'
                      and feature_name = 'whale_pressure'
                      and bucket = 'high_whale_pressure'
                      and avg_realized_edge_after_slippage < {max_realized_edge}
                      then 'high_whale_pressure_negative_edge'
                    when feature_family = 'regime'
                      and feature_name = 'tail_risk'
                      and bucket = 'heavy_tail'
                      and max_drawdown >= {max_drawdown}
                      then 'heavy_tail_high_drawdown'
                    when feature_family = 'sentiment'
                      and coalesce(avg_sentiment_disagreement, 0) >= {max_sentiment_disagreement}
                      and coalesce(avg_fill_rate, 0) < {min_fill_rate}
                      then 'sentiment_divergence_poor_fill_rate'
                    when avg_realized_edge_after_slippage < {max_realized_edge}
                      and adverse_edge_rate >= {min_adverse_edge_rate}
                      then 'negative_edge_high_adverse_rate'
                    when avg_realized_edge_after_slippage < {max_realized_edge}
                      then 'negative_edge'
                    when adverse_edge_rate >= {min_adverse_edge_rate}
                      then 'high_adverse_rate'
                    else 'diagnostic_only'
                end as candidate_reason,
                signals >= {min_samples}
                  and (
                    (
                      feature_family = 'regime'
                      and feature_name = 'whale_pressure'
                      and bucket = 'high_whale_pressure'
                      and avg_realized_edge_after_slippage < {max_realized_edge}
                    )
                    or (
                      feature_family = 'regime'
                      and feature_name = 'tail_risk'
                      and bucket = 'heavy_tail'
                      and max_drawdown >= {max_drawdown}
                    )
                    or (
                      feature_family = 'sentiment'
                      and coalesce(avg_sentiment_disagreement, 0) >= {max_sentiment_disagreement}
                      and coalesce(avg_fill_rate, 0) < {min_fill_rate}
                    )
                    or avg_realized_edge_after_slippage < {max_realized_edge}
                    or adverse_edge_rate >= {min_adverse_edge_rate}
                  ) as should_block_candidate,
                false as can_apply_live
            from research_feature_bucket_performance
            """
        )


def export_feature_blocklist_candidate_report(
    db_path: Path,
    output_dir: Path,
    lookback_ms: int = 86_400_000,
    min_samples: int = 10,
    max_realized_edge: float = 0.0,
    min_adverse_edge_rate: float = 0.5,
    max_drawdown: float = 0.10,
    max_sentiment_disagreement: float = 0.50,
    min_fill_rate: float = 0.25,
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    create_feature_blocklist_candidate_views(
        db_path,
        lookback_ms=lookback_ms,
        min_samples=min_samples,
        max_realized_edge=max_realized_edge,
        min_adverse_edge_rate=min_adverse_edge_rate,
        max_drawdown=max_drawdown,
        max_sentiment_disagreement=max_sentiment_disagreement,
        min_fill_rate=min_fill_rate,
    )
    counts: dict[str, int] = {}
    candidate_segments: list[dict[str, object]] = []
    with duckdb.connect(str(db_path)) as conn:
        for view_name in FEATURE_BLOCKLIST_OUTPUTS:
            target = output_dir / f"{view_name}.parquet"
            conn.execute(
                f"copy (select * from {view_name}) to '{duckdb_literal(target.as_posix())}' (format parquet)"
            )
            row = conn.execute(f"select count(*) from {view_name}").fetchone()
            counts[view_name] = int(row[0]) if row else 0
        rows = conn.execute(
            """
            select
                candidate_id,
                feature_family,
                feature_name,
                bucket,
                strategy,
                side,
                candidate_reason,
                signals,
                filled_signals,
                avg_fill_rate,
                avg_realized_edge_after_slippage,
                adverse_edge_rate,
                max_drawdown
            from research_feature_blocklist_candidates
            where should_block_candidate
            order by feature_family, feature_name, bucket, strategy, side
            """
        ).fetchall()
        for row in rows:
            candidate_segments.append(
                {
                    "candidate_id": row[0],
                    "feature_family": row[1],
                    "feature_name": row[2],
                    "bucket": row[3],
                    "strategy": row[4],
                    "side": row[5],
                    "reason": row[6],
                    "signals": row[7],
                    "filled_signals": row[8],
                    "avg_fill_rate": row[9],
                    "avg_realized_edge_after_slippage": row[10],
                    "adverse_edge_rate": row[11],
                    "max_drawdown": row[12],
                    "candidate_only": True,
                }
            )
    candidates_payload: dict[str, object] = {
        "version": "feature_blocklist_candidates_v1",
        "candidate_only": True,
        "can_apply_live": False,
        "segments": candidate_segments,
    }
    (output_dir / "blocked_segments_candidates.json").write_text(
        json.dumps(candidates_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    report: dict[str, object] = {
        "report_version": FEATURE_BLOCKLIST_REPORT_VERSION,
        "decision_policy": "offline_diagnostics_only",
        "can_execute_trades": False,
        "can_apply_live": False,
        "lookback_ms": lookback_ms,
        "thresholds": {
            "min_samples": min_samples,
            "max_realized_edge": max_realized_edge,
            "min_adverse_edge_rate": min_adverse_edge_rate,
            "max_drawdown": max_drawdown,
            "max_sentiment_disagreement": max_sentiment_disagreement,
            "min_fill_rate": min_fill_rate,
        },
        "counts": {
            **counts,
            "blocked_segment_candidates": len(candidate_segments),
        },
    }
    (output_dir / "feature_blocklist_candidates.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(prog="research-feature-blocklist-candidates")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--lookback-ms", type=int, default=86_400_000)
    parser.add_argument("--min-samples", type=int, default=10)
    parser.add_argument("--max-realized-edge", type=float, default=0.0)
    parser.add_argument("--min-adverse-edge-rate", type=float, default=0.5)
    parser.add_argument("--max-drawdown", type=float, default=0.10)
    parser.add_argument("--max-sentiment-disagreement", type=float, default=0.50)
    parser.add_argument("--min-fill-rate", type=float, default=0.25)
    args = parser.parse_args()

    report = export_feature_blocklist_candidate_report(
        Path(args.duckdb),
        Path(args.output_dir),
        lookback_ms=args.lookback_ms,
        min_samples=args.min_samples,
        max_realized_edge=args.max_realized_edge,
        min_adverse_edge_rate=args.min_adverse_edge_rate,
        max_drawdown=args.max_drawdown,
        max_sentiment_disagreement=args.max_sentiment_disagreement,
        min_fill_rate=args.min_fill_rate,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
