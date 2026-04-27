import argparse
import json
from pathlib import Path

import duckdb

from src.research.backtest import create_backtest_views, duckdb_literal
from src.research.sentiment_features import create_sentiment_feature_views


SENTIMENT_LIFT_REPORT_VERSION = "sentiment_lift_evaluation_v1"
SENTIMENT_LIFT_OUTPUTS = (
    "sentiment_lift_trade_context",
    "sentiment_lift_bucket_performance",
    "sentiment_lift_drawdown",
    "sentiment_lift_summary",
)


def create_sentiment_lift_views(db_path: Path, lookback_ms: int = 86_400_000) -> None:
    if lookback_ms <= 0:
        raise ValueError("lookback_ms must be positive")
    create_backtest_views(db_path)
    create_sentiment_feature_views(db_path, lookback_ms=lookback_ms)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            create or replace view sentiment_lift_trade_context as
            select
                t.signal_id,
                t.market_id,
                t.asset_id,
                t.side,
                t.strategy,
                t.model_version::varchar as model_version,
                t.data_version::varchar as data_version,
                t.feature_version::varchar as backtest_feature_version,
                t.signal_timestamp_ms,
                t.signal_price,
                t.signal_size,
                t.confidence,
                t.order_id,
                t.status,
                t.filled_price,
                t.filled_size,
                t.fill_rate,
                t.slippage,
                t.model_edge,
                t.realized_edge_after_slippage,
                coalesce(t.realized_edge_after_slippage, 0) * coalesce(t.filled_size, 0) as realized_edge_pnl,
                case
                    when coalesce(t.realized_edge_after_slippage, 0) < 0 then 1.0
                    else 0.0
                end as adverse_edge_event,
                s.feature_id as sentiment_feature_id,
                s.evidence_id as sentiment_evidence_id,
                s.available_at_ms as sentiment_available_at_ms,
                s.feature_timestamp_ms as sentiment_feature_timestamp_ms,
                s.direction as sentiment_direction,
                s.sentiment_score,
                s.net_sentiment,
                s.evidence_count,
                s.source_count,
                s.sentiment_disagreement,
                s.source_quality,
                s.confidence as sentiment_confidence,
                case
                    when s.feature_id is null then 'missing'
                    when s.net_sentiment >= 0.20 then 'strong_positive'
                    when s.net_sentiment >= 0.05 then 'weak_positive'
                    when s.net_sentiment <= -0.20 then 'strong_negative'
                    when s.net_sentiment <= -0.05 then 'weak_negative'
                    else 'neutral'
                end as sentiment_bucket,
                case
                    when s.feature_id is null then 'missing'
                    when s.direction = 'NEUTRAL' then 'neutral'
                    when t.side = 'BUY' and s.direction = 'YES' then 'aligned'
                    when t.side = 'SELL' and s.direction = 'NO' then 'aligned'
                    else 'opposed'
                end as signal_sentiment_alignment
            from backtest_trades t
            left join lateral (
                select *
                from sentiment_feature_candidates s
                where s.market_id = t.market_id
                  and coalesce(s.asset_id, '') = coalesce(t.asset_id, '')
                  and s.available_at_ms <= t.signal_timestamp_ms
                order by s.available_at_ms desc, s.feature_timestamp_ms desc, s.feature_id
                limit 1
            ) s on true
            """
        )
        conn.execute(
            """
            create or replace view sentiment_lift_bucket_performance as
            select
                sentiment_bucket,
                signal_sentiment_alignment,
                coalesce(strategy, 'unknown') as strategy,
                side,
                count(distinct signal_id) as signals,
                count(distinct case when filled_size > 0 then signal_id else null end) as filled_signals,
                avg(fill_rate) as avg_fill_rate,
                avg(realized_edge_after_slippage) as avg_realized_edge_after_slippage,
                sum(realized_edge_pnl) as realized_edge_pnl,
                avg(adverse_edge_event) as adverse_edge_rate,
                avg(slippage) as avg_slippage,
                avg(sentiment_disagreement) as avg_sentiment_disagreement,
                avg(evidence_count) as avg_evidence_count,
                avg(source_count) as avg_source_count,
                avg(source_quality) as avg_source_quality,
                avg(sentiment_confidence) as avg_sentiment_confidence,
                case
                    when count(distinct signal_id) > 0 then sum(realized_edge_pnl) / count(distinct signal_id)
                    else null
                end as pnl_per_signal
            from sentiment_lift_trade_context
            group by sentiment_bucket, signal_sentiment_alignment, strategy, side
            """
        )
        conn.execute(
            """
            create or replace view sentiment_lift_drawdown as
            with equity as (
                select
                    sentiment_bucket,
                    signal_sentiment_alignment,
                    coalesce(strategy, 'unknown') as strategy,
                    side,
                    signal_timestamp_ms,
                    signal_id,
                    realized_edge_pnl,
                    sum(realized_edge_pnl) over (
                        partition by sentiment_bucket, signal_sentiment_alignment, coalesce(strategy, 'unknown'), side
                        order by signal_timestamp_ms nulls last, signal_id
                        rows between unbounded preceding and current row
                    ) as cumulative_pnl
                from sentiment_lift_trade_context
            ),
            drawdowns as (
                select
                    *,
                    max(cumulative_pnl) over (
                        partition by sentiment_bucket, signal_sentiment_alignment, strategy, side
                        order by signal_timestamp_ms nulls last, signal_id
                        rows between unbounded preceding and current row
                    ) - cumulative_pnl as drawdown
                from equity
            )
            select
                sentiment_bucket,
                signal_sentiment_alignment,
                strategy,
                side,
                max(drawdown) as max_drawdown
            from drawdowns
            group by sentiment_bucket, signal_sentiment_alignment, strategy, side
            """
        )
        conn.execute(
            """
            create or replace view sentiment_lift_summary as
            with overall as (
                select
                    coalesce(strategy, 'unknown') as strategy,
                    side,
                    avg(fill_rate) as baseline_fill_rate,
                    avg(realized_edge_after_slippage) as baseline_realized_edge_after_slippage,
                    avg(adverse_edge_event) as baseline_adverse_edge_rate
                from sentiment_lift_trade_context
                group by strategy, side
            )
            select
                b.*,
                coalesce(d.max_drawdown, 0) as max_drawdown,
                o.baseline_fill_rate,
                o.baseline_realized_edge_after_slippage,
                o.baseline_adverse_edge_rate,
                b.avg_fill_rate - o.baseline_fill_rate as fill_rate_lift,
                b.avg_realized_edge_after_slippage - o.baseline_realized_edge_after_slippage as realized_edge_lift,
                b.adverse_edge_rate - o.baseline_adverse_edge_rate as adverse_edge_rate_lift
            from sentiment_lift_bucket_performance b
            join overall o on o.strategy = b.strategy and o.side = b.side
            left join sentiment_lift_drawdown d
              on d.sentiment_bucket = b.sentiment_bucket
             and d.signal_sentiment_alignment = b.signal_sentiment_alignment
             and d.strategy = b.strategy
             and d.side = b.side
            """
        )


def export_sentiment_lift_report(
    db_path: Path, output_dir: Path, lookback_ms: int = 86_400_000
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    create_sentiment_lift_views(db_path, lookback_ms=lookback_ms)
    counts: dict[str, int] = {}
    with duckdb.connect(str(db_path)) as conn:
        for view_name in SENTIMENT_LIFT_OUTPUTS:
            target = output_dir / f"{view_name}.parquet"
            conn.execute(
                f"copy (select * from {view_name}) to '{duckdb_literal(target.as_posix())}' (format parquet)"
            )
            row = conn.execute(f"select count(*) from {view_name}").fetchone()
            counts[view_name] = int(row[0]) if row else 0
    report: dict[str, object] = {
        "report_version": SENTIMENT_LIFT_REPORT_VERSION,
        "decision_policy": "offline_diagnostics_only",
        "can_execute_trades": False,
        "lookback_ms": lookback_ms,
        "counts": counts,
    }
    (output_dir / "sentiment_lift.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(prog="research-sentiment-lift")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--lookback-ms", type=int, default=86_400_000)
    args = parser.parse_args()

    report = export_sentiment_lift_report(
        Path(args.duckdb), Path(args.output_dir), lookback_ms=args.lookback_ms
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
