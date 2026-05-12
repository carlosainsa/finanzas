import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd  # type: ignore[import-untyped]

from src.research.backtest import duckdb_literal
from src.research.game_theory import create_game_theory_views


REPORT_VERSION = "fill_toxicity_v1"


@dataclass(frozen=True)
class FillToxicityConfig:
    pre_fill_window_ms: int = 60_000
    min_filled_events: int = 3
    max_adverse_30s_rate: float = 0.50
    min_avg_pnl_30s: float = 0.0
    min_fill_rate: float = 0.01

    def __post_init__(self) -> None:
        if self.pre_fill_window_ms <= 0:
            raise ValueError("pre_fill_window_ms must be positive")
        if self.min_filled_events <= 0:
            raise ValueError("min_filled_events must be positive")
        if not 0 <= self.max_adverse_30s_rate <= 1:
            raise ValueError("max_adverse_30s_rate must be between 0 and 1")
        if not 0 <= self.min_fill_rate <= 1:
            raise ValueError("min_fill_rate must be between 0 and 1")


def create_fill_toxicity_report(
    db_path: Path,
    output_dir: Path,
    config: FillToxicityConfig = FillToxicityConfig(),
) -> dict[str, object]:
    create_fill_toxicity_views(db_path, config)
    output_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
        counts = copy_views(
            conn,
            output_dir,
            (
                "fill_toxicity_events",
                "fill_toxicity_by_asset_strategy",
                "fill_toxicity_by_segment",
                "fill_toxicity_summary",
            ),
        )
        summary = normalize_records(
            conn.execute("select * from fill_toxicity_summary").fetch_df().to_dict(
                orient="records"
            )
        )
        top_toxic_segments = normalize_records(
            conn.execute(
                """
                select *
                from fill_toxicity_by_segment
                order by
                    case decision
                        when 'REJECT_TOXICITY' then 1
                        when 'KEEP_DIAGNOSTIC' then 2
                        when 'INSUFFICIENT_SAMPLE' then 3
                        else 4
                    end,
                    adverse_30s_rate desc nulls last,
                    avg_pnl_30s asc nulls last,
                    filled_events desc
                limit 25
                """
            )
            .fetch_df()
            .to_dict(orient="records")
        )
        blocked_segments = normalize_records(
            conn.execute(
                """
                select
                    market_id,
                    asset_id,
                    side,
                    strategy,
                    model_version,
                    spread_bucket,
                    timing_bucket,
                    'fill_toxicity' as reason,
                    filled_events,
                    signals,
                    fill_rate,
                    adverse_30s_rate,
                    avg_pnl_30s,
                    toxicity_score
                from fill_toxicity_by_segment
                where decision = 'REJECT_TOXICITY'
                order by
                    toxicity_score desc nulls last,
                    adverse_30s_rate desc nulls last,
                    avg_pnl_30s asc nulls last,
                    filled_events desc,
                    asset_id
                """
            )
            .fetch_df()
            .to_dict(orient="records")
        )
    blocked_payload: dict[str, object] = {
        "version": "blocked_segments_v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "source_report_version": REPORT_VERSION,
        "decision_policy": "offline_fill_toxicity_blocklist_candidate_only",
        "segments": blocked_segments,
    }
    (output_dir / "blocked_segments.json").write_text(
        json.dumps(blocked_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    report: dict[str, object] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_fill_toxicity_diagnostics_only",
        "config": asdict(config),
        "counts": counts,
        "summary": summary[0] if summary else {},
        "top_toxic_segments": top_toxic_segments,
        "blocked_segments_path": str(output_dir / "blocked_segments.json"),
        "outputs": [
            "fill_toxicity_events.parquet",
            "fill_toxicity_by_asset_strategy.parquet",
            "fill_toxicity_by_segment.parquet",
            "fill_toxicity_summary.parquet",
            "blocked_segments.json",
            "fill_toxicity.json",
        ],
    }
    (output_dir / "fill_toxicity.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return report


def create_fill_toxicity_views(
    db_path: Path,
    config: FillToxicityConfig = FillToxicityConfig(),
) -> None:
    create_game_theory_views(db_path)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            create or replace view fill_toxicity_market_mark_changes as
            select
                *,
                case
                    when bid_depth + ask_depth > 0
                    then (bid_depth - ask_depth) / (bid_depth + ask_depth)
                    else null
                end as depth_imbalance,
                case
                    when best_bid is distinct from lag(best_bid) over (
                        partition by market_id, asset_id order by event_timestamp_ms
                    )
                    or best_ask is distinct from lag(best_ask) over (
                        partition by market_id, asset_id order by event_timestamp_ms
                    )
                    then 1.0
                    else 0.0
                end as quote_changed
            from market_marks
            """
        )
        conn.execute(
            f"""
            create or replace view fill_toxicity_signal_context as
            select
                s.*,
                case
                    when s.signal_spread is null then 'unknown'
                    when s.signal_spread <= 0.005 then '000_050bps'
                    when s.signal_spread <= 0.010 then '050_100bps'
                    when s.signal_spread <= 0.025 then '100_250bps'
                    when s.signal_spread <= 0.050 then '250_500bps'
                    else '500bps_plus'
                end as spread_bucket,
                pre_signal.pre_signal_snapshots,
                pre_signal.pre_signal_quote_changes,
                pre_signal.pre_signal_quote_change_rate,
                case
                    when pre_signal.pre_signal_snapshots is null
                      or pre_signal.pre_signal_snapshots = 0 then 'unknown'
                    when coalesce(pre_signal.pre_signal_quote_change_rate, 0) <= 0.10
                      then 'stable'
                    when coalesce(pre_signal.pre_signal_quote_change_rate, 0) <= 0.50
                      then 'rotating'
                    else 'volatile'
                end as timing_bucket
            from signal_market_context s
            left join lateral (
                select
                    count(*) as pre_signal_snapshots,
                    sum(quote_changed) as pre_signal_quote_changes,
                    avg(quote_changed) as pre_signal_quote_change_rate
                from fill_toxicity_market_mark_changes marks
                where marks.market_id = s.market_id
                  and marks.asset_id = s.asset_id
                  and marks.event_timestamp_ms >= s.signal_timestamp_ms - {config.pre_fill_window_ms}
                  and marks.event_timestamp_ms <= s.signal_timestamp_ms
            ) pre_signal on true
            """
        )
        conn.execute(
            f"""
            create or replace view fill_toxicity_events as
            select
                f.signal_id,
                f.order_id,
                f.market_id,
                f.asset_id,
                f.side,
                f.strategy,
                coalesce(f.model_version, 'unknown') as model_version,
                coalesce(f.feature_version, 'unknown') as feature_version,
                f.signal_timestamp_ms,
                f.fill_timestamp_ms,
                case
                    when f.signal_timestamp_ms is not null and f.fill_timestamp_ms is not null
                    then f.fill_timestamp_ms - f.signal_timestamp_ms
                    else null
                end as time_to_fill_ms,
                f.signal_price,
                f.signal_size,
                f.filled_price,
                f.filled_size,
                f.signal_mid_price,
                signal_ctx.signal_spread,
                f.distance_to_mid,
                signal_ctx.spread_bucket,
                signal_ctx.timing_bucket,
                f.pnl_5s,
                f.pnl_30s,
                f.pnl_300s,
                case when f.pnl_30s < 0 then 1.0 else 0.0 end as adverse_30s,
                case when f.pnl_30s < 0 then abs(f.pnl_30s) else 0.0 end as adverse_magnitude_30s,
                case
                    when f.filled_price > 0 then f.pnl_30s / f.filled_price * 10000
                    else null
                end as markout_30s_bps,
                pre_fill.pre_fill_snapshots,
                pre_fill.pre_fill_quote_changes,
                pre_fill.pre_fill_quote_change_rate,
                pre_fill.avg_pre_fill_spread,
                pre_fill.avg_pre_fill_depth,
                pre_fill.avg_pre_fill_depth_imbalance
            from post_fill_pnl_horizons f
            left join fill_toxicity_signal_context signal_ctx
              on signal_ctx.signal_id = f.signal_id
            left join lateral (
                select
                    count(*) as pre_fill_snapshots,
                    sum(quote_changed) as pre_fill_quote_changes,
                    avg(quote_changed) as pre_fill_quote_change_rate,
                    avg(spread) as avg_pre_fill_spread,
                    avg(bid_depth + ask_depth) as avg_pre_fill_depth,
                    avg(depth_imbalance) as avg_pre_fill_depth_imbalance
                from fill_toxicity_market_mark_changes marks
                where marks.market_id = f.market_id
                  and marks.asset_id = f.asset_id
                  and marks.event_timestamp_ms >= f.signal_timestamp_ms - {config.pre_fill_window_ms}
                  and marks.event_timestamp_ms <= f.fill_timestamp_ms
            ) pre_fill on true
            """
        )
        conn.execute(
            f"""
            create or replace view fill_toxicity_by_asset_strategy as
            with signal_counts as (
                select
                    market_id,
                    asset_id,
                    side,
                    strategy,
                    coalesce(model_version, 'unknown') as model_version,
                    count(*) as signals
                from signal_market_context
                group by market_id, asset_id, side, strategy, coalesce(model_version, 'unknown')
            ),
            toxicity as (
                select
                    market_id,
                    asset_id,
                    side,
                    strategy,
                    model_version,
                    count(*) as filled_events,
                    avg(pnl_5s) as avg_pnl_5s,
                    avg(pnl_30s) as avg_pnl_30s,
                    avg(pnl_300s) as avg_pnl_300s,
                    avg(adverse_30s) as adverse_30s_rate,
                    avg(adverse_magnitude_30s) as avg_adverse_magnitude_30s,
                    avg(markout_30s_bps) as avg_markout_30s_bps,
                    avg(time_to_fill_ms) as avg_time_to_fill_ms,
                    avg(distance_to_mid) as avg_distance_to_mid,
                    avg(pre_fill_quote_change_rate) as avg_pre_fill_quote_change_rate,
                    avg(avg_pre_fill_spread) as avg_pre_fill_spread,
                    avg(avg_pre_fill_depth) as avg_pre_fill_depth,
                    avg(avg_pre_fill_depth_imbalance) as avg_pre_fill_depth_imbalance
                from fill_toxicity_events
                group by market_id, asset_id, side, strategy, model_version
            )
            select
                signals.market_id,
                signals.asset_id,
                signals.side,
                signals.strategy,
                signals.model_version,
                signals.signals,
                coalesce(toxicity.filled_events, 0) as filled_events,
                case
                    when signals.signals > 0 then coalesce(toxicity.filled_events, 0)::double / signals.signals
                    else null
                end as fill_rate,
                toxicity.avg_pnl_5s,
                toxicity.avg_pnl_30s,
                toxicity.avg_pnl_300s,
                toxicity.adverse_30s_rate,
                toxicity.avg_adverse_magnitude_30s,
                toxicity.avg_markout_30s_bps,
                toxicity.avg_time_to_fill_ms,
                toxicity.avg_distance_to_mid,
                toxicity.avg_pre_fill_quote_change_rate,
                toxicity.avg_pre_fill_spread,
                toxicity.avg_pre_fill_depth,
                toxicity.avg_pre_fill_depth_imbalance,
                case
                    when coalesce(toxicity.filled_events, 0) < {config.min_filled_events}
                    then 'INSUFFICIENT_SAMPLE'
                    when coalesce(toxicity.adverse_30s_rate, 0) > {config.max_adverse_30s_rate}
                      or coalesce(toxicity.avg_pnl_30s, 0) < {config.min_avg_pnl_30s}
                    then 'REJECT_TOXICITY'
                    when signals.signals > 0
                      and coalesce(toxicity.filled_events, 0)::double / signals.signals < {config.min_fill_rate}
                    then 'KEEP_DIAGNOSTIC'
                    else 'PROMOTE_TO_OBSERVATION'
                end as decision
            from signal_counts signals
            left join toxicity
              on toxicity.market_id = signals.market_id
             and toxicity.asset_id = signals.asset_id
             and toxicity.side = signals.side
             and toxicity.strategy = signals.strategy
             and toxicity.model_version = signals.model_version
            """
        )
        conn.execute(
            f"""
            create or replace view fill_toxicity_by_segment as
            with signal_counts as (
                select
                    market_id,
                    asset_id,
                    side,
                    strategy,
                    coalesce(model_version, 'unknown') as model_version,
                    spread_bucket,
                    timing_bucket,
                    count(*) as signals
                from fill_toxicity_signal_context
                group by
                    market_id,
                    asset_id,
                    side,
                    strategy,
                    coalesce(model_version, 'unknown'),
                    spread_bucket,
                    timing_bucket
            ),
            toxicity as (
                select
                    market_id,
                    asset_id,
                    side,
                    strategy,
                    model_version,
                    spread_bucket,
                    timing_bucket,
                    count(*) as filled_events,
                    avg(pnl_5s) as avg_pnl_5s,
                    avg(pnl_30s) as avg_pnl_30s,
                    avg(pnl_300s) as avg_pnl_300s,
                    avg(adverse_30s) as adverse_30s_rate,
                    avg(adverse_magnitude_30s) as avg_adverse_magnitude_30s,
                    avg(markout_30s_bps) as avg_markout_30s_bps,
                    avg(time_to_fill_ms) as avg_time_to_fill_ms,
                    avg(distance_to_mid) as avg_distance_to_mid,
                    avg(pre_fill_quote_change_rate) as avg_pre_fill_quote_change_rate,
                    avg(avg_pre_fill_spread) as avg_pre_fill_spread,
                    avg(avg_pre_fill_depth) as avg_pre_fill_depth,
                    avg(avg_pre_fill_depth_imbalance) as avg_pre_fill_depth_imbalance
                from fill_toxicity_events
                group by
                    market_id,
                    asset_id,
                    side,
                    strategy,
                    model_version,
                    spread_bucket,
                    timing_bucket
            )
            select
                signals.market_id,
                signals.asset_id,
                signals.side,
                signals.strategy,
                signals.model_version,
                signals.spread_bucket,
                signals.timing_bucket,
                signals.signals,
                coalesce(toxicity.filled_events, 0) as filled_events,
                case
                    when signals.signals > 0 then coalesce(toxicity.filled_events, 0)::double / signals.signals
                    else null
                end as fill_rate,
                toxicity.avg_pnl_5s,
                toxicity.avg_pnl_30s,
                toxicity.avg_pnl_300s,
                toxicity.adverse_30s_rate,
                toxicity.avg_adverse_magnitude_30s,
                toxicity.avg_markout_30s_bps,
                toxicity.avg_time_to_fill_ms,
                toxicity.avg_distance_to_mid,
                toxicity.avg_pre_fill_quote_change_rate,
                toxicity.avg_pre_fill_spread,
                toxicity.avg_pre_fill_depth,
                toxicity.avg_pre_fill_depth_imbalance,
                (
                    coalesce(toxicity.adverse_30s_rate, 0) * 100
                    + greatest(0, -coalesce(toxicity.avg_pnl_30s, 0)) * 10000
                    + case
                        when coalesce(toxicity.filled_events, 0) >= {config.min_filled_events}
                        then 10
                        else 0
                      end
                ) as toxicity_score,
                case
                    when coalesce(toxicity.filled_events, 0) < {config.min_filled_events}
                    then 'INSUFFICIENT_SAMPLE'
                    when coalesce(toxicity.adverse_30s_rate, 0) > {config.max_adverse_30s_rate}
                      or coalesce(toxicity.avg_pnl_30s, 0) < {config.min_avg_pnl_30s}
                    then 'REJECT_TOXICITY'
                    when signals.signals > 0
                      and coalesce(toxicity.filled_events, 0)::double / signals.signals < {config.min_fill_rate}
                    then 'KEEP_DIAGNOSTIC'
                    else 'PROMOTE_TO_OBSERVATION'
                end as decision
            from signal_counts signals
            left join toxicity
              on toxicity.market_id = signals.market_id
             and toxicity.asset_id = signals.asset_id
             and toxicity.side = signals.side
             and toxicity.strategy = signals.strategy
             and toxicity.model_version = signals.model_version
             and toxicity.spread_bucket = signals.spread_bucket
             and toxicity.timing_bucket = signals.timing_bucket
            """
        )
        conn.execute(
            """
            create or replace view fill_toxicity_summary as
            select
                count(*) as segments,
                sum(signals) as signals,
                sum(filled_events) as filled_events,
                case when sum(signals) > 0 then sum(filled_events)::double / sum(signals) else null end as fill_rate,
                avg(avg_pnl_30s) as avg_pnl_30s,
                avg(adverse_30s_rate) as adverse_30s_rate,
                sum(case when decision = 'REJECT_TOXICITY' then 1 else 0 end) as rejected_segments,
                sum(case when decision = 'PROMOTE_TO_OBSERVATION' then 1 else 0 end) as promoted_segments,
                sum(case when decision = 'KEEP_DIAGNOSTIC' then 1 else 0 end) as diagnostic_segments,
                sum(case when decision = 'INSUFFICIENT_SAMPLE' then 1 else 0 end) as insufficient_sample_segments,
                (
                    select count(*)
                    from fill_toxicity_by_segment
                ) as bucketed_segments,
                (
                    select sum(case when decision = 'REJECT_TOXICITY' then 1 else 0 end)
                    from fill_toxicity_by_segment
                ) as bucketed_rejected_segments
            from fill_toxicity_by_asset_strategy
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
    parser = argparse.ArgumentParser(description="Export offline fill toxicity diagnostics")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--pre-fill-window-ms",
        type=int,
        default=FillToxicityConfig.pre_fill_window_ms,
    )
    parser.add_argument(
        "--min-filled-events",
        type=int,
        default=FillToxicityConfig.min_filled_events,
    )
    parser.add_argument(
        "--max-adverse-30s-rate",
        type=float,
        default=FillToxicityConfig.max_adverse_30s_rate,
    )
    parser.add_argument(
        "--min-avg-pnl-30s",
        type=float,
        default=FillToxicityConfig.min_avg_pnl_30s,
    )
    parser.add_argument(
        "--min-fill-rate",
        type=float,
        default=FillToxicityConfig.min_fill_rate,
    )
    args = parser.parse_args()

    report = create_fill_toxicity_report(
        Path(args.duckdb),
        Path(args.output_dir),
        FillToxicityConfig(
            pre_fill_window_ms=args.pre_fill_window_ms,
            min_filled_events=args.min_filled_events,
            max_adverse_30s_rate=args.max_adverse_30s_rate,
            min_avg_pnl_30s=args.min_avg_pnl_30s,
            min_fill_rate=args.min_fill_rate,
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
