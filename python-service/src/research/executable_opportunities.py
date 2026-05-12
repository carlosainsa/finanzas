import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd  # type: ignore[import-untyped]

from src.research.backtest import duckdb_literal
from src.research.game_theory import relation_exists
from src.research.quote_execution_diagnostics import (
    QuoteExecutionDiagnosticsConfig,
    create_quote_execution_diagnostics_views,
)


REPORT_VERSION = "executable_opportunities_v1"


@dataclass(frozen=True)
class ExecutableOpportunitiesConfig:
    max_future_window_ms: int = 300_000
    pre_signal_window_ms: int = 60_000
    examples_limit: int = 100

    def __post_init__(self) -> None:
        if self.max_future_window_ms <= 0:
            raise ValueError("max_future_window_ms must be positive")
        if self.pre_signal_window_ms <= 0:
            raise ValueError("pre_signal_window_ms must be positive")
        if self.examples_limit <= 0:
            raise ValueError("examples_limit must be positive")


def create_executable_opportunities_report(
    db_path: Path,
    output_dir: Path,
    config: ExecutableOpportunitiesConfig = ExecutableOpportunitiesConfig(),
) -> dict[str, object]:
    create_executable_opportunities_views(db_path, config)
    output_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
        counts = copy_views(
            conn,
            output_dir,
            (
                "executable_opportunities",
                "executable_opportunity_examples",
                "executable_opportunity_summary",
            ),
        )
        summary = normalize_records(
            conn.execute("select * from executable_opportunity_summary")
            .fetch_df()
            .to_dict(orient="records")
        )
        examples = normalize_records(
            conn.execute(
                f"""
                select *
                from executable_opportunity_examples
                order by executable_score desc nulls last, signal_timestamp_ms, signal_id
                limit {config.examples_limit}
                """
            )
            .fetch_df()
            .to_dict(orient="records")
        )
    report: dict[str, object] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_executable_opportunity_dataset_only",
        "config": asdict(config),
        "counts": counts,
        "summary": summary[0] if summary else {},
        "examples": examples,
        "outputs": [
            "executable_opportunities.parquet",
            "executable_opportunity_examples.parquet",
            "executable_opportunity_summary.parquet",
            "executable_opportunities.json",
        ],
    }
    (output_dir / "executable_opportunities.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return report


def create_executable_opportunities_views(
    db_path: Path,
    config: ExecutableOpportunitiesConfig = ExecutableOpportunitiesConfig(),
) -> None:
    create_quote_execution_diagnostics_views(
        db_path,
        QuoteExecutionDiagnosticsConfig(
            max_future_window_ms=config.max_future_window_ms,
            examples_limit=config.examples_limit,
        ),
    )
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            create or replace view executable_opportunity_mark_changes as
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
            from orderbook_snapshots
            """
        )
        pnl_join = executable_pnl_join_sql(conn)
        conn.execute(
            f"""
            create or replace view executable_opportunities as
            select
                outcomes.signal_id,
                outcomes.market_id,
                outcomes.asset_id,
                outcomes.side,
                outcomes.strategy,
                coalesce(outcomes.model_version, 'unknown') as model_version,
                coalesce(outcomes.feature_version, 'unknown') as feature_version,
                outcomes.signal_timestamp_ms,
                outcomes.signal_price,
                outcomes.signal_size,
                outcomes.confidence,
                outcomes.best_bid,
                outcomes.best_ask,
                outcomes.spread,
                outcomes.bid_depth,
                outcomes.ask_depth,
                outcomes.mid_at_signal,
                outcomes.touch_price_at_signal,
                outcomes.quote_relation,
                outcomes.distance_to_touch,
                outcomes.distance_to_mid,
                case
                    when outcomes.spread is null then 'unknown'
                    when outcomes.spread <= 0.005000001 then '000_050bps'
                    when outcomes.spread <= 0.010000001 then '050_100bps'
                    when outcomes.spread <= 0.025000001 then '100_250bps'
                    when outcomes.spread <= 0.050000001 then '250_500bps'
                    else '500bps_plus'
                end as spread_bucket,
                case
                    when outcomes.distance_to_touch is null then 'unknown'
                    when outcomes.distance_to_touch <= 0.000000001 then 'at_touch'
                    when outcomes.distance_to_touch <= 0.005000001 then 'within_half_cent'
                    when outcomes.distance_to_touch <= 0.010000001 then 'within_one_cent'
                    when outcomes.distance_to_touch <= 0.020000001 then 'within_two_cent'
                    else 'far_from_touch'
                end as distance_to_touch_bucket,
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
                end as timing_bucket,
                case
                    when outcomes.bid_depth is not null and outcomes.ask_depth is not null
                    then least(outcomes.bid_depth, outcomes.ask_depth)
                    else null
                end as available_depth,
                case
                    when outcomes.side = 'BUY' then outcomes.confidence - outcomes.signal_price
                    when outcomes.side = 'SELL' then outcomes.signal_price - (1 - outcomes.confidence)
                    else null
                end as expected_edge,
                outcomes.observed_filled_size > 0 as observed_filled,
                outcomes.synthetic_filled_size > 0 as synthetic_filled,
                outcomes.future_touched_limit,
                outcomes.observed_fill_rate,
                outcomes.synthetic_fill_rate,
                outcomes.adjusted_synthetic_fill_indicator,
                outcomes.required_quote_move,
                outcomes.ms_to_first_future_touch,
                outcomes.root_cause,
                outcomes.execution_path,
                pnl.pnl_5s,
                pnl.pnl_30s,
                pnl.pnl_300s,
                case
                    when pnl.pnl_30s is null then null
                    when pnl.pnl_30s < 0 then 1.0
                    else 0.0
                end as adverse_30s,
                (
                    coalesce(case when outcomes.observed_filled_size > 0 then 1.0 else 0.0 end, 0)
                    + coalesce(outcomes.adjusted_synthetic_fill_indicator, 0) * 0.25
                    + least(greatest(coalesce(outcomes.confidence - outcomes.signal_price, 0), 0) * 10.0, 1.0)
                    + least(coalesce(least(outcomes.bid_depth, outcomes.ask_depth), 0) / 100.0, 1.0) * 0.10
                    - case when pnl.pnl_30s is not null and pnl.pnl_30s < 0 then 0.50 else 0.0 end
                ) as executable_score,
                (
                    outcomes.observed_filled_size > 0
                    or outcomes.future_touched_limit
                    or outcomes.synthetic_filled_size > 0
                ) as is_executable
            from quote_execution_outcomes outcomes
            left join lateral (
                select
                    count(*) as pre_signal_snapshots,
                    sum(quote_changed) as pre_signal_quote_changes,
                    avg(quote_changed) as pre_signal_quote_change_rate
                from executable_opportunity_mark_changes marks
                where marks.market_id = outcomes.market_id
                  and marks.asset_id = outcomes.asset_id
                  and marks.event_timestamp_ms >= outcomes.signal_timestamp_ms - {config.pre_signal_window_ms}
                  and marks.event_timestamp_ms <= outcomes.signal_timestamp_ms
            ) pre_signal on true
            {pnl_join}
            """
        )
        conn.execute(
            """
            create or replace view executable_opportunity_summary as
            select
                count(*) as opportunities,
                sum(case when is_executable then 1 else 0 end) as executable_opportunities,
                avg(case when is_executable then 1.0 else 0.0 end) as executable_opportunity_rate,
                sum(case when observed_filled then 1 else 0 end) as observed_fills,
                avg(case when observed_filled then 1.0 else 0.0 end) as observed_fill_rate,
                sum(case when synthetic_filled then 1 else 0 end) as synthetic_fills,
                avg(case when synthetic_filled then 1.0 else 0.0 end) as synthetic_fill_rate,
                avg(expected_edge) as avg_expected_edge,
                avg(available_depth) as avg_available_depth,
                avg(adverse_30s) as adverse_30s_rate,
                avg(pnl_30s) as avg_pnl_30s,
                avg(executable_score) as avg_executable_score
            from executable_opportunities
            """
        )
        conn.execute(
            """
            create or replace view executable_opportunity_examples as
            select *
            from executable_opportunities
            where is_executable
            """
        )


def executable_pnl_join_sql(conn: duckdb.DuckDBPyConnection) -> str:
    if relation_exists(conn, "fill_toxicity_events"):
        return """
            left join fill_toxicity_events pnl
              on pnl.signal_id = outcomes.signal_id
            """
    return """
        left join (
            select
                cast(null as varchar) as signal_id,
                cast(null as double) as pnl_5s,
                cast(null as double) as pnl_30s,
                cast(null as double) as pnl_300s
            where false
        ) pnl on pnl.signal_id = outcomes.signal_id
    """


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
    parser = argparse.ArgumentParser(description="Export offline executable opportunities")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--max-future-window-ms",
        type=int,
        default=ExecutableOpportunitiesConfig.max_future_window_ms,
    )
    parser.add_argument(
        "--pre-signal-window-ms",
        type=int,
        default=ExecutableOpportunitiesConfig.pre_signal_window_ms,
    )
    parser.add_argument(
        "--examples-limit",
        type=int,
        default=ExecutableOpportunitiesConfig.examples_limit,
    )
    args = parser.parse_args()
    report = create_executable_opportunities_report(
        Path(args.duckdb),
        Path(args.output_dir),
        ExecutableOpportunitiesConfig(
            max_future_window_ms=args.max_future_window_ms,
            pre_signal_window_ms=args.pre_signal_window_ms,
            examples_limit=args.examples_limit,
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
