import argparse
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from src.research.backtest import duckdb_literal
from src.research.market_opportunity_selector import MarketOpportunityConfig
from src.research.runtime_touch_ranking import normalize_records


REPORT_VERSION = "market_fillability_score_v1"


@dataclass(frozen=True)
class MarketFillabilityScoreConfig:
    min_signals: int = 1
    min_fillability_score: float = 0.0
    max_synthetic_gap: float = 0.50
    limit: int = 100
    lookback_ms: int = 3_600_000
    max_candidate_signals: int = 20_000
    max_candidate_snapshots: int = 100_000
    max_future_window_ms: int = 300_000

    def __post_init__(self) -> None:
        if self.min_signals < 0:
            raise ValueError("min_signals must be non-negative")
        if self.max_synthetic_gap < 0:
            raise ValueError("max_synthetic_gap must be non-negative")
        if self.limit <= 0:
            raise ValueError("limit must be positive")
        if self.lookback_ms <= 0:
            raise ValueError("lookback_ms must be positive")
        if self.max_candidate_signals <= 0:
            raise ValueError("max_candidate_signals must be positive")
        if self.max_candidate_snapshots <= 0:
            raise ValueError("max_candidate_snapshots must be positive")
        if self.max_future_window_ms <= 0:
            raise ValueError("max_future_window_ms must be positive")


def create_market_fillability_score_report(
    db_path: Path,
    output_dir: Path,
    config: MarketFillabilityScoreConfig = MarketFillabilityScoreConfig(),
) -> dict[str, Any]:
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB does not exist: {db_path}")
    create_market_fillability_score_views(db_path, config)
    output_dir.mkdir(parents=True, exist_ok=True)
    ranking_path = output_dir / "market_fillability_score_ranking.parquet"
    selected_path = output_dir / "selected_market_fillability_scores.parquet"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            f"""
            copy (
                select *
                from market_fillability_score_ranking
                order by rank
            ) to '{duckdb_literal(ranking_path.as_posix())}' (format parquet)
            """
        )
        ranked_assets = int(
            scalar_count(conn, "select count(*) from market_fillability_score_ranking")
        )
        candidate_signals = int(
            scalar_count(conn, "select count(*) from market_fillability_candidate_signals")
        )
        candidate_snapshots = int(
            scalar_count(conn, "select count(*) from market_fillability_candidate_books")
        )
        selected = conn.execute(
            f"""
            select *
            from market_fillability_score_ranking
            where recommendation = 'PROMOTE_TO_DISCOVERY_BATCH'
            order by rank
            limit {config.limit}
            """
        ).fetch_df()
        conn.execute(
            f"""
            copy (
                select *
                from market_fillability_score_ranking
                where recommendation = 'PROMOTE_TO_DISCOVERY_BATCH'
                order by rank
                limit {config.limit}
            ) to '{duckdb_literal(selected_path.as_posix())}' (format parquet)
            """
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
            "candidate_signals": candidate_signals,
            "candidate_snapshots": candidate_snapshots,
            "ranked_assets": ranked_assets,
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


def scalar_count(conn: duckdb.DuckDBPyConnection, query: str) -> int:
    row = conn.execute(query).fetchone()
    if row is None:
        return 0
    return int(row[0])


def create_market_fillability_score_views(
    db_path: Path,
    config: MarketFillabilityScoreConfig = MarketFillabilityScoreConfig(),
) -> None:
    opportunity_config = MarketOpportunityConfig(limit=config.limit)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            f"""
            create or replace table market_fillability_candidate_signals as
            with bounds as (
                select max(event_timestamp_ms) as max_signal_timestamp_ms
                from signals
                where event_timestamp_ms is not null
            ),
            recent as (
                select
                    signals.*,
                    row_number() over (
                        order by signals.event_timestamp_ms desc, signals.signal_id
                    ) as recent_rank
                from signals, bounds
                where signals.signal_id is not null
                  and signals.market_id is not null
                  and signals.asset_id is not null
                  and signals.event_timestamp_ms is not null
                  and (
                    bounds.max_signal_timestamp_ms is null
                    or signals.event_timestamp_ms
                        >= bounds.max_signal_timestamp_ms - {config.lookback_ms}
                  )
            )
            select *
            exclude (recent_rank)
            from recent
            where recent_rank <= {config.max_candidate_signals}
            """
        )
        conn.execute(
            f"""
            create or replace table market_fillability_candidate_books as
            with signal_bounds as (
                select
                    min(event_timestamp_ms) as min_signal_timestamp_ms,
                    max(event_timestamp_ms) as max_signal_timestamp_ms
                from market_fillability_candidate_signals
            ),
            candidate_books as (
                select
                    book.*,
                    row_number() over (
                        order by book.event_timestamp_ms desc, book.market_id, book.asset_id
                    ) as recent_rank
                from orderbook_snapshots book
                join (
                    select distinct market_id, asset_id
                    from market_fillability_candidate_signals
                ) assets
                  on assets.market_id = book.market_id
                 and assets.asset_id = book.asset_id
                cross join signal_bounds
                where book.event_timestamp_ms is not null
                  and (
                    signal_bounds.min_signal_timestamp_ms is null
                    or book.event_timestamp_ms
                        >= signal_bounds.min_signal_timestamp_ms
                            - {config.max_future_window_ms}
                  )
                  and (
                    signal_bounds.max_signal_timestamp_ms is null
                    or book.event_timestamp_ms
                        <= signal_bounds.max_signal_timestamp_ms
                            + {config.max_future_window_ms}
                  )
            )
            select *
            exclude (recent_rank)
            from candidate_books
            where recent_rank <= {config.max_candidate_snapshots}
            """
        )
        conn.execute(
            """
            create or replace view market_fillability_observed_reports as
            select
                signal_id,
                max(order_id) as observed_order_id,
                max(status) as observed_terminal_status,
                max(filled_price) as observed_filled_price,
                max(coalesce(cumulative_filled_size, filled_size, 0)) as observed_filled_size,
                max(event_timestamp_ms) as observed_event_timestamp_ms,
                count(*) as raw_observed_report_rows
            from execution_reports
            where signal_id in (
                select signal_id
                from market_fillability_candidate_signals
            )
            group by signal_id
            """
        )
        conn.execute(
            """
            create or replace view market_fillability_signal_books as
            select
                s.signal_id,
                s.market_id,
                s.asset_id,
                s.side,
                coalesce(s.strategy, 'unknown') as strategy,
                s.model_version::varchar as model_version,
                s.data_version::varchar as data_version,
                s.feature_version::varchar as feature_version,
                s.price as signal_price,
                s.size as signal_size,
                s.confidence,
                s.event_timestamp_ms as signal_timestamp_ms,
                book.best_bid,
                book.best_ask,
                book.spread,
                book.bid_depth,
                book.ask_depth,
                book.event_timestamp_ms as book_timestamp_ms,
                case
                    when book.event_timestamp_ms is not null
                    then s.event_timestamp_ms - book.event_timestamp_ms
                    else null
                end as book_age_ms,
                case
                    when book.best_bid is not null and book.best_ask is not null
                    then (book.best_bid + book.best_ask) / 2
                    else null
                end as mid_at_signal,
                case
                    when s.side = 'BUY' then book.best_ask
                    when s.side = 'SELL' then book.best_bid
                    else null
                end as touch_price_at_signal,
                case
                    when s.side = 'BUY' and book.best_ask is not null then book.best_ask - s.price
                    when s.side = 'SELL' and book.best_bid is not null then s.price - book.best_bid
                    else null
                end as signed_distance_to_touch,
                case
                    when s.side = 'BUY' and book.best_ask is not null then greatest(book.best_ask - s.price, 0)
                    when s.side = 'SELL' and book.best_bid is not null then greatest(s.price - book.best_bid, 0)
                    else null
                end as distance_to_touch,
                case
                    when book.best_bid is not null and book.best_ask is not null
                    then abs(s.price - ((book.best_bid + book.best_ask) / 2))
                    else null
                end as distance_to_mid,
                case
                    when book.best_bid is null or book.best_ask is null then 'no_book'
                    when s.side = 'BUY' and s.price > book.best_ask then 'crossing_touch'
                    when s.side = 'BUY' and abs(s.price - book.best_ask) <= 0.000000001 then 'at_touch'
                    when s.side = 'BUY' and s.price > book.best_bid then 'inside_spread'
                    when s.side = 'BUY' then 'behind_touch'
                    when s.side = 'SELL' and s.price < book.best_bid then 'crossing_touch'
                    when s.side = 'SELL' and abs(s.price - book.best_bid) <= 0.000000001 then 'at_touch'
                    when s.side = 'SELL' and s.price < book.best_ask then 'inside_spread'
                    when s.side = 'SELL' then 'behind_touch'
                    else 'unsupported_side'
                end as quote_relation
            from market_fillability_candidate_signals s
            left join lateral (
                select *
                from market_fillability_candidate_books book
                where book.market_id = s.market_id
                  and book.asset_id = s.asset_id
                  and book.event_timestamp_ms <= s.event_timestamp_ms
                order by book.event_timestamp_ms desc
                limit 1
            ) book on true
            """
        )
        conn.execute(
            f"""
            create or replace view market_fillability_future_books as
            select
                books.signal_id,
                count(future_book.event_timestamp_ms) as future_book_snapshots,
                sum(
                    case
                        when (
                            books.side = 'BUY'
                            and future_book.best_ask <= books.signal_price
                        )
                        or (
                            books.side = 'SELL'
                            and future_book.best_bid >= books.signal_price
                        )
                        then 1
                        else 0
                    end
                ) as future_touches,
                min(future_book.event_timestamp_ms) as first_future_book_timestamp_ms,
                min(
                    case
                        when (
                            books.side = 'BUY'
                            and future_book.best_ask <= books.signal_price
                        )
                        or (
                            books.side = 'SELL'
                            and future_book.best_bid >= books.signal_price
                        )
                        then future_book.event_timestamp_ms
                        else null
                    end
                ) as first_future_touch_timestamp_ms,
                case
                    when books.side = 'BUY' then min(future_book.best_ask)
                    when books.side = 'SELL' then max(future_book.best_bid)
                    else null
                end as best_future_touch_price
            from market_fillability_signal_books books
            left join market_fillability_candidate_books future_book
              on future_book.market_id = books.market_id
             and future_book.asset_id = books.asset_id
             and future_book.event_timestamp_ms > books.signal_timestamp_ms
             and future_book.event_timestamp_ms
                <= books.signal_timestamp_ms + {config.max_future_window_ms}
            group by books.signal_id, books.side
            """
        )
        conn.execute(
            """
            create or replace view market_fillability_signal_outcomes as
            select
                books.*,
                coalesce(reports.raw_observed_report_rows, 0) as raw_observed_report_rows,
                reports.observed_order_id,
                reports.observed_terminal_status,
                reports.observed_filled_price,
                coalesce(reports.observed_filled_size, 0) as observed_filled_size,
                coalesce(reports.observed_order_id, '') like 'dry-run-%' as dry_run_created,
                coalesce(reports.observed_order_id, '') like 'dry-run-%'
                    and coalesce(reports.observed_filled_size, 0) > 0 as dry_run_filled,
                case
                    when reports.observed_event_timestamp_ms is not null
                    then reports.observed_event_timestamp_ms - books.signal_timestamp_ms
                    else null
                end as dry_run_terminal_latency_ms,
                coalesce(future.future_book_snapshots, 0) as future_book_snapshots,
                coalesce(future.future_touches, 0) as future_touches,
                future.first_future_book_timestamp_ms,
                future.first_future_touch_timestamp_ms,
                future.best_future_touch_price,
                coalesce(future.future_touches, 0) > 0 as future_touched_limit,
                case
                    when future.best_future_touch_price is null then null
                    when books.side = 'BUY' then greatest(future.best_future_touch_price - books.signal_price, 0)
                    when books.side = 'SELL' then greatest(books.signal_price - future.best_future_touch_price, 0)
                    else null
                end as required_quote_move,
                coalesce(future.future_touches, 0) > 0 as synthetic_filled,
                case
                    when coalesce(reports.observed_filled_size, 0) > 0 then 'observed_fill'
                    when coalesce(future.future_touches, 0) > 0 then 'synthetic_only'
                    when coalesce(reports.observed_order_id, '') like 'dry-run-%' then 'dry_run_unfilled'
                    else 'neither'
                end as execution_path
            from market_fillability_signal_books books
            left join market_fillability_observed_reports reports
              on reports.signal_id = books.signal_id
            left join market_fillability_future_books future
              on future.signal_id = books.signal_id
            """
        )
        conn.execute(
            """
            create or replace view market_fillability_signal_features as
            select
                market_id,
                asset_id,
                count(*) as signals,
                sum(case when dry_run_created then 1 else 0 end) as dry_run_signal_lifecycles,
                sum(case when dry_run_filled then 1 else 0 end) as dry_run_filled_signals,
                sum(case when synthetic_filled then 1 else 0 end) as synthetic_filled_signals,
                sum(case when execution_path = 'synthetic_only' then 1 else 0 end) as synthetic_only_signals,
                sum(case when execution_path = 'neither' then 1 else 0 end) as neither_signals,
                case
                    when count(*) > 0
                    then sum(case when dry_run_filled then 1 else 0 end)::double / count(*)
                    else 0
                end as observed_fill_rate,
                case
                    when count(*) > 0
                    then sum(case when synthetic_filled then 1 else 0 end)::double / count(*)
                    else 0
                end as synthetic_fill_rate,
                case
                    when count(*) > 0
                    then sum(case when execution_path = 'synthetic_only' then 1 else 0 end)::double
                        / count(*)
                    else 0
                end as synthetic_only_rate,
                avg(case when future_touched_limit then 1.0 else 0.0 end) as future_touch_rate,
                avg(case when quote_relation = 'inside_spread' then 1.0 else 0.0 end) as inside_spread_rate,
                avg(case when quote_relation = 'behind_touch' then 1.0 else 0.0 end) as behind_touch_rate,
                avg(spread) as avg_spread_at_signal,
                avg(distance_to_touch) as avg_distance_to_touch,
                avg(required_quote_move) as avg_required_quote_move,
                avg(book_age_ms) as avg_book_age_ms
            from market_fillability_signal_outcomes
            group by market_id, asset_id
            """
        )
        conn.execute(
            f"""
            create or replace view market_fillability_opportunity_snapshots as
            select
                market_id,
                asset_id,
                event_timestamp_ms,
                best_bid,
                best_ask,
                spread,
                bid_depth,
                ask_depth,
                event_timestamp_ms - lag(event_timestamp_ms) over (
                    partition by market_id, asset_id
                    order by event_timestamp_ms
                ) as snapshot_gap_ms,
                spread >= {opportunity_config.min_spread}
                    and spread <= {opportunity_config.max_spread}
                    and best_bid is not null
                    and best_ask is not null
                    and bid_depth > 0
                    and ask_depth > 0 as is_spread_opportunity
            from market_fillability_candidate_books
            where event_timestamp_ms is not null
            """
        )
        conn.execute(
            f"""
            create or replace view market_fillability_opportunity_ranking as
            with summary as (
                select
                    market_id,
                    asset_id,
                    count(*) as snapshots,
                    sum(case when is_spread_opportunity then 1 else 0 end)
                        as opportunity_snapshots,
                    avg(case when is_spread_opportunity then spread else null end)
                        as avg_opportunity_spread,
                    avg(spread) as avg_spread,
                    avg(bid_depth + ask_depth) as avg_total_depth,
                    coalesce(
                        avg(
                            case
                                when coalesce(snapshot_gap_ms, 0)
                                    > {opportunity_config.stale_gap_ms}
                                then 1.0
                                else 0.0
                            end
                        ),
                        0
                    ) as stale_rate,
                    case
                        when count(*) > 0 then
                            sum(case when is_spread_opportunity then 1 else 0 end)::double
                            / count(*)
                        else 0
                    end as spread_opportunity_density
                from market_fillability_opportunity_snapshots
                group by market_id, asset_id
            ),
            latest_metadata as (
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
            ),
            eligible as (
                select
                    summary.*,
                    coalesce(metadata.liquidity, 0) as liquidity,
                    coalesce(metadata.volume, 0) as volume,
                    (
                        summary.spread_opportunity_density * 100
                        + least(coalesce(metadata.liquidity, 0), 100000) / 100000
                        + least(coalesce(metadata.volume, 0), 100000) / 200000
                        - summary.stale_rate * 10
                    ) as opportunity_score
                from summary
                left join latest_metadata metadata
                  on metadata.market_id = summary.market_id
                 and metadata.asset_id = summary.asset_id
                where summary.snapshots >= {opportunity_config.min_snapshots}
                  and summary.spread_opportunity_density
                    >= {opportunity_config.min_opportunity_density}
                  and coalesce(metadata.liquidity, 0) >= {opportunity_config.min_liquidity}
                  and summary.stale_rate <= {opportunity_config.max_stale_rate}
                  and coalesce(metadata.active, true)
                  and not coalesce(metadata.closed, false)
                  and not coalesce(metadata.archived, false)
                  and coalesce(metadata.enable_order_book, true)
            )
            select
                row_number() over (
                    order by
                        opportunity_score desc,
                        spread_opportunity_density desc,
                        snapshots desc,
                        asset_id
                ) as rank,
                *
            from eligible
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
            full outer join market_fillability_opportunity_ranking opportunity
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
    parser.add_argument("--lookback-ms", type=int, default=3_600_000)
    parser.add_argument("--max-candidate-signals", type=int, default=20_000)
    parser.add_argument("--max-candidate-snapshots", type=int, default=100_000)
    parser.add_argument("--max-future-window-ms", type=int, default=300_000)
    args = parser.parse_args()
    report = create_market_fillability_score_report(
        Path(args.duckdb),
        Path(args.output_dir),
        MarketFillabilityScoreConfig(
            min_signals=args.min_signals,
            min_fillability_score=args.min_fillability_score,
            max_synthetic_gap=args.max_synthetic_gap,
            limit=args.limit,
            lookback_ms=args.lookback_ms,
            max_candidate_signals=args.max_candidate_signals,
            max_candidate_snapshots=args.max_candidate_snapshots,
            max_future_window_ms=args.max_future_window_ms,
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
