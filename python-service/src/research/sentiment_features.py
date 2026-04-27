import json
from pathlib import Path

import duckdb

from src.research.game_theory import duckdb_literal, relation_exists


SENTIMENT_FEATURE_REPORT_VERSION = "sentiment_feature_builder_v1"
SENTIMENT_FEATURE_OUTPUTS = ("sentiment_feature_candidates",)


def create_sentiment_feature_views(db_path: Path, lookback_ms: int = 86_400_000) -> None:
    if lookback_ms <= 0:
        raise ValueError("lookback_ms must be positive")
    with duckdb.connect(str(db_path)) as conn:
        ensure_external_evidence_view(conn)
        conn.execute(
            f"""
            create or replace view sentiment_feature_candidates as
            with candidate_windows as (
                select
                    market_id,
                    asset_id,
                    available_at_ms as window_end_ms
                from external_evidence
                where available_at_ms is not null
                group by market_id, asset_id, available_at_ms
            ),
            window_evidence as (
                select
                    w.market_id,
                    w.asset_id,
                    w.window_end_ms,
                    e.evidence_id,
                    e.source,
                    e.observed_at_ms,
                    e.available_at_ms,
                    e.direction,
                    e.sentiment_score,
                    e.source_quality,
                    e.confidence
                from candidate_windows w
                join external_evidence e
                  on e.market_id = w.market_id
                 and coalesce(e.asset_id, '') = coalesce(w.asset_id, '')
                 and e.available_at_ms <= w.window_end_ms
                 and e.available_at_ms >= w.window_end_ms - {lookback_ms}
            ),
            base_aggregates as (
                select
                    market_id,
                    asset_id,
                    window_end_ms,
                    min(evidence_id) as representative_evidence_id,
                    md5(string_agg(evidence_id, ',' order by evidence_id)) as evidence_ids_hash,
                    count(*) as evidence_count,
                    count(distinct source) as source_count,
                    max(observed_at_ms) as observed_at_ms,
                    max(available_at_ms) as available_at_ms,
                    avg(sentiment_score) as net_sentiment,
                    avg(source_quality) as source_quality,
                    avg(confidence) as confidence
                from window_evidence
                group by market_id, asset_id, window_end_ms
            ),
            aggregates as (
                select
                    b.*,
                    avg(abs(e.sentiment_score - b.net_sentiment)) as sentiment_disagreement
                from base_aggregates b
                join window_evidence e
                  on e.market_id = b.market_id
                 and coalesce(e.asset_id, '') = coalesce(b.asset_id, '')
                 and e.window_end_ms = b.window_end_ms
                group by
                    b.market_id,
                    b.asset_id,
                    b.window_end_ms,
                    b.representative_evidence_id,
                    b.evidence_ids_hash,
                    b.evidence_count,
                    b.source_count,
                    b.observed_at_ms,
                    b.available_at_ms,
                    b.net_sentiment,
                    b.source_quality,
                    b.confidence
            )
            select
                'sentiment-' || market_id || '-' || coalesce(asset_id, 'market') || '-' || window_end_ms::varchar as feature_id,
                representative_evidence_id as evidence_id,
                market_id,
                asset_id,
                observed_at_ms,
                available_at_ms,
                window_end_ms as feature_timestamp_ms,
                case
                    when net_sentiment > 0.05 then 'YES'
                    when net_sentiment < -0.05 then 'NO'
                    when abs(net_sentiment) <= 0.05 then 'NEUTRAL'
                    else 'UNKNOWN'
                end as direction,
                net_sentiment as sentiment_score,
                net_sentiment,
                {lookback_ms} as lookback_ms,
                evidence_count,
                source_count,
                evidence_ids_hash,
                null as sentiment_momentum,
                sentiment_disagreement,
                source_quality,
                confidence,
                'deterministic_sentiment_aggregate_v1' as model_version,
                'external_evidence_v1' as data_version,
                'sentiment_window_features_v1' as feature_version
            from aggregates
            """
        )


def export_sentiment_feature_report(
    db_path: Path,
    output_dir: Path,
    lookback_ms: int = 86_400_000,
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    create_sentiment_feature_views(db_path, lookback_ms=lookback_ms)
    counts: dict[str, int] = {}
    with duckdb.connect(str(db_path)) as conn:
        for view_name in SENTIMENT_FEATURE_OUTPUTS:
            target = output_dir / f"{view_name}.parquet"
            conn.execute(
                f"copy (select * from {view_name}) to '{duckdb_literal(target.as_posix())}' (format parquet)"
            )
            row = conn.execute(f"select count(*) from {view_name}").fetchone()
            counts[view_name] = int(row[0]) if row else 0
    report: dict[str, object] = {
        "report_version": SENTIMENT_FEATURE_REPORT_VERSION,
        "decision_policy": "offline_feature_builder_only",
        "can_execute_trades": False,
        "lookback_ms": lookback_ms,
        "counts": counts,
        "outputs": list(SENTIMENT_FEATURE_OUTPUTS),
    }
    (output_dir / "sentiment_features.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return report


def ensure_external_evidence_view(conn: duckdb.DuckDBPyConnection) -> None:
    if relation_exists(conn, "external_evidence"):
        return
    conn.execute(
        """
        create or replace view external_evidence as
        select
            cast(null as varchar) as evidence_id,
            cast(null as varchar) as source,
            cast(null as varchar) as source_type,
            cast(null as bigint) as published_at_ms,
            cast(null as bigint) as observed_at_ms,
            cast(null as bigint) as available_at_ms,
            cast(null as varchar) as market_id,
            cast(null as varchar) as asset_id,
            cast(null as varchar) as raw_reference_hash,
            cast(null as varchar) as direction,
            cast(null as double) as sentiment_score,
            cast(null as double) as source_quality,
            cast(null as double) as confidence,
            cast(null as varchar) as data_version
        where false
        """
    )


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="research-sentiment-features")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--lookback-ms", type=int, default=86_400_000)
    args = parser.parse_args()

    report = export_sentiment_feature_report(
        Path(args.duckdb),
        Path(args.output_dir),
        lookback_ms=args.lookback_ms,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
