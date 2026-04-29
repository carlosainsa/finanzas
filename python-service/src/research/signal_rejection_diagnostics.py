import json
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd  # type: ignore[import-untyped]

from src.config import settings
from src.ml.predictor import Predictor
from src.schemas import OrderBook


REPORT_VERSION = "signal_rejection_diagnostics_v1"
DEFAULT_PROFILES = ("conservative_v1", "balanced_v1")


@dataclass(frozen=True)
class DiagnosticsConfig:
    profiles: tuple[str, ...] = DEFAULT_PROFILES
    quote_placement: str = "near_touch"
    baseline_profile: str = "conservative_v1"
    candidate_profile: str = "balanced_v1"
    max_snapshots: int = 0


def create_signal_rejection_diagnostics(
    db_path: Path,
    output_dir: Path,
    config: DiagnosticsConfig = DiagnosticsConfig(),
) -> dict[str, object]:
    snapshots = load_orderbook_snapshots(db_path, limit=config.max_snapshots)
    rows: list[dict[str, object]] = []
    original_profile = settings.predictor_strategy_profile
    original_quote_placement = settings.predictor_quote_placement
    try:
        settings.predictor_quote_placement = config.quote_placement
        for profile in config.profiles:
            settings.predictor_strategy_profile = profile
            predictor = Predictor()
            for snapshot in snapshots:
                orderbook = orderbook_from_snapshot(snapshot)
                decision = predictor.evaluate(orderbook)
                rows.append(
                    {
                        "profile": profile,
                        "market_id": orderbook.market_id,
                        "asset_id": orderbook.asset_id,
                        "event_timestamp_ms": orderbook.timestamp_ms,
                        "best_bid": snapshot.get("best_bid"),
                        "best_ask": snapshot.get("best_ask"),
                        "bid_depth": snapshot.get("bid_depth"),
                        "ask_depth": snapshot.get("ask_depth"),
                        "spread": decision.spread,
                        "confidence": decision.confidence,
                        "top_change_count": decision.top_change_count,
                        "accepted": decision.accepted,
                        "rejection_reason": decision.rejection_reason,
                        "model_version": decision.model_version,
                        "feature_version": decision.feature_version,
                    }
                )
    finally:
        settings.predictor_strategy_profile = original_profile
        settings.predictor_quote_placement = original_quote_placement

    output_dir.mkdir(parents=True, exist_ok=True)
    if rows:
        pd.DataFrame(rows).to_parquet(
            output_dir / "signal_rejection_diagnostics.parquet", index=False
        )
    summary = summarize(rows)
    report: dict[str, object] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_diagnostics_only",
        "config": asdict(config),
        "counts": {
            "snapshots": len(snapshots),
            "diagnostic_rows": len(rows),
            "profiles": len(config.profiles),
        },
        "summary": summary,
        "profile_comparison": compare_profiles(
            summary, config.baseline_profile, config.candidate_profile
        ),
        "outputs": ["signal_rejection_diagnostics.parquet"],
    }
    (output_dir / "signal_rejection_diagnostics.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return report


def load_orderbook_snapshots(db_path: Path, limit: int = 0) -> list[dict[str, object]]:
    with duckdb.connect(str(db_path)) as conn:
        ensure_orderbook_snapshots(conn)
        limit_clause = f"limit {int(limit)}" if limit > 0 else ""
        frame = conn.execute(
            f"""
            select
                market_id,
                asset_id,
                event_timestamp_ms,
                best_bid,
                best_ask,
                bid_depth,
                ask_depth
            from orderbook_snapshots
            where event_timestamp_ms is not null
            order by market_id, asset_id, event_timestamp_ms
            {limit_clause}
            """
        ).fetch_df()
    return frame.to_dict(orient="records")


def ensure_orderbook_snapshots(conn: duckdb.DuckDBPyConnection) -> None:
    exists = conn.execute(
        """
        select count(*)
        from information_schema.tables
        where table_name = 'orderbook_snapshots'
        """
    ).fetchone()
    if exists and int(exists[0]) > 0:
        return
    conn.execute(
        """
        create or replace view orderbook_snapshots as
        select
            cast(null as varchar) as market_id,
            cast(null as varchar) as asset_id,
            cast(null as bigint) as event_timestamp_ms,
            cast(null as double) as best_bid,
            cast(null as double) as best_ask,
            cast(null as double) as bid_depth,
            cast(null as double) as ask_depth
        where false
        """
    )


def orderbook_from_snapshot(snapshot: dict[str, object]) -> OrderBook:
    best_bid = numeric(snapshot.get("best_bid"))
    best_ask = numeric(snapshot.get("best_ask"))
    bid_depth = max(0.0, numeric(snapshot.get("bid_depth")) or 0.0)
    ask_depth = max(0.0, numeric(snapshot.get("ask_depth")) or 0.0)
    return OrderBook.model_validate(
        {
            "market_id": str(snapshot.get("market_id") or ""),
            "asset_id": str(snapshot.get("asset_id") or ""),
            "bids": [] if best_bid is None else [{"price": best_bid, "size": bid_depth}],
            "asks": [] if best_ask is None else [{"price": best_ask, "size": ask_depth}],
            "timestamp_ms": int(numeric(snapshot.get("event_timestamp_ms")) or 0),
        }
    )


def summarize(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    by_profile: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        by_profile.setdefault(str(row["profile"]), []).append(row)
    summary: list[dict[str, object]] = []
    for profile, profile_rows in sorted(by_profile.items()):
        reason_counts = Counter(str(row["rejection_reason"]) for row in profile_rows)
        accepted = int(reason_counts.get("accepted", 0))
        snapshots = len(profile_rows)
        summary.append(
            {
                "profile": profile,
                "snapshots": snapshots,
                "accepted": accepted,
                "acceptance_rate": accepted / snapshots if snapshots > 0 else None,
                "rejection_counts": dict(sorted(reason_counts.items())),
            }
        )
    return summary


def compare_profiles(
    summary: list[dict[str, object]],
    baseline_profile: str,
    candidate_profile: str,
) -> dict[str, object]:
    baseline = summary_by_profile(summary, baseline_profile)
    candidate = summary_by_profile(summary, candidate_profile)
    if not baseline or not candidate:
        return {
            "status": "missing_profile",
            "baseline_profile": baseline_profile,
            "candidate_profile": candidate_profile,
        }
    baseline_reasons = typed_dict(baseline.get("rejection_counts"))
    candidate_reasons = typed_dict(candidate.get("rejection_counts"))
    reason_deltas: list[dict[str, object]] = []
    for reason in sorted(set(baseline_reasons) | set(candidate_reasons)):
        baseline_count = int_value(baseline_reasons.get(reason))
        candidate_count = int_value(candidate_reasons.get(reason))
        reason_deltas.append(
            {
                "rejection_reason": reason,
                "baseline": baseline_count,
                "candidate": candidate_count,
                "delta": candidate_count - baseline_count,
            }
        )
    accepted_delta = int_value(candidate.get("accepted")) - int_value(
        baseline.get("accepted")
    )
    primary_gap = None
    if accepted_delta < 0:
        worse_rejections = [
            row
            for row in reason_deltas
            if row["rejection_reason"] != "accepted" and int_value(row["delta"]) > 0
        ]
        if worse_rejections:
            primary_gap = max(worse_rejections, key=lambda row: int_value(row["delta"]))[
                "rejection_reason"
            ]
    return {
        "status": "compared",
        "baseline_profile": baseline_profile,
        "candidate_profile": candidate_profile,
        "accepted_delta": accepted_delta,
        "candidate_less_active": accepted_delta < 0,
        "primary_gap_reason": primary_gap,
        "rejection_reason_deltas": reason_deltas,
    }


def summary_by_profile(
    summary: list[dict[str, object]], profile: str
) -> dict[str, object] | None:
    for row in summary:
        if row.get("profile") == profile:
            return row
    return None


def numeric(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and pd.notna(value):
        return float(value)
    return None


def int_value(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)) and pd.notna(value):
        return int(value)
    return 0


def typed_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def parse_profiles(value: str) -> tuple[str, ...]:
    profiles = tuple(item.strip() for item in value.split(",") if item.strip())
    return profiles or DEFAULT_PROFILES


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="signal-rejection-diagnostics")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--profiles",
        default=",".join(DEFAULT_PROFILES),
        help="comma-separated predictor profiles to evaluate",
    )
    parser.add_argument("--quote-placement", default=settings.predictor_quote_placement)
    parser.add_argument("--baseline-profile", default="conservative_v1")
    parser.add_argument("--candidate-profile", default="balanced_v1")
    parser.add_argument("--max-snapshots", type=int, default=0)
    args = parser.parse_args()

    report = create_signal_rejection_diagnostics(
        Path(args.duckdb),
        Path(args.output_dir),
        DiagnosticsConfig(
            profiles=parse_profiles(args.profiles),
            quote_placement=args.quote_placement,
            baseline_profile=args.baseline_profile,
            candidate_profile=args.candidate_profile,
            max_snapshots=args.max_snapshots,
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
