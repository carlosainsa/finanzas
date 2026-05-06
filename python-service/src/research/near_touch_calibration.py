import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from collections.abc import Sequence
from typing import Iterable

import duckdb
import pandas as pd  # type: ignore[import-untyped]

from src.config import settings
from src.ml.predictor import Predictor
from src.research.backtest import duckdb_literal
from src.research.signal_rejection_diagnostics import (
    load_orderbook_snapshots,
    orderbook_from_snapshot,
)


REPORT_VERSION = "near_touch_fraction_calibration_v1"
DEFAULT_GRID = (0.60, 0.65, 0.70, 0.75, 0.80, 0.85)


@dataclass(frozen=True)
class NearTouchCalibrationConfig:
    fractions: tuple[float, ...] = DEFAULT_GRID
    profile: str = "execution_probe_v4"
    max_future_window_ms: int = 300_000
    min_signals: int = 50
    min_adjusted_synthetic_fill_rate: float = 0.02
    max_adjusted_synthetic_fill_rate: float = 0.15
    max_raw_synthetic_fill_rate: float = 0.50
    synthetic_only_weight: float = 0.25
    max_snapshots: int = 0
    min_market_coverage: int = 1

    def __post_init__(self) -> None:
        if not self.fractions:
            raise ValueError("fractions cannot be empty")
        if any(value < 0 or value > 1 for value in self.fractions):
            raise ValueError("fractions must be between 0 and 1")
        if self.max_future_window_ms <= 0:
            raise ValueError("max_future_window_ms must be positive")
        if self.min_signals < 0:
            raise ValueError("min_signals must be non-negative")
        if self.synthetic_only_weight < 0 or self.synthetic_only_weight > 1:
            raise ValueError("synthetic_only_weight must be between 0 and 1")
        if self.min_market_coverage <= 0:
            raise ValueError("min_market_coverage must be positive")


@dataclass(frozen=True)
class CalibrationSnapshotSet:
    source_index: int
    source_db: Path
    snapshots: list[dict[str, object]]


def create_near_touch_calibration_report(
    db_path: Path | Sequence[Path],
    output_dir: Path,
    config: NearTouchCalibrationConfig = NearTouchCalibrationConfig(),
) -> dict[str, object]:
    db_paths = normalize_db_paths(db_path)
    snapshot_sets = [
        CalibrationSnapshotSet(
            source_index=index,
            source_db=path,
            snapshots=load_orderbook_snapshots(path, limit=config.max_snapshots),
        )
        for index, path in enumerate(db_paths)
    ]
    rows: list[dict[str, object]] = []
    original_profile = settings.predictor_strategy_profile
    original_quote_placement = settings.predictor_quote_placement
    original_fraction = settings.predictor_execution_probe_v4_near_touch_max_spread_fraction
    try:
        settings.predictor_strategy_profile = config.profile
        settings.predictor_quote_placement = "near_touch"
        for fraction in config.fractions:
            settings.predictor_execution_probe_v4_near_touch_max_spread_fraction = fraction
            predictor = Predictor()
            for snapshot_set in snapshot_sets:
                snapshots = snapshot_set.snapshots
                for index, snapshot in enumerate(snapshots):
                    orderbook = orderbook_from_snapshot(snapshot)
                    decision = predictor.evaluate(orderbook)
                    if not decision.accepted or decision.signal is None:
                        continue
                    future = first_future_touch(
                        snapshots,
                        current_index=index,
                        market_id=orderbook.market_id,
                        asset_id=orderbook.asset_id,
                        side=decision.signal.side,
                        price=decision.signal.price,
                        signal_timestamp_ms=orderbook.timestamp_ms,
                        max_future_window_ms=config.max_future_window_ms,
                    )
                    future_touches = int_value(future["future_touches"])
                    rows.append(
                        {
                            "fraction": fraction,
                            "profile": config.profile,
                            "source_index": snapshot_set.source_index,
                            "source_db": str(snapshot_set.source_db),
                            "market_id": orderbook.market_id,
                            "asset_id": orderbook.asset_id,
                            "signal_timestamp_ms": orderbook.timestamp_ms,
                            "best_bid": snapshot.get("best_bid"),
                            "best_ask": snapshot.get("best_ask"),
                            "spread": decision.spread,
                            "confidence": decision.confidence,
                            "top_change_count": decision.top_change_count,
                            "signal_price": decision.signal.price,
                            "future_touches": future_touches,
                            "first_future_touch_timestamp_ms": future[
                                "first_future_touch_timestamp_ms"
                            ],
                            "ms_to_first_future_touch": future[
                                "ms_to_first_future_touch"
                            ],
                            "synthetic_fill": future_touches > 0,
                            "synthetic_evidence_weight": (
                                config.synthetic_only_weight
                                if future_touches > 0
                                else 0.0
                            ),
                        }
                    )
    finally:
        settings.predictor_strategy_profile = original_profile
        settings.predictor_quote_placement = original_quote_placement
        settings.predictor_execution_probe_v4_near_touch_max_spread_fraction = (
            original_fraction
        )

    ranking = rank_fraction_rows(rows, config)
    selected = select_fraction(ranking, config)
    output_dir.mkdir(parents=True, exist_ok=True)
    write_outputs(output_dir, rows, ranking)
    report: dict[str, object] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_near_touch_fraction_calibration_only",
        "config": asdict(config),
        "sources": [str(path) for path in db_paths],
        "counts": {
            "snapshots": sum(len(snapshot_set.snapshots) for snapshot_set in snapshot_sets),
            "candidate_signals": len(rows),
            "fractions": len(config.fractions),
            "sources": len(db_paths),
        },
        "selected_fraction": selected,
        "ranking": ranking,
        "outputs": [
            "near_touch_fraction_candidates.parquet",
            "near_touch_fraction_ranking.parquet",
            "near_touch_fraction_calibration.json",
            "execution_probe_v5_fraction_selection.json",
        ],
    }
    (output_dir / "near_touch_fraction_calibration.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    if selected is not None:
        write_v5_selection(output_dir, selected)
    return report


def normalize_db_paths(db_path: Path | Sequence[Path]) -> tuple[Path, ...]:
    if isinstance(db_path, Path):
        return (db_path,)
    paths = tuple(Path(path) for path in db_path)
    if not paths:
        raise ValueError("at least one DuckDB path is required")
    return paths


def first_future_touch(
    snapshots: list[dict[str, object]],
    *,
    current_index: int,
    market_id: str,
    asset_id: str,
    side: str,
    price: float,
    signal_timestamp_ms: int,
    max_future_window_ms: int,
) -> dict[str, object]:
    future_touches = 0
    first_touch_timestamp_ms: int | None = None
    max_timestamp_ms = signal_timestamp_ms + max_future_window_ms
    for snapshot in snapshots[current_index + 1 :]:
        if snapshot.get("market_id") != market_id or snapshot.get("asset_id") != asset_id:
            continue
        timestamp_ms = int_value(snapshot.get("event_timestamp_ms"))
        if timestamp_ms <= signal_timestamp_ms:
            continue
        if timestamp_ms > max_timestamp_ms:
            break
        best_ask = numeric_or_none(snapshot.get("best_ask"))
        best_bid = numeric_or_none(snapshot.get("best_bid"))
        touched = (
            side == "BUY"
            and best_ask is not None
            and best_ask <= price
        ) or (
            side == "SELL"
            and best_bid is not None
            and best_bid >= price
        )
        if touched:
            future_touches += 1
            if first_touch_timestamp_ms is None:
                first_touch_timestamp_ms = timestamp_ms
    return {
        "future_touches": future_touches,
        "first_future_touch_timestamp_ms": first_touch_timestamp_ms,
        "ms_to_first_future_touch": (
            first_touch_timestamp_ms - signal_timestamp_ms
            if first_touch_timestamp_ms is not None
            else None
        ),
    }


def rank_fraction_rows(
    rows: list[dict[str, object]],
    config: NearTouchCalibrationConfig,
) -> list[dict[str, object]]:
    by_fraction: dict[float, list[dict[str, object]]] = {
        fraction: [] for fraction in config.fractions
    }
    for row in rows:
        by_fraction.setdefault(float_value(row["fraction"]), []).append(row)
    ranking: list[dict[str, object]] = []
    for fraction in config.fractions:
        fraction_rows = by_fraction.get(fraction, [])
        signals = len(fraction_rows)
        synthetic_fills = sum(1 for row in fraction_rows if bool(row["synthetic_fill"]))
        covered_assets = {
            f"{row.get('market_id')}:{row.get('asset_id')}" for row in fraction_rows
        }
        adjusted_synthetic_fill_rate = (
            sum(float_value(row["synthetic_evidence_weight"]) for row in fraction_rows)
            / signals
            if signals
            else 0.0
        )
        synthetic_fill_rate = synthetic_fills / signals if signals else 0.0
        avg_future_touches = (
            sum(int_value(row["future_touches"]) for row in fraction_rows) / signals
            if signals
            else None
        )
        avg_ms_to_first_touch = average(
            row["ms_to_first_future_touch"] for row in fraction_rows
        )
        status, blockers = classify_fraction(
            signals=signals,
            covered_markets=len(covered_assets),
            synthetic_fill_rate=synthetic_fill_rate,
            adjusted_synthetic_fill_rate=adjusted_synthetic_fill_rate,
            config=config,
        )
        ranking.append(
            {
                "fraction": fraction,
                "signals": signals,
                "covered_markets": len(covered_assets),
                "synthetic_filled_signals": synthetic_fills,
                "synthetic_fill_rate": synthetic_fill_rate,
                "adjusted_synthetic_fill_rate": adjusted_synthetic_fill_rate,
                "adjusted_fill_rate_gap": adjusted_synthetic_fill_rate,
                "avg_future_touches": avg_future_touches,
                "avg_ms_to_first_future_touch": avg_ms_to_first_touch,
                "status": status,
                "blockers": blockers,
            }
        )
    return ranking


def classify_fraction(
    *,
    signals: int,
    covered_markets: int,
    synthetic_fill_rate: float,
    adjusted_synthetic_fill_rate: float,
    config: NearTouchCalibrationConfig,
) -> tuple[str, list[str]]:
    blockers: list[str] = []
    if signals < config.min_signals:
        blockers.append("insufficient_activity")
    if covered_markets < config.min_market_coverage:
        blockers.append("insufficient_market_coverage")
    if adjusted_synthetic_fill_rate < config.min_adjusted_synthetic_fill_rate:
        blockers.append("too_passive")
    if adjusted_synthetic_fill_rate > config.max_adjusted_synthetic_fill_rate:
        blockers.append("synthetic_optimism_risk")
    if synthetic_fill_rate > config.max_raw_synthetic_fill_rate:
        blockers.append("raw_synthetic_touch_risk")
    return ("candidate" if not blockers else "rejected", blockers)


def select_fraction(
    ranking: list[dict[str, object]],
    config: NearTouchCalibrationConfig,
) -> dict[str, object] | None:
    candidates = [row for row in ranking if row["status"] == "candidate"]
    if candidates:
        return {
            **min(candidates, key=lambda row: float_value(row["fraction"])),
            "selection_reason": "lowest_fraction_with_activity_and_bounded_synthetic_gap",
        }
    boundary = [
        row
        for row in ranking
        if int_value(row["signals"]) >= config.min_signals
        and int_value(row["covered_markets"]) >= config.min_market_coverage
        and int_value(row["synthetic_filled_signals"]) > 0
    ]
    if boundary:
        return {
            **min(boundary, key=lambda row: float_value(row["fraction"])),
            "selection_reason": "lowest_fraction_with_future_touches_requires_observation_validation",
        }
    viable_activity = [
        row
        for row in ranking
        if int_value(row["signals"]) >= config.min_signals
        and int_value(row["covered_markets"]) >= config.min_market_coverage
        and float_value(row["adjusted_synthetic_fill_rate"])
        <= config.max_adjusted_synthetic_fill_rate
        and float_value(row["synthetic_fill_rate"]) <= config.max_raw_synthetic_fill_rate
    ]
    if viable_activity:
        return {
            **max(
                viable_activity,
                key=lambda row: float_value(row["adjusted_synthetic_fill_rate"]),
            ),
            "selection_reason": "best_available_below_synthetic_risk_bounds",
        }
    return None


def write_outputs(
    output_dir: Path,
    rows: list[dict[str, object]],
    ranking: list[dict[str, object]],
) -> None:
    with duckdb.connect() as conn:
        candidates_path = output_dir / "near_touch_fraction_candidates.parquet"
        ranking_path = output_dir / "near_touch_fraction_ranking.parquet"
        pd.DataFrame(rows).to_parquet(candidates_path, index=False)
        pd.DataFrame(ranking).to_parquet(ranking_path, index=False)
        conn.execute(
            f"select count(*) from read_parquet('{duckdb_literal(candidates_path.as_posix())}')"
        )
        conn.execute(
            f"select count(*) from read_parquet('{duckdb_literal(ranking_path.as_posix())}')"
        )


def write_v5_selection(output_dir: Path, selected: dict[str, object]) -> None:
    payload = {
        "version": "execution_probe_fraction_selection_v1",
        "profile": "execution_probe_v5",
        "near_touch_max_spread_fraction": selected["fraction"],
        "decision_policy": "offline_fraction_selection_only",
        "can_execute_trades": False,
        "selection_reason": selected.get("selection_reason"),
        "source_report": str(output_dir / "near_touch_fraction_calibration.json"),
        "selected_metrics": selected,
    }
    (output_dir / "execution_probe_v5_fraction_selection.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def average(values: Iterable[object]) -> float | None:
    numeric_values = [float(value) for value in values if isinstance(value, (int, float))]
    return sum(numeric_values) / len(numeric_values) if numeric_values else None


def numeric_or_none(value: object) -> float | None:
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return None


def float_value(value: object) -> float:
    numeric = numeric_or_none(value)
    return numeric if numeric is not None else 0.0


def int_value(value: object) -> int:
    numeric = numeric_or_none(value)
    return int(numeric) if numeric is not None else 0


def parse_fractions(value: str) -> tuple[float, ...]:
    return tuple(float(item.strip()) for item in value.split(",") if item.strip())


def main() -> int:
    parser = argparse.ArgumentParser(prog="near-touch-calibration")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument(
        "--additional-duckdb",
        action="append",
        default=[],
        help="Additional DuckDB file to aggregate into the same offline calibration.",
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fractions", default=",".join(str(item) for item in DEFAULT_GRID))
    parser.add_argument("--profile", default=NearTouchCalibrationConfig.profile)
    parser.add_argument(
        "--max-future-window-ms",
        type=int,
        default=NearTouchCalibrationConfig.max_future_window_ms,
    )
    parser.add_argument("--min-signals", type=int, default=NearTouchCalibrationConfig.min_signals)
    parser.add_argument(
        "--min-adjusted-synthetic-fill-rate",
        type=float,
        default=NearTouchCalibrationConfig.min_adjusted_synthetic_fill_rate,
    )
    parser.add_argument(
        "--max-adjusted-synthetic-fill-rate",
        type=float,
        default=NearTouchCalibrationConfig.max_adjusted_synthetic_fill_rate,
    )
    parser.add_argument(
        "--max-raw-synthetic-fill-rate",
        type=float,
        default=NearTouchCalibrationConfig.max_raw_synthetic_fill_rate,
    )
    parser.add_argument(
        "--synthetic-only-weight",
        type=float,
        default=NearTouchCalibrationConfig.synthetic_only_weight,
    )
    parser.add_argument("--max-snapshots", type=int, default=0)
    parser.add_argument(
        "--min-market-coverage",
        type=int,
        default=NearTouchCalibrationConfig.min_market_coverage,
    )
    args = parser.parse_args()

    report = create_near_touch_calibration_report(
        tuple(Path(path) for path in [args.duckdb, *args.additional_duckdb]),
        Path(args.output_dir),
        NearTouchCalibrationConfig(
            fractions=parse_fractions(args.fractions),
            profile=args.profile,
            max_future_window_ms=args.max_future_window_ms,
            min_signals=args.min_signals,
            min_adjusted_synthetic_fill_rate=args.min_adjusted_synthetic_fill_rate,
            max_adjusted_synthetic_fill_rate=args.max_adjusted_synthetic_fill_rate,
            max_raw_synthetic_fill_rate=args.max_raw_synthetic_fill_rate,
            synthetic_only_weight=args.synthetic_only_weight,
            max_snapshots=args.max_snapshots,
            min_market_coverage=args.min_market_coverage,
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
