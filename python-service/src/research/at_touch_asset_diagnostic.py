import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd  # type: ignore[import-untyped]


REPORT_VERSION = "at_touch_asset_diagnostic_v1"


@dataclass(frozen=True)
class AtTouchAssetDiagnosticConfig:
    min_asset_signals: int = 10
    min_future_touch_rate: float = 0.05
    max_required_quote_move: float = 0.005
    stale_future_touch_rate: float = 0.01
    stale_inside_spread_rate: float = 0.80

    def __post_init__(self) -> None:
        if self.min_asset_signals <= 0:
            raise ValueError("min_asset_signals must be positive")
        if not 0 <= self.min_future_touch_rate <= 1:
            raise ValueError("min_future_touch_rate must be between 0 and 1")
        if self.max_required_quote_move < 0:
            raise ValueError("max_required_quote_move must be non-negative")
        if not 0 <= self.stale_future_touch_rate <= 1:
            raise ValueError("stale_future_touch_rate must be between 0 and 1")
        if not 0 <= self.stale_inside_spread_rate <= 1:
            raise ValueError("stale_inside_spread_rate must be between 0 and 1")


def create_at_touch_asset_diagnostic_report(
    report_root: Path,
    output_dir: Path | None = None,
    asset_ids: list[str] | None = None,
    config: AtTouchAssetDiagnosticConfig = AtTouchAssetDiagnosticConfig(),
) -> dict[str, object]:
    resolved_output_dir = output_dir or report_root / "at_touch_asset_diagnostic"
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    selected_asset_ids = asset_ids or asset_ids_from_evidence(report_root)
    by_asset = load_frame(
        report_root
        / "quote_execution_diagnostics"
        / "quote_execution_by_market_asset.parquet"
    )
    no_fill = load_frame(
        report_root
        / "quote_execution_diagnostics"
        / "quote_execution_no_fill_diagnostics.parquet"
    )
    by_asset = filter_assets(by_asset, selected_asset_ids)
    no_fill = filter_assets(no_fill, selected_asset_ids)
    diagnostic = build_asset_diagnostics(by_asset, no_fill, config)
    no_fill_examples = top_no_fill_examples(no_fill)
    summary = summary_payload(diagnostic)
    report: dict[str, object] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_at_touch_asset_diagnostic_only",
        "report_root": str(report_root),
        "config": asdict(config),
        "asset_filter": selected_asset_ids,
        "summary": summary,
        "assets": normalize_records(diagnostic.to_dict(orient="records")),
        "outputs": [
            "at_touch_asset_diagnostic.parquet",
            "at_touch_no_fill_examples.parquet",
            "at_touch_asset_diagnostic.json",
        ],
    }
    diagnostic.to_parquet(
        resolved_output_dir / "at_touch_asset_diagnostic.parquet", index=False
    )
    no_fill_examples.to_parquet(
        resolved_output_dir / "at_touch_no_fill_examples.parquet", index=False
    )
    write_json(resolved_output_dir / "at_touch_asset_diagnostic.json", report)
    return report


def asset_ids_from_evidence(report_root: Path) -> list[str]:
    evidence = read_json(report_root / "real_dry_run_evidence.json")
    direct_ids = evidence.get("market_asset_ids")
    if isinstance(direct_ids, list):
        return [str(item) for item in direct_ids if item]
    universe_path = evidence.get("execution_probe_universe_selection_path")
    if not universe_path:
        return []
    universe = read_json(Path(str(universe_path)))
    universe_ids = universe.get("market_asset_ids")
    if isinstance(universe_ids, list):
        return [str(item) for item in universe_ids if item]
    return []


def build_asset_diagnostics(
    by_asset: pd.DataFrame,
    no_fill: pd.DataFrame,
    config: AtTouchAssetDiagnosticConfig,
) -> pd.DataFrame:
    if by_asset.empty:
        return empty_asset_diagnostics_frame()
    normalized = normalize_numeric_columns(by_asset.copy())
    top_no_fill = no_fill_top_causes(no_fill)
    if not top_no_fill.empty:
        normalized = normalized.merge(
            top_no_fill,
            on=["market_id", "asset_id", "side"],
            how="left",
        )
    for column in ("root_cause_top", "quote_relation_top"):
        if column not in normalized.columns:
            normalized[column] = None
    normalized["observed_fill_rate"] = normalized.apply(observed_fill_rate, axis=1)
    normalized["decision"] = normalized.apply(
        lambda row: classify_asset(row, config), axis=1
    )
    normalized["decision_reason"] = normalized.apply(
        lambda row: decision_reason(row, config), axis=1
    )
    columns = [
        "market_id",
        "asset_id",
        "side",
        "strategy",
        "model_version",
        "signals",
        "dry_run_signal_lifecycles",
        "dry_run_filled_signals",
        "observed_fill_rate",
        "synthetic_filled_signals",
        "at_touch_rate",
        "inside_spread_rate",
        "behind_touch_rate",
        "future_touch_rate",
        "avg_distance_to_touch",
        "avg_required_quote_move",
        "avg_ms_to_first_future_touch",
        "root_cause_top",
        "quote_relation_top",
        "decision",
        "decision_reason",
    ]
    return normalized.reindex(columns=columns)


def normalize_numeric_columns(frame: pd.DataFrame) -> pd.DataFrame:
    for column in (
        "signals",
        "dry_run_signal_lifecycles",
        "dry_run_filled_signals",
        "synthetic_filled_signals",
        "at_touch_rate",
        "inside_spread_rate",
        "behind_touch_rate",
        "future_touch_rate",
        "avg_distance_to_touch",
        "avg_required_quote_move",
        "avg_ms_to_first_future_touch",
    ):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0)
    return frame


def no_fill_top_causes(no_fill: pd.DataFrame) -> pd.DataFrame:
    if no_fill.empty:
        return pd.DataFrame(
            columns=[
                "market_id",
                "asset_id",
                "side",
                "root_cause_top",
                "quote_relation_top",
                "avg_ms_to_first_future_touch",
            ]
        )
    frame = normalize_numeric_columns(no_fill.copy())
    frame["signals"] = pd.to_numeric(frame.get("signals", 0), errors="coerce").fillna(0)
    frame = frame.sort_values(
        ["market_id", "asset_id", "side", "signals"],
        ascending=[True, True, True, False],
    )
    first = frame.groupby(["market_id", "asset_id", "side"], as_index=False).first()
    return first.rename(
        columns={
            "root_cause": "root_cause_top",
            "quote_relation": "quote_relation_top",
        }
    )[
        [
            "market_id",
            "asset_id",
            "side",
            "root_cause_top",
            "quote_relation_top",
            "avg_ms_to_first_future_touch",
        ]
    ]


def top_no_fill_examples(no_fill: pd.DataFrame) -> pd.DataFrame:
    if no_fill.empty:
        return pd.DataFrame()
    frame = normalize_numeric_columns(no_fill.copy())
    if "signals" in frame.columns:
        frame["signals"] = pd.to_numeric(frame["signals"], errors="coerce").fillna(0)
        return frame.sort_values("signals", ascending=False).head(50)
    return frame.head(50)


def observed_fill_rate(row: pd.Series) -> float:
    signals = float(row.get("signals") or 0)
    filled = float(row.get("dry_run_filled_signals") or 0)
    if signals <= 0:
        return 0.0
    return filled / signals


def classify_asset(
    row: pd.Series,
    config: AtTouchAssetDiagnosticConfig,
) -> str:
    signals = float(row.get("signals") or 0)
    fill_rate = float(row.get("observed_fill_rate") or 0)
    future_touch_rate = float(row.get("future_touch_rate") or 0)
    required_quote_move = float(row.get("avg_required_quote_move") or 0)
    inside_spread_rate = float(row.get("inside_spread_rate") or 0)
    if signals < config.min_asset_signals:
        return "NEEDS_SAMPLE"
    if fill_rate > 0:
        return "REPEAT_AT_TOUCH"
    if (
        future_touch_rate >= config.min_future_touch_rate
        and required_quote_move <= config.max_required_quote_move
    ):
        return "RETUNE_TO_AT_TOUCH"
    if (
        future_touch_rate <= config.stale_future_touch_rate
        and inside_spread_rate >= config.stale_inside_spread_rate
    ):
        return "DROP_RUNTIME_STALE"
    return "KEEP_DIAGNOSTIC"


def decision_reason(
    row: pd.Series,
    config: AtTouchAssetDiagnosticConfig,
) -> str:
    decision = classify_asset(row, config)
    if decision == "NEEDS_SAMPLE":
        return "asset_has_insufficient_runtime_signals"
    if decision == "REPEAT_AT_TOUCH":
        return "asset_has_observed_dry_run_fills"
    if decision == "RETUNE_TO_AT_TOUCH":
        return "asset_quote_was_near_future_touch_but_not_aggressive_enough"
    if decision == "DROP_RUNTIME_STALE":
        return "inside_spread_quotes_were_not_touched_by_runtime_market"
    return "asset_needs_more_diagnostic_evidence"


def summary_payload(frame: pd.DataFrame) -> dict[str, object]:
    if frame.empty:
        return {
            "assets": 0,
            "signals": 0,
            "observed_fill_rate": 0.0,
            "future_touch_rate": 0.0,
            "decisions": {},
        }
    decision_counts = frame["decision"].value_counts().to_dict()
    signals = float(frame["signals"].sum())
    filled = float(frame["dry_run_filled_signals"].sum())
    weighted_future_touch = weighted_average(frame, "future_touch_rate", "signals")
    return {
        "assets": int(len(frame)),
        "signals": int(signals),
        "observed_fill_rate": filled / signals if signals else 0.0,
        "future_touch_rate": weighted_future_touch,
        "decisions": {str(key): int(value) for key, value in decision_counts.items()},
    }


def weighted_average(frame: pd.DataFrame, value_column: str, weight_column: str) -> float:
    if frame.empty or value_column not in frame.columns or weight_column not in frame.columns:
        return 0.0
    weights = pd.to_numeric(frame[weight_column], errors="coerce").fillna(0)
    values = pd.to_numeric(frame[value_column], errors="coerce").fillna(0)
    total = float(weights.sum())
    if total <= 0:
        return 0.0
    return float((values * weights).sum() / total)


def filter_assets(frame: pd.DataFrame, asset_ids: list[str]) -> pd.DataFrame:
    if frame.empty or not asset_ids:
        return frame
    return frame[frame["asset_id"].astype(str).isin(set(asset_ids))].copy()


def empty_asset_diagnostics_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "market_id",
            "asset_id",
            "side",
            "strategy",
            "model_version",
            "signals",
            "dry_run_signal_lifecycles",
            "dry_run_filled_signals",
            "observed_fill_rate",
            "synthetic_filled_signals",
            "at_touch_rate",
            "inside_spread_rate",
            "behind_touch_rate",
            "future_touch_rate",
            "avg_distance_to_touch",
            "avg_required_quote_move",
            "avg_ms_to_first_future_touch",
            "root_cause_top",
            "quote_relation_top",
            "decision",
            "decision_reason",
        ]
    )


def load_frame(path: Path) -> pd.DataFrame:
    try:
        return pd.read_parquet(path)
    except (FileNotFoundError, OSError, ValueError):
        return pd.DataFrame()


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


def read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Diagnose whether selected assets need at-touch retuning"
    )
    parser.add_argument("--report-root", required=True)
    parser.add_argument("--output-dir")
    parser.add_argument("--asset-id", action="append", default=[])
    parser.add_argument(
        "--min-asset-signals",
        type=int,
        default=AtTouchAssetDiagnosticConfig.min_asset_signals,
    )
    parser.add_argument(
        "--min-future-touch-rate",
        type=float,
        default=AtTouchAssetDiagnosticConfig.min_future_touch_rate,
    )
    parser.add_argument(
        "--max-required-quote-move",
        type=float,
        default=AtTouchAssetDiagnosticConfig.max_required_quote_move,
    )
    args = parser.parse_args()
    report_root = Path(args.report_root)
    output_dir = Path(args.output_dir) if args.output_dir else None
    report = create_at_touch_asset_diagnostic_report(
        report_root,
        output_dir,
        asset_ids=[str(item) for item in args.asset_id],
        config=AtTouchAssetDiagnosticConfig(
            min_asset_signals=args.min_asset_signals,
            min_future_touch_rate=args.min_future_touch_rate,
            max_required_quote_move=args.max_required_quote_move,
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
