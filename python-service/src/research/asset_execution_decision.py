import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import pandas as pd  # type: ignore[import-untyped]


REPORT_VERSION = "asset_execution_decision_v1"
Decision = Literal["REPEAT_ASSET", "RETUNE_ASSET", "BLOCK_ASSET"]
SEGMENT_KEYS = ("market_id", "asset_id", "side", "strategy", "model_version")


@dataclass(frozen=True)
class AssetExecutionDecisionConfig:
    min_asset_signals: int = 10
    min_repeat_fill_rate: float = 0.01
    max_synthetic_observed_gap: float = 0.05
    max_missing_report_rate: float = 0.05
    max_rejection_rate: float = 0.10
    min_order_creation_rate: float = 0.95
    min_future_touch_rate: float = 0.10
    min_required_quote_move: float = 0.005

    def __post_init__(self) -> None:
        if self.min_asset_signals <= 0:
            raise ValueError("min_asset_signals must be positive")
        for field_name, value in asdict(self).items():
            if field_name == "min_asset_signals":
                continue
            if value < 0:
                raise ValueError(f"{field_name} must be non-negative")


def create_asset_execution_decision_report(
    report_root: Path,
    output_dir: Path | None = None,
    config: AssetExecutionDecisionConfig = AssetExecutionDecisionConfig(),
) -> dict[str, object]:
    resolved_output_dir = output_dir or report_root / "asset_execution_decision"
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    evidence = read_json(report_root / "real_dry_run_evidence.json")
    quote = load_frame(
        report_root / "quote_execution_diagnostics" / "quote_execution_by_market_asset.parquet"
    )
    signal_to_order = load_frame(
        report_root
        / "signal_to_order_conversion"
        / "signal_to_order_by_asset_strategy.parquet"
    )
    promotion = load_frame(
        report_root / "pre_live_promotion" / "pre_live_promotion_segments.parquet"
    )
    blocked = load_frame(
        report_root / "pre_live_promotion" / "pre_live_blocked_segments.parquet"
    )

    evidence_frame = merge_asset_evidence(quote, signal_to_order, promotion, blocked)
    decision_rows = [
        classify_asset(row, config=config)
        for row in evidence_frame.to_dict(orient="records")
    ]
    decisions = pd.DataFrame(decision_rows)
    if decisions.empty:
        decisions = empty_decisions_frame()
    blocked_payload = blocked_segments_payload(decisions)
    summary = summary_payload(decisions)
    report = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "decision_policy": "offline_asset_execution_classification_only",
        "can_execute_trades": False,
        "profile": evidence.get("predictor_strategy_profile"),
        "report_root": str(report_root),
        "config": asdict(config),
        "summary": summary,
        "assets": normalize_records(decisions.to_dict(orient="records")),
        "outputs": [
            "asset_execution_decisions.parquet",
            "asset_execution_evidence.parquet",
            "blocked_asset_segments_candidate.json",
            "asset_execution_decision.json",
        ],
    }

    evidence_frame.to_parquet(
        resolved_output_dir / "asset_execution_evidence.parquet", index=False
    )
    decisions.to_parquet(
        resolved_output_dir / "asset_execution_decisions.parquet", index=False
    )
    write_json_atomic(
        resolved_output_dir / "blocked_asset_segments_candidate.json", blocked_payload
    )
    write_json_atomic(resolved_output_dir / "asset_execution_decision.json", report)
    return report


def merge_asset_evidence(
    quote: pd.DataFrame,
    signal_to_order: pd.DataFrame,
    promotion: pd.DataFrame,
    blocked: pd.DataFrame,
) -> pd.DataFrame:
    frames = [
        normalize_frame(quote),
        normalize_frame(signal_to_order),
        normalize_frame(promotion),
    ]
    merged: pd.DataFrame | None = None
    for frame in frames:
        if frame.empty:
            continue
        if merged is None:
            merged = frame
        else:
            merged = merged.merge(frame, on=list(SEGMENT_KEYS), how="outer")
    if merged is None or merged.empty:
        return empty_evidence_frame()
    blocked_keys = {
        segment_key(row)
        for row in normalize_frame(blocked).to_dict(orient="records")
    }
    merged["pre_live_blocked"] = [
        segment_key(row) in blocked_keys for row in merged.to_dict(orient="records")
    ]
    blocked_reasons = {
        segment_key(row): str(row.get("block_reason") or row.get("reason") or "")
        for row in normalize_frame(blocked).to_dict(orient="records")
    }
    merged["pre_live_block_reason"] = [
        blocked_reasons.get(segment_key(row), "")
        for row in merged.to_dict(orient="records")
    ]
    return merged.fillna(value=pd.NA)


def classify_asset(
    row: dict[str, Any],
    *,
    config: AssetExecutionDecisionConfig,
) -> dict[str, object]:
    reasons: list[str] = []
    next_step = "repeat_same_research_setup"
    signals = numeric_or_none(row.get("signals"))
    quote_signals = numeric_or_none(row.get("signals_quote"))
    order_signals = numeric_or_none(row.get("signals_order"))
    promotion_signals = numeric_or_none(row.get("signals_promotion"))
    resolved_signals = first_number(signals, quote_signals, order_signals, promotion_signals, 0.0)
    filled = first_number(
        numeric_or_none(row.get("dry_run_filled_signals")),
        numeric_or_none(row.get("filled_signals_order")),
        numeric_or_none(row.get("filled_signals_promotion")),
        0.0,
    )
    observed_fill_rate = first_number(
        numeric_or_none(row.get("dry_run_observed_fill_rate")),
        numeric_or_none(row.get("fill_rate")),
        safe_rate(filled, resolved_signals),
        0.0,
    )
    synthetic_fill_rate = numeric_or_none(row.get("synthetic_fill_rate"))
    synthetic_gap = (
        synthetic_fill_rate - observed_fill_rate
        if synthetic_fill_rate is not None
        else None
    )
    future_touch_rate = numeric_or_none(row.get("future_touch_rate"))
    avg_required_quote_move = numeric_or_none(row.get("avg_required_quote_move"))
    missing_report_rate = numeric_or_none(row.get("missing_report_rate"))
    rejection_rate = numeric_or_none(row.get("rejection_rate"))
    error_rate = numeric_or_none(row.get("error_rate"))
    order_creation_rate = numeric_or_none(row.get("order_creation_rate"))
    realized_edge = numeric_or_none(row.get("realized_edge"))
    max_drawdown = numeric_or_none(row.get("max_drawdown"))

    if bool(row.get("pre_live_blocked")):
        decision: Decision = "BLOCK_ASSET"
        primary_reason = str(row.get("pre_live_block_reason") or "pre_live_blocked_segment")
        reasons.append(primary_reason)
        next_step = "exclude_asset_from_next_research_cycle"
    elif hard_quality_failure(
        resolved_signals=resolved_signals,
        missing_report_rate=missing_report_rate,
        rejection_rate=rejection_rate,
        error_rate=error_rate,
        order_creation_rate=order_creation_rate,
        config=config,
    ):
        decision = "RETUNE_ASSET"
        primary_reason = "signal_to_order_quality_gap"
        reasons.extend(
            quality_reasons(
                missing_report_rate=missing_report_rate,
                rejection_rate=rejection_rate,
                error_rate=error_rate,
                order_creation_rate=order_creation_rate,
                config=config,
            )
        )
        next_step = "retune_signal_to_order_pipeline_or_filters"
    elif (
        resolved_signals >= config.min_asset_signals
        and filled <= 0
        and (
            low_or_missing(future_touch_rate, config.min_future_touch_rate)
            or high_or_present(avg_required_quote_move, config.min_required_quote_move)
        )
    ):
        decision = "RETUNE_ASSET"
        primary_reason = "at_touch_orders_unmatched_low_future_touch"
        reasons.append("no_observed_fills")
        if low_or_missing(future_touch_rate, config.min_future_touch_rate):
            reasons.append("future_touch_rate_below_threshold")
        if high_or_present(avg_required_quote_move, config.min_required_quote_move):
            reasons.append("required_quote_move_above_threshold")
        next_step = "retune_market_timing_or_quote_filters"
    elif (
        observed_fill_rate >= config.min_repeat_fill_rate
        and (
            synthetic_gap is None
            or synthetic_gap <= config.max_synthetic_observed_gap
        )
        and (realized_edge is None or realized_edge >= 0)
        and (max_drawdown is None or max_drawdown <= 0)
    ):
        decision = "REPEAT_ASSET"
        primary_reason = "observed_fills_without_synthetic_or_risk_regression"
        reasons.append(primary_reason)
    else:
        decision = "REPEAT_ASSET"
        primary_reason = "insufficient_negative_evidence"
        reasons.append(primary_reason)

    return {
        "market_id": string_value(row.get("market_id")),
        "asset_id": string_value(row.get("asset_id")),
        "side": string_value(row.get("side")),
        "strategy": string_value(row.get("strategy")),
        "model_version": string_value(row.get("model_version")),
        "decision": decision,
        "primary_reason": primary_reason,
        "reasons": reasons,
        "next_step": next_step,
        "metrics": {
            "signals": finite_float(resolved_signals),
            "filled_signals": finite_float(filled),
            "observed_fill_rate": finite_float(observed_fill_rate),
            "synthetic_fill_rate": finite_float(synthetic_fill_rate),
            "synthetic_observed_gap": finite_float(synthetic_gap),
            "future_touch_rate": finite_float(future_touch_rate),
            "avg_required_quote_move": finite_float(avg_required_quote_move),
            "report_rate": finite_float(row.get("report_rate")),
            "order_creation_rate": finite_float(order_creation_rate),
            "missing_report_rate": finite_float(missing_report_rate),
            "rejection_rate": finite_float(rejection_rate),
            "error_rate": finite_float(error_rate),
            "realized_edge": finite_float(realized_edge),
            "max_drawdown": finite_float(max_drawdown),
        },
    }


def hard_quality_failure(
    *,
    resolved_signals: float,
    missing_report_rate: float | None,
    rejection_rate: float | None,
    error_rate: float | None,
    order_creation_rate: float | None,
    config: AssetExecutionDecisionConfig,
) -> bool:
    if resolved_signals < config.min_asset_signals:
        return False
    return bool(
        high_or_present(missing_report_rate, config.max_missing_report_rate)
        or high_or_present(rejection_rate, config.max_rejection_rate)
        or high_or_present(error_rate, 0.0)
        or (
            order_creation_rate is not None
            and order_creation_rate < config.min_order_creation_rate
        )
    )


def quality_reasons(
    *,
    missing_report_rate: float | None,
    rejection_rate: float | None,
    error_rate: float | None,
    order_creation_rate: float | None,
    config: AssetExecutionDecisionConfig,
) -> list[str]:
    reasons: list[str] = []
    if high_or_present(missing_report_rate, config.max_missing_report_rate):
        reasons.append("missing_report_rate_above_threshold")
    if high_or_present(rejection_rate, config.max_rejection_rate):
        reasons.append("rejection_rate_above_threshold")
    if high_or_present(error_rate, 0.0):
        reasons.append("error_rate_above_zero")
    if (
        order_creation_rate is not None
        and order_creation_rate < config.min_order_creation_rate
    ):
        reasons.append("order_creation_rate_below_threshold")
    return reasons


def blocked_segments_payload(decisions: pd.DataFrame) -> dict[str, object]:
    segments = [
        {
            "market_id": row["market_id"],
            "asset_id": row["asset_id"],
            "side": row["side"],
            "strategy": row["strategy"],
            "model_version": row["model_version"],
            "reason": row["primary_reason"],
            "metrics": row["metrics"],
        }
        for row in normalize_records(decisions.to_dict(orient="records"))
        if row.get("decision") == "BLOCK_ASSET"
    ]
    return {
        "version": "blocked_segments_v1",
        "source_report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_apply_live": False,
        "can_execute_trades": False,
        "segments": segments,
    }


def summary_payload(decisions: pd.DataFrame) -> dict[str, object]:
    counts = (
        decisions["decision"].value_counts().to_dict() if "decision" in decisions else {}
    )
    return {
        "assets": int(len(decisions)),
        "repeat_assets": int(counts.get("REPEAT_ASSET", 0)),
        "retune_assets": int(counts.get("RETUNE_ASSET", 0)),
        "block_assets": int(counts.get("BLOCK_ASSET", 0)),
    }


def normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return empty_evidence_frame()
    normalized = frame.copy()
    normalized = normalized.rename(
        columns={
            "signals": (
                "signals_quote"
                if "dry_run_signal_lifecycles" in normalized.columns
                else "signals"
            ),
            "filled_signals": "filled_signals_promotion",
            "fill_rate": "fill_rate_promotion",
        }
    )
    if "reports" in normalized.columns:
        normalized = normalized.rename(columns={"signals": "signals_order"})
    for key in SEGMENT_KEYS:
        if key not in normalized.columns:
            normalized[key] = ""
        normalized[key] = normalized[key].fillna("").astype(str)
    return normalized


def empty_evidence_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(SEGMENT_KEYS))


def empty_decisions_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            *SEGMENT_KEYS,
            "decision",
            "primary_reason",
            "reasons",
            "next_step",
            "metrics",
        ]
    )


def load_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def segment_key(row: dict[str, Any]) -> tuple[str, str, str, str, str]:
    return tuple(string_value(row.get(key)) for key in SEGMENT_KEYS)  # type: ignore[return-value]


def first_number(*values: float | None) -> float:
    for value in values:
        if value is not None:
            return value
    return 0.0


def safe_rate(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return numerator / denominator


def low_or_missing(value: float | None, threshold: float) -> bool:
    return value is None or value < threshold


def high_or_present(value: float | None, threshold: float) -> bool:
    return value is not None and value > threshold


def numeric_or_none(value: object) -> float | None:
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def finite_float(value: object) -> float | None:
    number = numeric_or_none(value)
    return number if number is not None and pd.notna(number) else None


def string_value(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value)


def normalize_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [normalize_value(row) for row in rows]


def normalize_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): normalize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [normalize_value(item) for item in value]
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return normalize_value(value.item())
    return value


def main() -> int:
    parser = argparse.ArgumentParser(prog="asset-execution-decision")
    parser.add_argument("--report-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    report = create_asset_execution_decision_report(
        args.report_root,
        output_dir=args.output_dir,
    )
    if args.json or args.output_dir is None:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
