import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPORT_VERSION = "execution_failure_diagnostics_v1"


def create_execution_failure_diagnostics(report_root: Path) -> dict[str, object]:
    signal_to_order = read_first_json(
        report_root / "signal_to_order_conversion.json",
        report_root / "signal_to_order_conversion" / "signal_to_order_conversion.json",
    )
    quote = read_first_json(
        report_root / "quote_execution_diagnostics.json",
        report_root / "quote_execution_diagnostics" / "quote_execution_diagnostics.json",
    )
    conversion_summary = typed_dict(signal_to_order.get("summary"))
    quote_summary = typed_dict(quote.get("summary"))
    root_causes = list_of_dicts(signal_to_order.get("top_root_causes"))
    quote_no_fill = list_of_dicts(quote.get("no_fill_diagnostics"))

    signals = int_number(conversion_summary.get("signals"))
    reports = int_number(conversion_summary.get("reports"))
    errors = int_number(conversion_summary.get("error_reports"))
    missing_reports = int_number(conversion_summary.get("missing_reports"))
    filled = int_number(conversion_summary.get("filled_signals"))
    partial = int_number(conversion_summary.get("partial_signals"))
    orders_created = int_number(conversion_summary.get("orders_created"))
    unmatched = sum(
        int_number(row.get("signals"))
        for row in root_causes
        if str(row.get("terminal_status") or "").upper() == "UNMATCHED"
        or str(row.get("root_cause") or "") == "order_created_unfilled"
    )

    error_diagnostics = [
        error_row(row)
        for row in root_causes
        if str(row.get("terminal_status") or "").upper() == "ERROR"
        or str(row.get("root_cause") or "") == "executor_reported_error"
        or int_number(row.get("error_reports")) > 0
    ]
    unmatched_diagnostics = [
        unmatched_row(row)
        for row in quote_no_fill
        if str(row.get("root_cause") or "") == "dry_run_created_unmatched"
    ]
    if not unmatched_diagnostics and unmatched > 0:
        unmatched_diagnostics = [
            unmatched_root_cause_row(row)
            for row in root_causes
            if str(row.get("terminal_status") or "").upper() == "UNMATCHED"
            or str(row.get("root_cause") or "") == "order_created_unfilled"
        ]

    report = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_error_vs_unmatched_diagnostics_only",
        "source_reports": {
            "signal_to_order_conversion": bool(signal_to_order),
            "quote_execution_diagnostics": bool(quote),
        },
        "counts": {
            "signals": signals,
            "reports": reports,
            "orders_created": orders_created,
            "errors": errors,
            "unmatched": unmatched,
            "missing_reports": missing_reports,
            "filled": filled,
            "partial": partial,
        },
        "summary": {
            "dominant_failure_family": dominant_failure_family(
                errors=errors,
                unmatched=unmatched,
                missing_reports=missing_reports,
                filled=filled,
                signals=signals,
            ),
            "error_rate": ratio(errors, signals),
            "unmatched_rate": ratio(unmatched, signals),
            "missing_report_rate": ratio(missing_reports, signals),
            "fill_rate": ratio(filled + partial, signals),
            "order_creation_rate": ratio(orders_created, signals),
            "report_rate": ratio(reports, signals),
            "no_fill_future_touch_rate": numeric_or_none(
                quote_summary.get("no_fill_future_touch_rate")
            ),
            "avg_required_quote_move": numeric_or_none(
                quote_summary.get("avg_required_quote_move")
            ),
        },
        "error_diagnostics": error_diagnostics,
        "unmatched_diagnostics": unmatched_diagnostics,
        "recommended_next_action": recommended_next_action(
            signals=signals,
            errors=errors,
            unmatched=unmatched,
            missing_reports=missing_reports,
            filled=filled,
            partial=partial,
            no_fill_future_touch_rate=numeric_or_none(
                quote_summary.get("no_fill_future_touch_rate")
            ),
        ),
        "outputs": ["execution_failure_diagnostics.json"],
    }
    return report


def write_execution_failure_diagnostics(
    report_root: Path,
    output_path: Path | None = None,
) -> dict[str, object]:
    report = create_execution_failure_diagnostics(report_root)
    resolved_output_path = output_path or report_root / "execution_failure_diagnostics.json"
    write_json_atomic(resolved_output_path, report)
    return report


def error_row(row: dict[str, Any]) -> dict[str, object]:
    return {
        "asset_id": row.get("asset_id"),
        "strategy": row.get("strategy"),
        "terminal_status": row.get("terminal_status"),
        "root_cause": row.get("root_cause"),
        "signals": int_number(row.get("signals")),
        "reports": int_number(row.get("reports")),
        "error_reports": int_number(row.get("error_reports")),
        "orders_created": int_number(row.get("orders_created")),
        "avg_report_latency_ms": numeric_or_none(row.get("avg_report_latency_ms")),
    }


def unmatched_row(row: dict[str, Any]) -> dict[str, object]:
    return {
        "market_id": row.get("market_id"),
        "asset_id": row.get("asset_id"),
        "side": row.get("side"),
        "strategy": row.get("strategy"),
        "model_version": row.get("model_version"),
        "feature_version": row.get("feature_version"),
        "quote_relation": row.get("quote_relation"),
        "root_cause": row.get("root_cause"),
        "signals": int_number(row.get("signals")),
        "avg_distance_to_touch": numeric_or_none(row.get("avg_distance_to_touch")),
        "avg_required_quote_move": numeric_or_none(row.get("avg_required_quote_move")),
        "future_touch_rate": numeric_or_none(row.get("future_touch_rate")),
    }


def unmatched_root_cause_row(row: dict[str, Any]) -> dict[str, object]:
    return {
        "asset_id": row.get("asset_id"),
        "strategy": row.get("strategy"),
        "terminal_status": row.get("terminal_status"),
        "root_cause": row.get("root_cause"),
        "signals": int_number(row.get("signals")),
        "orders_created": int_number(row.get("orders_created")),
        "avg_report_latency_ms": numeric_or_none(row.get("avg_report_latency_ms")),
    }


def dominant_failure_family(
    *,
    errors: int,
    unmatched: int,
    missing_reports: int,
    filled: int,
    signals: int,
) -> str:
    if signals <= 0:
        return "NO_SIGNALS"
    families = {
        "ERROR": errors,
        "UNMATCHED": unmatched,
        "MISSING_REPORT": missing_reports,
        "FILLED": filled,
    }
    return max(families.items(), key=lambda item: item[1])[0]


def recommended_next_action(
    *,
    signals: int,
    errors: int,
    unmatched: int,
    missing_reports: int,
    filled: int,
    partial: int,
    no_fill_future_touch_rate: float | None,
) -> str:
    if signals <= 0:
        return "COLLECT_RUNTIME_SAMPLE"
    if missing_reports > max(errors, unmatched, filled + partial):
        return "FIX_REPORTING_OR_RECONCILIATION"
    if errors > 0 and errors >= unmatched:
        return "FIX_RISK_OR_EXECUTOR_ERRORS"
    if unmatched > 0:
        if no_fill_future_touch_rate is not None and no_fill_future_touch_rate <= 0:
            return "CHANGE_MARKET_OR_TIMING_FILTERS"
        return "TUNE_QUOTE_AGGRESSION"
    if filled + partial > 0:
        return "EVALUATE_RISK_METRICS"
    return "COLLECT_MORE_SAMPLE"


def ratio(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator


def int_number(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    return 0


def numeric_or_none(value: object) -> float | None:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return None


def typed_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def list_of_dicts(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def read_first_json(*paths: Path) -> dict[str, object]:
    for path in paths:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(value, dict):
            return value
    return {}


def write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Separate execution ERROR diagnostics from UNMATCHED quote diagnostics."
    )
    parser.add_argument("--report-root", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report = write_execution_failure_diagnostics(args.report_root, args.output)
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
