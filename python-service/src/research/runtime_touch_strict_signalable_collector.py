import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPORT_VERSION = "runtime_touch_strict_signalable_collector_v1"


def create_strict_signalable_collector_summary(
    ladder_reports: list[Path],
    *,
    output_path: Path | None = None,
    max_windows: int | None = None,
    min_assets: int = 2,
) -> dict[str, Any]:
    if min_assets < 2:
        raise ValueError("min_assets must be >= 2")
    windows = [
        summarize_ladder_report(index=index, ladder_path=path, min_assets=min_assets)
        for index, path in enumerate(ladder_reports, start=1)
    ]
    selected_window = next(
        (window for window in windows if window["strict_ready"] is True),
        None,
    )
    status = "ready" if selected_window else "collect_more_windows"
    next_action = (
        "RUN_STRICT_RUNTIME_TOUCH_AB"
        if selected_window
        else "EXPAND_MARKET_DISCOVERY_OR_CHANGE_TIME_WINDOW"
    )
    summary: dict[str, Any] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "can_promote_live": False,
        "decision_policy": "runtime_touch_strict_signalable_collection_only",
        "status": status,
        "next_action": next_action,
        "min_assets": min_assets,
        "max_windows": max_windows,
        "windows_evaluated": len(windows),
        "selected_window": selected_window,
        "recommended_next_run": (
            selected_window.get("next_run") if selected_window else None
        ),
        "blockers": collect_blockers(windows, min_assets=min_assets),
        "windows": windows,
    }
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return summary


def summarize_ladder_report(
    *,
    index: int,
    ladder_path: Path,
    min_assets: int,
) -> dict[str, Any]:
    report = read_json_object(ladder_path)
    strict_attempt = object_value(report.get("strict_attempt")) or find_attempt(
        report, "strict_signalable"
    )
    selected_attempt = object_value(report.get("selected_attempt"))
    selected_label = selected_attempt.get("label")
    strict_count = int_value(strict_attempt.get("market_asset_ids_count"))
    route_result = str(report.get("route_result") or infer_route_result(report))
    strict_ready = (
        route_result == "READY_FOR_STRICT_AB_RETRY"
        and selected_label == "strict_signalable"
        and strict_count >= min_assets
    )
    return {
        "window_index": index,
        "ladder_report": str(ladder_path),
        "route_result": route_result,
        "recommendation": report.get("recommendation"),
        "strict_ready": strict_ready,
        "strict_status": strict_attempt.get("status"),
        "strict_asset_count": strict_count,
        "strict_selection_reason": strict_attempt.get("selection_reason"),
        "selected_attempt_label": selected_label,
        "selected_asset_count": int_value(
            selected_attempt.get("market_asset_ids_count")
        ),
        "requires_allow_gate_bypass": bool(
            report.get("requires_allow_gate_bypass", False)
        ),
        "next_run": report.get("next_run") if strict_ready else None,
    }


def collect_blockers(windows: list[dict[str, Any]], *, min_assets: int) -> list[str]:
    if not windows:
        return ["no_windows_evaluated"]
    if any(window["strict_ready"] is True for window in windows):
        return []
    blockers: list[str] = []
    if all(int_value(window.get("strict_asset_count")) < min_assets for window in windows):
        blockers.append("strict_signalable_assets_below_minimum")
    if any(
        window.get("route_result") == "DEPTH_STRICT_UNIVERSE_TOO_NARROW"
        for window in windows
    ):
        blockers.append("depth_strict_universe_too_narrow")
    if any(window.get("selected_attempt_label") for window in windows):
        blockers.append("non_strict_attempt_selected_only")
    return blockers or ["strict_signalable_not_ready"]


def infer_route_result(report: dict[str, Any]) -> str:
    selected_attempt = object_value(report.get("selected_attempt"))
    selected_label = selected_attempt.get("label")
    if selected_label == "strict_signalable":
        return "READY_FOR_STRICT_AB_RETRY"
    if selected_label:
        return "DEPTH_STRICT_UNIVERSE_TOO_NARROW"
    return "NO_RUNTIME_TOUCH_AB_ATTEMPT_READY"


def find_attempt(report: dict[str, Any], label: str) -> dict[str, Any]:
    attempts = report.get("attempts")
    if not isinstance(attempts, list):
        return {}
    for attempt in attempts:
        if isinstance(attempt, dict) and attempt.get("label") == label:
            return attempt
    return {}


def read_json_object(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"expected JSON object: {path}")
    return data


def object_value(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def int_value(value: Any) -> int:
    if value is None or value == "":
        return 0
    return int(value)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ladder-report", action="append", default=[])
    parser.add_argument("--output", required=True)
    parser.add_argument("--max-windows", type=int)
    parser.add_argument("--min-assets", type=int, default=2)
    args = parser.parse_args()
    report_paths = [Path(item) for item in args.ladder_report]
    summary = create_strict_signalable_collector_summary(
        report_paths,
        output_path=Path(args.output),
        max_windows=args.max_windows,
        min_assets=args.min_assets,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
