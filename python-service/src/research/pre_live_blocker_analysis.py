import argparse
import json
from datetime import UTC, datetime
from numbers import Real
from pathlib import Path
from typing import Any

import pandas as pd  # type: ignore[import-untyped]

from src.research.pre_live_promotion import BLOCKED_SEGMENTS_VERSION

REPORT_VERSION = "pre_live_blocker_diagnostics_v1"
PROMOTION_SEGMENTS = "pre_live_promotion/pre_live_promotion_segments.parquet"
ADVERSE_SELECTION = "game_theory/adverse_selection_by_strategy.parquet"
GO_NO_GO = "go_no_go.json"


def create_blocker_diagnostics(
    report_root: Path,
    *,
    output_dir: Path | None = None,
    max_candidates: int = 20,
    min_segment_signals: int = 10,
    min_adverse_filled_events: int = 10,
) -> dict[str, object]:
    go_no_go = read_json(report_root / GO_NO_GO)
    config = object_dict(go_no_go.get("config"))
    drawdown_threshold = float_value(config.get("max_drawdown"), 0.10)
    adverse_threshold = float_value(config.get("max_adverse_selection_rate"), 0.40)
    segments = read_parquet(report_root / PROMOTION_SEGMENTS)
    adverse = read_parquet(report_root / ADVERSE_SELECTION)

    drawdown_segments = top_drawdown_segments(
        segments,
        threshold=drawdown_threshold,
        min_signals=min_segment_signals,
        limit=max_candidates,
    )
    adverse_segments = top_adverse_selection_segments(
        segments,
        adverse,
        threshold=adverse_threshold,
        min_filled_events=min_adverse_filled_events,
        limit=max_candidates,
    )
    adverse_candidate_segments = top_adverse_selection_segments(
        segments,
        adverse,
        threshold=adverse_threshold,
        min_filled_events=min_adverse_filled_events,
        limit=max_candidates,
        require_negative_edge=True,
    )
    candidate_segments = merge_candidate_segments(
        drawdown_segments + adverse_candidate_segments,
        limit=max_candidates,
    )
    blocked_segments = blocked_segments_payload(
        candidate_segments,
        report_root=report_root,
        config={
            "max_drawdown": drawdown_threshold,
            "max_adverse_selection_rate": adverse_threshold,
            "min_segment_signals": min_segment_signals,
            "min_adverse_filled_events": min_adverse_filled_events,
            "max_candidates": max_candidates,
        },
    )
    resolved_output_dir = output_dir or report_root / "blocker_diagnostics"
    blocked_path = resolved_output_dir / "blocked_segments_candidate.json"
    diagnostics_path = resolved_output_dir / "pre_live_blocker_diagnostics.json"
    report = {
        "report_version": REPORT_VERSION,
        "source_report_root": str(report_root),
        "generated_at": datetime.now(UTC).isoformat(),
        "thresholds": {
            "max_drawdown": drawdown_threshold,
            "max_adverse_selection_rate": adverse_threshold,
            "min_segment_signals": min_segment_signals,
            "min_adverse_filled_events": min_adverse_filled_events,
        },
        "go_no_go_decision": go_no_go.get("decision"),
        "go_no_go_blockers": go_no_go.get("blockers")
        if isinstance(go_no_go.get("blockers"), list)
        else [],
        "summary": {
            "drawdown_segments": len(drawdown_segments),
            "adverse_selection_segments": len(adverse_segments),
            "adverse_selection_candidate_segments": len(adverse_candidate_segments),
            "candidate_blocked_segments": len(candidate_segments),
        },
        "top_drawdown_segments": drawdown_segments,
        "top_adverse_selection_segments": adverse_segments,
        "candidate_blocked_segments": candidate_segments,
        "blocked_segments_path": str(blocked_path),
        "next_restricted_run": {
            "command": (
                f"PREDICTOR_BLOCKED_SEGMENTS_PATH={blocked_path} "
                "scripts/run_pre_live_dry_run.sh --duration-seconds 900"
            ),
            "compare_command": (
                "PYTHONPATH=python-service python3 -m src.research.compare_runs "
                f"--baseline-report-root {report_root} "
                "--candidate-report-root <restricted-report-root>"
            ),
        },
        "can_execute_trades": False,
    }
    write_outputs(resolved_output_dir, diagnostics_path, blocked_path, report, blocked_segments)
    return report


def top_drawdown_segments(
    segments: pd.DataFrame,
    *,
    threshold: float,
    min_signals: int,
    limit: int,
) -> list[dict[str, object]]:
    if segments.empty:
        return []
    frame = segments.copy()
    frame["max_drawdown"] = pd.to_numeric(frame.get("max_drawdown"), errors="coerce")
    frame["signals"] = pd.to_numeric(frame.get("signals"), errors="coerce").fillna(0)
    frame = frame[
        (frame["signals"] >= min_signals)
        & (frame["max_drawdown"].fillna(0) > threshold)
    ]
    if frame.empty:
        return []
    frame["diagnostic_score"] = frame["max_drawdown"] - threshold
    return [
        segment_record(row, reason="bounded_drawdown")
        for _, row in frame.sort_values("diagnostic_score", ascending=False)
        .head(limit)
        .iterrows()
    ]


def top_adverse_selection_segments(
    segments: pd.DataFrame,
    adverse: pd.DataFrame,
    *,
    threshold: float,
    min_filled_events: int,
    limit: int,
    require_negative_edge: bool = False,
) -> list[dict[str, object]]:
    if segments.empty or adverse.empty:
        return []
    adverse_frame = adverse.copy()
    adverse_frame["adverse_30s_rate"] = pd.to_numeric(
        adverse_frame.get("adverse_30s_rate"), errors="coerce"
    )
    adverse_frame["filled_events"] = pd.to_numeric(
        adverse_frame.get("filled_events"), errors="coerce"
    ).fillna(0)
    adverse_frame = adverse_frame[
        (adverse_frame["filled_events"] >= min_filled_events)
        & (adverse_frame["adverse_30s_rate"].fillna(0) > threshold)
    ]
    if adverse_frame.empty:
        return []
    merged = segments.merge(
        adverse_frame[
            [
                "market_id",
                "side",
                "strategy",
                "filled_events",
                "adverse_30s_rate",
                "avg_pnl_30s",
            ]
        ],
        on=["market_id", "side", "strategy"],
        how="inner",
    )
    if merged.empty:
        return []
    merged["filled_signals"] = pd.to_numeric(
        merged.get("filled_signals"), errors="coerce"
    ).fillna(0)
    merged = merged[merged["filled_signals"] > 0]
    if require_negative_edge:
        merged["pnl"] = pd.to_numeric(merged.get("pnl"), errors="coerce")
        merged["realized_edge"] = pd.to_numeric(
            merged.get("realized_edge"), errors="coerce"
        )
        merged = merged[
            (merged["pnl"].fillna(0) < 0)
            | (merged["realized_edge"].fillna(0) < 0)
        ]
    if merged.empty:
        return []
    merged["diagnostic_score"] = merged["adverse_30s_rate"] - threshold
    return [
        segment_record(row, reason="adverse_selection")
        for _, row in merged.sort_values(
            ["diagnostic_score", "filled_events"], ascending=False
        )
        .head(limit)
        .iterrows()
    ]


def segment_record(row: pd.Series, reason: str) -> dict[str, object]:
    return {
        "market_id": str(row.get("market_id") or ""),
        "asset_id": str(row.get("asset_id") or ""),
        "side": str(row.get("side") or ""),
        "strategy": str(row.get("strategy") or ""),
        "model_version": str(row.get("model_version") or ""),
        "reason": reason,
        "metrics": {
            "signals": finite_float(row.get("signals")),
            "filled_signals": finite_float(row.get("filled_signals")),
            "fill_rate": finite_float(row.get("fill_rate")),
            "realized_edge": finite_float(row.get("realized_edge")),
            "pnl": finite_float(row.get("pnl")),
            "max_drawdown": finite_float(row.get("max_drawdown")),
            "drawdown_per_signal": finite_float(row.get("drawdown_per_signal")),
            "adverse_30s_rate": finite_float(row.get("adverse_30s_rate")),
            "adverse_filled_events": finite_float(row.get("filled_events")),
            "avg_pnl_30s": finite_float(row.get("avg_pnl_30s")),
            "diagnostic_score": finite_float(row.get("diagnostic_score")),
        },
    }


def merge_candidate_segments(
    segments: list[dict[str, object]], *, limit: int
) -> list[dict[str, object]]:
    merged: dict[tuple[str, str, str, str], dict[str, object]] = {}
    for segment in segments:
        key = (
            str(segment.get("market_id") or ""),
            str(segment.get("asset_id") or ""),
            str(segment.get("side") or ""),
            str(segment.get("model_version") or ""),
        )
        current = merged.get(key)
        if current is None:
            copied = dict(segment)
            copied["metrics"] = dict(object_dict(segment.get("metrics")))
            merged[key] = copied
            continue
        reasons = {
            item
            for item in str(current.get("reason") or "").split(",")
            if item
        }
        reasons.add(str(segment.get("reason") or "unknown"))
        current["reason"] = ",".join(sorted(reasons))
        current_metrics = object_dict(current.get("metrics"))
        new_metrics = object_dict(segment.get("metrics"))
        current_metrics.update(
            {key: value for key, value in new_metrics.items() if value is not None}
        )
        current["metrics"] = current_metrics
    return sorted(
        merged.values(),
        key=lambda item: float_value(object_dict(item.get("metrics")).get("diagnostic_score"), 0),
        reverse=True,
    )[:limit]


def blocked_segments_payload(
    segments: list[dict[str, object]],
    *,
    report_root: Path,
    config: dict[str, object],
) -> dict[str, object]:
    return {
        "version": BLOCKED_SEGMENTS_VERSION,
        "source_report_version": REPORT_VERSION,
        "source_report_root": str(report_root),
        "generated_at": datetime.now(UTC).isoformat(),
        "decision_policy": "candidate_requires_restricted_run_comparison",
        "can_apply_live": False,
        "config": config,
        "segments": segments,
    }


def write_outputs(
    output_dir: Path,
    diagnostics_path: Path,
    blocked_path: Path,
    report: dict[str, object],
    blocked_segments: dict[str, object],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    write_json_atomic(diagnostics_path, report)
    write_json_atomic(blocked_path, blocked_segments)


def write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def object_dict(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


def finite_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, Real):
        return float(value)
    if not isinstance(value, str):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def float_value(value: object, default: float) -> float:
    parsed = finite_float(value)
    return parsed if parsed is not None else default


def main() -> int:
    parser = argparse.ArgumentParser(prog="pre-live-blocker-analysis")
    parser.add_argument("--report-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--max-candidates", type=int, default=20)
    parser.add_argument("--min-segment-signals", type=int, default=10)
    parser.add_argument("--min-adverse-filled-events", type=int, default=10)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    report = create_blocker_diagnostics(
        args.report_root,
        output_dir=args.output_dir,
        max_candidates=args.max_candidates,
        min_segment_signals=args.min_segment_signals,
        min_adverse_filled_events=args.min_adverse_filled_events,
    )
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(format_summary(report))
    return 0


def format_summary(report: dict[str, object]) -> str:
    summary = object_dict(report.get("summary"))
    lines = [
        "pre_live_blocker_diagnostics",
        f"source_report_root={report.get('source_report_root')}",
        f"go_no_go_decision={report.get('go_no_go_decision')}",
        f"drawdown_segments={summary.get('drawdown_segments')}",
        f"adverse_selection_segments={summary.get('adverse_selection_segments')}",
        f"adverse_selection_candidate_segments={summary.get('adverse_selection_candidate_segments')}",
        f"candidate_blocked_segments={summary.get('candidate_blocked_segments')}",
        f"blocked_segments_path={report.get('blocked_segments_path')}",
    ]
    next_run = object_dict(report.get("next_restricted_run"))
    if next_run.get("command"):
        lines.append(f"next_command={next_run['command']}")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
