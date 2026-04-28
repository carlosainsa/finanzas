import argparse
import hashlib
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
COMPARABILITY_POLICY_VERSION = "segment_comparability_v2"
MIN_SHARED_SEGMENT_RATIO = 0.80
MIN_SHARED_SIGNAL_COVERAGE_RATIO = 0.50
MIN_SHARED_FILL_COVERAGE_RATIO = 0.50


def create_blocker_diagnostics(
    report_root: Path,
    *,
    output_dir: Path | None = None,
    max_candidates: int = 20,
    min_segment_signals: int = 10,
    min_adverse_filled_events: int = 10,
    candidate_limits: tuple[int, ...] = (1,),
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
    defensive_segments = defensive_candidate_segments(
        segments,
        adverse,
        drawdown_threshold=drawdown_threshold,
        adverse_threshold=adverse_threshold,
        min_segment_signals=min_segment_signals,
        min_adverse_filled_events=min_adverse_filled_events,
        limit=max_candidates,
    )
    explanatory_buckets = bucket_attribution(
        segments,
        adverse,
        drawdown_threshold=drawdown_threshold,
        adverse_threshold=adverse_threshold,
        min_segment_signals=min_segment_signals,
        min_adverse_filled_events=min_adverse_filled_events,
        limit=max_candidates,
    )
    fixed_universe = fixed_market_universe_payload(segments, report_root=report_root)
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
    evaluation_contract = blocklist_evaluation_contract(
        candidate_segments,
        segments=segments,
        report_root=report_root,
        fixed_universe=fixed_universe,
        min_segment_signals=min_segment_signals,
        min_adverse_filled_events=min_adverse_filled_events,
    )
    blocked_segments["evaluation_contract"] = evaluation_contract
    resolved_output_dir = output_dir or report_root / "blocker_diagnostics"
    blocked_path = resolved_output_dir / "blocked_segments_candidate.json"
    defensive_blocked_path = resolved_output_dir / "blocked_segments_defensive_candidate.json"
    fixed_universe_path = resolved_output_dir / "fixed_market_universe.json"
    diagnostics_path = resolved_output_dir / "pre_live_blocker_diagnostics.json"
    defensive_blocked_segments = blocked_segments_payload(
        defensive_segments,
        report_root=report_root,
        config={
            "max_drawdown": drawdown_threshold,
            "max_adverse_selection_rate": adverse_threshold,
            "min_segment_signals": min_segment_signals,
            "min_adverse_filled_events": min_adverse_filled_events,
            "max_candidates": max_candidates,
            "policy": "defensive_drawdown_adverse_selection",
        },
    )
    defensive_blocked_segments["evaluation_contract"] = blocklist_evaluation_contract(
        defensive_segments,
        segments=segments,
        report_root=report_root,
        fixed_universe=fixed_universe,
        min_segment_signals=min_segment_signals,
        min_adverse_filled_events=min_adverse_filled_events,
    )
    narrow_variants = narrow_candidate_variants(
        candidate_segments,
        segments=segments,
        report_root=report_root,
        output_dir=resolved_output_dir,
        candidate_limits=candidate_limits,
        config={
            "max_drawdown": drawdown_threshold,
            "max_adverse_selection_rate": adverse_threshold,
            "min_segment_signals": min_segment_signals,
            "min_adverse_filled_events": min_adverse_filled_events,
            "max_candidates": max_candidates,
        },
        fixed_universe=fixed_universe,
        min_segment_signals=min_segment_signals,
        min_adverse_filled_events=min_adverse_filled_events,
    )
    defensive_variants = narrow_candidate_variants(
        defensive_segments,
        segments=segments,
        report_root=report_root,
        output_dir=resolved_output_dir,
        candidate_limits=candidate_limits,
        config={
            "max_drawdown": drawdown_threshold,
            "max_adverse_selection_rate": adverse_threshold,
            "min_segment_signals": min_segment_signals,
            "min_adverse_filled_events": min_adverse_filled_events,
            "max_candidates": max_candidates,
            "policy": "defensive_drawdown_adverse_selection",
        },
        fixed_universe=fixed_universe,
        min_segment_signals=min_segment_signals,
        min_adverse_filled_events=min_adverse_filled_events,
        filename_prefix="blocked_segments_defensive_candidate_top",
        variant_name_prefix="defensive_top",
    )
    fixed_universe_prefix = (
        f"MARKET_ASSET_IDS={shell_quote(str(fixed_universe['market_asset_ids_csv']))} "
        if fixed_universe.get("market_asset_ids_csv")
        else ""
    )
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
            "defensive_candidate_blocked_segments": len(defensive_segments),
            "fixed_market_asset_ids": fixed_universe.get("market_asset_ids_count", 0),
        },
        "top_drawdown_segments": drawdown_segments,
        "top_adverse_selection_segments": adverse_segments,
        "top_explanatory_buckets": explanatory_buckets,
        "defensive_candidate_blocked_segments": defensive_segments,
        "candidate_blocked_segments": candidate_segments,
        "narrow_candidate_variants": narrow_variants,
        "defensive_candidate_variants": defensive_variants,
        "fixed_market_universe": fixed_universe,
        "fixed_market_universe_path": str(fixed_universe_path),
        "evaluation_contract": evaluation_contract,
        "blocked_segments_path": str(blocked_path),
        "defensive_blocked_segments_path": str(defensive_blocked_path),
        "next_restricted_run": {
            "command": (
                f"{fixed_universe_prefix}PREDICTOR_BLOCKED_SEGMENTS_PATH={blocked_path} "
                "scripts/run_pre_live_dry_run.sh --duration-seconds 900"
            ),
            "defensive_command": (
                f"{fixed_universe_prefix}PREDICTOR_BLOCKED_SEGMENTS_PATH={defensive_blocked_path} "
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
    write_outputs(
        resolved_output_dir,
        diagnostics_path,
        blocked_path,
        report,
        blocked_segments,
    )
    write_json_atomic(defensive_blocked_path, defensive_blocked_segments)
    write_json_atomic(fixed_universe_path, fixed_universe)
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


def defensive_candidate_segments(
    segments: pd.DataFrame,
    adverse: pd.DataFrame,
    *,
    drawdown_threshold: float,
    adverse_threshold: float,
    min_segment_signals: int,
    min_adverse_filled_events: int,
    limit: int,
) -> list[dict[str, object]]:
    if segments.empty:
        return []
    frame = segments.copy()
    for column in ("signals", "filled_signals", "max_drawdown", "realized_edge", "pnl"):
        frame[column] = pd.to_numeric(frame.get(column), errors="coerce")
    frame["signals"] = frame["signals"].fillna(0)
    frame["filled_signals"] = frame["filled_signals"].fillna(0)
    frame["max_drawdown"] = frame["max_drawdown"].fillna(0)
    frame["realized_edge"] = frame["realized_edge"].fillna(0)
    frame["pnl"] = frame["pnl"].fillna(0)
    frame = frame[frame["signals"] >= min_segment_signals]
    if frame.empty:
        return []
    if not adverse.empty and {
        "market_id",
        "side",
        "strategy",
        "filled_events",
        "adverse_30s_rate",
    }.issubset(set(adverse.columns)):
        adverse_frame = adverse.copy()
        adverse_frame["filled_events"] = pd.to_numeric(
            adverse_frame.get("filled_events"), errors="coerce"
        ).fillna(0)
        adverse_frame["adverse_30s_rate"] = pd.to_numeric(
            adverse_frame.get("adverse_30s_rate"), errors="coerce"
        ).fillna(0)
        frame = frame.merge(
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
            how="left",
        )
    else:
        frame["filled_events"] = 0.0
        frame["adverse_30s_rate"] = 0.0
        frame["avg_pnl_30s"] = None
    frame["filled_events"] = pd.to_numeric(
        frame.get("filled_events"), errors="coerce"
    ).fillna(0)
    frame["adverse_30s_rate"] = pd.to_numeric(
        frame.get("adverse_30s_rate"), errors="coerce"
    ).fillna(0)
    frame["drawdown_excess"] = (frame["max_drawdown"] - drawdown_threshold).clip(
        lower=0
    )
    frame["adverse_excess"] = (
        frame["adverse_30s_rate"] - adverse_threshold
    ).clip(lower=0)
    frame = frame[
        (
            (frame["drawdown_excess"] > 0)
            | (
                (frame["adverse_excess"] > 0)
                & (frame["filled_events"] >= min_adverse_filled_events)
            )
        )
        & ((frame["realized_edge"] <= 0) | (frame["pnl"] <= 0))
    ]
    if frame.empty:
        return []
    frame["sample_weight"] = (frame["signals"] + frame["filled_signals"]).clip(lower=1)
    frame["diagnostic_score"] = (
        frame["drawdown_excess"]
        + frame["adverse_excess"]
        + frame["realized_edge"].clip(upper=0).abs()
    ) * frame["sample_weight"].pow(0.5)
    return [
        segment_record(row, reason=defensive_reason(row))
        for _, row in frame.sort_values(
            ["diagnostic_score", "signals"], ascending=False
        )
        .head(limit)
        .iterrows()
    ]


def defensive_reason(row: pd.Series) -> str:
    reasons = []
    if float_value(row.get("drawdown_excess"), 0) > 0:
        reasons.append("bounded_drawdown")
    if float_value(row.get("adverse_excess"), 0) > 0:
        reasons.append("adverse_selection")
    if float_value(row.get("realized_edge"), 0) <= 0 or float_value(row.get("pnl"), 0) <= 0:
        reasons.append("negative_edge")
    return ",".join(reasons) if reasons else "defensive_risk"


def bucket_attribution(
    segments: pd.DataFrame,
    adverse: pd.DataFrame,
    *,
    drawdown_threshold: float,
    adverse_threshold: float,
    min_segment_signals: int,
    min_adverse_filled_events: int,
    limit: int,
) -> list[dict[str, object]]:
    if segments.empty:
        return []
    frame = segments.copy()
    for column in ("signals", "filled_signals", "pnl", "realized_edge", "max_drawdown"):
        frame[column] = pd.to_numeric(frame.get(column), errors="coerce").fillna(0)
    if not adverse.empty and {
        "market_id",
        "side",
        "strategy",
        "filled_events",
        "adverse_30s_rate",
    }.issubset(set(adverse.columns)):
        adverse_frame = adverse.copy()
        adverse_frame["filled_events"] = pd.to_numeric(
            adverse_frame.get("filled_events"), errors="coerce"
        ).fillna(0)
        adverse_frame["adverse_30s_rate"] = pd.to_numeric(
            adverse_frame.get("adverse_30s_rate"), errors="coerce"
        ).fillna(0)
        frame = frame.merge(
            adverse_frame[
                ["market_id", "side", "strategy", "filled_events", "adverse_30s_rate"]
            ],
            on=["market_id", "side", "strategy"],
            how="left",
        )
    else:
        frame["filled_events"] = 0.0
        frame["adverse_30s_rate"] = 0.0
    frame["filled_events"] = pd.to_numeric(
        frame.get("filled_events"), errors="coerce"
    ).fillna(0)
    frame["adverse_30s_rate"] = pd.to_numeric(
        frame.get("adverse_30s_rate"), errors="coerce"
    ).fillna(0)
    bucket_specs = (
        ("market", ["market_id"]),
        ("market_asset", ["market_id", "asset_id"]),
        ("strategy", ["strategy"]),
        ("market_asset_strategy", ["market_id", "asset_id", "strategy"]),
        ("segment", ["market_id", "asset_id", "side", "strategy", "model_version"]),
    )
    records: list[dict[str, object]] = []
    for bucket_type, keys in bucket_specs:
        if any(key not in frame.columns for key in keys):
            continue
        grouped = frame.groupby(keys, dropna=False)
        for group_key, group in grouped:
            signals = float(group["signals"].sum())
            if signals < min_segment_signals:
                continue
            filled_signals = float(group["filled_signals"].sum())
            pnl = float(group["pnl"].sum())
            realized_edge = weighted_average(
                group["realized_edge"],
                group["filled_signals"],
            )
            max_drawdown = float(group["max_drawdown"].max())
            filled_events = float(group["filled_events"].max())
            adverse_rate = float(group["adverse_30s_rate"].max())
            drawdown_excess = max(0.0, max_drawdown - drawdown_threshold)
            adverse_excess = (
                max(0.0, adverse_rate - adverse_threshold)
                if filled_events >= min_adverse_filled_events
                else 0.0
            )
            bad_segments = int(
                (
                    (group["max_drawdown"] > drawdown_threshold)
                    | (group["realized_edge"] < 0)
                    | (group["pnl"] < 0)
                ).sum()
            )
            candidate_segments = int(
                (
                    (group["max_drawdown"] > drawdown_threshold)
                    | (
                        (group["adverse_30s_rate"] > adverse_threshold)
                        & (group["filled_events"] >= min_adverse_filled_events)
                    )
                ).sum()
            )
            if drawdown_excess <= 0 and adverse_excess <= 0 and bad_segments == 0:
                continue
            diagnostic_score = (
                drawdown_excess
                + adverse_excess
                + abs(min(realized_edge or 0.0, 0.0))
            ) * max(signals, 1.0) ** 0.5
            records.append(
                {
                    "bucket_type": bucket_type,
                    "bucket": bucket_value(keys, group_key),
                    "segments": int(len(group.index)),
                    "signals": signals,
                    "filled_signals": filled_signals,
                    "pnl": pnl,
                    "realized_edge": realized_edge,
                    "max_drawdown": max_drawdown,
                    "drawdown_excess": drawdown_excess,
                    "adverse_30s_rate": adverse_rate,
                    "adverse_excess": adverse_excess,
                    "adverse_filled_events": filled_events,
                    "bad_segment_count": bad_segments,
                    "candidate_segment_count": candidate_segments,
                    "diagnostic_score": diagnostic_score,
                }
            )
    return sorted(
        records,
        key=lambda item: float_value(item.get("diagnostic_score"), 0),
        reverse=True,
    )[:limit]


def weighted_average(values: pd.Series, weights: pd.Series) -> float | None:
    clean_values = pd.to_numeric(values, errors="coerce").fillna(0)
    clean_weights = pd.to_numeric(weights, errors="coerce").fillna(0)
    total_weight = float(clean_weights.sum())
    if total_weight <= 0:
        return None
    return float((clean_values * clean_weights).sum() / total_weight)


def bucket_value(keys: list[str], group_key: object) -> dict[str, str]:
    values = group_key if isinstance(group_key, tuple) else (group_key,)
    return {key: str(value or "") for key, value in zip(keys, values, strict=True)}


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


def narrow_candidate_variants(
    candidate_segments: list[dict[str, object]],
    *,
    segments: pd.DataFrame,
    report_root: Path,
    output_dir: Path,
    candidate_limits: tuple[int, ...],
    config: dict[str, object],
    fixed_universe: dict[str, object],
    min_segment_signals: int,
    min_adverse_filled_events: int,
    filename_prefix: str = "blocked_segments_candidate_top",
    variant_name_prefix: str = "top",
) -> list[dict[str, object]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    variants: list[dict[str, object]] = []
    for limit in sorted({item for item in candidate_limits if item > 0}):
        if limit >= len(candidate_segments):
            continue
        variant_segments = candidate_segments[:limit]
        path = output_dir / f"{filename_prefix}_{limit}.json"
        payload = blocked_segments_payload(
            variant_segments,
            report_root=report_root,
            config={**config, "variant_candidate_limit": limit},
        )
        payload["evaluation_contract"] = blocklist_evaluation_contract(
            variant_segments,
            segments=segments,
            report_root=report_root,
            fixed_universe=fixed_universe,
            min_segment_signals=min_segment_signals,
            min_adverse_filled_events=min_adverse_filled_events,
        )
        variants.append(
            {
                "name": f"{variant_name_prefix}_{limit}",
                "candidate_limit": limit,
                "blocked_segments": len(variant_segments),
                "path": str(path),
                "next_command": (
                    f"{fixed_universe_command_prefix(fixed_universe)}"
                    f"PREDICTOR_BLOCKED_SEGMENTS_PATH={path} "
                    "scripts/run_pre_live_dry_run.sh --duration-seconds 900"
                ),
            }
        )
        write_json_atomic(path, payload)
    return variants


def blocklist_evaluation_contract(
    candidate_segments: list[dict[str, object]],
    *,
    segments: pd.DataFrame,
    report_root: Path,
    fixed_universe: dict[str, object],
    min_segment_signals: int,
    min_adverse_filled_events: int,
) -> dict[str, object]:
    expected_removed = [segment_identity(segment) for segment in candidate_segments]
    coverage = expected_coverage_impact(segments, candidate_segments)
    return {
        "version": "blocked_segments_evaluation_contract_v1",
        "comparability_policy_version": COMPARABILITY_POLICY_VERSION,
        "source_report_root": str(report_root),
        "can_promote_live": False,
        "required_outcome": "restricted_run_must_remain_comparable",
        "fixed_market_universe": fixed_universe,
        "expected_removed_segments": expected_removed,
        "expected_removed_segments_count": len(expected_removed),
        "expected_coverage_impact": coverage,
        "minimums": {
            "min_shared_segment_ratio": MIN_SHARED_SEGMENT_RATIO,
            "min_shared_signal_coverage_ratio": MIN_SHARED_SIGNAL_COVERAGE_RATIO,
            "min_shared_fill_coverage_ratio": MIN_SHARED_FILL_COVERAGE_RATIO,
            "min_segment_signals": min_segment_signals,
            "min_adverse_filled_events": min_adverse_filled_events,
        },
        "acceptance_criteria": [
            "compare_runs verdict is not no_comparable",
            "restricted run uses the fixed MARKET_ASSET_IDS universe recorded in this contract",
            "all missing candidate segments are listed in expected_removed_segments",
            "shared segment, signal, and fill coverage meet minimums",
            "realized_edge does not regress",
            "fill_rate does not regress",
            "max_abs_simulator_fill_rate_delta does not regress",
            "reconciliation_divergence_rate does not regress",
            "can_execute_trades remains false until a separate live gate approves",
        ],
        "rejection_criteria": [
            "unexpected candidate segment loss",
            "unexpected new candidate segments",
            "candidate market universe hash does not match the fixed contract",
            "insufficient shared coverage",
            "simulator quality regression",
            "single-run-only evidence",
        ],
    }


def fixed_market_universe_payload(
    segments: pd.DataFrame,
    *,
    report_root: Path,
) -> dict[str, object]:
    asset_ids: list[str] = []
    if not segments.empty and "asset_id" in segments.columns:
        asset_ids = sorted(
            {
                str(value)
                for value in segments["asset_id"].dropna().tolist()
                if str(value)
            }
        )
    asset_ids_csv = ",".join(asset_ids)
    return {
        "version": "fixed_market_universe_v1",
        "source_report_root": str(report_root),
        "market_asset_ids": asset_ids,
        "market_asset_ids_count": len(asset_ids),
        "market_asset_ids_csv": asset_ids_csv,
        "market_asset_ids_sha256": hashlib.sha256(
            asset_ids_csv.encode("utf-8")
        ).hexdigest(),
    }


def fixed_universe_command_prefix(fixed_universe: dict[str, object]) -> str:
    value = fixed_universe.get("market_asset_ids_csv")
    if not isinstance(value, str) or not value:
        return ""
    return f"MARKET_ASSET_IDS={shell_quote(value)} "


def shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def expected_coverage_impact(
    segments: pd.DataFrame,
    candidate_segments: list[dict[str, object]],
) -> dict[str, object]:
    if segments.empty:
        return {
            "baseline_signals": 0.0,
            "candidate_blocked_signals": 0.0,
            "remaining_signals": 0.0,
            "signal_coverage_rate": None,
            "baseline_filled_signals": 0.0,
            "candidate_blocked_filled_signals": 0.0,
            "remaining_filled_signals": 0.0,
            "filled_signal_coverage_rate": None,
        }
    keys = ["market_id", "asset_id", "side", "strategy", "model_version"]
    frame = segments.copy()
    frame["_segment_key"] = frame.apply(
        lambda row: tuple(str(row.get(key) or "") for key in keys),
        axis=1,
    )
    candidate_keys = {
        tuple(str(segment.get(key) or "") for key in keys)
        for segment in candidate_segments
    }
    frame["signals"] = pd.to_numeric(frame.get("signals"), errors="coerce").fillna(0)
    frame["filled_signals"] = pd.to_numeric(
        frame.get("filled_signals"), errors="coerce"
    ).fillna(0)
    blocked = frame[frame["_segment_key"].isin(candidate_keys)]
    baseline_signals = float(frame["signals"].sum())
    blocked_signals = float(blocked["signals"].sum())
    baseline_fills = float(frame["filled_signals"].sum())
    blocked_fills = float(blocked["filled_signals"].sum())
    return {
        "baseline_signals": baseline_signals,
        "candidate_blocked_signals": blocked_signals,
        "remaining_signals": baseline_signals - blocked_signals,
        "signal_coverage_rate": safe_ratio(
            baseline_signals - blocked_signals, baseline_signals
        ),
        "baseline_filled_signals": baseline_fills,
        "candidate_blocked_filled_signals": blocked_fills,
        "remaining_filled_signals": baseline_fills - blocked_fills,
        "filled_signal_coverage_rate": safe_ratio(
            baseline_fills - blocked_fills, baseline_fills
        ),
    }


def segment_identity(segment: dict[str, object]) -> dict[str, str]:
    return {
        "market_id": str(segment.get("market_id") or ""),
        "asset_id": str(segment.get("asset_id") or ""),
        "side": str(segment.get("side") or ""),
        "strategy": str(segment.get("strategy") or ""),
        "model_version": str(segment.get("model_version") or ""),
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


def safe_ratio(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator


def main() -> int:
    parser = argparse.ArgumentParser(prog="pre-live-blocker-analysis")
    parser.add_argument("--report-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--max-candidates", type=int, default=20)
    parser.add_argument("--min-segment-signals", type=int, default=10)
    parser.add_argument("--min-adverse-filled-events", type=int, default=10)
    parser.add_argument(
        "--candidate-limits",
        default="1",
        help="comma-separated narrower candidate limits to export, e.g. 1,2",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    report = create_blocker_diagnostics(
        args.report_root,
        output_dir=args.output_dir,
        max_candidates=args.max_candidates,
        min_segment_signals=args.min_segment_signals,
        min_adverse_filled_events=args.min_adverse_filled_events,
        candidate_limits=parse_candidate_limits(args.candidate_limits),
    )
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(format_summary(report))
    return 0


def parse_candidate_limits(value: str) -> tuple[int, ...]:
    limits: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        limits.append(int(part))
    return tuple(limits)


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
    variants = report.get("narrow_candidate_variants")
    if isinstance(variants, list):
        for item in variants:
            if isinstance(item, dict) and item.get("next_command"):
                lines.append(f"variant_{item.get('name')}_command={item['next_command']}")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
