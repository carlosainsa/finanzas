import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.research.profile_observation_comparison import (
    create_profile_observation_comparison,
    list_of_dicts,
    numeric_or_none,
    read_json,
    typed_dict,
)


REPORT_VERSION = "execution_probe_touch_comparison_v1"


def create_execution_probe_touch_comparison(
    report_roots: list[Path],
) -> dict[str, object]:
    base = create_profile_observation_comparison(report_roots)
    observations = [enrich_observation(item) for item in list_of_dicts(base.get("observations"))]
    candidate = observations[-1] if observations else {}
    baselines = observations[:-1]
    diagnosis = diagnose_candidate(candidate, baselines)
    return {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_execution_probe_touch_comparison_only",
        "counts": {
            "report_roots": len(report_roots),
            "observations": len(observations),
            "baselines": len(baselines),
        },
        "diagnosis": diagnosis,
        "observations": observations,
        "profile_observation_comparison": base,
        "artifact_paths": [str(root) for root in report_roots],
    }


def enrich_observation(observation: dict[str, object]) -> dict[str, object]:
    report_root_value = observation.get("report_root")
    report_root = Path(str(report_root_value)) if report_root_value else Path()
    enriched = dict(observation)
    enriched["at_touch_diagnostic"] = read_json(
        report_root / "at_touch_asset_diagnostic" / "at_touch_asset_diagnostic.json"
    )
    enriched["runtime_touch_ranking"] = read_json(
        report_root / "runtime_touch_ranking" / "runtime_touch_ranking.json"
    )
    enriched["touch_comparison_metrics"] = touch_comparison_metrics(enriched)
    return enriched


def touch_comparison_metrics(observation: dict[str, object]) -> dict[str, object]:
    fills = typed_dict(observation.get("fills"))
    quote_policy = typed_dict(observation.get("quote_policy"))
    diagnostic = typed_dict(observation.get("at_touch_diagnostic"))
    diagnostic_summary = typed_dict(diagnostic.get("summary"))
    runtime_ranking = typed_dict(observation.get("runtime_touch_ranking"))
    runtime_counts = typed_dict(runtime_ranking.get("counts"))
    return {
        "observed_fill_rate": numeric_or_none(fills.get("observed_fill_rate")),
        "synthetic_fill_rate": numeric_or_none(fills.get("synthetic_fill_rate")),
        "fill_rate_gap": numeric_or_none(fills.get("fill_rate_gap")),
        "no_fill_future_touch_rate": numeric_or_none(
            quote_policy.get("no_fill_future_touch_rate")
        ),
        "avg_required_quote_move": numeric_or_none(
            quote_policy.get("avg_required_quote_move")
        ),
        "at_touch_assets": diagnostic_summary.get("assets"),
        "at_touch_decisions": diagnostic_summary.get("decisions"),
        "runtime_touch_ranked_assets": runtime_counts.get("runtime_touch_ranking"),
        "runtime_touch_selected_assets": runtime_counts.get(
            "selected_runtime_touch_markets"
        ),
    }


def diagnose_candidate(
    candidate: dict[str, object],
    baselines: list[dict[str, object]],
) -> dict[str, object]:
    metrics = typed_dict(candidate.get("touch_comparison_metrics"))
    observed_fill_rate = numeric_or_none(metrics.get("observed_fill_rate")) or 0.0
    synthetic_fill_rate = numeric_or_none(metrics.get("synthetic_fill_rate")) or 0.0
    no_fill_future_touch_rate = (
        numeric_or_none(metrics.get("no_fill_future_touch_rate")) or 0.0
    )
    at_touch_decisions = typed_dict(metrics.get("at_touch_decisions"))
    retune_assets = numeric_or_none(at_touch_decisions.get("RETUNE_TO_AT_TOUCH")) or 0.0
    stale_assets = numeric_or_none(at_touch_decisions.get("DROP_RUNTIME_STALE")) or 0.0
    fill_rate_gap = numeric_or_none(metrics.get("fill_rate_gap")) or (
        synthetic_fill_rate - observed_fill_rate
    )
    if not candidate:
        diagnosis = "missing_candidate_observation"
        next_action = "GENERATE_OBSERVATION"
    elif observed_fill_rate <= 0 and retune_assets > 0:
        diagnosis = "quote_aggressiveness_blocker"
        next_action = "RETUNE_TO_AT_TOUCH"
    elif observed_fill_rate <= 0 and stale_assets > 0 and no_fill_future_touch_rate <= 0:
        diagnosis = "runtime_touch_stale_or_quotes_not_reachable"
        next_action = "CHANGE_MARKET_OR_TIMING_FILTERS"
    elif observed_fill_rate <= 0 and no_fill_future_touch_rate > 0:
        diagnosis = "quote_aggressiveness_blocker"
        next_action = "RETUNE_TO_AT_TOUCH"
    elif fill_rate_gap >= 0.10:
        diagnosis = "synthetic_optimism_detected"
        next_action = "REDUCE_SYNTHETIC_DEPENDENCE"
    else:
        diagnosis = "touch_evidence_improved"
        next_action = "REPEAT_LONGER_OBSERVATION"
    return {
        "candidate_run_id": candidate.get("run_id"),
        "candidate_profile": candidate.get("profile"),
        "baseline_run_ids": [item.get("run_id") for item in baselines],
        "diagnosis": diagnosis,
        "next_action": next_action,
        "can_promote_live": False,
        "reason": "touch comparison is offline diagnostic evidence only",
        "candidate_metrics": metrics,
    }


def main() -> int:
    parser = argparse.ArgumentParser(prog="execution-probe-touch-comparison")
    parser.add_argument("--report-root", action="append", required=True)
    parser.add_argument("--output")
    args = parser.parse_args()
    report = create_execution_probe_touch_comparison(
        [Path(value) for value in args.report_root]
    )
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        Path(args.output).write_text(payload, encoding="utf-8")
    print(payload, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
