#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASELINE_REPORT_ROOT=""
DIAGNOSTICS_PATH=""
BLOCKLIST_KIND="candidate"
DURATION_SECONDS="${REAL_DRY_RUN_SECONDS:-900}"
CANDIDATE_REPORT_ROOT=""
OUTPUT_DIR=""
PRINT_PLAN=0
RANKING_OBSERVATION_ROOTS=()
RANKING_OUTPUT_DIR=""

usage() {
  cat <<'EOF'
Usage: scripts/run_restricted_blocklist_observation.sh --baseline-report-root PATH [options]

Runs or evaluates a restricted blocklist observation using the fixed market
universe recorded by pre_live_blocker_diagnostics.json. It writes:
  comparison.json
  research_promotion_decision.json
  restricted_blocklist_ranking.json
  restricted_blocklist_observation_history.json
  restricted_blocklist_next_variant.json when a migrated-risk variant is indicated
  restricted_blocklist_observation_failure.json when pre-live evidence is insufficient

Options:
  --baseline-report-root PATH   Unrestricted baseline report root.
  --diagnostics PATH            Defaults to BASELINE/blocker_diagnostics/pre_live_blocker_diagnostics.json.
  --blocklist-kind KIND         candidate, defensive, top_1, defensive_top_1,
                                restricted_input_plus_top_migrated_risk,
                                restricted_input_plus_all_migrated_risk, or
                                migrated_risk_only. Default: candidate.
  --duration-seconds N          Restricted dry-run duration. Default: REAL_DRY_RUN_SECONDS or 900.
  --candidate-report-root PATH  Skip execution and evaluate an existing restricted report root.
  --output-dir PATH             Defaults to candidate report root when available, otherwise baseline/restricted_observation.
  --ranking-observation-root PATH
                                Additional restricted observation root to include in ranking. Repeatable.
  --ranking-output-dir PATH      Defaults to output dir.
  --print-plan                  Print resolved JSON plan and exit.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --baseline-report-root)
      BASELINE_REPORT_ROOT="${2:-}"
      shift 2
      ;;
    --diagnostics)
      DIAGNOSTICS_PATH="${2:-}"
      shift 2
      ;;
    --blocklist-kind)
      BLOCKLIST_KIND="${2:-}"
      shift 2
      ;;
    --duration-seconds)
      DURATION_SECONDS="${2:-}"
      shift 2
      ;;
    --candidate-report-root)
      CANDIDATE_REPORT_ROOT="${2:-}"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="${2:-}"
      shift 2
      ;;
    --ranking-observation-root)
      RANKING_OBSERVATION_ROOTS+=("${2:-}")
      shift 2
      ;;
    --ranking-output-dir)
      RANKING_OUTPUT_DIR="${2:-}"
      shift 2
      ;;
    --print-plan)
      PRINT_PLAN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 64
      ;;
  esac
done

if [[ -z "$BASELINE_REPORT_ROOT" ]]; then
  echo "--baseline-report-root is required" >&2
  usage >&2
  exit 64
fi
if [[ ! -d "$BASELINE_REPORT_ROOT" ]]; then
  echo "baseline report root does not exist: $BASELINE_REPORT_ROOT" >&2
  exit 64
fi
if ! [[ "$DURATION_SECONDS" =~ ^[0-9]+$ ]] || (( DURATION_SECONDS < 60 )); then
  echo "duration must be an integer >= 60 seconds" >&2
  exit 64
fi

DIAGNOSTICS_PATH="${DIAGNOSTICS_PATH:-$BASELINE_REPORT_ROOT/blocker_diagnostics/pre_live_blocker_diagnostics.json}"
if [[ ! -f "$DIAGNOSTICS_PATH" ]]; then
  echo "diagnostics file does not exist: $DIAGNOSTICS_PATH" >&2
  exit 64
fi

PLAN_JSON="$(
  PYTHONPATH="$ROOT_DIR/python-service" python3 - "$BASELINE_REPORT_ROOT" "$DIAGNOSTICS_PATH" "$BLOCKLIST_KIND" "$DURATION_SECONDS" "$CANDIDATE_REPORT_ROOT" "$OUTPUT_DIR" <<'PY'
import json
import os
import sys
from pathlib import Path

baseline = Path(sys.argv[1])
diagnostics_path = Path(sys.argv[2])
kind = sys.argv[3]
duration = int(sys.argv[4])
candidate_arg = sys.argv[5]
output_arg = sys.argv[6]

diagnostics = json.loads(diagnostics_path.read_text(encoding="utf-8"))
fixed = diagnostics.get("fixed_market_universe")

def variant_path(collection_name: str, variant_name: str) -> str:
    variants = diagnostics.get(collection_name)
    if not isinstance(variants, list):
        raise SystemExit(f"diagnostics missing {collection_name}")
    for item in variants:
        if isinstance(item, dict) and item.get("name") == variant_name:
            path = item.get("path")
            if isinstance(path, str) and path:
                return path
    raise SystemExit(f"variant not found: {variant_name}")

def migrated_variant_path(variant_name: str) -> str:
    for container_name in ("migrated_risk_variants", "restricted_blocklist_variants"):
        container = diagnostics.get(container_name)
        if isinstance(container, dict):
            variants = container.get("variants")
            if isinstance(variants, list):
                for item in variants:
                    if isinstance(item, dict) and item.get("name") == variant_name:
                        path = item.get("path")
                        if isinstance(path, str) and path:
                            return path
    variants = diagnostics.get("variants")
    if isinstance(variants, list):
        for item in variants:
            if isinstance(item, dict) and item.get("name") == variant_name:
                path = item.get("path")
                if isinstance(path, str) and path:
                    return path
    raise SystemExit(f"migrated risk variant not found: {variant_name}")

if kind == "candidate":
    if not isinstance(fixed, dict):
        raise SystemExit("diagnostics missing fixed_market_universe")
    blocklist_path = diagnostics.get("blocked_segments_path")
elif kind == "defensive":
    if not isinstance(fixed, dict):
        raise SystemExit("diagnostics missing fixed_market_universe")
    blocklist_path = diagnostics.get("defensive_blocked_segments_path")
elif kind == "top_1":
    if not isinstance(fixed, dict):
        raise SystemExit("diagnostics missing fixed_market_universe")
    blocklist_path = variant_path("narrow_candidate_variants", "top_1")
elif kind == "defensive_top_1":
    if not isinstance(fixed, dict):
        raise SystemExit("diagnostics missing fixed_market_universe")
    blocklist_path = variant_path("defensive_candidate_variants", "defensive_top_1")
elif kind in {
    "restricted_input_plus_top_migrated_risk",
    "restricted_input_plus_all_migrated_risk",
    "migrated_risk_only",
}:
    blocklist_path = migrated_variant_path(kind)
else:
    raise SystemExit("unsupported blocklist kind")

if not isinstance(blocklist_path, str) or not blocklist_path:
    raise SystemExit(f"diagnostics missing blocklist path for {kind}")
if not Path(blocklist_path).exists():
    raise SystemExit(f"blocklist path does not exist: {blocklist_path}")
if not isinstance(fixed, dict):
    blocklist_payload = json.loads(Path(blocklist_path).read_text(encoding="utf-8"))
    contract = blocklist_payload.get("evaluation_contract")
    if isinstance(contract, dict):
        fixed = contract.get("fixed_market_universe")
if not isinstance(fixed, dict):
    raise SystemExit("blocklist missing fixed_market_universe")
market_asset_ids_csv = fixed.get("market_asset_ids_csv")
if not isinstance(market_asset_ids_csv, str) or not market_asset_ids_csv:
    raise SystemExit("fixed_market_universe missing market_asset_ids_csv")

candidate_root = Path(candidate_arg) if candidate_arg else None
output_dir = Path(output_arg) if output_arg else (
    candidate_root if candidate_root else baseline / "restricted_observation"
)

print(json.dumps({
    "baseline_report_root": str(baseline),
    "delegates_to": "scripts/run_pre_live_dry_run.sh" if not candidate_arg else None,
    "final_delegates_to": "scripts/run_real_dry_run_research.sh" if not candidate_arg else None,
    "diagnostics_path": str(diagnostics_path),
    "blocklist_kind": kind,
    "blocklist_path": blocklist_path,
    "market_asset_ids_csv": market_asset_ids_csv,
    "market_asset_ids_count": fixed.get("market_asset_ids_count"),
    "market_asset_ids_sha256": fixed.get("market_asset_ids_sha256"),
    "duration_seconds": duration,
    "preflight_enabled": os.environ.get("REAL_DRY_RUN_PREFLIGHT_ENABLED", "1"),
    "preflight_seconds": int(os.environ.get("REAL_DRY_RUN_PREFLIGHT_SECONDS", "120")),
    "preflight_poll_seconds": int(os.environ.get("REAL_DRY_RUN_PREFLIGHT_POLL_SECONDS", "5")),
    "preflight_require_reports": os.environ.get(
        "REAL_DRY_RUN_PREFLIGHT_REQUIRE_REPORTS",
        "true" if not candidate_arg else "false",
    ),
    "candidate_report_root": str(candidate_root) if candidate_root else None,
    "output_dir": str(output_dir),
    "can_execute_trades": False,
}, sort_keys=True))
PY
)"

if [[ "$PRINT_PLAN" == "1" ]]; then
  echo "$PLAN_JSON"
  exit 0
fi

BLOCKLIST_PATH="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["blocklist_path"])' <<<"$PLAN_JSON")"
MARKET_IDS_CSV="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["market_asset_ids_csv"])' <<<"$PLAN_JSON")"

if [[ -z "$CANDIDATE_REPORT_ROOT" ]]; then
  REPORT_TIMESTAMP="${REPORT_TIMESTAMP:-restricted-dry-run-$(date -u +%Y%m%dT%H%M%SZ)}"
  export REPORT_TIMESTAMP
  export MARKET_ASSET_IDS="$MARKET_IDS_CSV"
  export PREDICTOR_BLOCKED_SEGMENTS_PATH="$BLOCKLIST_PATH"
  export REAL_DRY_RUN_PREFLIGHT_REQUIRE_REPORTS="${REAL_DRY_RUN_PREFLIGHT_REQUIRE_REPORTS:-true}"
  EXPECTED_DATA_LAKE_ROOT="${ROOT_DIR}/.tmp/real-dry-run-data-lake/${REPORT_TIMESTAMP}"
  EXPECTED_REPORT_ROOT="${EXPECTED_DATA_LAKE_ROOT}/reports/${REPORT_TIMESTAMP}"
  set +e
  dry_run_output="$("$ROOT_DIR/scripts/run_pre_live_dry_run.sh" --duration-seconds "$DURATION_SECONDS" 2>&1)"
  dry_run_status=$?
  set -e
  printf '%s\n' "$dry_run_output"
  if [[ "$dry_run_status" != "0" && "$dry_run_status" != "20" ]]; then
    OUTPUT_DIR="${OUTPUT_DIR:-$EXPECTED_REPORT_ROOT}"
    dry_run_reason="$(printf '%s\n' "$dry_run_output" | tail -n 1)"
    dry_run_tail="$(printf '%s\n' "$dry_run_output" | tail -n 80)"
    set +e
    PYTHONPATH="$ROOT_DIR/python-service" python3 -m src.research.restricted_blocklist_failure \
      --plan-json "$PLAN_JSON" \
      --output-dir "$OUTPUT_DIR" \
      --candidate-report-root "$EXPECTED_REPORT_ROOT" \
      --exit-code "$dry_run_status" \
      --reason "$dry_run_reason" \
      --stage "pre_live_dry_run" \
      --output-tail "$dry_run_tail" \
      --json
    failure_write_status=$?
    set -e
    if [[ "$failure_write_status" != "0" ]]; then
      echo "warning: failed to write restricted_blocklist_observation_failure.json; preserving dry-run exit code $dry_run_status" >&2
    else
      set +e
      PYTHONPATH="$ROOT_DIR/python-service" python3 -m src.research.restricted_blocklist_ranking \
        --observation-root "$OUTPUT_DIR" \
        --output "$OUTPUT_DIR/restricted_blocklist_ranking.json"
      failure_ranking_status=$?
      PYTHONPATH="$ROOT_DIR/python-service" python3 -m src.research.restricted_blocklist_history \
        --observation-root "$OUTPUT_DIR" \
        --output "$OUTPUT_DIR/restricted_blocklist_observation_history.json"
      failure_history_status=$?
      MANIFEST_ROOT="$(python3 - "$EXPECTED_REPORT_ROOT" <<'PY'
import sys
from pathlib import Path

report_root = Path(sys.argv[1]).resolve()
print(report_root.parent.parent / "research_runs")
PY
)"
      PYTHONPATH="$ROOT_DIR/python-service" python3 -m src.research.run_manifest \
        --report-root "$OUTPUT_DIR" \
        --manifest-root "$MANIFEST_ROOT" \
        --run-id "$(basename "$OUTPUT_DIR")" \
        --source "restricted_blocklist_observation_failure" \
        > "$OUTPUT_DIR/research_manifest.json"
      failure_manifest_status=$?
      set -e
      if [[ "$failure_ranking_status" != "0" || "$failure_history_status" != "0" || "$failure_manifest_status" != "0" ]]; then
        echo "warning: failed to write one or more restricted failure index artifacts; preserving dry-run exit code $dry_run_status" >&2
      fi
    fi
    exit "$dry_run_status"
  fi
  CANDIDATE_REPORT_ROOT="${RESEARCH_REPORT_ROOT:-${DATA_LAKE_ROOT:-$ROOT_DIR/.tmp/real-dry-run-data-lake/$REPORT_TIMESTAMP}/reports/$REPORT_TIMESTAMP}"
fi

if [[ ! -d "$CANDIDATE_REPORT_ROOT" ]]; then
  echo "candidate report root does not exist: $CANDIDATE_REPORT_ROOT" >&2
  exit 64
fi

OUTPUT_DIR="${OUTPUT_DIR:-$CANDIDATE_REPORT_ROOT}"
mkdir -p "$OUTPUT_DIR"

PYTHONPATH="$ROOT_DIR/python-service" python3 -m src.research.compare_runs \
  --baseline-report-root "$BASELINE_REPORT_ROOT" \
  --candidate-report-root "$CANDIDATE_REPORT_ROOT" \
  --json > "$OUTPUT_DIR/comparison.json"

PYTHONPATH="$ROOT_DIR/python-service" python3 -m src.research.restricted_blocklist_diagnostics \
  --baseline-report-root "$BASELINE_REPORT_ROOT" \
  --candidate-report-root "$CANDIDATE_REPORT_ROOT" \
  --output "$OUTPUT_DIR/restricted_blocklist_diagnostics.json" \
  --variants-output-dir "$OUTPUT_DIR" \
  --json

set +e
PYTHONPATH="$ROOT_DIR/python-service" python3 -m src.research.research_promotion_decision \
  --baseline-report-root "$BASELINE_REPORT_ROOT" \
  --candidate-report-root "$CANDIDATE_REPORT_ROOT" \
  --output "$OUTPUT_DIR/research_promotion_decision.json" \
  --json
decision_status=$?
set -e

if [[ "$decision_status" != "0" && "$decision_status" != "2" ]]; then
  exit "$decision_status"
fi

PYTHONPATH="$ROOT_DIR/python-service" python3 -m src.research.restricted_blocklist_summary \
  --plan-json "$PLAN_JSON" \
  --candidate-report-root "$CANDIDATE_REPORT_ROOT" \
  --output-dir "$OUTPUT_DIR" \
  --decision-status "$decision_status" \
  --output "$OUTPUT_DIR/restricted_blocklist_observation_summary.json" \
  --json

PYTHONPATH="$ROOT_DIR/python-service" python3 -m src.research.restricted_blocklist_decision \
  --observation-root "$OUTPUT_DIR" \
  --json

PYTHONPATH="$ROOT_DIR/python-service" python3 -m src.research.restricted_blocklist_summary \
  --plan-json "$PLAN_JSON" \
  --candidate-report-root "$CANDIDATE_REPORT_ROOT" \
  --output-dir "$OUTPUT_DIR" \
  --decision-status "$decision_status" \
  --output "$OUTPUT_DIR/restricted_blocklist_observation_summary.json" \
  --json

RANKING_OUTPUT_DIR="${RANKING_OUTPUT_DIR:-$OUTPUT_DIR}"
mkdir -p "$RANKING_OUTPUT_DIR"
RANKING_ARGS=(--observation-root "$OUTPUT_DIR")
for root in "${RANKING_OBSERVATION_ROOTS[@]}"; do
  if [[ -z "$root" ]]; then
    echo "ranking observation root cannot be empty" >&2
    exit 64
  fi
  if [[ ! -d "$root" ]]; then
    echo "ranking observation root does not exist: $root" >&2
    exit 64
  fi
  if [[ "$root" != "$OUTPUT_DIR" ]]; then
    RANKING_ARGS+=(--observation-root "$root")
  fi
done

PYTHONPATH="$ROOT_DIR/python-service" python3 -m src.research.restricted_blocklist_ranking \
  "${RANKING_ARGS[@]}" \
  --output "$RANKING_OUTPUT_DIR/restricted_blocklist_ranking.json"

PYTHONPATH="$ROOT_DIR/python-service" python3 -m src.research.restricted_blocklist_history \
  "${RANKING_ARGS[@]}" \
  --output "$RANKING_OUTPUT_DIR/restricted_blocklist_observation_history.json"

PYTHONPATH="$ROOT_DIR/python-service" python3 -m src.research.restricted_blocklist_family_decision \
  --history "$RANKING_OUTPUT_DIR/restricted_blocklist_observation_history.json" \
  --output "$RANKING_OUTPUT_DIR/restricted_blocklist_family_decision.json" \
  --json

PYTHONPATH="$ROOT_DIR/python-service" python3 -m src.research.restricted_blocklist_next_variant \
  --ranking "$RANKING_OUTPUT_DIR/restricted_blocklist_ranking.json" \
  --output-dir "$RANKING_OUTPUT_DIR" \
  --json

if [[ "$RANKING_OUTPUT_DIR" == "$CANDIDATE_REPORT_ROOT" && -f "$CANDIDATE_REPORT_ROOT/research_manifest.json" ]]; then
  MANIFEST_ROOT="$(python3 - "$CANDIDATE_REPORT_ROOT" <<'PY'
import sys
from pathlib import Path

report_root = Path(sys.argv[1]).resolve()
print(report_root.parent.parent / "research_runs")
PY
)"
  PYTHONPATH="$ROOT_DIR/python-service" python3 -m src.research.run_manifest \
    --report-root "$CANDIDATE_REPORT_ROOT" \
    --manifest-root "$MANIFEST_ROOT" \
    --run-id "$(basename "$CANDIDATE_REPORT_ROOT")" \
    --source "restricted_blocklist_observation" \
    > "$CANDIDATE_REPORT_ROOT/research_manifest.json"
fi

exit 0
