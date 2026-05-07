import json
import os
import subprocess
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]


def test_pre_live_dry_run_print_plan_is_safe_and_pinned() -> None:
    env = {
        **os.environ,
        "EXECUTION_MODE": "dry_run",
        "DISABLE_MARKET_WS": "false",
    }

    completed = subprocess.run(
        [
            "bash",
            "scripts/run_pre_live_dry_run.sh",
            "--duration-seconds",
            "900",
            "--print-plan",
        ],
        cwd=ROOT_DIR,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    plan = json.loads(completed.stdout)
    assert plan["delegates_to"] == "scripts/run_real_dry_run_research.sh"
    assert plan["execution_mode"] == "dry_run"
    assert plan["disable_market_ws"] == "false"
    assert plan["go_no_go_profile"] == "pre_live"
    assert plan["real_dry_run_seconds"] == 900
    assert plan["pre_live_min_capture_duration_ms"] == 900_000
    assert plan["pre_live_min_signals"] == 250
    assert plan["resource_mode"] == "resource_limited"
    assert plan["market_regime_limits"] == {
        "max_snapshots_per_asset": 250,
        "max_trade_context_rows": 2000,
    }


def test_pre_live_dry_run_refuses_live_mode() -> None:
    env = {**os.environ, "EXECUTION_MODE": "live"}

    completed = subprocess.run(
        ["bash", "scripts/run_pre_live_dry_run.sh", "--print-plan"],
        cwd=ROOT_DIR,
        env=env,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 64
    assert "EXECUTION_MODE must be dry_run" in completed.stderr


def test_real_dry_run_script_persists_profile_and_gates_readiness() -> None:
    script = (ROOT_DIR / "scripts" / "run_real_dry_run_research.sh").read_text(
        encoding="utf-8"
    )

    assert '"go_no_go_profile": os.environ["GO_NO_GO_PROFILE"]' in script
    assert 'if [[ "$readiness_status" != "0"' in script
    assert "ALLOW_RESEARCH_GATE_FAILURE" in script
    assert "src.research.real_dry_run_preflight" in script
    assert "real_dry_run_preflight.json" in script
    assert "while True:" in script
    assert "count=1000" in script
    assert 'next_max = f"({last_id}"' in script
    assert 'RESEARCH_RESOURCE_MODE="${RESEARCH_RESOURCE_MODE:-resource_limited}"' in script
    assert (
        'MARKET_REGIME_MAX_SNAPSHOTS_PER_ASSET="${MARKET_REGIME_MAX_SNAPSHOTS_PER_ASSET:-250}"'
        in script
    )
    api_ready = 'raise SystemExit("operator API did not become ready")'
    assert script.index(api_ready) < script.index(
        "\nrun_preflight_with_service_monitoring\ncapture_with_service_monitoring"
    )
    assert (
        'scripts/summarize_pre_live_readiness.sh "$RESEARCH_REPORT_ROOT/pre_live_readiness.json" || true'
        in script
    )
    assert '"market_asset_ids_sha256"' in script


def test_research_loop_passes_market_regime_resource_limits() -> None:
    script = (ROOT_DIR / "scripts" / "run_research_loop.sh").read_text(
        encoding="utf-8"
    )

    assert 'MARKET_REGIME_ARGS=(--resource-mode "$RESEARCH_RESOURCE_MODE")' in script
    assert (
        'MARKET_REGIME_ARGS+=(--max-snapshots-per-asset "$MARKET_REGIME_MAX_SNAPSHOTS_PER_ASSET")'
        in script
    )
    assert (
        'MARKET_REGIME_ARGS+=(--max-trade-context-rows "$MARKET_REGIME_MAX_TRADE_CONTEXT_ROWS")'
        in script
    )
    assert '"${MARKET_REGIME_ARGS[@]}" > "$REPORT_ROOT/market_regime.json"' in script


def test_research_loop_generates_execution_probe_next_decision() -> None:
    script = (ROOT_DIR / "scripts" / "run_research_loop.sh").read_text(
        encoding="utf-8"
    )

    assert "src.research.execution_probe_next_decision" in script
    assert "execution_probe_next_decision.json" in script
    assert 'summary["execution_probe_next_decision"]' in script


def test_prepare_execution_probe_cycle_prints_post_run_decision_command() -> None:
    script = (ROOT_DIR / "scripts" / "prepare_execution_probe_cycle.sh").read_text(
        encoding="utf-8"
    )

    assert "profile_compare_after_run" in script
    assert "decide_after_run" in script
    assert "src.research.execution_probe_next_decision" in script


def test_execution_probe_v6_cycle_print_plan_is_safe_and_pinned(
    tmp_path: Path,
) -> None:
    universe_duckdb = tmp_path / "research.duckdb"
    universe_duckdb.write_bytes(b"placeholder")
    baseline = tmp_path / "reports" / "baseline"
    baseline.mkdir(parents=True)

    completed = subprocess.run(
        [
            "bash",
            "scripts/run_execution_probe_v6_cycle.sh",
            "--universe-duckdb",
            str(universe_duckdb),
            "--baseline-report-root",
            str(baseline),
            "--duration-seconds",
            "1800",
            "--print-plan",
        ],
        cwd=ROOT_DIR,
        check=True,
        capture_output=True,
        text=True,
    )

    plan = json.loads(completed.stdout)
    assert plan["script"] == "scripts/run_execution_probe_v6_cycle.sh"
    assert plan["can_execute_trades"] is False
    assert plan["execution_mode"] == "dry_run"
    assert plan["profile"] == "execution_probe_v6"
    assert plan["universe_duckdb"] == str(universe_duckdb)
    assert plan["baseline_report_root"] == str(baseline)
    assert plan["duration_seconds"] == 1800
    assert plan["universe_min_assets"] == 5
    assert plan["market_timing_filter"] == "none"
    assert "execution_probe_universe_selection.json" in plan["universe_selection_path"]
    assert "src.research.execution_probe_next_decision" in plan["delegates_to"]
    outputs = plan["outputs"]
    assert "profile_observation_comparison.json" in outputs[
        "profile_observation_comparison"
    ]
    assert "execution_probe_next_decision.json" in outputs[
        "execution_probe_next_decision"
    ]


def test_execution_probe_v6_cycle_refuses_missing_universe_duckdb() -> None:
    completed = subprocess.run(
        ["bash", "scripts/run_execution_probe_v6_cycle.sh", "--print-plan"],
        cwd=ROOT_DIR,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 64
    assert "--universe-duckdb is required" in completed.stderr


def test_execution_probe_v6_cycle_contract_runs_compare_and_decision() -> None:
    script = (ROOT_DIR / "scripts" / "run_execution_probe_v6_cycle.sh").read_text(
        encoding="utf-8"
    )

    assert "prepare_execution_probe_cycle.sh" in script
    assert "run_execution_probe_v6_observation.sh" in script
    assert "PROFILE_OBSERVATION_COMPARISON_REPORT_ROOTS" in script
    assert "RESEARCH_REPORT_ROOT" in script
    assert "src.research.profile_observation_comparison" in script
    assert "src.research.execution_probe_next_decision" in script
    assert "execution_probe_v6_cycle_summary.json" in script


def test_execution_probe_v7_observation_print_plan_is_safe_and_pinned(
    tmp_path: Path,
) -> None:
    universe = tmp_path / "execution_probe_universe_selection.json"
    universe.write_text(
        json.dumps(
            {
                "can_execute_trades": False,
                "status": "ready",
                "profile": "execution_probe_v7",
                "market_asset_ids": ["asset-1", "asset-2"],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    completed = subprocess.run(
        [
            "bash",
            "scripts/run_execution_probe_v7_observation.sh",
            "--universe-selection",
            str(universe),
            "--duration-seconds",
            "1800",
            "--print-plan",
        ],
        cwd=ROOT_DIR,
        check=True,
        capture_output=True,
        text=True,
    )

    plan = json.loads(completed.stdout)
    assert plan["script"] == "scripts/run_execution_probe_v7_observation.sh"
    assert plan["delegates_to"] == "scripts/run_pre_live_dry_run.sh"
    assert plan["execution_mode"] == "dry_run"
    assert plan["predictor_strategy_profile"] == "execution_probe_v7"
    assert plan["predictor_quote_placement"] == "near_touch"
    assert plan["predictor_execution_probe_v7_near_touch_max_spread_fraction"] == 0.85
    assert plan["predictor_execution_probe_v7_offset_ticks"] == 1
    assert plan["go_no_go_profile"] == "pre_live"
    assert plan["real_dry_run_seconds"] == 1800


def test_execution_probe_v7_cycle_print_plan_is_safe_and_pinned(
    tmp_path: Path,
) -> None:
    universe_duckdb = tmp_path / "research.duckdb"
    universe_duckdb.write_bytes(b"placeholder")
    baseline = tmp_path / "reports" / "baseline"
    baseline.mkdir(parents=True)

    completed = subprocess.run(
        [
            "bash",
            "scripts/run_execution_probe_v7_cycle.sh",
            "--universe-duckdb",
            str(universe_duckdb),
            "--baseline-report-root",
            str(baseline),
            "--duration-seconds",
            "1800",
            "--print-plan",
        ],
        cwd=ROOT_DIR,
        check=True,
        capture_output=True,
        text=True,
    )

    plan = json.loads(completed.stdout)
    assert plan["script"] == "scripts/run_execution_probe_v7_cycle.sh"
    assert plan["can_execute_trades"] is False
    assert plan["execution_mode"] == "dry_run"
    assert plan["profile"] == "execution_probe_v7"
    assert plan["baseline_report_root"] == str(baseline)
    assert plan["universe_min_assets"] == 3
    assert plan["market_timing_filter"] == "future_touch"
    assert plan["min_future_touch_rate"] == 0.10
    assert plan["min_timing_signals"] == 5
    assert plan["min_avg_opportunity_spread"] == 0.01
    assert "scripts/run_execution_probe_v7_observation.sh" in plan["delegates_to"]
    assert "execution_probe_next_decision.json" in plan["outputs"][
        "execution_probe_next_decision"
    ]


def test_restricted_blocklist_observation_requires_preflight_reports() -> None:
    script = (
        ROOT_DIR / "scripts" / "run_restricted_blocklist_observation.sh"
    ).read_text(encoding="utf-8")

    assert (
        'REAL_DRY_RUN_PREFLIGHT_REQUIRE_REPORTS="${REAL_DRY_RUN_PREFLIGHT_REQUIRE_REPORTS:-true}"'
        in script
    )
    assert "src.research.restricted_blocklist_history" in script
    assert "restricted_blocklist_observation_history.json" in script
    assert "restricted_blocklist_observation_failure" in script
    assert "restricted_blocklist_observation_failure\" \\" in script


def test_restricted_blocklist_observation_print_plan_uses_fixed_universe(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "reports" / "baseline"
    diagnostics_dir = baseline / "blocker_diagnostics"
    diagnostics_dir.mkdir(parents=True)
    blocklist_path = diagnostics_dir / "blocked_segments_candidate.json"
    blocklist_path.write_text(
        json.dumps(
            {
                "version": "blocked_segments_v1",
                "segments": [
                    {
                        "market_id": "market-1",
                        "asset_id": "asset-1",
                        "side": "BUY",
                        "strategy": "near_touch",
                        "model_version": "predictor_v1",
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    diagnostics_path = diagnostics_dir / "pre_live_blocker_diagnostics.json"
    diagnostics_path.write_text(
        json.dumps(
            {
                "fixed_market_universe": {
                    "market_asset_ids_csv": "asset-1,asset-2",
                    "market_asset_ids_count": 2,
                    "market_asset_ids_sha256": "hash",
                },
                "blocked_segments_path": str(blocklist_path),
            }
        )
        + "\n",
        encoding="utf-8",
    )

    completed = subprocess.run(
        [
            "bash",
            "scripts/run_restricted_blocklist_observation.sh",
            "--baseline-report-root",
            str(baseline),
            "--print-plan",
        ],
        cwd=ROOT_DIR,
        check=True,
        capture_output=True,
        text=True,
    )

    plan = json.loads(completed.stdout)
    assert plan["baseline_report_root"] == str(baseline)
    assert plan["blocklist_kind"] == "candidate"
    assert plan["delegates_to"] == "scripts/run_pre_live_dry_run.sh"
    assert plan["final_delegates_to"] == "scripts/run_real_dry_run_research.sh"
    assert plan["blocklist_path"] == str(blocklist_path)
    assert plan["market_asset_ids_csv"] == "asset-1,asset-2"
    assert plan["can_execute_trades"] is False
    assert plan["preflight_require_reports"] == "true"
    assert plan["preflight_enabled"] == "1"
    assert plan["preflight_poll_seconds"] == 5


def test_restricted_blocklist_observation_print_plan_uses_migrated_variant(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "reports" / "baseline"
    baseline.mkdir(parents=True)
    variants_dir = baseline / "restricted" / "migrated_risk_variants"
    variants_dir.mkdir(parents=True)
    variant_path = variants_dir / "blocked_segments_migrated_risk_only.json"
    variant_path.write_text(
        json.dumps(
            {
                "version": "blocked_segments_v1",
                "segments": [
                    {
                        "market_id": "market-2",
                        "asset_id": "asset-2",
                        "side": "BUY",
                        "strategy": "near_touch",
                        "model_version": "predictor_v1",
                    }
                ],
                "evaluation_contract": {
                    "fixed_market_universe": {
                        "market_asset_ids_csv": "asset-1,asset-2",
                        "market_asset_ids_count": 2,
                        "market_asset_ids_sha256": "hash",
                    }
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    variants_path = baseline / "restricted" / "migrated_risk_blocklist_variants.json"
    variants_path.write_text(
        json.dumps(
            {
                "report_version": "migrated_risk_blocklist_variants_v1",
                "variants": [
                    {
                        "name": "migrated_risk_only",
                        "path": str(variant_path),
                        "blocked_segments": 1,
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    completed = subprocess.run(
        [
            "bash",
            "scripts/run_restricted_blocklist_observation.sh",
            "--baseline-report-root",
            str(baseline),
            "--diagnostics",
            str(variants_path),
            "--blocklist-kind",
            "migrated_risk_only",
            "--print-plan",
        ],
        cwd=ROOT_DIR,
        check=True,
        capture_output=True,
        text=True,
    )

    plan = json.loads(completed.stdout)
    assert plan["blocklist_kind"] == "migrated_risk_only"
    assert plan["blocklist_path"] == str(variant_path)
    assert plan["market_asset_ids_csv"] == "asset-1,asset-2"


def test_restricted_blocklist_observation_print_plan_uses_top_migrated_variant(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "reports" / "baseline"
    baseline.mkdir(parents=True)
    variants_dir = baseline / "restricted" / "migrated_risk_variants"
    variants_dir.mkdir(parents=True)
    variant_path = (
        variants_dir / "blocked_segments_restricted_input_plus_top_migrated_risk.json"
    )
    variant_path.write_text(
        json.dumps(
            {
                "version": "blocked_segments_v1",
                "segments": [
                    {
                        "market_id": "market-2",
                        "asset_id": "asset-2",
                        "side": "BUY",
                        "strategy": "near_touch",
                        "model_version": "predictor_v1",
                    }
                ],
                "evaluation_contract": {
                    "fixed_market_universe": {
                        "market_asset_ids_csv": "asset-1,asset-2",
                        "market_asset_ids_count": 2,
                        "market_asset_ids_sha256": "hash",
                    }
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    variants_path = baseline / "restricted" / "migrated_risk_blocklist_variants.json"
    variants_path.write_text(
        json.dumps(
            {
                "report_version": "migrated_risk_blocklist_variants_v1",
                "variants": [
                    {
                        "name": "restricted_input_plus_top_migrated_risk",
                        "path": str(variant_path),
                        "blocked_segments": 1,
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    completed = subprocess.run(
        [
            "bash",
            "scripts/run_restricted_blocklist_observation.sh",
            "--baseline-report-root",
            str(baseline),
            "--diagnostics",
            str(variants_path),
            "--blocklist-kind",
            "restricted_input_plus_top_migrated_risk",
            "--print-plan",
        ],
        cwd=ROOT_DIR,
        check=True,
        capture_output=True,
        text=True,
    )

    plan = json.loads(completed.stdout)
    assert plan["blocklist_kind"] == "restricted_input_plus_top_migrated_risk"
    assert plan["blocklist_path"] == str(variant_path)
    assert plan["market_asset_ids_csv"] == "asset-1,asset-2"


def test_restricted_blocklist_observation_finalizes_decision() -> None:
    script = (ROOT_DIR / "scripts" / "run_restricted_blocklist_observation.sh").read_text(
        encoding="utf-8"
    )

    assert "src.research.restricted_blocklist_diagnostics" in script
    assert "restricted_blocklist_diagnostics.json" in script
    assert "src.research.restricted_blocklist_decision" in script
    assert "src.research.restricted_blocklist_ranking" in script
    assert "restricted_blocklist_ranking.json" in script
    assert "src.research.restricted_blocklist_summary" in script
    assert "restricted_blocklist_observation_summary.json" in script
    assert "src.research.restricted_blocklist_family_decision" in script
    assert "restricted_blocklist_family_decision.json" in script
    assert "src.research.restricted_blocklist_next_variant" in script
    assert "restricted_blocklist_next_variant.json" in script
    assert "src.research.restricted_blocklist_failure" in script
    assert "restricted_blocklist_observation_failure.json" in script
    assert "failure_write_status" in script
    assert "preserving dry-run exit code" in script
    assert 'exit "$dry_run_status"' in script
    assert "--ranking-observation-root" in script
    assert "src.research.run_manifest" in script
    assert "restricted_blocklist_observation" in script
    assert "--observation-root \"$OUTPUT_DIR\"" in script
