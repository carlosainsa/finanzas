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
    assert (
        'scripts/summarize_pre_live_readiness.sh "$RESEARCH_REPORT_ROOT/pre_live_readiness.json" || true'
        in script
    )
    assert '"market_asset_ids_sha256"' in script


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
    assert plan["blocklist_path"] == str(blocklist_path)
    assert plan["market_asset_ids_csv"] == "asset-1,asset-2"
    assert plan["can_execute_trades"] is False


def test_restricted_blocklist_observation_finalizes_decision() -> None:
    script = (ROOT_DIR / "scripts" / "run_restricted_blocklist_observation.sh").read_text(
        encoding="utf-8"
    )

    assert "src.research.restricted_blocklist_diagnostics" in script
    assert "restricted_blocklist_diagnostics.json" in script
    assert "src.research.restricted_blocklist_decision" in script
    assert "--observation-root \"$OUTPUT_DIR\"" in script
