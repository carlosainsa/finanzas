import json

import pytest

from src import cli


def test_cli_prints_json_output(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def fake_dispatch(args: object) -> dict[str, object]:
        return {"status": "ok"}

    monkeypatch.setattr(cli, "dispatch", fake_dispatch)

    exit_code = cli.main(["--output", "json", "status"])

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out) == {"status": "ok"}


def test_kill_switch_off_requires_confirm() -> None:
    args = cli.build_parser().parse_args(
        ["kill-switch", "off", "--reason", "resume test"]
    )

    with pytest.raises(SystemExit):
        cli.dispatch_kill_switch(client=None, args=args)  # type: ignore[arg-type]


def test_cli_supports_metrics_and_control_results() -> None:
    parser = cli.build_parser()

    assert parser.parse_args(["metrics"]).command == "metrics"
    assert parser.parse_args(["nim-budget"]).command == "nim-budget"
    assert parser.parse_args(["research-go-no-go"]).command == "research-go-no-go"
    assert parser.parse_args(["pre-live-readiness"]).command == "pre-live-readiness"
    assert (
        parser.parse_args(["restricted-blocklist-ranking"]).command
        == "restricted-blocklist-ranking"
    )
    assert parser.parse_args(["research-runs"]).command == "research-runs"
    run_args = parser.parse_args(["research-run", "run-1"])
    assert run_args.command == "research-run"
    assert run_args.run_id == "run-1"
    assert parser.parse_args(["reconciliation"]).command == "reconciliation"
    args = parser.parse_args(["control-results", "--limit", "5"])

    assert args.command == "control-results"
    assert args.limit == 5


def test_cli_supports_cancel_previews_without_confirmation_phrase() -> None:
    parser = cli.build_parser()

    cancel_all = parser.parse_args(["cancel-all", "--reason", "audit", "--preview"])
    cancel_bot = parser.parse_args(["cancel-bot-open", "--reason", "audit", "--preview"])

    assert cancel_all.preview is True
    assert cancel_all.confirmation_phrase is None
    assert cancel_bot.preview is True


def test_cli_prefers_control_token_for_writes() -> None:
    args = cli.build_parser().parse_args(
        [
            "--read-token",
            "read",
            "--control-token",
            "control",
            "cancel-bot-open",
            "--reason",
            "test",
        ]
    )

    assert cli.read_token(args) == "read"
    assert cli.control_token(args) == "control"


def test_cli_prints_order_table(capsys: pytest.CaptureFixture[str]) -> None:
    cli.print_command_table(
        "orders",
        {
            "orders": [
                {
                    "order_id": "order-1",
                    "status": "PARTIAL",
                    "filled_size": 2.5,
                    "remaining_size": 7.5,
                    "signal_id": "signal-1",
                }
            ]
        },
    )

    output = capsys.readouterr().out
    assert "order_id" in output
    assert "remaining_size" in output
    assert "order-1" in output
    assert "PARTIAL" in output


def test_cli_prints_metrics_labels(capsys: pytest.CaptureFixture[str]) -> None:
    cli.print_command_table(
        "metrics",
        {
            "signals_received": 3,
            "execution_reports_by_status": {"PARTIAL": 1, "MATCHED": 2},
        },
    )

    output = capsys.readouterr().out
    assert "signals_received: 3" in output
    assert "execution_reports_by_status:" in output
    assert "PARTIAL" in output


def test_cli_prints_control_result_audit_columns(capsys: pytest.CaptureFixture[str]) -> None:
    cli.print_command_table(
        "control-results",
        {
            "results": [
                {
                    "command_id": "command-1",
                    "command_type": "cancel_bot_open",
                    "status": "CONFIRMED",
                    "operator": "operator-1",
                    "reason": "rebalance",
                    "completed_at_ms": 2,
                }
            ]
        },
    )

    output = capsys.readouterr().out
    assert "operator" in output
    assert "operator-1" in output
    assert "rebalance" in output


def test_cli_prints_reconciliation_summary(capsys: pytest.CaptureFixture[str]) -> None:
    cli.print_command_table(
        "reconciliation",
        {
            "status": "warning",
            "open_local_orders": 2,
            "pending_cancel_requests": 1,
            "diverged_cancel_requests": 0,
            "stale_orders": 1,
            "source": "postgres",
        },
    )

    output = capsys.readouterr().out
    assert "open_local_orders" in output
    assert "warning" in output


def test_cli_prints_nim_budget_summary(capsys: pytest.CaptureFixture[str]) -> None:
    cli.print_command_table(
        "nim-budget",
        {
            "status": "ok",
            "budget_status": "OK",
            "run_id": "run-1",
            "nim_model": "deepseek-ai/deepseek-v3.2",
            "total_tokens": 266,
            "latency_ms_avg": 9625.576,
            "estimated_cost": 0.0,
            "budget_violations": [],
        },
    )

    output = capsys.readouterr().out
    assert "budget_status" in output
    assert "deepseek-ai/deepseek-v3.2" in output
    assert "266" in output


def test_cli_prints_research_runs_summary(capsys: pytest.CaptureFixture[str]) -> None:
    cli.print_command_table(
        "research-runs",
        {
            "runs": [
                {
                    "run_id": "run-2",
                    "created_at": "2026-04-27T00:00:00+00:00",
                    "passed": True,
                    "realized_edge": 0.04,
                    "fill_rate": 0.5,
                    "nim_budget_status": "OK",
                    "nim_total_tokens": 266,
                }
            ]
        },
    )

    output = capsys.readouterr().out
    assert "run-2" in output
    assert "realized_edge" in output
    assert "OK" in output


def test_cli_prints_go_no_go_summary(capsys: pytest.CaptureFixture[str]) -> None:
    cli.print_command_table(
        "research-go-no-go",
        {
            "run_id": "run-2",
            "decision": "NO_GO",
            "passed": False,
            "reason": "quantitative_gate_failure",
            "blockers": [{"check_name": "positive_realized_edge", "passed": False}],
        },
    )

    output = capsys.readouterr().out
    assert "NO_GO" in output
    assert "positive_realized_edge" in output


def test_cli_prints_pre_live_readiness_summary(capsys: pytest.CaptureFixture[str]) -> None:
    cli.print_command_table(
        "pre-live-readiness",
        {
            "status": "blocked",
            "run_id": "run-2",
            "go_no_go": {"profile": "pre_live", "decision": "NO_GO"},
            "audit": {"status": "ok"},
            "blockers": [{"check_name": "positive_realized_edge"}],
        },
    )

    output = capsys.readouterr().out
    assert "pre_live" in output
    assert "blocked" in output
    assert "NO_GO" in output


def test_cli_prints_restricted_blocklist_ranking_summary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli.print_command_table(
        "restricted-blocklist-ranking",
        {
            "status": "ok",
            "run_id": "run-2",
            "summary": {
                "observations": 2,
                "blocked_observations": 2,
                "repeat_observation_candidates": 0,
            },
            "top_candidate": {
                "blocklist_kind": "migrated_risk_only",
                "recommendation": "test_migrated_risk_variant",
            },
        },
    )

    output = capsys.readouterr().out
    assert "migrated_risk_only" in output
    assert "test_migrated_risk_variant" in output
    assert "observations" in output
