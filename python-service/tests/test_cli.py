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
    args = parser.parse_args(["control-results", "--limit", "5"])

    assert args.command == "control-results"
    assert args.limit == 5


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
