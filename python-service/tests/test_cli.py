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
