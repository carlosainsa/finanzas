import json
from pathlib import Path

from src.api.research_service import latest_nim_budget


def test_latest_nim_budget_returns_missing_when_index_absent(tmp_path: Path) -> None:
    result = latest_nim_budget(tmp_path / "research_runs")

    assert result["status"] == "missing"
    assert result["can_execute_trades"] is False
    assert result["budget_violations"] == []


def test_latest_nim_budget_reads_latest_manifest_row(tmp_path: Path) -> None:
    manifest_root = tmp_path / "research_runs"
    manifest_root.mkdir()
    index = manifest_root / "research_runs.jsonl"
    index.write_text(
        "\n".join(
            [
                json.dumps({"run_id": "run-1", "counts": {}}),
                json.dumps(
                    {
                        "run_id": "run-2",
                        "created_at": "2026-04-27T00:00:00+00:00",
                        "report_root": "/tmp/run-2",
                        "versions": {"nim_advisory_model": "deepseek-ai/deepseek-v3.2"},
                        "counts": {
                            "nim_advisory_enabled": True,
                            "nim_advisory_annotations": 1,
                            "nim_advisory_failures": 0,
                            "nim_advisory_prompt_tokens": 200,
                            "nim_advisory_completion_tokens": 66,
                            "nim_advisory_total_tokens": 266,
                            "nim_advisory_latency_ms_avg": 9625.576,
                            "nim_advisory_estimated_cost": 0.0,
                            "nim_advisory_budget_status": "OK",
                            "nim_advisory_budget_violations": "[]",
                        },
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = latest_nim_budget(manifest_root)

    assert result["status"] == "ok"
    assert result["run_id"] == "run-2"
    assert result["nim_model"] == "deepseek-ai/deepseek-v3.2"
    assert result["total_tokens"] == 266
    assert result["budget_status"] == "OK"
    assert result["budget_violations"] == []
    assert result["can_execute_trades"] is False
