from pathlib import Path

import duckdb

from src.research.data_lake import create_duckdb_views, export_external_evidence
from src.research.llm.nim_client import NIMAdvisoryResult, NIMResearchConfig
from src.research.nim_advisory import (
    NIM_ADVISORY_REPORT_VERSION,
    NIMAdvisoryConfig,
    export_nim_advisory_report,
)


class FakeNIMClient:
    def __init__(
        self,
        *,
        prompt_tokens: int = 10,
        completion_tokens: int = 3,
        latency_ms: float = 12.5,
    ) -> None:
        self.config = NIMResearchConfig(
            enabled=True,
            api_key="test",
            model="fake-nim",
            base_url="https://nim.test/v1",
        )
        self.calls: list[tuple[str, str, float, int]] = []
        self.prompt_tokens = prompt_tokens
        self.completion_tokens = completion_tokens
        self.latency_ms = latency_ms

    def generate(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        temperature: float = 0.0,
        max_tokens: int = 512,
    ) -> NIMAdvisoryResult:
        self.calls.append((system_prompt, user_prompt, temperature, max_tokens))
        return NIMAdvisoryResult(
            text='{"direction":"YES","confidence":0.82,"contradiction_score":0.12,"rationale":"offline note"}',
            model=self.config.model,
            model_version="nvidia_nim_research_client_v1",
            decision_policy="offline_advisory_only",
            can_execute_trades=False,
            usage={
                "prompt_tokens": self.prompt_tokens,
                "completion_tokens": self.completion_tokens,
                "total_tokens": self.prompt_tokens + self.completion_tokens,
            },
            finish_reason="stop",
            latency_ms=self.latency_ms,
        )


def test_nim_advisory_exports_annotations_without_live_authority(tmp_path: Path) -> None:
    db_path = seed_evidence_db(tmp_path)
    output_dir = tmp_path / "nim_advisory"
    fake = FakeNIMClient()

    report = export_nim_advisory_report(
        db_path,
        output_dir,
        NIMAdvisoryConfig(enabled=True, limit=10),
        client=fake,
    )

    assert report["report_version"] == NIM_ADVISORY_REPORT_VERSION
    assert report["decision_policy"] == "offline_advisory_only"
    assert report["can_execute_trades"] is False
    assert report["enabled"] is True
    assert len(fake.calls) == 2
    assert (output_dir / "nim_advisory.json").exists()
    assert (output_dir / "nim_advisory_annotations.parquet").exists()
    assert (output_dir / "nim_advisory_summary.parquet").exists()
    assert (output_dir / "nim_advisory_cost_summary.parquet").exists()
    assert (output_dir / "nim_advisory_cost_summary.json").exists()

    with duckdb.connect(str(db_path)) as conn:
        rows = conn.execute(
            f"""
            select
                evidence_id,
                nim_model,
                prompt_version,
                direction,
                confidence,
                contradiction_score,
                request_latency_ms,
                prompt_tokens,
                completion_tokens,
                total_tokens,
                decision_policy,
                can_execute_trades
            from read_parquet('{(output_dir / "nim_advisory_annotations.parquet").as_posix()}')
            order by evidence_id
            """
        ).fetchall()
    assert rows == [
        (
            "evidence-1",
            "fake-nim",
            "nim_evidence_advisory_prompt_v1",
            "YES",
            0.82,
            0.12,
            12.5,
            10,
            3,
            13,
            "offline_advisory_only",
            False,
        ),
        (
            "evidence-2",
            "fake-nim",
            "nim_evidence_advisory_prompt_v1",
            "YES",
            0.82,
            0.12,
            12.5,
            10,
            3,
            13,
            "offline_advisory_only",
            False,
        ),
    ]
    with duckdb.connect(str(db_path)) as conn:
        cost_row = conn.execute(
            f"""
            select
                requests_attempted,
                requests_succeeded,
                prompt_tokens,
                completion_tokens,
                total_tokens,
                latency_ms_avg,
                can_execute_trades
            from read_parquet('{(output_dir / "nim_advisory_cost_summary.parquet").as_posix()}')
            """
        ).fetchone()
    assert cost_row == (2, 2, 20, 6, 26, 12.5, False)


def test_nim_advisory_disabled_writes_empty_artifacts_without_calling_client(
    tmp_path: Path,
) -> None:
    db_path = seed_evidence_db(tmp_path)
    output_dir = tmp_path / "nim_advisory"
    fake = FakeNIMClient()

    report = export_nim_advisory_report(
        db_path,
        output_dir,
        NIMAdvisoryConfig(enabled=False),
        client=fake,
    )

    assert report["enabled"] is False
    assert report["status"] == "disabled"
    assert report["counts"] == {
        "nim_advisory_annotations": 0,
        "nim_advisory_cost_summary": 1,
    }
    assert fake.calls == []
    assert (output_dir / "nim_advisory_annotations.parquet").exists()
    assert (output_dir / "nim_advisory_summary.parquet").exists()
    assert (output_dir / "nim_advisory_cost_summary.parquet").exists()


def test_nim_advisory_handles_missing_external_evidence_view(tmp_path: Path) -> None:
    db_path = tmp_path / "research.duckdb"
    output_dir = tmp_path / "nim_advisory"
    fake = FakeNIMClient()

    report = export_nim_advisory_report(
        db_path,
        output_dir,
        NIMAdvisoryConfig(enabled=True),
        client=fake,
    )

    assert report["status"] == "ok"
    assert report["counts"] == {
        "nim_advisory_annotations": 0,
        "nim_advisory_cost_summary": 1,
    }
    assert fake.calls == []


def test_nim_advisory_applies_max_evidence_per_run_cap(tmp_path: Path) -> None:
    db_path = seed_evidence_db(tmp_path)
    output_dir = tmp_path / "nim_advisory"
    fake = FakeNIMClient()

    report = export_nim_advisory_report(
        db_path,
        output_dir,
        NIMAdvisoryConfig(enabled=True, limit=10, max_evidence_per_run=1),
        client=fake,
    )

    assert len(fake.calls) == 1
    assert report["counts"] == {
        "nim_advisory_annotations": 1,
        "nim_advisory_cost_summary": 1,
    }
    assert report["limits"] == {
        "requested_limit": 10,
        "max_evidence_per_run": 1,
        "max_requests_per_run": 25,
        "max_tokens_per_run": 0,
        "max_latency_ms_per_run": 0.0,
        "max_cost_per_run": 0.0,
        "effective_limit": 1,
    }


def test_nim_advisory_applies_max_requests_before_calling_client(
    tmp_path: Path,
) -> None:
    db_path = seed_evidence_db(tmp_path)
    output_dir = tmp_path / "nim_advisory"
    fake = FakeNIMClient()

    report = export_nim_advisory_report(
        db_path,
        output_dir,
        NIMAdvisoryConfig(
            enabled=True,
            limit=10,
            max_evidence_per_run=10,
            max_requests_per_run=1,
        ),
        client=fake,
    )

    assert len(fake.calls) == 1
    assert report["limits"] == {
        "requested_limit": 10,
        "max_evidence_per_run": 10,
        "max_requests_per_run": 1,
        "max_tokens_per_run": 0,
        "max_latency_ms_per_run": 0.0,
        "max_cost_per_run": 0.0,
        "effective_limit": 1,
    }


def test_nim_advisory_marks_budget_exceeded_without_live_authority(
    tmp_path: Path,
) -> None:
    db_path = seed_evidence_db(tmp_path)
    output_dir = tmp_path / "nim_advisory"
    fake = FakeNIMClient(prompt_tokens=20, completion_tokens=5, latency_ms=50.0)

    report = export_nim_advisory_report(
        db_path,
        output_dir,
        NIMAdvisoryConfig(
            enabled=True,
            limit=2,
            max_tokens_per_run=10,
            max_latency_ms_per_run=40,
            max_cost_per_run=0.00001,
            input_cost_per_million_tokens=100.0,
            output_cost_per_million_tokens=100.0,
        ),
        client=fake,
    )

    summary = report["summary"]
    cost_summary = report["cost_summary"]
    assert isinstance(summary, dict)
    assert isinstance(cost_summary, dict)
    assert report["status"] == "budget_exceeded"
    assert summary["budget_status"] == "BUDGET_EXCEEDED"
    assert summary["advisory_acceptable"] is False
    assert cost_summary["budget_status"] == "BUDGET_EXCEEDED"
    assert cost_summary["budget_violations"] == [
        "max_tokens_per_run",
        "max_latency_ms_per_run",
        "max_cost_per_run",
    ]
    assert report["can_execute_trades"] is False


def seed_evidence_db(tmp_path: Path) -> Path:
    export_external_evidence(
        tmp_path,
        [
            evidence("evidence-1", "source-a", 1_000, 1_100, 0.6),
            evidence("evidence-2", "source-b", 1_800, 1_900, -0.2),
        ],
    )
    db_path = tmp_path / "research.duckdb"
    create_duckdb_views(tmp_path, db_path)
    return db_path


def evidence(
    evidence_id: str,
    source: str,
    published_at_ms: int,
    available_at_ms: int,
    sentiment_score: float,
) -> dict[str, object]:
    return {
        "evidence_id": evidence_id,
        "source": source,
        "source_type": "news",
        "published_at_ms": published_at_ms,
        "observed_at_ms": available_at_ms,
        "available_at_ms": available_at_ms,
        "market_id": "market-1",
        "asset_id": "asset-yes",
        "raw_reference_hash": f"sha256:{evidence_id}",
        "direction": "YES" if sentiment_score > 0 else "NO",
        "sentiment_score": sentiment_score,
        "source_quality": 0.8,
        "confidence": 0.7,
        "data_version": "external_evidence_v1",
    }
