from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import duckdb
import pandas as pd  # type: ignore[import-untyped]

from src.config import settings
from src.research.llm.nim_client import (
    NIMAdvisoryResult,
    NIMResearchClient,
    NIMResearchConfig,
)
from src.research.sentiment_features import ensure_external_evidence_view


NIM_ADVISORY_REPORT_VERSION = "nim_advisory_offline_v1"
NIM_ADVISORY_DATA_VERSION = "external_evidence_v1"
NIM_ADVISORY_FEATURE_VERSION = "nim_evidence_annotations_v1"
DEFAULT_PROMPT_VERSION = "nim_evidence_advisory_prompt_v1"


class AdvisoryClient(Protocol):
    config: NIMResearchConfig

    def generate(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        temperature: float = 0.0,
        max_tokens: int = 512,
    ) -> NIMAdvisoryResult:
        ...


@dataclass(frozen=True)
class NIMAdvisoryConfig:
    enabled: bool = False
    limit: int = 25
    prompt_version: str = DEFAULT_PROMPT_VERSION


def export_nim_advisory_report(
    db_path: Path,
    output_dir: Path,
    config: NIMAdvisoryConfig | None = None,
    client: AdvisoryClient | None = None,
) -> dict[str, object]:
    resolved_config = config or NIMAdvisoryConfig(enabled=settings.enable_nim_advisory)
    if resolved_config.limit <= 0:
        raise ValueError("limit must be positive")
    output_dir.mkdir(parents=True, exist_ok=True)

    if not resolved_config.enabled:
        return write_outputs(
            output_dir,
            rows=[],
            report=base_report(
                resolved_config,
                status="disabled",
                reason="ENABLE_NIM_ADVISORY is false",
                nim_model=settings.nim_model,
                annotations=0,
                failures=0,
            ),
        )

    resolved_client = client or NIMResearchClient()
    evidence_rows = load_evidence_rows(db_path, resolved_config.limit)
    annotations: list[dict[str, object]] = []
    failures = 0
    for row in evidence_rows:
        try:
            result = resolved_client.generate(
                system_prompt=system_prompt(),
                user_prompt=evidence_prompt(row, resolved_config.prompt_version),
                temperature=0.0,
                max_tokens=512,
            )
        except Exception as exc:
            failures += 1
            annotations.append(error_annotation(row, resolved_config, str(exc)))
            continue
        annotations.append(result_annotation(row, resolved_config, result))

    status = "ok" if failures == 0 else "partial"
    report = base_report(
        resolved_config,
        status=status,
        reason=None,
        nim_model=resolved_client.config.model,
        annotations=len(annotations),
        failures=failures,
    )
    return write_outputs(output_dir, annotations, report)


def load_evidence_rows(db_path: Path, limit: int) -> list[dict[str, object]]:
    with duckdb.connect(str(db_path)) as conn:
        ensure_external_evidence_view(conn)
        rows = conn.execute(
            """
            select
                evidence_id,
                source,
                source_type,
                published_at_ms,
                observed_at_ms,
                available_at_ms,
                market_id,
                asset_id,
                direction,
                sentiment_score,
                source_quality,
                confidence,
                raw_reference_hash
            from external_evidence
            where evidence_id is not null
            order by available_at_ms, evidence_id
            limit ?
            """,
            [limit],
        ).fetchall()
    keys = (
        "evidence_id",
        "source",
        "source_type",
        "published_at_ms",
        "observed_at_ms",
        "available_at_ms",
        "market_id",
        "asset_id",
        "direction",
        "sentiment_score",
        "source_quality",
        "confidence",
        "raw_reference_hash",
    )
    return [dict(zip(keys, row)) for row in rows]


def system_prompt() -> str:
    return (
        "You are an offline research reviewer for a prediction-market trading system. "
        "Return concise JSON only. Do not recommend live trades. Allowed keys: "
        "direction, confidence, contradiction_score, rationale."
    )


def evidence_prompt(row: dict[str, object], prompt_version: str) -> str:
    return json.dumps(
        {
            "prompt_version": prompt_version,
            "task": "annotate_external_evidence_for_offline_research",
            "can_execute_trades": False,
            "evidence": row,
        },
        sort_keys=True,
    )


def result_annotation(
    row: dict[str, object],
    config: NIMAdvisoryConfig,
    result: NIMAdvisoryResult,
) -> dict[str, object]:
    parsed = parse_model_json(result.text)
    return {
        "advisory_id": advisory_id(row, config.prompt_version),
        "source_evidence_ids_hash": hash_json([row.get("evidence_id")]),
        "evidence_id": row.get("evidence_id"),
        "market_id": row.get("market_id"),
        "asset_id": row.get("asset_id"),
        "observed_at_ms": row.get("observed_at_ms"),
        "available_at_ms": row.get("available_at_ms"),
        "nim_model": result.model,
        "nim_model_version": result.model_version,
        "prompt_version": config.prompt_version,
        "output_hash": hash_text(result.text),
        "direction": normalized_string(parsed.get("direction"), default="UNKNOWN"),
        "confidence": normalized_float(parsed.get("confidence")),
        "contradiction_score": normalized_float(parsed.get("contradiction_score")),
        "rationale_reference_hash": hash_text(
            normalized_string(parsed.get("rationale"), default=result.text)
        ),
        "status": "OK",
        "error": None,
        "data_version": NIM_ADVISORY_DATA_VERSION,
        "feature_version": NIM_ADVISORY_FEATURE_VERSION,
        "decision_policy": "offline_advisory_only",
        "can_execute_trades": False,
    }


def error_annotation(
    row: dict[str, object], config: NIMAdvisoryConfig, error: str
) -> dict[str, object]:
    return {
        "advisory_id": advisory_id(row, config.prompt_version),
        "source_evidence_ids_hash": hash_json([row.get("evidence_id")]),
        "evidence_id": row.get("evidence_id"),
        "market_id": row.get("market_id"),
        "asset_id": row.get("asset_id"),
        "observed_at_ms": row.get("observed_at_ms"),
        "available_at_ms": row.get("available_at_ms"),
        "nim_model": settings.nim_model,
        "nim_model_version": "nvidia_nim_research_client_v1",
        "prompt_version": config.prompt_version,
        "output_hash": None,
        "direction": "UNKNOWN",
        "confidence": None,
        "contradiction_score": None,
        "rationale_reference_hash": None,
        "status": "ERROR",
        "error": error[:500],
        "data_version": NIM_ADVISORY_DATA_VERSION,
        "feature_version": NIM_ADVISORY_FEATURE_VERSION,
        "decision_policy": "offline_advisory_only",
        "can_execute_trades": False,
    }


def write_outputs(
    output_dir: Path,
    rows: list[dict[str, object]],
    report: dict[str, object],
) -> dict[str, object]:
    annotations_path = output_dir / "nim_advisory_annotations.parquet"
    summary_path = output_dir / "nim_advisory_summary.parquet"
    pd.DataFrame(rows, columns=annotation_columns()).to_parquet(
        annotations_path, index=False
    )
    counts = typed_dict(report.get("counts"))
    report_summary = typed_dict(report.get("summary"))
    summary = {
        "report_version": report["report_version"],
        "status": report["status"],
        "enabled": report["enabled"],
        "nim_model": report["nim_model"],
        "annotations": counts.get("nim_advisory_annotations"),
        "failures": report_summary.get("failures"),
        "can_execute_trades": False,
        "decision_policy": "offline_advisory_only",
    }
    pd.DataFrame([summary]).to_parquet(summary_path, index=False)
    report["outputs"] = [
        "nim_advisory_annotations.parquet",
        "nim_advisory_summary.parquet",
    ]
    (output_dir / "nim_advisory.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return report


def base_report(
    config: NIMAdvisoryConfig,
    *,
    status: str,
    reason: str | None,
    nim_model: str,
    annotations: int,
    failures: int,
) -> dict[str, object]:
    return {
        "report_version": NIM_ADVISORY_REPORT_VERSION,
        "model_version": "nvidia_nim_research_client_v1",
        "data_version": NIM_ADVISORY_DATA_VERSION,
        "feature_version": NIM_ADVISORY_FEATURE_VERSION,
        "prompt_version": config.prompt_version,
        "decision_policy": "offline_advisory_only",
        "can_execute_trades": False,
        "enabled": config.enabled,
        "status": status,
        "reason": reason,
        "nim_model": nim_model,
        "summary": {
            "annotations": annotations,
            "failures": failures,
            "advisory_acceptable": failures == 0,
            "can_execute_trades": False,
        },
        "counts": {"nim_advisory_annotations": annotations},
    }


def parse_model_json(text: str) -> dict[str, object]:
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def typed_dict(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


def normalized_string(value: object, default: str) -> str:
    return value if isinstance(value, str) and value else default


def normalized_float(value: object) -> float | None:
    return float(value) if isinstance(value, (int, float)) else None


def advisory_id(row: dict[str, object], prompt_version: str) -> str:
    return "nim-" + hash_json([row.get("evidence_id"), prompt_version])[:24]


def hash_json(value: object) -> str:
    return hash_text(json.dumps(value, sort_keys=True, default=str))


def hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def annotation_columns() -> list[str]:
    return [
        "advisory_id",
        "source_evidence_ids_hash",
        "evidence_id",
        "market_id",
        "asset_id",
        "observed_at_ms",
        "available_at_ms",
        "nim_model",
        "nim_model_version",
        "prompt_version",
        "output_hash",
        "direction",
        "confidence",
        "contradiction_score",
        "rationale_reference_hash",
        "status",
        "error",
        "data_version",
        "feature_version",
        "decision_policy",
        "can_execute_trades",
    ]


def main() -> int:
    parser = argparse.ArgumentParser(prog="research-nim-advisory")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--enabled", action="store_true")
    args = parser.parse_args()

    enabled = args.enabled or settings.enable_nim_advisory
    report = export_nim_advisory_report(
        Path(args.duckdb),
        Path(args.output_dir),
        NIMAdvisoryConfig(enabled=enabled, limit=args.limit),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
