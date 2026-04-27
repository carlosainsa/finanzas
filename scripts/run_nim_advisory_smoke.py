#!/usr/bin/env python3
from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path

import duckdb

from src.config import settings
from src.research.data_lake import create_duckdb_views, export_external_evidence
from src.research.nim_advisory import NIMAdvisoryConfig, export_nim_advisory_report


def main() -> int:
    if not settings.nvidia_nim_api_key:
        raise SystemExit("NVIDIA_NIM_API_KEY is not configured in environment or .env")

    root = Path(tempfile.mkdtemp(prefix="nim-advisory-smoke-"))
    lake = root / "lake"
    db_path = root / "research.duckdb"
    output_dir = root / "out"
    now_ms = int(time.time() * 1000)
    export_external_evidence(
        lake,
        [
            {
                "evidence_id": "nim-smoke-evidence-1",
                "source": "local-smoke",
                "source_type": "other",
                "published_at_ms": now_ms - 1_000,
                "observed_at_ms": now_ms,
                "available_at_ms": now_ms,
                "market_id": "smoke-market",
                "asset_id": "smoke-asset",
                "raw_reference_hash": "sha256:smoke",
                "direction": "YES",
                "sentiment_score": 0.1,
                "source_quality": 0.5,
                "confidence": 0.5,
                "data_version": "external_evidence_v1",
            }
        ],
    )
    create_duckdb_views(lake, db_path)
    report = export_nim_advisory_report(
        db_path,
        output_dir,
        NIMAdvisoryConfig(
            enabled=True,
            limit=1,
            max_evidence_per_run=1,
            input_cost_per_million_tokens=settings.nim_input_cost_per_million_tokens,
            output_cost_per_million_tokens=settings.nim_output_cost_per_million_tokens,
            cost_currency=settings.nim_cost_currency,
        ),
    )
    with duckdb.connect(str(db_path)) as conn:
        flags = conn.execute(
            f"""
            select decision_policy, can_execute_trades, status
            from read_parquet('{(output_dir / "nim_advisory_annotations.parquet").as_posix()}')
            """
        ).fetchall()

    if report["can_execute_trades"] is not False:
        raise SystemExit("NIM advisory report unexpectedly allowed trading")
    if flags != [("offline_advisory_only", False, "OK")]:
        raise SystemExit(f"unexpected NIM advisory flags: {flags}")
    if report["summary"]["budget_status"] != "OK":
        raise SystemExit(f"unexpected NIM budget status: {report['summary']['budget_status']}")

    print(
        json.dumps(
            {
                "smoke": "ok",
                "workdir": str(root),
                "status": report["status"],
                "annotations": report["counts"]["nim_advisory_annotations"],
                "can_execute_trades": report["can_execute_trades"],
                "total_tokens": report["summary"]["total_tokens"],
                "latency_ms_avg": report["summary"]["latency_ms_avg"],
                "estimated_cost": report["summary"]["estimated_cost"],
                "budget_status": report["summary"]["budget_status"],
                "annotation_flags": flags,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
