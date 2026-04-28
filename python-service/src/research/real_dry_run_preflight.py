import argparse
import asyncio
import hashlib
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import redis.asyncio as redis


REPORT_VERSION = "real_dry_run_preflight_v1"
PREFLIGHT_FAILURE_EXIT_CODE = 75
VALID_DRY_RUN_STATUSES = {"DELAYED", "UNMATCHED", "MATCHED", "PARTIAL"}


def build_preflight_report(
    *,
    run_id: str,
    started_at: str,
    finished_at: str,
    elapsed_seconds: float,
    stream_names: dict[str, str],
    start_lengths: dict[str, int],
    end_lengths: dict[str, int],
    recent_reports: list[dict[str, object]],
    require_reports: bool,
    market_asset_ids: list[str],
    blocked_segments_path: str | None,
    check_seconds: int,
    capture_seconds: int,
) -> dict[str, object]:
    stream_progress = {
        name: {
            "stream": stream_names[name],
            "start_length": start_lengths.get(name, 0),
            "end_length": end_lengths.get(name, 0),
            "delta": end_lengths.get(name, 0) - start_lengths.get(name, 0),
            "required_min_delta": 1,
        }
        for name in ("orderbook", "signals", "reports")
    }
    dry_run_report_found = any(
        str(item.get("order_id", "")).startswith("dry-run-")
        for item in recent_reports
    )
    valid_report_status_seen = any(
        item.get("status") in VALID_DRY_RUN_STATUSES for item in recent_reports
    )
    blockers = preflight_blockers(
        stream_progress=stream_progress,
        dry_run_report_found=dry_run_report_found,
        valid_report_status_seen=valid_report_status_seen,
        require_reports=require_reports,
    )
    market_asset_ids_csv = ",".join(market_asset_ids)
    status = "ok" if not blockers else "failed"
    return {
        "report_version": REPORT_VERSION,
        "status": status,
        "classification": "ok" if status == "ok" else "preflight_no_stream_progress",
        "started_at": started_at,
        "finished_at": finished_at,
        "elapsed_seconds": elapsed_seconds,
        "run_id": run_id,
        "check_seconds": check_seconds,
        "capture_seconds": capture_seconds,
        "streams": stream_progress,
        "blockers": blockers,
        "dry_run_report_found": dry_run_report_found,
        "valid_report_status_seen": valid_report_status_seen,
        "recent_report_status_counts": report_status_counts(recent_reports),
        "market_asset_ids_count": len(market_asset_ids),
        "market_asset_ids_sha256": hashlib.sha256(
            market_asset_ids_csv.encode("utf-8")
        ).hexdigest(),
        "blocked_segments_enabled": blocked_segments_path is not None,
        "blocked_segments_path": blocked_segments_path,
        "recommendation": preflight_recommendation(blockers),
        "exit_code": 0 if status == "ok" else PREFLIGHT_FAILURE_EXIT_CODE,
        "can_execute_trades": False,
    }


def preflight_blockers(
    *,
    stream_progress: dict[str, dict[str, object]],
    dry_run_report_found: bool,
    valid_report_status_seen: bool,
    require_reports: bool,
) -> list[str]:
    blockers: list[str] = []
    for name in ("orderbook", "signals"):
        if numeric(stream_progress[name].get("delta")) < 1:
            blockers.append(f"missing_{name}_stream_progress")
    if require_reports and numeric(stream_progress["reports"].get("delta")) < 1:
        blockers.append("missing_execution_reports_stream_progress")
    if require_reports and not dry_run_report_found:
        blockers.append("missing_dry_run_execution_report")
    if require_reports and not valid_report_status_seen:
        blockers.append("missing_valid_dry_run_report_status")
    return sorted(set(blockers))


def preflight_recommendation(blockers: list[str]) -> str:
    if not blockers:
        return "continue_capture"
    if "missing_orderbook_stream_progress" in blockers:
        return "repair_market_data_pipeline_before_repeat"
    if "missing_signals_stream_progress" in blockers:
        return "repair_predictor_pipeline_before_repeat"
    return "repair_executor_pipeline_before_repeat"


def report_status_counts(reports: list[dict[str, object]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in reports:
        status = str(item.get("status", "UNKNOWN"))
        counts[status] = counts.get(status, 0) + 1
    return counts


async def collect_stream_lengths(
    client: redis.Redis,
    stream_names: dict[str, str],
) -> dict[str, int]:
    return {
        name: int(await client.xlen(stream))
        for name, stream in stream_names.items()
    }


async def collect_recent_reports(
    client: redis.Redis,
    reports_stream: str,
    count: int = 100,
) -> list[dict[str, object]]:
    rows = await client.xrevrange(reports_stream, count=count)
    parsed: list[dict[str, object]] = []
    for _, fields in rows:
        payload = fields.get("payload")
        if not isinstance(payload, str):
            continue
        try:
            value = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            parsed.append(value)
    return parsed


async def wait_for_preflight(
    *,
    redis_url: str,
    output: Path,
    stream_names: dict[str, str],
    check_seconds: int,
    poll_seconds: int,
    require_reports: bool,
    market_asset_ids: list[str],
    blocked_segments_path: str | None,
    run_id: str,
    capture_seconds: int,
) -> dict[str, object]:
    client = redis.from_url(redis_url, decode_responses=True)
    started_at = datetime.now(UTC).isoformat()
    started = time.monotonic()
    start_lengths = await collect_stream_lengths(client, stream_names)
    end_lengths = start_lengths
    recent_reports: list[dict[str, object]] = []
    deadline = started + max(0, check_seconds)
    while True:
        await asyncio.sleep(min(max(1, poll_seconds), max(0.0, deadline - time.monotonic())))
        end_lengths = await collect_stream_lengths(client, stream_names)
        recent_reports = await collect_recent_reports(client, stream_names["reports"])
        candidate = build_preflight_report(
            run_id=run_id,
            started_at=started_at,
            finished_at=datetime.now(UTC).isoformat(),
            elapsed_seconds=time.monotonic() - started,
            stream_names=stream_names,
            start_lengths=start_lengths,
            end_lengths=end_lengths,
            recent_reports=recent_reports,
            require_reports=require_reports,
            market_asset_ids=market_asset_ids,
            blocked_segments_path=blocked_segments_path,
            check_seconds=check_seconds,
            capture_seconds=capture_seconds,
        )
        if candidate["status"] == "ok" or time.monotonic() >= deadline:
            await client.aclose()
            write_json_atomic(output, candidate)
            return candidate


def numeric(value: object) -> float:
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return 0.0


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(prog="real-dry-run-preflight")
    parser.add_argument("--redis-url", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--check-seconds", type=int, default=120)
    parser.add_argument("--poll-seconds", type=int, default=5)
    parser.add_argument("--capture-seconds", type=int, default=900)
    parser.add_argument("--require-reports", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    stream_names = {
        "orderbook": os.getenv("ORDERBOOK_STREAM", "orderbook:stream"),
        "signals": os.getenv("SIGNALS_STREAM", "signals:stream"),
        "reports": os.getenv("EXECUTION_REPORTS_STREAM", "execution:reports:stream"),
    }
    payload = asyncio.run(
        wait_for_preflight(
            redis_url=args.redis_url,
            output=args.output,
            stream_names=stream_names,
            check_seconds=args.check_seconds,
            poll_seconds=args.poll_seconds,
            require_reports=args.require_reports,
            market_asset_ids=parse_csv(os.getenv("MARKET_ASSET_IDS", "")),
            blocked_segments_path=os.getenv("PREDICTOR_BLOCKED_SEGMENTS_PATH"),
            run_id=os.getenv("REPORT_TIMESTAMP", "unknown"),
            capture_seconds=args.capture_seconds,
        )
    )
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload["status"] == "ok" else PREFLIGHT_FAILURE_EXIT_CODE


if __name__ == "__main__":
    raise SystemExit(main())
