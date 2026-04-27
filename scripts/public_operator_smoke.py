#!/usr/bin/env python3
"""Smoke test the public Operator dashboard reverse proxy.

The test is read-only except for control preview calls. It never sends
kill-switch, resume, cancel-bot-open, or cancel-all commands.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from typing import Any

import httpx


@dataclass(frozen=True)
class SmokeConfig:
    url: str
    read_token: str | None
    control_token: str | None
    timeout_seconds: float
    expect_execution_mode: str | None
    include_control_previews: bool
    skip_dashboard: bool


def parse_args() -> SmokeConfig:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url",
        default=os.getenv("PUBLIC_OPERATOR_URL") or os.getenv("OPERATOR_PUBLIC_URL"),
        help="Public same-origin dashboard URL, for example https://operator.example.com",
    )
    parser.add_argument("--read-token", default=os.getenv("OPERATOR_READ_TOKEN"))
    parser.add_argument("--control-token", default=os.getenv("OPERATOR_CONTROL_TOKEN"))
    parser.add_argument(
        "--timeout",
        type=float,
        default=float(os.getenv("OPERATOR_PROXY_SMOKE_TIMEOUT_SECONDS", "10")),
    )
    parser.add_argument(
        "--expect-execution-mode",
        default=os.getenv("EXPECTED_EXECUTION_MODE", "dry_run"),
    )
    parser.add_argument("--include-control-previews", action="store_true")
    parser.add_argument("--skip-dashboard", action="store_true")
    args = parser.parse_args()
    if not args.url:
        parser.error("--url or PUBLIC_OPERATOR_URL is required")
    return SmokeConfig(
        url=str(args.url).rstrip("/") + "/",
        read_token=args.read_token or None,
        control_token=args.control_token or None,
        timeout_seconds=args.timeout,
        expect_execution_mode=args.expect_execution_mode or None,
        include_control_previews=args.include_control_previews,
        skip_dashboard=args.skip_dashboard,
    )


def auth_headers(token: str | None) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"} if token else {}


def record_failure(
    failures: list[dict[str, object]],
    check: str,
    detail: str,
    **extra: object,
) -> None:
    payload: dict[str, object] = {"check": check, "detail": detail}
    payload.update(extra)
    failures.append(payload)


def get_json(
    client: httpx.Client,
    path: str,
    headers: dict[str, str],
    failures: list[dict[str, object]],
) -> dict[str, Any] | None:
    started = time.monotonic()
    response = client.get(path, headers=headers)
    latency_ms = round((time.monotonic() - started) * 1000, 2)
    if response.status_code != 200:
        record_failure(
            failures,
            path,
            "unexpected status",
            status_code=response.status_code,
            latency_ms=latency_ms,
            body=response.text[:500],
        )
        return None
    try:
        payload = response.json()
    except ValueError as exc:
        record_failure(failures, path, "invalid json", error=str(exc))
        return None
    if not isinstance(payload, dict):
        record_failure(failures, path, "payload is not an object")
        return None
    return payload


def check_dashboard(client: httpx.Client, failures: list[dict[str, object]]) -> None:
    response = client.get("/")
    if response.status_code != 200:
        record_failure(
            failures,
            "dashboard",
            "unexpected status",
            status_code=response.status_code,
            body=response.text[:500],
        )
        return
    if '<div id="root">' not in response.text:
        record_failure(failures, "dashboard", "React root mount node not found")


def check_health(client: httpx.Client, failures: list[dict[str, object]]) -> None:
    payload = get_json(client, "/api/health", {}, failures)
    if payload is not None and payload.get("status") != "ok":
        record_failure(failures, "api_health", "status is not ok", payload=payload)


def check_read_api(
    client: httpx.Client,
    config: SmokeConfig,
    failures: list[dict[str, object]],
) -> dict[str, object]:
    headers = auth_headers(config.read_token)
    summary: dict[str, object] = {}

    status_payload = get_json(client, "/api/status", headers, failures)
    if status_payload is not None:
        summary["kill_switch"] = status_payload.get("kill_switch")
        if status_payload.get("status") != "ok":
            record_failure(failures, "api_status", "status is not ok", payload=status_payload)

    streams_payload = get_json(client, "/api/streams", headers, failures)
    if streams_payload is not None:
        streams = streams_payload.get("streams")
        if not isinstance(streams, list):
            record_failure(failures, "api_streams", "streams is not a list")
        else:
            summary["stream_count"] = len(streams)
            summary["stream_lengths"] = {
                str(item.get("stream")): item.get("length")
                for item in streams
                if isinstance(item, dict)
            }

    risk_payload = get_json(client, "/api/risk", headers, failures)
    if risk_payload is not None:
        summary["execution_mode"] = risk_payload.get("execution_mode")
        if (
            config.expect_execution_mode
            and risk_payload.get("execution_mode") != config.expect_execution_mode
        ):
            record_failure(
                failures,
                "api_risk",
                "execution mode mismatch",
                expected=config.expect_execution_mode,
                actual=risk_payload.get("execution_mode"),
            )

    for path in (
        "/api/orders/open",
        "/api/positions",
        "/api/execution-reports?limit=5",
        "/api/control/results?limit=5",
        "/api/reconciliation/status?limit=5",
        "/api/metrics?limit=100",
    ):
        get_json(client, path, headers, failures)

    metrics_response = client.get("/api/metrics/prometheus", headers=headers)
    if metrics_response.status_code != 200:
        record_failure(
            failures,
            "api_metrics_prometheus",
            "unexpected status",
            status_code=metrics_response.status_code,
            body=metrics_response.text[:500],
        )
    elif (
        "execution_reports_by_status" not in metrics_response.text
        and "control_results_by_type" not in metrics_response.text
    ):
        record_failure(
            failures,
            "api_metrics_prometheus",
            "expected core metric names not found",
        )

    return summary


def check_negative_auth(
    client: httpx.Client,
    config: SmokeConfig,
    failures: list[dict[str, object]],
) -> None:
    if not config.read_token:
        return
    response = client.get("/api/status")
    if response.status_code != 401:
        record_failure(
            failures,
            "read_auth_negative",
            "status without token should be rejected when tokens are configured",
            status_code=response.status_code,
        )


def check_control_previews(
    client: httpx.Client,
    config: SmokeConfig,
    failures: list[dict[str, object]],
) -> None:
    if not config.include_control_previews:
        return
    if not config.control_token:
        record_failure(
            failures,
            "control_preview",
            "--include-control-previews requires OPERATOR_CONTROL_TOKEN",
        )
        return

    no_auth_response = client.post("/api/control/preview/cancel-all")
    if no_auth_response.status_code != 401:
        record_failure(
            failures,
            "control_preview_auth_negative",
            "control preview without token should be rejected",
            status_code=no_auth_response.status_code,
        )

    control_response = client.post(
        "/api/control/preview/cancel-all", headers=auth_headers(config.control_token)
    )
    if control_response.status_code not in {200, 503}:
        record_failure(
            failures,
            "control_preview_cancel_all",
            "unexpected status",
            status_code=control_response.status_code,
            body=control_response.text[:500],
        )


def run_smoke(config: SmokeConfig) -> dict[str, object]:
    failures: list[dict[str, object]] = []
    summary: dict[str, object] = {}
    with httpx.Client(base_url=config.url, timeout=config.timeout_seconds) as client:
        if not config.skip_dashboard:
            check_dashboard(client, failures)
        check_health(client, failures)
        summary = check_read_api(client, config, failures)
        check_negative_auth(client, config, failures)
        check_control_previews(client, config, failures)

    return {
        "status": "ok" if not failures else "failed",
        "url": config.url.rstrip("/"),
        "checked_control_previews": config.include_control_previews,
        "summary": summary,
        "failures": failures,
    }


def main() -> int:
    result = run_smoke(parse_args())
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
