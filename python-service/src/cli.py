import argparse
import json
import sys
from typing import Any

import httpx

from src.config import settings


JsonObject = dict[str, Any]
QueryValue = str | int | float | bool


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        response = dispatch(args)
    except httpx.HTTPError as exc:
        print(f"operator API request failed: {exc}", file=sys.stderr)
        return 1

    if args.output == "json":
        print(json.dumps(response, indent=2, sort_keys=True))
    else:
        print_command_table(args.command, response)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="polymarket-operator")
    parser.add_argument("--api-url", default=settings.operator_api_url)
    parser.add_argument("--token", default=settings.operator_api_token)
    parser.add_argument("--read-token", default=settings.operator_read_token)
    parser.add_argument("--control-token", default=settings.operator_control_token)
    parser.add_argument("--output", choices=("table", "json"), default="table")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("status")
    subparsers.add_parser("risk")
    subparsers.add_parser("streams")
    subparsers.add_parser("orders")
    subparsers.add_parser("positions")
    subparsers.add_parser("metrics")
    subparsers.add_parser("nim-budget")
    subparsers.add_parser("research-go-no-go")
    subparsers.add_parser("pre-live-readiness")
    research_runs = subparsers.add_parser("research-runs")
    research_runs.add_argument("--limit", type=int)
    research_run = subparsers.add_parser("research-run")
    research_run.add_argument("run_id")
    reconciliation = subparsers.add_parser("reconciliation")
    reconciliation.add_argument("--limit", type=int)
    control_results = subparsers.add_parser("control-results")
    control_results.add_argument("--limit", type=int)
    cancel_all = subparsers.add_parser("cancel-all")
    cancel_all.add_argument("--reason", required=True)
    cancel_all.add_argument("--operator")
    cancel_all.add_argument("--confirm", action="store_true")
    cancel_all.add_argument("--confirmation-phrase")
    cancel_all.add_argument("--preview", action="store_true")
    cancel_bot_open = subparsers.add_parser("cancel-bot-open")
    cancel_bot_open.add_argument("--reason", required=True)
    cancel_bot_open.add_argument("--operator")
    cancel_bot_open.add_argument("--preview", action="store_true")
    discover = subparsers.add_parser("discover-markets")
    discover.add_argument("--limit", type=int)
    discover.add_argument("--query")
    discover.add_argument("--min-liquidity", type=float)
    discover.add_argument("--min-volume", type=float)

    kill_switch = subparsers.add_parser("kill-switch")
    kill_switch.add_argument("state", choices=("on", "off"))
    kill_switch.add_argument("--reason", required=True)
    kill_switch.add_argument("--operator")
    kill_switch.add_argument("--confirm", action="store_true")

    return parser


def dispatch(args: argparse.Namespace) -> JsonObject:
    api_url = str(args.api_url).rstrip("/")
    with httpx.Client(base_url=api_url, timeout=10.0) as client:
        if args.command == "status":
            return request_json(client, "GET", "/status", token=read_token(args))
        if args.command == "risk":
            return request_json(client, "GET", "/risk", token=read_token(args))
        if args.command == "streams":
            return request_json(client, "GET", "/streams", token=read_token(args))
        if args.command == "orders":
            return request_json(client, "GET", "/orders/open", token=read_token(args))
        if args.command == "positions":
            return request_json(client, "GET", "/positions", token=read_token(args))
        if args.command == "metrics":
            return request_json(client, "GET", "/metrics", token=read_token(args))
        if args.command == "nim-budget":
            return request_json(client, "GET", "/research/nim-budget", token=read_token(args))
        if args.command == "research-go-no-go":
            return request_json(client, "GET", "/research/go-no-go", token=read_token(args))
        if args.command == "pre-live-readiness":
            return request_json(
                client,
                "GET",
                "/research/pre-live-readiness",
                token=read_token(args),
            )
        if args.command == "research-runs":
            return request_json(
                client,
                "GET",
                "/research/runs",
                params=optional_params({"limit": args.limit}),
                token=read_token(args),
            )
        if args.command == "research-run":
            return request_json(
                client,
                "GET",
                f"/research/runs/{args.run_id}",
                token=read_token(args),
            )
        if args.command == "reconciliation":
            return request_json(
                client,
                "GET",
                "/reconciliation/status",
                params=optional_params({"limit": args.limit}),
                token=read_token(args),
            )
        if args.command == "control-results":
            return request_json(
                client,
                "GET",
                "/control/results",
                params=optional_params({"limit": args.limit}),
                token=read_token(args),
            )
        if args.command == "cancel-all":
            if args.preview:
                return request_json(
                    client,
                    "POST",
                    "/control/preview/cancel-all",
                    token=control_token(args),
                )
            if not args.confirm:
                raise SystemExit("cancel-all requires --confirm")
            if args.confirmation_phrase is None:
                raise SystemExit("cancel-all requires --confirmation-phrase")
            return request_json(
                client,
                "POST",
                "/orders/cancel-all",
                json={
                    "reason": args.reason,
                    "operator": args.operator,
                    "confirm": True,
                    "confirmation_phrase": args.confirmation_phrase,
                },
                token=control_token(args),
            )
        if args.command == "cancel-bot-open":
            if args.preview:
                return request_json(
                    client,
                    "POST",
                    "/control/preview/cancel-bot-open",
                    token=control_token(args),
                )
            return request_json(
                client,
                "POST",
                "/orders/cancel-bot-open",
                json={"reason": args.reason, "operator": args.operator},
                token=control_token(args),
            )
        if args.command == "discover-markets":
            params = optional_params(
                {
                    "limit": args.limit,
                    "query": args.query,
                    "min_liquidity": args.min_liquidity,
                    "min_volume": args.min_volume,
                }
            )
            return request_json(
                client, "GET", "/markets/discover", params=params, token=read_token(args)
            )
        if args.command == "kill-switch":
            return dispatch_kill_switch(client, args)
    raise ValueError(f"unsupported command: {args.command}")


def dispatch_kill_switch(client: httpx.Client, args: argparse.Namespace) -> JsonObject:
    payload = {
        "reason": args.reason,
        "operator": args.operator,
    }
    if args.state == "on":
        return request_json(
            client, "POST", "/control/kill-switch", json=payload, token=control_token(args)
        )
    if not args.confirm:
        raise SystemExit("kill-switch off requires --confirm")
    payload["confirm"] = True
    return request_json(
        client, "POST", "/control/resume", json=payload, token=control_token(args)
    )


def request_json(
    client: httpx.Client,
    method: str,
    path: str,
    json: dict[str, object] | None = None,
    params: dict[str, QueryValue] | None = None,
    token: str | None = None,
) -> JsonObject:
    headers = {"Authorization": f"Bearer {token}"} if token else None
    response = client.request(method, path, json=json, params=params, headers=headers)
    if response.status_code >= 400:
        raise httpx.HTTPStatusError(
            f"{method} {path} returned {response.status_code}: {response.text}",
            request=response.request,
            response=response,
        )
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("operator API returned a non-object response")
    return payload


def optional_params(params: dict[str, QueryValue | None]) -> dict[str, QueryValue]:
    return {key: value for key, value in params.items() if value is not None}


def read_token(args: argparse.Namespace) -> str | None:
    return args.read_token or args.token or args.control_token


def control_token(args: argparse.Namespace) -> str | None:
    return args.control_token or args.token


def print_command_table(command: str, value: object) -> None:
    if command == "orders" and isinstance(value, dict):
        print_rows(
            value.get("orders"),
            ["order_id", "status", "filled_size", "remaining_size", "signal_id"],
        )
        return
    if command == "positions" and isinstance(value, dict):
        print_rows(value.get("positions"), ["market_id", "asset_id", "position"])
        return
    if command == "control-results" and isinstance(value, dict):
        print_rows(
            value.get("results"),
            [
                "command_id",
                "command_type",
                "status",
                "operator",
                "reason",
                "completed_at_ms",
                "error",
            ],
        )
        return
    if command in {"cancel-all", "cancel-bot-open"} and isinstance(value, dict):
        if "affected_orders" in value:
            print_rows(
                [
                    {
                        "command_type": value.get("command_type"),
                        "scope": value.get("scope"),
                        "affected_count": value.get("affected_count"),
                        "source": value.get("source"),
                        "warnings": value.get("warnings"),
                    }
                ],
                ["command_type", "scope", "affected_count", "source", "warnings"],
            )
            return
    if command == "metrics" and isinstance(value, dict):
        print_metrics(value)
        return
    if command == "nim-budget" and isinstance(value, dict):
        print_rows(
            [
                {
                    "status": value.get("status"),
                    "budget_status": value.get("budget_status"),
                    "run_id": value.get("run_id"),
                    "model": value.get("nim_model"),
                    "tokens": value.get("total_tokens"),
                    "latency_ms_avg": value.get("latency_ms_avg"),
                    "estimated_cost": value.get("estimated_cost"),
                    "violations": value.get("budget_violations"),
                }
            ],
            [
                "status",
                "budget_status",
                "run_id",
                "model",
                "tokens",
                "latency_ms_avg",
                "estimated_cost",
                "violations",
            ],
        )
        return
    if command == "research-runs" and isinstance(value, dict):
        print_rows(
            value.get("runs"),
            [
                "run_id",
                "created_at",
                "passed",
                "realized_edge",
                "fill_rate",
                "nim_budget_status",
                "nim_total_tokens",
            ],
        )
        return
    if command == "research-go-no-go" and isinstance(value, dict):
        print_rows(
            [
                {
                    "run_id": value.get("run_id"),
                    "decision": value.get("decision"),
                    "passed": value.get("passed"),
                    "reason": value.get("reason"),
                    "blockers": [
                        item.get("check_name")
                        for item in value.get("blockers", [])
                        if isinstance(item, dict)
                    ],
                }
            ],
            ["run_id", "decision", "passed", "reason", "blockers"],
        )
        return
    if command == "pre-live-readiness" and isinstance(value, dict):
        go_no_go_value = value.get("go_no_go")
        audit_value = value.get("audit")
        go_no_go: dict[str, object] = (
            go_no_go_value if isinstance(go_no_go_value, dict) else {}
        )
        audit: dict[str, object] = audit_value if isinstance(audit_value, dict) else {}
        print_rows(
            [
                {
                    "status": value.get("status"),
                    "run_id": value.get("run_id"),
                    "profile": go_no_go.get("profile"),
                    "decision": go_no_go.get("decision"),
                    "audit": audit.get("status"),
                    "blockers": len(value.get("blockers", []))
                    if isinstance(value.get("blockers"), list)
                    else 0,
                }
            ],
            ["status", "run_id", "profile", "decision", "audit", "blockers"],
        )
        return
    if command == "research-run" and isinstance(value, dict):
        print_table(value)
        return
    if command == "reconciliation" and isinstance(value, dict):
        print_rows(
            [
                {
                    "status": value.get("status"),
                    "open_local_orders": value.get("open_local_orders"),
                    "pending_cancel_requests": value.get("pending_cancel_requests"),
                    "diverged_cancel_requests": value.get("diverged_cancel_requests"),
                    "stale_orders": value.get("stale_orders"),
                    "source": value.get("source"),
                }
            ],
            [
                "status",
                "open_local_orders",
                "pending_cancel_requests",
                "diverged_cancel_requests",
                "stale_orders",
                "source",
            ],
        )
        return
    print_table(value)


def print_rows(value: object, columns: list[str]) -> None:
    if not isinstance(value, list) or not value:
        print("(empty)")
        return
    rows = [row for row in value if isinstance(row, dict)]
    if not rows:
        print("(empty)")
        return
    widths = {
        column: max(len(column), *(len(format_cell(row.get(column))) for row in rows))
        for column in columns
    }
    print(" | ".join(column.ljust(widths[column]) for column in columns))
    print("-+-".join("-" * widths[column] for column in columns))
    for row in rows:
        print(
            " | ".join(
                format_cell(row.get(column)).ljust(widths[column]) for column in columns
            )
        )


def print_metrics(value: dict[str, object]) -> None:
    scalar_keys = [
        "signals_received",
        "signals_rejected",
        "orders_submitted",
        "clob_errors",
        "execution_reports",
        "control_results",
        "ws_to_signal_latency_ms",
        "signal_to_order_latency_ms",
        "order_to_report_latency_ms",
        "ws_to_report_latency_ms",
    ]
    for key in scalar_keys:
        if key in value:
            print(f"{key}: {format_cell(value.get(key))}")
    for key in (
        "execution_reports_by_status",
        "control_results_by_type",
        "clob_errors_by_type",
    ):
        item = value.get(key)
        if isinstance(item, dict) and item:
            print(f"{key}:")
            print_rows(
                [{"label": label, "value": count} for label, count in sorted(item.items())],
                ["label", "value"],
            )


def format_cell(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6g}"
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return str(value)


def print_table(value: object, indent: int = 0) -> None:
    prefix = " " * indent
    if isinstance(value, dict):
        for key, item in value.items():
            if isinstance(item, (dict, list)):
                print(f"{prefix}{key}:")
                print_table(item, indent + 2)
            else:
                print(f"{prefix}{key}: {item}")
        return
    if isinstance(value, list):
        if not value:
            print(f"{prefix}(empty)")
            return
        for item in value:
            if isinstance(item, dict):
                fields = " | ".join(f"{key}={field}" for key, field in item.items())
                print(f"{prefix}{fields}")
            else:
                print(f"{prefix}{item}")
        return
    print(f"{prefix}{value}")


if __name__ == "__main__":
    raise SystemExit(main())
