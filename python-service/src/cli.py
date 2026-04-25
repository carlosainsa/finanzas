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
        print_table(response)
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
    control_results = subparsers.add_parser("control-results")
    control_results.add_argument("--limit", type=int)
    cancel_all = subparsers.add_parser("cancel-all")
    cancel_all.add_argument("--reason", required=True)
    cancel_all.add_argument("--operator")
    cancel_all.add_argument("--confirm", action="store_true")
    cancel_all.add_argument("--confirmation-phrase", required=True)
    cancel_bot_open = subparsers.add_parser("cancel-bot-open")
    cancel_bot_open.add_argument("--reason", required=True)
    cancel_bot_open.add_argument("--operator")
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
        if args.command == "control-results":
            return request_json(
                client,
                "GET",
                "/control/results",
                params=optional_params({"limit": args.limit}),
                token=read_token(args),
            )
        if args.command == "cancel-all":
            if not args.confirm:
                raise SystemExit("cancel-all requires --confirm")
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
