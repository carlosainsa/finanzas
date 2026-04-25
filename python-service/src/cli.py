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
    parser.add_argument("--output", choices=("table", "json"), default="table")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("status")
    subparsers.add_parser("risk")
    subparsers.add_parser("streams")
    subparsers.add_parser("orders")
    subparsers.add_parser("positions")
    subparsers.add_parser("cancel-all")
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
    headers = {"Authorization": f"Bearer {args.token}"} if args.token else None
    with httpx.Client(base_url=api_url, timeout=10.0, headers=headers) as client:
        if args.command == "status":
            return request_json(client, "GET", "/status")
        if args.command == "risk":
            return request_json(client, "GET", "/risk")
        if args.command == "streams":
            return request_json(client, "GET", "/streams")
        if args.command == "orders":
            return request_json(client, "GET", "/orders/open")
        if args.command == "positions":
            return request_json(client, "GET", "/positions")
        if args.command == "cancel-all":
            return request_json(
                client,
                "POST",
                "/orders/cancel-all",
                json={"reason": "operator cancel all", "operator": "cli"},
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
            return request_json(client, "GET", "/markets/discover", params=params)
        if args.command == "kill-switch":
            return dispatch_kill_switch(client, args)
    raise ValueError(f"unsupported command: {args.command}")


def dispatch_kill_switch(client: httpx.Client, args: argparse.Namespace) -> JsonObject:
    payload = {
        "reason": args.reason,
        "operator": args.operator,
    }
    if args.state == "on":
        return request_json(client, "POST", "/control/kill-switch", json=payload)
    if not args.confirm:
        raise SystemExit("kill-switch off requires --confirm")
    payload["confirm"] = True
    return request_json(client, "POST", "/control/resume", json=payload)


def request_json(
    client: httpx.Client,
    method: str,
    path: str,
    json: dict[str, object] | None = None,
    params: dict[str, QueryValue] | None = None,
) -> JsonObject:
    response = client.request(method, path, json=json, params=params)
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
