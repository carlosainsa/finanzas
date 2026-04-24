# Operator Interface Plan

The official v1 interface is **Operator API + CLI**. There is no dashboard web in this phase.

## Current API

Implemented in `python-service/src/api/app.py`:

| Method | Path | Status | Purpose |
| --- | --- | --- | --- |
| `GET` | `/health` | Existing | Redis-backed service health check. |
| `GET` | `/status` | Existing | Stream lengths and predictor settings. |
| `GET` | `/risk` | Existing | Read-only note that Rust enforces risk. |

## Planned Operator API

These endpoints should be added after the state model is stable:

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/control/kill-switch` | Enable kill switch and force Rust to reject new signals. |
| `POST` | `/control/resume` | Disable kill switch after operator confirmation. |
| `POST` | `/orders/cancel-all` | Cancel open Polymarket orders through Rust execution controls. |
| `GET` | `/orders/open` | Return open orders from Postgres plus latest CLOB reconciliation. |
| `GET` | `/positions` | Return positions, balances, and market exposure. |
| `GET` | `/streams` | Return Redis Stream lengths, pending counts, and dead-letter counts. |

API responses should be JSON-first and include enough IDs for debugging: `signal_id`, `order_id`, `market_id`, and `asset_id` where applicable.

## Planned CLI

The CLI should target operators and scripts. It should call the Operator API first and only read Redis/Postgres directly if the API lacks a capability.

| Command | Purpose |
| --- | --- |
| `status` | Show service status, stream health, and current execution mode. |
| `risk` | Show kill switch, exposure, max order size, max daily loss, and stale-signal window. |
| `streams` | Show Redis Stream length, pending count, and dead-letter count. |
| `orders` | List open orders. |
| `cancel-all` | Cancel open orders after explicit confirmation. |
| `kill-switch on` | Enable the kill switch. |
| `kill-switch off` | Resume trading after operator confirmation. |

Output modes:

- `table` for humans.
- `json` for scripts and agents.

## Web Dashboard

A dashboard web is intentionally deferred. It should be built only after the Operator API supports status, risk, streams, orders, positions, and controls.

See [architecture_plan.md](architecture_plan.md) for system context and [implementation_roadmap.md](implementation_roadmap.md) for phase ordering.
