# Top Tier+ Architecture Plan

This document turns the external repo analysis in [repo_ideas.md](repo_ideas.md) into the target architecture for this project.

## Current State

- **Rust engine** handles Polymarket market WebSocket ingestion, Redis Stream publishing, trade signal consumption, risk gating, operator control commands, optional Postgres persistence, and dry-run/live execution.
- **Python service** consumes normalized orderbooks from Redis Streams, validates schemas with Pydantic, runs the predictor, and publishes trade signals.
- **Redis Streams** are the internal event bus:
  - `orderbook:stream`
  - `signals:stream`
  - `execution:reports:stream`
  - `operator:commands:stream`
  - `operator:results:stream`
  - `orderbook:deadletter`
  - `signals:deadletter`
- **Postgres** is optional today and should become the recommended state store for real operation.
- **FastAPI** exposes Operator API endpoints, optional bearer auth, `/api/*` browser aliases, and serves the built dashboard when `frontend/dist` exists.
- **Operator controls** distinguish bot-scoped cancellation from emergency account-wide cancellation.
- **Production mode** is explicit through `APP_ENV=production`; Python and Rust both fail startup if required production settings are missing.
- **Observability** includes JSON logs in Rust, runtime JSON metrics, Prometheus metrics, and command result streams.

## Target Architecture

The next stage should improve trading quality before adding a web dashboard.

1. **Market data local book**
   - Maintain a per-asset book from `book` snapshots plus `price_change` deltas.
   - Publish normalized snapshots only after ordering, liquidity, and staleness checks.
   - Derive second-best levels, size-aware best bid/ask, midpoint depth, and stale-book status.

2. **Execution reconciliation**
   - Add authenticated user WebSocket ingestion for `order` and `trade` events.
   - Reconcile open orders and fills faster than REST polling.
   - Persist order lifecycle events idempotently by `order_id` and `signal_id`.

3. **Risk and state**
   - Keep deterministic risk gates in Rust before any live execution.
   - Promote Postgres from optional convenience to recommended production dependency.
   - Store signals, orders, reports, positions, balances, and risk snapshots.

4. **Operator interface**
   - Build Operator API and CLI as the official v1 interface.
   - Serve the dashboard from FastAPI after the API and state model are stable.
   - Protect operator routes with bearer auth when role tokens are configured.
   - Prefer `OPERATOR_READ_TOKEN` and `OPERATOR_CONTROL_TOKEN` for production role separation.
   - Keep read-only dashboard sessions unable to trigger control actions.

5. **Research loop**
   - Add Parquet/DuckDB data lake for historical market data and strategy evaluation.
   - Add market discovery and evidence scoring after execution and state are reliable.

## Priorities

1. Local orderbook with `price_change` support.
2. User WebSocket reconciliation.
3. Operator API + CLI.
4. Research data lake with Parquet/DuckDB.
5. Market discovery and evidence scoring.
6. Optional dashboard web, served through FastAPI.

See [interface_plan.md](interface_plan.md) for the API/CLI surface and [implementation_roadmap.md](implementation_roadmap.md) for execution phases.
