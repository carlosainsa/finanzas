# Implementation Roadmap

This roadmap converts [repo_ideas.md](repo_ideas.md) and [architecture_plan.md](architecture_plan.md) into implementation phases.

## Phase 1: Local Orderbook

- Maintain an in-memory orderbook per `asset_id`.
- Apply `book` snapshots and `price_change` deltas.
- Publish normalized orderbooks only after sorting, liquidity, and stale-book checks.
- Add tests for snapshots, deltas, deletion at size zero, invalid side, and stale timestamps.

## Phase 2: User WebSocket Reconciliation

- Subscribe to the authenticated Polymarket user WebSocket.
- Process `order` and `trade` events.
- Persist lifecycle updates idempotently by `order_id`, `trade_id`, and `signal_id`.
- Use live user events to update open orders, fills, and positions.
- Keep `dry_run` usable without credentials by simulating the lifecycle from `DELAYED` to `UNMATCHED`.
- Use `USER_MARKET_IDS` condition IDs for the user channel; do not reuse `MARKET_ASSET_IDS` token IDs.

## Phase 3: Operator API + CLI

- Extend FastAPI from informational endpoints to operator controls described in [interface_plan.md](interface_plan.md).
- Add CLI commands for `status`, `risk`, `streams`, `orders`, `cancel-all`, and `kill-switch on|off`.
- Keep CLI output dual-mode: `table` and `json`.
- Keep dashboard work in Phase 6 so controls are API-first.
- Runtime kill switch is backed by Redis key `operator:kill_switch` and read by Rust before each signal is accepted.
- `cancel-all` publishes `cancel_all` to `operator:commands:stream`; Rust consumes it through `rust-control` and calls CLOB `cancel_all_orders()` in live mode.
- `cancel-bot-open` is the preferred control path and only cancels orders known by this bot.
- `cancel-all` is emergency-only and requires `confirmation_phrase = "CANCEL ALL OPEN ORDERS"`.
- Operator routes support optional role-based bearer auth through `OPERATOR_READ_TOKEN`, `OPERATOR_CONTROL_TOKEN`, and legacy `OPERATOR_API_TOKEN`.

## Phase 4: Research Data Lake

- Write normalized market, orderbook, signal, execution, order, and position snapshots to Parquet.
- Use DuckDB for calibration, realized edge, maker/taker style analysis, and strategy PnL reports.
- Keep Postgres as operational state; use Parquet/DuckDB for research and backtesting.
- Initial implementation exports Redis Streams to partitioned Parquet and creates DuckDB views as described in [data_lake_plan.md](data_lake_plan.md).

## Phase 5: Market Discovery and Evidence Scoring

- Add Gamma market metadata ingestion.
- Rank candidate markets using liquidity, spread, activity, odds movement, and external evidence.
- Use social/news/search evidence as a scoring input, never as direct trade execution authority.
- Feed ranked markets into the predictor only after deterministic filters pass.
- Initial implementation exposes read-only Gamma discovery via API/CLI and deterministic metadata scoring as described in [market_discovery_plan.md](market_discovery_plan.md).

## Phase 6: Optional Web Dashboard

- Build a dashboard only after the Operator API is stable.
- Show status, streams, risk, orders, positions, execution reports, and strategy metrics.
- All dashboard actions must call the same Operator API used by the CLI.
- Initial implementation is a TypeScript React dashboard in `frontend/` that consumes only Operator API endpoints.
- FastAPI serves the built dashboard at `/`, while `/api/*` aliases keep the browser client and standalone API compatible.
- Frontend API types and the typed OpenAPI client are generated from OpenAPI with `npm run generate:types`.
- Local verification is consolidated in `scripts/check_all.sh`.

## Acceptance Criteria

- Every phase has tests before live trading is enabled.
- `EXECUTION_MODE=dry_run` remains default.
- Any live execution must pass Rust risk gates.
- Redis Streams remain the internal service boundary.
- Postgres is recommended for operation, even if optional for local dry-run development.
