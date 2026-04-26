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
- Track partial fills by accumulating `filled_size` and `remaining_size` per `order_id`.
- Use live user events to update open orders, fills, and positions.
- Keep `dry_run` usable without credentials by simulating the lifecycle from `DELAYED` to `UNMATCHED`.
- Use `USER_MARKET_IDS` condition IDs for the user channel; do not reuse `MARKET_ASSET_IDS` token IDs.

## Phase 3: Operator API + CLI

- Extend FastAPI from informational endpoints to operator controls described in [interface_plan.md](interface_plan.md).
- Add CLI commands for `status`, `risk`, `streams`, `orders`, `cancel-all`, and `kill-switch on|off`.
- Keep CLI output dual-mode: `table` and `json`.
- Use command-specific table columns for orders, positions, metrics, and control results.
- Keep dashboard work in Phase 6 so controls are API-first.
- Runtime kill switch is backed by Redis key `operator:kill_switch` and read by Rust before each signal is accepted.
- `cancel-all` publishes `cancel_all` to `operator:commands:stream`; Rust consumes it through `rust-control` and calls CLOB `cancel_all_orders()` in live mode.
- `cancel-bot-open` is the preferred control path and only cancels orders known by this bot.
- `cancel-all` is emergency-only and requires `confirmation_phrase = "CANCEL ALL OPEN ORDERS"`.
- Cancellation requests move through `SENT`, `CONFIRMED`, `DIVERGED`, or `FAILED`; HTTP acceptance alone is not treated as final cancellation.
- Operator routes support optional role-based bearer auth through `OPERATOR_READ_TOKEN`, `OPERATOR_CONTROL_TOKEN`, and legacy `OPERATOR_API_TOKEN`.

## Phase 4: Research Data Lake

- Write normalized market, orderbook, signal, execution, order, and position snapshots to Parquet.
- Use DuckDB for calibration, realized edge, maker/taker style analysis, and strategy PnL reports.
- Keep Postgres as operational state; use Parquet/DuckDB for research and backtesting.
- Initial implementation exports Redis Streams to partitioned Parquet and creates DuckDB views as described in [data_lake_plan.md](data_lake_plan.md).
- Financial, learning, and game-theory model plans are documented in [modeling_plan.md](modeling_plan.md) and [game_theory_plan.md](game_theory_plan.md).

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
- Local integration smoke testing uses `docker-compose.test.yml` plus `scripts/integration_smoke.py`.
- Managed local integration testing uses `scripts/run_integration_smoke.sh` and disables the market WebSocket with `DISABLE_MARKET_WS=true`.
- GitHub Actions runs `scripts/check_all.sh` on push and pull request, with Cargo, pip, and npm caches.
- CI rejects stale generated OpenAPI/TypeScript artifacts.
- The dashboard separates read/control tokens and shows recent `/control/results`.

## Platform-First Next Steps

These steps improve the trading platform before introducing heavier models. The order is intentional: live trading should wait until state, controls, observability, and replayability are solid.

1. Production state authority
   - Make Postgres the required source for orders, trades, positions, balances, cancel requests, and control results when `APP_ENV=production`.
   - Remove Redis fallback from production API reads; Redis Streams remain the transport boundary, not the long-term operational state.
   - Add versioned migrations for every operational table and keep Python startup validation aligned with Rust migrations.

2. Time-series storage strategy
   - Use normal Postgres tables for canonical operational state that needs strict constraints and idempotency: orders, trades, fills, positions, balances, cancel requests, control results, and risk snapshots.
   - Use partitioned Postgres tables or optional TimescaleDB hypertables for high-volume time-series: orderbook snapshots, price changes, runtime metrics, latency samples, signal history, and fill history.
   - Keep TimescaleDB optional at first; promote it when retention, compression, continuous aggregates, or query volume justify the operational dependency.
   - Keep Parquet/DuckDB as the offline research and backtesting store for reproducible historical analysis.
   - Add retention policies by dataset class: short retention for raw orderbook ticks, longer retention for normalized snapshots, and permanent retention for orders/fills/control audit records.

3. Operator safety and controls
   - Keep `cancel-bot-open` as the default cancellation action and reserve `cancel-all` for emergencies.
   - Show command lifecycle clearly in CLI and dashboard: `QUEUED`, `SENT`, `CONFIRMED`, `DIVERGED`, `FAILED`.
   - Add operator audit fields everywhere: `command_id`, `operator`, `reason`, `created_at`, `completed_at`, and final error.
   - Add a dry-run command preview endpoint before dangerous control actions.

4. Reconciliation hardening
   - Treat User WebSocket events as the preferred confirmation path for orders, fills, and cancellations.
   - Use CLOB polling only as fallback with timeout and divergence tracking.
   - Persist partial fills idempotently by `trade_id` and reject duplicate fill accounting.
   - Add reconciliation reports that compare local Postgres state against CLOB open orders and recent fills.

5. Observability and runbooks
   - Expand `/metrics/prometheus` with bounded labels for command type, report status, rejection reason, and CLOB error type.
   - Add latency histograms for WS to signal, signal to order, order to report, and command to confirmation.
   - Keep structured JSON logs with `signal_id`, `order_id`, `command_id`, `market_id`, and `asset_id`.
   - Add runbook steps for degraded WebSocket, Redis outage, Postgres outage, CLOB API errors, and emergency cancellation.

6. Dashboard and CLI ergonomics
   - Make dashboard pages task-based: Overview, Orders, Positions, Controls, Streams, Metrics, Research.
   - Add filtering by market, asset, status, strategy, command type, and time window.
   - Add clear stale-data indicators when API state is older than expected.
   - Keep CLI parity with dashboard controls and ensure every command supports `--json`.

7. Integration and CI confidence
   - Promote the managed smoke test to a scheduled or manually required pre-release job.
   - Add integration coverage for Redis restart, Postgres restart, stale signal rejection, duplicate fill events, and cancel divergence.
   - Add local fixtures for representative Polymarket WebSocket and User WebSocket messages.
   - Keep `scripts/check_all.sh` as the fast required gate and leave heavier end-to-end checks opt-in until runtime is acceptable.

8. Data quality foundations
   - Incremental data lake export state is implemented with `_export_state.json`; next work is operational monitoring for exporter lag.
   - Add market metadata snapshots to the data lake so results can be grouped by liquidity, category, end date, and market type.
   - Generate time-windowed datasets for orderbook, signals, execution reports, fills, and control events.
   - Add explicit model/data version fields to signal and research outputs, even before ML models exist.

9. Research and model readiness
   - Implement an offline deterministic baseline using spread, depth, orderbook imbalance, short-horizon momentum, and stale-market filters.
   - Run [game_theory_plan.md](game_theory_plan.md) reports over real dry-run/live-like data before promoting any strategy.
   - Use the pre-live gate report as the first promotion check, then add walk-forward splits, Brier score, log loss, reliability buckets, and realized edge by probability bucket.
   - Evaluate gradient boosting only after the deterministic baseline is reproducible, calibrated, and better than `passive_spread_capture_v1`.

10. Live promotion gates
   - Add a pre-live report that combines realized edge, fill-rate, adverse-selection rate, slippage, drawdown, stale-data rate, and reconciliation divergence rate.
   - Require positive realized edge after slippage and no persistent adverse selection before enabling `EXECUTION_MODE=live`.
   - Require clean operator controls, confirmed cancellation behavior, and passing integration smoke before any live deployment.
   - Keep Rust risk limits as the final authority for size, exposure, stale signals, kill switch, and cancellation behavior.

## Acceptance Criteria

- Every phase has tests before live trading is enabled.
- `EXECUTION_MODE=dry_run` remains default.
- Any live execution must pass Rust risk gates.
- Redis Streams remain the internal service boundary.
- Postgres is recommended for operation, even if optional for local dry-run development.
- `APP_ENV=production` must fail startup unless required production settings are present.
