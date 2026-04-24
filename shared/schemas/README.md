# Redis channel contracts

The Rust engine and Python service exchange only schema-validated JSON over Redis.

| Channel | Producer | Consumer | Schema |
| --- | --- | --- | --- |
| `orderbook:stream` | `rust-engine` WebSocket client | `python-service` consumer group `python-predictor` | `orderbook.json` |
| `signals:stream` | `python-service` predictor | `rust-engine` consumer group `rust-executor` | `trade_signal.json` |
| `execution:reports:stream` | `rust-engine` executor | API/monitoring service | `execution_report.json` |
| `orderbook:deadletter` | `python-service` consumer | operators | invalid orderbook payload diagnostics |
| `signals:deadletter` | `rust-engine` executor | operators | invalid trade signal diagnostics |

Runtime configuration:

| Variable | Default | Purpose |
| --- | --- | --- |
| `MARKET_ASSET_IDS` | empty | Comma-separated Polymarket CLOB token IDs to subscribe to. |
| `USER_MARKET_IDS` | empty | Comma-separated Polymarket condition IDs for authenticated user order/trade reconciliation. Required in `live` mode. |
| `EXECUTION_MODE` | `dry_run` | Use `live` only when order execution should be enabled. |
| `POLYMARKET_USER_WS_URL` | `wss://ws-subscriptions-clob.polymarket.com/ws/user` | Authenticated user WebSocket endpoint for order/trade lifecycle updates. |
| `POLYMARKET_API_KEY` | unset | Polymarket CLOB API key for the user WebSocket subscription. Required in `live` mode. |
| `POLYMARKET_API_SECRET` | unset | Polymarket CLOB API secret for the user WebSocket subscription. Required in `live` mode. |
| `POLYMARKET_API_PASSPHRASE` | unset | Polymarket CLOB API passphrase for the user WebSocket subscription. Required in `live` mode. |
| `ORDER_RECONCILIATION_TIMEOUT_MS` | `10000` | Dry-run delay before a submitted order is reconciled to `UNMATCHED`. |
| `OPERATOR_KILL_SWITCH_KEY` | `operator:kill_switch` | Redis key written by the Operator API and read by Rust before accepting each signal. |
| `MAX_ORDER_SIZE` | `10.0` | Rust-side hard cap per trade signal. |
| `MIN_CONFIDENCE` | `0.55` | Rust-side minimum model confidence. |
| `SIGNAL_MAX_AGE_MS` | `5000` | Rust-side stale signal rejection window. |
| `MAX_MARKET_EXPOSURE` | `100.0` | Rust-side projected exposure cap per market. |
| `MAX_DAILY_LOSS` | `50.0` | Rust-side daily realized loss kill limit. |
| `KILL_SWITCH` | `false` | Reject all trade signals when enabled. |
| `DATABASE_URL` | unset | Optional Postgres state store for idempotent signals/reports. |
| `PREDICTOR_MIN_SPREAD` | `0.03` | Python strategy spread threshold. |
| `PREDICTOR_ORDER_SIZE` | `1.0` | Python strategy target order size. |
| `PREDICTOR_MIN_CONFIDENCE` | `0.55` | Python strategy minimum confidence. |
| `GAMMA_API_URL` | `https://gamma-api.polymarket.com` | Public Gamma API base URL for market discovery. |
| `DISCOVERY_LIMIT` | `50` | Default number of Gamma markets to fetch for discovery. |
| `DISCOVERY_MIN_LIQUIDITY` | `100.0` | Minimum Gamma liquidity for ranked discovery candidates. |
| `DISCOVERY_MIN_VOLUME` | `100.0` | Minimum Gamma volume for ranked discovery candidates. |

When `DATABASE_URL` is set, `rust-engine` also stores submitted orders and reconciled trade lifecycle events in Postgres so live user WebSocket events can be correlated by `order_id` after a restart.

Operator runtime controls:

| Key/Stream | Writer | Reader | Purpose |
| --- | --- | --- | --- |
| `operator:kill_switch` | Operator API / CLI | `rust-engine` executor | Runtime kill switch. Values `1`, `true`, `yes`, or `on` reject new signals. |
| `operator:commands:stream` | Operator API / CLI | operators | Audit trail for operator control commands. |

Research data lake:

| Dataset | Source |
| --- | --- |
| `orderbooks` | `orderbook:stream` |
| `signals` | `signals:stream` |
| `execution_reports` | `execution:reports:stream` |
| `orderbook_deadletter` | `orderbook:deadletter` |
| `signals_deadletter` | `signals:deadletter` |
| `operator_commands` | `operator:commands:stream` |

See `docs/data_lake_plan.md` for the Parquet/DuckDB layout.

Market discovery is read-only and advisory. It ranks Gamma markets for review, but it does not publish to `signals:stream`.
