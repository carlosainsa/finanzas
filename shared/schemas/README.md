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
| `EXECUTION_MODE` | `dry_run` | Use `live` only when order execution should be enabled. |
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
