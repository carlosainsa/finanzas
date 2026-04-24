# Redis channel contracts

The Rust engine and Python service exchange only schema-validated JSON over Redis.

| Channel | Producer | Consumer | Schema |
| --- | --- | --- | --- |
| `orderbook:raw` | `rust-engine` WebSocket client | `python-service` consumer | `orderbook.json` |
| `signals:trade` | `python-service` predictor | `rust-engine` executor | `trade_signal.json` |
| `execution:reports` | `rust-engine` executor | API/monitoring service | `execution_report.json` |

Runtime configuration:

| Variable | Default | Purpose |
| --- | --- | --- |
| `MARKET_ASSET_IDS` | empty | Comma-separated Polymarket CLOB token IDs to subscribe to. |
| `EXECUTION_MODE` | `dry_run` | Use `live` only when order execution should be enabled. |
| `MAX_ORDER_SIZE` | `10.0` | Rust-side hard cap per trade signal. |
| `MIN_CONFIDENCE` | `0.55` | Rust-side minimum model confidence. |
| `PREDICTOR_MIN_SPREAD` | `0.03` | Python strategy spread threshold. |
| `PREDICTOR_ORDER_SIZE` | `1.0` | Python strategy target order size. |
| `PREDICTOR_MIN_CONFIDENCE` | `0.55` | Python strategy minimum confidence. |
