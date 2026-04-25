# Production Runbook

This runbook is the operational checklist for running the Polymarket trading system outside local dry-run development.

## Required Environment

Use `.env.production.example` as the template for an `.env.production` file or equivalent secret manager. Do not commit real secrets.

```bash
APP_ENV=production
EXECUTION_MODE=dry_run
DATABASE_URL=postgres://user:password@host:5432/finanzas
REDIS_URL=redis://host:6379

OPERATOR_READ_TOKEN=replace-with-read-token
OPERATOR_CONTROL_TOKEN=replace-with-control-token

POLYMARKET_PRIVATE_KEY=replace-only-for-live
POLYMARKET_API_KEY=replace-only-for-live
POLYMARKET_API_SECRET=replace-only-for-live
POLYMARKET_API_PASSPHRASE=replace-only-for-live

MARKET_ASSET_IDS=token_id_1,token_id_2
USER_MARKET_IDS=condition_id_1,condition_id_2
CANCEL_CONFIRMATION_TIMEOUT_MS=10000
```

Production startup fails if `DATABASE_URL`, `OPERATOR_READ_TOKEN`, `OPERATOR_CONTROL_TOKEN`, or `EXECUTION_MODE` is missing. Keep `EXECUTION_MODE=dry_run` until Redis, Postgres, User WS reconciliation, and operator controls are verified.

## Migrations

Rust applies versioned migrations from `shared/migrations/` on startup through `StateStore`. Python validates the latest shared schema version when `APP_ENV=production` or `REQUIRE_POSTGRES_STATE=true`.

Required schema marker:

```sql
select version from schema_migrations order by applied_at;
```

The current required version is the highest `*.sql` file in `shared/migrations/`.

## Dependencies

Python dependencies are split by use:

- `python-service/requirements.txt`: runtime API, Redis, Postgres, config, HTTP.
- `python-service/requirements-dev.txt`: CI/dev checks, tests, mypy, and research export dependencies.
- `python-service/requirements-ml.txt`: optional ML stack, including torch.

Install the smallest file that matches the process being deployed. API and consumer deployment should not install the ML stack unless the predictor is explicitly changed to require it.

## Integration Smoke Test

Start disposable Redis and Postgres:

```bash
docker compose -f docker-compose.test.yml up -d
```

With the API, Python consumer, and Rust engine running in `EXECUTION_MODE=dry_run`, run:

```bash
PYTHONPATH=python-service python scripts/integration_smoke.py
```

The smoke test publishes a valid orderbook, waits for a signal, waits for a dry-run execution report, and verifies the Operator API status endpoint.

## Startup

Run Redis and Postgres first, then start services:

```bash
set -a
. ./.env.production
set +a

cd rust-engine
cargo run

PYTHONPATH=python-service uvicorn src.api.app:app --app-dir python-service --host 127.0.0.1 --port 8000
PYTHONPATH=python-service python -m src.data.consumer
```

Expose FastAPI only through a reverse proxy with TLS. The application should listen on localhost or a private network interface. Do not expose the raw Uvicorn port publicly.

## Operator CLI

Read-only checks:

```bash
PYTHONPATH=python-service python -m src.cli --read-token "$OPERATOR_READ_TOKEN" status
PYTHONPATH=python-service python -m src.cli --read-token "$OPERATOR_READ_TOKEN" risk
PYTHONPATH=python-service python -m src.cli --read-token "$OPERATOR_READ_TOKEN" metrics
PYTHONPATH=python-service python -m src.cli --read-token "$OPERATOR_READ_TOKEN" control-results
```

Control actions:

```bash
PYTHONPATH=python-service python -m src.cli --control-token "$OPERATOR_CONTROL_TOKEN" kill-switch on --reason "manual pause"
PYTHONPATH=python-service python -m src.cli --control-token "$OPERATOR_CONTROL_TOKEN" cancel-bot-open --reason "stale market"
PYTHONPATH=python-service python -m src.cli --control-token "$OPERATOR_CONTROL_TOKEN" cancel-all --reason "emergency" --confirm --confirmation-phrase "CANCEL ALL OPEN ORDERS"
```

Prefer `cancel-bot-open`. Use `cancel-all` only as an account-wide emergency action.

## Cancellation Reconciliation

Live cancellation requests are recorded as `SENT`. Final confirmation comes from:

1. User WebSocket cancellation event.
2. Fallback polling after `CANCEL_CONFIRMATION_TIMEOUT_MS`.

Statuses:

- `SENT`: HTTP cancel accepted; waiting for confirmation.
- `CONFIRMED`: User WS or fallback polling confirmed the order is no longer open.
- `DIVERGED`: CLOB/open-order state disagrees with expected cancellation.
- `FAILED`: CLOB explicitly returned `not_canceled`.

Check recent outcomes:

```bash
PYTHONPATH=python-service python -m src.cli --read-token "$OPERATOR_READ_TOKEN" control-results --output json
```

## Metrics And Logs

Prometheus endpoint:

```bash
curl -H "Authorization: Bearer $OPERATOR_READ_TOKEN" http://127.0.0.1:8000/metrics/prometheus
```

Rust emits JSON logs. Keep `command_id`, `signal_id`, and `order_id` in incident notes.

Core metrics:

- `ws_to_signal_latency_ms`
- `signal_to_order_latency_ms`
- `order_to_report_latency_ms`
- `ws_to_report_latency_ms`
- `clob_errors_by_type` labeled by controlled `error_type`
- `control_results_by_type` labeled by controlled `command_type`
- `execution_reports_by_status` labeled by controlled `status`

## Rollback

1. Enable kill switch.
2. Cancel bot-open orders.
3. Verify `/control/results` and `/orders/open`.
4. Stop Python consumer, Rust engine, then API.
5. Deploy the previous commit.
6. Start in `EXECUTION_MODE=dry_run`.
7. Resume only after status, risk, streams, and reconciliation look correct.

Do not roll back database migrations destructively during an incident. Add forward migrations for schema fixes.
