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

For a fully managed local smoke run, use:

```bash
scripts/run_integration_smoke.sh
```

The managed smoke starts Redis, Postgres, Rust, the API, and the Python consumer locally with `DISABLE_MARKET_WS=true`, so it does not depend on external Polymarket market data.

For real public market data while still preventing live execution, run:

```bash
REAL_DRY_RUN_SECONDS=300 scripts/run_real_dry_run_research.sh
```

This managed dry-run refuses `EXECUTION_MODE=live`, discovers market token IDs
through Gamma when `MARKET_ASSET_IDS` is unset, enables the public market
WebSocket, waits for real orderbooks/signals/dry-run execution reports, and then
runs the research loop. Promotion gate failures are expected for short samples;
the infrastructure success criteria are populated Redis Streams, Parquet/DuckDB
exports, `research_summary.json`, and `research_manifest.json`.

In dry-run mode, Rust simulates fills from the normalized market orderbook when
the opposite best level touches the submitted limit price. These execution
reports remain simulation artifacts, but they exercise the same report and
state-store paths as live reconciliation.

For a longer research-quality run that does not mix with the default data lake:

```bash
REAL_DRY_RUN_SECONDS=900 scripts/run_real_dry_run_research.sh
```

Real dry-run research is isolated by default under `.tmp/real-dry-run-data-lake/<run_id>/`.
Set `REAL_DRY_RUN_ISOLATED=0` only when intentionally writing into the shared
`data_lake/` root.
Review `unfilled_reason_summary` and `observed_vs_synthetic_fill_summary`
before changing predictor thresholds or considering live mode. Review
`dry_run_simulator_quality` to verify fill-rate, slippage, time-to-fill, and
`PARTIAL`/`MATCHED` mix from the dry-run simulator.

## Startup

Run Redis and Postgres first, then start services from separate terminals:

```bash
set -a
. ./.env.production
set +a

(cd rust-engine && cargo run)
```

```bash
set -a
. ./.env.production
set +a

scripts/run_operator_api.sh
```

```bash
set -a
. ./.env.production
set +a

PYTHONPATH=python-service python -m src.data.consumer
```

Expose FastAPI only through a reverse proxy with TLS. The application should listen on localhost or a private network interface. Do not expose the raw Uvicorn port publicly.

## VM Network, Reverse Proxy, And Firewall

The standard non-conflicting VM ports for this project are:

- Operator API: `127.0.0.1:18000`.
- Operator dashboard dev/preview server: `127.0.0.1:5174`.

Start the Operator API:

```bash
scripts/run_operator_api.sh
```

Start the dashboard against that API:

```bash
scripts/run_operator_frontend.sh
```

For temporary VM testing without a reverse proxy, bind the dashboard to all interfaces:

```bash
OPERATOR_FRONTEND_HOST=0.0.0.0 scripts/run_operator_frontend.sh
```

Then open `http://<vm-public-ip>:5174/`. A private VM address such as `10.x.x.x` is not reachable from outside its private network. If the public URL times out, the cloud firewall or security group is still blocking `5174/tcp`.

Production should expose only the reverse proxy publicly on `80` and `443`; keep raw Vite and Uvicorn ports private.

Nginx example:

```nginx
server {
    listen 80;
    server_name example.com;

    location /api/ {
        proxy_pass http://127.0.0.1:18000/api/;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location / {
        proxy_pass http://127.0.0.1:5174/;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Firewall baseline:

```bash
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow OpenSSH
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw deny 5174/tcp
sudo ufw deny 18000/tcp
sudo ufw enable
sudo ufw status verbose
```

Local checks on the VM:

```bash
curl http://127.0.0.1:18000/health
curl http://127.0.0.1:5174/
```

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
curl -H "Authorization: Bearer $OPERATOR_READ_TOKEN" http://127.0.0.1:18000/metrics/prometheus
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
