# Operator Interface Plan

The official v1 interface is **Operator API + CLI**, with the web dashboard served as an operator console on top of the same API.

## Current API

Implemented in `python-service/src/api/app.py`:

| Method | Path | Status | Purpose |
| --- | --- | --- | --- |
| `GET` | `/health` | Existing | Redis-backed service health check. |
| `GET` | `/status` | Implemented | Service status, kill switch state, stream summaries, and predictor settings. |
| `GET` | `/risk` | Implemented | Operator kill switch state and configured risk limits; Rust remains the enforcement point. |
| `GET` | `/streams` | Implemented | Redis Stream lengths and pending summaries for known consumer groups. |
| `POST` | `/control/kill-switch` | Implemented | Enables runtime kill switch through Redis state consumed by Rust. |
| `POST` | `/control/resume` | Implemented | Disables runtime kill switch after explicit confirmation. |
| `GET` | `/orders/open` | Implemented | Canonical Postgres open orders when configured; Redis execution-report fallback for local dry-run. |
| `GET` | `/positions` | Implemented | Canonical Postgres positions when configured; Redis execution-report fallback for local dry-run. |
| `GET` | `/execution-reports` | Implemented | Recent execution reports for dashboard/audit views. |
| `GET` | `/strategy/metrics` | Implemented | Best-effort strategy metrics derived from recent execution reports. |
| `GET` | `/markets/discover` | Implemented | Read-only Gamma market discovery and deterministic ranking. |
| `POST` | `/orders/cancel-all` | Implemented | Publishes async `cancel_all` to Rust control and returns `202 Accepted` with `command_id`. |

Every operator route is also available under `/api/*` so the integrated dashboard can use same-origin requests when FastAPI serves `frontend/dist`.

## Operator API

Control payloads:

| Method | Path | Payload |
| --- | --- | --- |
| `POST` | `/control/kill-switch` | `{ "reason": "...", "operator": "..." }` |
| `POST` | `/control/resume` | `{ "confirm": true, "reason": "...", "operator": "..." }` |
| `POST` | `/orders/cancel-all` | `{ "reason": "...", "operator": "..." }` |

API responses should be JSON-first and include enough IDs for debugging: `signal_id`, `order_id`, `market_id`, and `asset_id` where applicable.

If `OPERATOR_API_TOKEN` is set, all operator routes except `/health` require `Authorization: Bearer <token>`. Local development can omit the token.

## Planned CLI

The CLI should target operators and scripts. It should call the Operator API first and only read Redis/Postgres directly if the API lacks a capability.

| Command | Purpose |
| --- | --- |
| `status` | Show service status, stream health, and current execution mode. |
| `risk` | Show kill switch, exposure, max order size, max daily loss, and stale-signal window. |
| `streams` | Show Redis Stream length, pending count, and dead-letter count. |
| `orders` | List canonical Postgres open orders when `DATABASE_URL` is configured, otherwise Redis fallback. |
| `positions` | List canonical Postgres positions when `DATABASE_URL` is configured, otherwise Redis fallback. |
| `discover-markets` | List ranked Gamma markets for operator review. |
| `cancel-all` | Calls API and enqueues an async Rust `cancel_all` command. |
| `kill-switch on` | Enable the kill switch. |
| `kill-switch off` | Resume trading after `--confirm`. |

Output modes:

- `table` for humans.
- `json` for scripts and agents.

Example:

```bash
PYTHONPATH=python-service python -m src.cli --output json status
PYTHONPATH=python-service python -m src.cli kill-switch on --reason "manual pause" --operator carlos
PYTHONPATH=python-service python -m src.cli kill-switch off --reason "resume" --confirm
```

## Web Dashboard

The dashboard lives in `frontend/` and consumes only the Operator API. It shows status, streams, risk limits, open orders, derived positions, execution metrics, read-only market discovery, and operator controls including kill switch and async cancel-all.

Run locally:

```bash
cd frontend
npm install
npm run dev
```

Serve through FastAPI after building:

```bash
cd frontend
npm run build
cd ..
PYTHONPATH=python-service uvicorn src.api.app:app --app-dir python-service --reload
```

When `frontend/dist/index.html` exists, FastAPI serves the dashboard at `/` and exposes API endpoints on both existing paths and `/api/*`.

Frontend API types are generated from FastAPI OpenAPI:

```bash
cd frontend
npm run generate:types
```

See [architecture_plan.md](architecture_plan.md) for system context and [implementation_roadmap.md](implementation_roadmap.md) for phase ordering.
