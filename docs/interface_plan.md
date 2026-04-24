# Operator Interface Plan

The official v1 interface is **Operator API + CLI**. There is no dashboard web in this phase.

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
| `GET` | `/orders/open` | Implemented | Best-effort open order view derived from recent execution reports. |
| `GET` | `/positions` | Implemented | Best-effort positions derived from recent matched execution reports and signals. |
| `GET` | `/execution-reports` | Implemented | Recent execution reports for dashboard/audit views. |
| `GET` | `/strategy/metrics` | Implemented | Best-effort strategy metrics derived from recent execution reports. |
| `GET` | `/markets/discover` | Implemented | Read-only Gamma market discovery and deterministic ranking. |
| `POST` | `/orders/cancel-all` | Not implemented | Returns `501` until Rust has real CLOB cancel support. |

## Operator API

Control payloads:

| Method | Path | Payload |
| --- | --- | --- |
| `POST` | `/control/kill-switch` | `{ "reason": "...", "operator": "..." }` |
| `POST` | `/control/resume` | `{ "confirm": true, "reason": "...", "operator": "..." }` |

API responses should be JSON-first and include enough IDs for debugging: `signal_id`, `order_id`, `market_id`, and `asset_id` where applicable.

## Planned CLI

The CLI should target operators and scripts. It should call the Operator API first and only read Redis/Postgres directly if the API lacks a capability.

| Command | Purpose |
| --- | --- |
| `status` | Show service status, stream health, and current execution mode. |
| `risk` | Show kill switch, exposure, max order size, max daily loss, and stale-signal window. |
| `streams` | Show Redis Stream length, pending count, and dead-letter count. |
| `orders` | List best-effort open orders from execution reports. |
| `positions` | List best-effort positions from matched reports and signals. |
| `discover-markets` | List ranked Gamma markets for operator review. |
| `cancel-all` | Calls API and currently returns not implemented until Rust CLOB cancellation exists. |
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

The dashboard lives in `frontend/` and consumes only the Operator API. It shows status, streams, risk limits, open orders, derived positions, execution metrics, and read-only market discovery. `cancel-all` remains unavailable until the backend implements real CLOB cancellation.

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

When `frontend/dist/index.html` exists, FastAPI serves the dashboard at `/` and keeps API endpoints on their existing paths.

See [architecture_plan.md](architecture_plan.md) for system context and [implementation_roadmap.md](implementation_roadmap.md) for phase ordering.
