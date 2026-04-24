# Market Discovery and Evidence Scoring

Phase 5 adds a read-only market discovery layer. It ranks Polymarket markets for operator review and research; it does not publish trade signals.

## Source

The discovery layer reads public Gamma market data from `https://gamma-api.polymarket.com`.

The initial implementation uses `GET /markets` with conservative filters:

- active markets
- not closed
- not archived
- order book enabled
- minimum liquidity
- minimum volume
- at least two CLOB token IDs

## Scoring

The score is deterministic and auditable:

- liquidity score
- volume score
- price quality score
- evidence metadata score

Evidence metadata is limited to information already present in Gamma market data: description, resolution source, tags, and end date. External news/social evidence is intentionally deferred.

## Interfaces

Operator API:

```text
GET /markets/discover
```

Supported query params:

- `limit`
- `query`
- `min_liquidity`
- `min_volume`

CLI:

```bash
PYTHONPATH=python-service python -m src.cli discover-markets --limit 25 --query bitcoin --output json
```

## Trading Boundary

Discovery is advisory only. It does not call the predictor, does not write `signals:stream`, and does not change Rust execution or risk enforcement.

A later phase may use discovery as an allowlist/filter before prediction, disabled by default.
