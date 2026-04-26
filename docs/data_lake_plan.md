# Research Data Lake

Phase 4 adds an offline research data lake. It is not part of the live trading path.

## Scope

- Export Redis Stream payloads to partitioned Parquet files.
- Track the last exported Redis Stream ID per dataset to support incremental exports.
- Keep the original event as `payload_json` plus stream metadata.
- Validate known contracts before writing:
  - `orderbook:stream`
  - `signals:stream`
  - `execution:reports:stream`
- Preserve operational state in Redis/Postgres; use Parquet/DuckDB for analysis and backtesting.

## Layout

Default root: `data_lake/`

```text
data_lake/
  _export_state.json
  orderbook_snapshots/date=YYYY-MM-DD/part-000.parquet
  orderbook_levels/date=YYYY-MM-DD/part-000.parquet
  signals/date=YYYY-MM-DD/part-000.parquet
  execution_reports/date=YYYY-MM-DD/part-000.parquet
  orderbook_deadletter/date=YYYY-MM-DD/part-000.parquet
  signals_deadletter/date=YYYY-MM-DD/part-000.parquet
  operator_commands/date=YYYY-MM-DD/part-000.parquet
  research.duckdb
```

Each Parquet row has:

- `stream`
- `stream_id`
- `schema_name`
- `event_timestamp_ms`
- `ingested_at_ms`
- `payload_json`

Known streams are also flattened into useful analytical columns. For example, `orderbook_snapshots` includes `best_bid`, `best_ask`, `spread`, `bid_depth`, and `ask_depth`; `orderbook_levels` contains one row per bid/ask level.

## Run

```bash
PYTHONPATH=python-service python -m src.research.data_lake \
  --root data_lake \
  --duckdb data_lake/research.duckdb \
  --count 1000
```

The exporter runs incrementally by default and stores offsets in `_export_state.json`. Use `--full-refresh` only for a deliberate rebuild from the beginning of the Redis Streams. The exporter creates DuckDB views for datasets with Parquet files.

Backtest reports can be generated from an exported DuckDB database:

```bash
PYTHONPATH=python-service python -m src.research.backtest \
  --duckdb data_lake/research.duckdb \
  --output-dir data_lake/backtest \
  --pre-live-gate
```

The report writes `backtest_trades.parquet`, `backtest_summary.parquet`, and optionally `pre_live_gate.json` with fill-rate, slippage, model edge, realized edge after slippage, total filled size, adverse-selection status when available, and error counts. Treat these metrics as a pre-live gate: `EXECUTION_MODE=live` should not be used until fill-rate and realized edge are acceptable for the target strategy and market class.

Game-theory reports can also be generated from the same DuckDB database:

```bash
PYTHONPATH=python-service python -m src.research.game_theory \
  --duckdb data_lake/research.duckdb \
  --output-dir data_lake/game_theory
```

This writes post-fill PnL horizons, fill-rate by distance to mid, adverse-selection summaries, quote competition, and binary no-arbitrage gaps. The model intent is documented in [game_theory_plan.md](game_theory_plan.md) and [modeling_plan.md](modeling_plan.md).

## Notes

- This exporter is append-oriented. It writes timestamped Parquet parts for newly observed stream IDs.
- `data_lake/` and `*.duckdb` are ignored by git.
- Delete `_export_state.json` only when intentionally rebuilding the data lake.
