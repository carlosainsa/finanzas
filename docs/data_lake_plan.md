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
  market_metadata/date=YYYY-MM-DD/part-000.parquet
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

`market_metadata` is a snapshot dataset sourced from Gamma metadata. It stores one row per market asset/outcome and includes `market_id`, `asset_id`, `outcome`, `outcome_index`, `liquidity`, `volume`, `end_date`, `tags_json`, and `outcome_price`. Research views use this mapping for YES/NO analysis instead of relying on lexicographic `asset_id` ordering.

`signals` preserves optional `model_version`, `data_version`, and `feature_version` fields. New predictors and offline baselines should populate them so backtests, calibration, and pre-live reports are traceable.

## Run

The complete research loop can be run with:

```bash
scripts/run_research_loop.sh
```

It exports the data lake, deterministic baseline, backtest, game-theory report,
calibration report, pre-live promotion report, agent advisory report, and
`research_summary.json` under `data_lake/reports/<timestamp>/`. The script exits
non-zero when promotion, advisory, pre-live, or calibration gates fail.

```bash
PYTHONPATH=python-service python -m src.research.data_lake \
  --root data_lake \
  --duckdb data_lake/research.duckdb \
  --count 1000 \
  --include-market-metadata
```

The exporter runs incrementally by default and stores offsets in `_export_state.json`. Use `--full-refresh` only for a deliberate rebuild from the beginning of the Redis Streams. The exporter creates DuckDB views for datasets with Parquet files.

Backtest reports can be generated from an exported DuckDB database:

```bash
PYTHONPATH=python-service python -m src.research.backtest \
  --duckdb data_lake/research.duckdb \
  --output-dir data_lake/backtest \
  --pre-live-gate
```

The report writes `backtest_trades.parquet`, `backtest_summary.parquet`, and optionally `pre_live_gate.json` with fill-rate, slippage, model edge, realized edge after slippage, total filled size, adverse-selection status when available, and error counts. `backtest_trades` is order-level, while `backtest_summary` counts unique signals separately from orders to avoid double-counting `PARTIAL -> MATCHED` report lifecycles. Treat these metrics as a pre-live gate: `EXECUTION_MODE=live` should not be used until fill-rate and realized edge are acceptable for the target strategy and market class.

The deterministic baseline can be generated offline:

```bash
PYTHONPATH=python-service python -m src.research.deterministic_baseline \
  --duckdb data_lake/research.duckdb \
  --output-dir data_lake/baseline
```

It writes baseline features, filter decisions, synthetic baseline signals, and a summary for `deterministic_microstructure_baseline_v1`.

Game-theory reports can also be generated from the same DuckDB database:

```bash
PYTHONPATH=python-service python -m src.research.game_theory \
  --duckdb data_lake/research.duckdb \
  --output-dir data_lake/game_theory
```

This writes post-fill PnL horizons, fill-rate by distance to mid, adverse-selection summaries, quote competition, and binary no-arbitrage gaps. The model intent is documented in [game_theory_plan.md](game_theory_plan.md) and [modeling_plan.md](modeling_plan.md).

Calibration and walk-forward reports can be generated from the same DuckDB database:

```bash
PYTHONPATH=python-service python -m src.research.calibration \
  --duckdb data_lake/research.duckdb \
  --output-dir data_lake/calibration \
  --train-fraction 0.70
```

This writes walk-forward splits, Brier score, log loss, reliability buckets, realized edge by confidence bucket, and `calibration_summary.json`.

The combined pre-live promotion report can be generated from the same DuckDB database:

```bash
PYTHONPATH=python-service python -m src.research.pre_live_promotion \
  --duckdb data_lake/research.duckdb \
  --output-dir data_lake/pre_live_promotion
```

This writes `pre_live_promotion.json` plus Parquet tables for metrics, checks,
drawdown, stale-data gaps, and reconciliation divergence. It combines realized
edge, fill-rate, slippage, adverse selection, drawdown, stale-data rate,
reconciliation divergence rate, and calibration quality into one offline gate.

Agent advisory diagnostics can be generated offline from the same DuckDB database:

```bash
PYTHONPATH=python-service python -m src.research.agent_advisory \
  --duckdb data_lake/research.duckdb \
  --output-dir data_lake/agent_advisory
```

This writes `agent_advisory.json`, `agent_advisory_evaluations.parquet`, and
`agent_advisory_summary.parquet`. The evaluators are deterministic and
auditable: edge quality, execution quality, calibration quality, data quality,
reconciliation quality, and adverse-selection checks. They do not publish to
Redis, do not create live signals, and do not decide trades. Their output is
advisory evidence for comparing model versions against realized offline metrics.

## Notes

- This exporter is append-oriented. It writes timestamped Parquet parts for newly observed stream IDs.
- `data_lake/` and `*.duckdb` are ignored by git.
- Delete `_export_state.json` only when intentionally rebuilding the data lake.
