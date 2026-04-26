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
  research_runs/research_runs.jsonl
  research_runs/research_runs.parquet
  research_runs/runs/<run_id>.json
  reports/<timestamp>/research_manifest.json
  reports/<timestamp>/research_summary.json
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
`research_summary.json` under `data_lake/reports/<timestamp>/`. It also writes
`research_manifest.json` and a persistent run index under
`data_lake/research_runs/`. The script exits non-zero when promotion, advisory,
pre-live, or calibration gates fail unless `ALLOW_RESEARCH_GATE_FAILURE=1`.

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

The report writes `backtest_trades.parquet`, `backtest_summary.parquet`,
`observed_vs_synthetic_fills.parquet`,
`observed_vs_synthetic_fill_summary.parquet`, and optionally
`pre_live_gate.json` with fill-rate, slippage, model edge, realized edge after
slippage, total filled size, adverse-selection status when available, and error
counts. `backtest_trades` is order-level, while `backtest_summary` counts unique
signals separately from orders to avoid double-counting `PARTIAL -> MATCHED`
report lifecycles. Treat these metrics as a pre-live gate:
`EXECUTION_MODE=live` should not be used until fill-rate and realized edge are
acceptable for the target strategy and market class.

The deterministic baseline can be generated offline:

```bash
PYTHONPATH=python-service python -m src.research.deterministic_baseline \
  --duckdb data_lake/research.duckdb \
  --output-dir data_lake/baseline
```

It writes baseline features, filter decisions, synthetic baseline signals, and a summary for `deterministic_microstructure_baseline_v1`.

Synthetic fills can be generated offline from future orderbook snapshots:

```bash
PYTHONPATH=python-service python -m src.research.synthetic_fills \
  --duckdb data_lake/research.duckdb \
  --output-dir data_lake/synthetic_fills
```

This writes `synthetic_fills.json`, `synthetic_fill_candidates.parquet`,
`synthetic_execution_reports.parquet`, and `synthetic_fill_summary.parquet`.
The model `conservative_orderbook_fill_v1` only fills a BUY signal if a later
best ask touches or improves the limit price, and only fills a SELL signal if a
later best bid touches or improves the limit price. These reports are offline
research artifacts; they are not published to Redis and do not affect live
execution.

The `observed_vs_synthetic_*` backtest outputs compare real execution reports
against conservative synthetic fills for the same signals. Use them to estimate
whether the offline fill model is optimistic or pessimistic before using
synthetic fills as a baseline.

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

## Research Run Manifest

`research_summary.json` is the gate summary for one run. `research_manifest.json`
is the audit and lineage record. It captures `run_id`, source, gate outcomes,
version fields, metrics, dataset counts, artifact paths, artifact byte sizes,
and SHA-256 hashes.

The manifest writer also rebuilds:

- `data_lake/research_runs/runs/<run_id>.json`;
- `data_lake/research_runs/research_runs.jsonl`;
- `data_lake/research_runs/research_runs.parquet`.

Use the Parquet index to compare runs by `run_id`, report version, model
version, data version, feature version, realized edge, fill-rate,
stale-data rate, reconciliation-divergence rate, and advisory failures.

Recent runs can be listed with:

```bash
PYTHONPATH=python-service python -m src.research.compare_runs --summary
```

The latest two runs can be compared with:

```bash
PYTHONPATH=python-service python -m src.research.compare_runs
```

For automation, add `--json`. To compare specific runs, pass
`--baseline-run-id` and `--candidate-run-id`.

## Real Market Dry-Run Research

To collect real public market data without live trading:

```bash
REAL_DRY_RUN_SECONDS=300 \
DISCOVERY_LIMIT=25 \
PREDICTOR_MIN_SPREAD=0.001 \
scripts/run_real_dry_run_research.sh
```

The script refuses to run unless `EXECUTION_MODE=dry_run` and
`DISABLE_MARKET_WS=false`. It starts Redis, Postgres, Rust, the API, and the
Python consumer; discovers `MARKET_ASSET_IDS` through Gamma when they are not
provided; waits for real orderbooks, signals, and dry-run execution reports;
then runs the research loop. A short dry-run may fail promotion or calibration
gates; that means the research data was collected but is not yet sufficient for
live promotion.

For longer isolated runs that should not mix with the default `data_lake/`, use:

```bash
REAL_DRY_RUN_SECONDS=900 \
DISCOVERY_LIMIT=25 \
PREDICTOR_MIN_SPREAD=0.001 \
scripts/run_real_dry_run_research.sh
```

The script runs isolated by default and writes reports, DuckDB, manifests, and Parquet parts under
`.tmp/real-dry-run-data-lake/<run_id>/`.
Use `REAL_DRY_RUN_ISOLATED=0` only when deliberately exporting into the shared
`data_lake/` root. Each run also writes `real_dry_run_evidence.json` with stream
lengths, report status counts, paths, and capture settings.

## Notes

- This exporter is append-oriented. It writes timestamped Parquet parts for newly observed stream IDs.
- `data_lake/` and `*.duckdb` are ignored by git.
- Delete `_export_state.json` only when intentionally rebuilding the data lake.
