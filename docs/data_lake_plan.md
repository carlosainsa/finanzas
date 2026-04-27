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
`observed_vs_synthetic_fill_summary.parquet`, `unfilled_signal_reasons.parquet`,
`unfilled_reason_summary.parquet`, `dry_run_simulator_quality.parquet`, and
optionally `pre_live_gate.json` with fill-rate, slippage, model edge, realized
edge after slippage, total filled size, adverse-selection status when available,
and error counts.
`backtest_trades` is order-level, while `backtest_summary` counts unique signals
separately from orders to avoid double-counting `PARTIAL -> MATCHED` report
lifecycles. Treat these metrics as a pre-live gate:
`EXECUTION_MODE=live` should not be used until fill-rate and realized edge are
acceptable for the target strategy and market class.

The deterministic baseline can be generated offline:

```bash
PYTHONPATH=python-service python -m src.research.deterministic_baseline \
  --duckdb data_lake/research.duckdb \
  --output-dir data_lake/baseline
```

It writes baseline features, filter decisions, synthetic baseline signals, and a summary for `deterministic_microstructure_baseline_v1`. The default quote placement is `passive_bid`, which preserves the conservative baseline. Research runs can opt into `--quote-placement near_touch` to evaluate whether near-touch quotes improve fill-rate; this emits separate near-touch model and feature versions so results are not mixed with the passive baseline.

Real market dry-run research defaults the live predictor to `PREDICTOR_QUOTE_PLACEMENT=near_touch` with `EXECUTION_MODE=dry_run`. This mode is research-only and is blocked for production/live operation; it exists to produce fill evidence before any live policy change.

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

The `unfilled_*` backtest outputs explain signals that had no observed fill.
They separate execution statuses such as `ERROR`, `DELAYED`, and `UNMATCHED`
from market-evidence reasons such as no future orderbook snapshot, no future
limit touch, or synthetic fill available without an observed fill.

The `dry_run_simulator_quality` output is the explicit simulator-quality report.
It compares observed dry-run fills against synthetic offline fills by market and
side, including fill-rate delta, slippage delta, average time-to-fill, and
`PARTIAL` vs `MATCHED` mix.

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
segments, segment checks, segment summary, blocked segments, drawdown,
stale-data gaps, and reconciliation divergence. It also writes
`blocked_segments.json`, a runtime-friendly artifact that the predictor can load
through `PREDICTOR_BLOCKED_SEGMENTS_PATH` to suppress weak market/asset
segments. It combines observed realized edge, observed fill-rate, slippage,
adverse selection, drawdown, stale-data rate, reconciliation divergence rate,
simulator-quality delta, and calibration quality into one offline gate.
The promotion stage materializes its expensive DuckDB relations before reading
checks and exports, so large dry-run samples do not repeatedly expand the
backtest, game-theory, calibration, and stale-data view graph.
Synthetic fills remain comparison evidence only; promotion PnL and drawdown use
observed trades. Segment exports include normalized diagnostics such as
`pnl_per_signal`, `pnl_per_filled_notional`, `drawdown_per_signal`, and
`drawdown_per_filled_notional`; these are diagnostics until thresholds are
calibrated across repeated dry-runs.

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

NVIDIA NIM advisory outputs are planned as optional offline artifacts described
in [nim_research_plan.md](nim_research_plan.md). When added to the data lake,
they should be stored as `nim_advisory.json`,
`nim_advisory_annotations.parquet`, and `nim_advisory_summary.parquet`, with
`can_execute_trades=false`, model name, prompt version, and input/output hashes.
They must not write Redis Streams, runtime blocklists, execution reports, or
live predictor state.

Market-regime diagnostics can also be generated from the same DuckDB database:

```bash
PYTHONPATH=python-service python -m src.research.market_regime \
  --duckdb data_lake/research.duckdb \
  --output-dir data_lake/market_regime
```

This writes `market_regime.json` plus Parquet outputs for fractal/tail-risk
diagnostics and whale-pressure features. These outputs are offline diagnostics:
they can explain drawdowns, propose segment blocks, and inform future risk
rules, but they do not authorize live trades. The report also exports
`market_regime_bucket_performance.parquet`, which joins regime and whale
diagnostics to `backtest_trades` so realized edge, fill-rate, adverse edge
rate, slippage, trade-level drawdown, and PnL per signal can be reviewed by
risk bucket. `market_regime_trade_context` uses point-in-time joins where
`regime_timestamp_ms <= signal_timestamp_ms`; aggregate regime files remain
post-run diagnostics for the full capture window.

External sentiment contracts are available as offline data-lake datasets:

- `external_evidence`, validated by `external_evidence.json`;
- `sentiment_features`, validated by `sentiment_feature.json`.

These datasets are not Redis streams and are not part of the live predictor.
They exist so future ingestion jobs can write timestamped evidence and derived
features without creating leakage-prone ad hoc JSON.

The deterministic offline sentiment builder can be run with:

```bash
PYTHONPATH=python-service python -m src.research.sentiment_features \
  --duckdb data_lake/research.duckdb \
  --output-dir data_lake/sentiment_features
```

It reads only loaded `external_evidence` rows and writes
`sentiment_feature_candidates.parquet`; it performs no scraping and has no live
execution authority.

Sentiment lift can then be evaluated against realized backtest trades:

```bash
PYTHONPATH=python-service python -m src.research.sentiment_lift \
  --duckdb data_lake/research.duckdb \
  --output-dir data_lake/sentiment_lift
```

This writes point-in-time trade context and lift summaries. The join condition
uses `sentiment_feature_candidates.available_at_ms <=
backtest_trades.signal_timestamp_ms`, so evidence that was observed but not yet
available to the pipeline is not used. Metrics include realized edge after
slippage, fill-rate, adverse edge rate, drawdown, and lift versus the same
strategy/side baseline without sentiment buckets.

Feature blocklist candidates can be exported from regime and sentiment reports:

```bash
PYTHONPATH=python-service python -m src.research.feature_blocklist_candidates \
  --duckdb data_lake/research.duckdb \
  --output-dir data_lake/feature_blocklist_candidates
```

This writes `research_feature_blocklist_candidates.parquet` and
`blocked_segments_candidates.json`. These outputs are candidate-only. They are
not runtime blocklists and must not be loaded into live execution without a
separate promotion step, comparable restricted dry-run, and Rust risk-gate
approval.

To compare feature research across two runs without applying anything live:

```bash
PYTHONPATH=python-service python -m src.research.feature_research_decision \
  --baseline-report-root data_lake/reports/run-a \
  --candidate-report-root data_lake/reports/run-b \
  --json
```

The decision is limited to `PROMOTE_FEATURE`, `KEEP_DIAGNOSTIC`, or
`REJECT_FEATURE`. It checks sentiment lift, drawdown, adverse edge lift, feature
blocklist candidate churn, and confirms candidate payloads remain
`can_apply_live=false`.

`scripts/run_research_loop.sh` now writes `feature_research_decision.json` for
each run before `research_manifest.json` is generated. On the first run, or any
run without a prior indexed baseline, the report stays advisory and returns
`KEEP_DIAGNOSTIC` instead of failing the research loop.

To review feature stability across many runs:

```bash
PYTHONPATH=python-service python -m src.research.feature_decision_history \
  --manifest-root data_lake/research_runs \
  --output-dir data_lake/feature_decision_history \
  --latest 20 \
  --json
```

This exports run-level decision history, bucket-level history, and bucket
stability Parquet files. It is still offline diagnostics only and cannot apply
runtime blocklists.

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

When comparing isolated report directories that have not been merged into a
shared manifest index, compare report roots directly:

```bash
PYTHONPATH=python-service python -m src.research.compare_runs \
  --baseline-report-root .tmp/real-dry-run-data-lake/<baseline>/reports/<baseline> \
  --candidate-report-root .tmp/real-dry-run-data-lake/<candidate>/reports/<candidate>
```

The comparison reports deltas for signals, fills, fill-rate, realized edge,
drawdown, stale-data/reconciliation quality, simulator delta, blocked segment
count, runtime blocklist count, segment-level improvements/regressions,
new/removed segments, and newly blocked/unblocked segment keys.
If segment exports are missing, segment keys differ, or required segment key
columns are absent, the comparison verdict is `no_comparable`; aggregate metrics
remain visible, but they must not be used as promotion evidence.

The automatic comparative promotion gate can be run over the same reports:

```bash
PYTHONPATH=python-service python -m src.research.research_promotion_decision \
  --baseline-report-root .tmp/real-dry-run-data-lake/<baseline>/reports/<baseline> \
  --candidate-report-root .tmp/real-dry-run-data-lake/<candidate>/reports/<candidate> \
  --json
```

It emits `PROMOTE`, `REJECT`, or `NEED_MORE_DATA`. The gate requires the
candidate absolute run gate to have passed, a `candidate_improved` comparison
verdict, comparable segment keys, no newly blocked segments, and objective
deltas for realized edge, fill-rate, drawdown, simulator quality, stale data,
and reconciliation divergence.

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

To run a restricted dry-run with a promotion-generated segment blocklist:

```bash
PREDICTOR_BLOCKED_SEGMENTS_PATH=/abs/path/to/blocked_segments.json \
REAL_DRY_RUN_SECONDS=300 \
scripts/run_real_dry_run_research.sh
```

The script validates that the blocklist path exists before starting services and
records the path in `real_dry_run_evidence.json`. Use the report-root comparator
above to compare the unrestricted and restricted runs before accepting a
blocklist.

Real dry-runs pass stricter promotion thresholds into the research loop by
default:

- `PRE_LIVE_MIN_CAPTURE_DURATION_MS`, defaulting to half the capture window;
- `PRE_LIVE_MIN_SIGNALS=10`;
- `PRE_LIVE_MIN_DRY_RUN_OBSERVED_FILL_RATE=0.01`;
- `PRE_LIVE_MAX_ABS_SIMULATOR_FILL_RATE_DELTA=0.75`.

These thresholds use observed dry-run execution evidence and simulator-quality
delta, not only synthetic fills. Tune them per market class after repeated runs;
do not loosen them to make a single short run pass.

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
