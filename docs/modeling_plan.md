# Financial and Learning Models

This document defines the intended modeling stack. Models are introduced offline first, then promoted to `python-service/src/ml/predictor.py` only after backtesting and calibration pass.

## Principles

- Rust remains the execution and risk authority.
- Python may score signals, but it does not bypass risk gates.
- Every model must produce deterministic inputs, versioned outputs, and measurable backtest results.
- `EXECUTION_MODE=dry_run` remains the default until model edge survives slippage, fill-rate, and adverse-selection checks.

## Financial Models

### Market Microstructure

Initial features:

- best bid, best ask, spread, mid-price;
- depth on bid and ask;
- orderbook imbalance;
- price momentum from normalized orderbook snapshots;
- distance from quote price to mid-price;
- quote competition and adverse-selection metrics from [game_theory_plan.md](game_theory_plan.md).

Purpose:

- decide whether there is enough liquidity to trade;
- estimate fill probability and slippage;
- avoid passive fills when informed flow is likely.

### Implied Probability and Edge

Polymarket prices are treated as implied probabilities. A signal needs a fair probability estimate:

```text
edge_buy_yes = fair_probability - ask_or_limit_price
edge_sell_yes = bid_or_limit_price - fair_probability
```

Existing backtest outputs already measure:

- `model_edge`;
- `slippage`;
- `realized_edge_after_slippage`;
- `fill_rate`.

Calibration outputs measure:

- walk-forward train/test splits ordered by `signal_timestamp_ms`;
- Brier score and log loss;
- reliability buckets by confidence range;
- realized edge by confidence bucket.

Until market-resolution labels exist, calibration uses an offline trading proxy: `realized_edge_after_slippage > 0`. This is useful for promotion discipline, but it is not a substitute for final outcome calibration.

### Binary No-Arbitrage

For paired outcomes:

```text
YES + NO ~= 1
```

No-arbitrage gaps are measured offline first. Execution requires a stricter live model that accounts for spread, fees, fill probability, latency, and cancellation risk.

### Market Making and Inventory

The target market-making policy is conservative:

- quote only when spread and edge compensate execution risk;
- cancel stale quotes;
- penalize inventory concentration;
- require higher edge when exposure is already high.

Sizing should start with capped fixed fractional sizing. Fractional Kelly can be evaluated offline later, but live sizing must stay capped by Rust risk limits.

## Learning Models

### Baseline Deterministic Model

First production-grade baseline:

- spread threshold;
- orderbook imbalance;
- short-horizon momentum;
- liquidity/depth threshold;
- stale-market filter;
- adverse-selection filter.

This baseline is implemented offline as `deterministic_microstructure_baseline_v1`. It is interpretable and should be the benchmark for any ML model.

Live decision policy must remain deterministic. A committee of agents can be useful for offline research review, feature proposals, and model diagnostics, but it should not place orders by conversational consensus. Any agent score must become a versioned, testable input that passes deterministic policy and Rust risk gates.

The offline agent advisory module implements this boundary in
`python-service/src/research/agent_advisory.py`. It runs deterministic
evaluators over the research DuckDB views and emits versioned diagnostics as
auditable artifacts. These diagnostics compare model versions against realized
metrics such as fill rate, realized edge after slippage, error rate,
calibration scores, and adverse selection. The advisory module is not imported
by the live predictor or Rust executor and must remain outside the live signal
path.

### Supervised Fair Probability Model

Candidate target variables:

- future mid-price movement;
- final market resolution where available;
- calibrated probability of YES;
- realized edge after slippage for a proposed action.

Candidate features:

- microstructure features;
- market metadata and liquidity;
- price movement windows;
- quote competition;
- no-arbitrage gap;
- historical fill/slippage behavior by market class.

### Calibration

Directional accuracy is not enough. Probability models must be calibrated with:

- Brier score;
- log loss;
- reliability buckets;
- calibration by market class and probability range.

Initial calibration methods:

- isotonic regression;
- Platt scaling;
- bucket-level post-calibration.

### Gradient Boosting

The first ML candidate should be gradient boosting over tabular features:

- LightGBM;
- XGBoost;
- CatBoost.

This is preferred before deep learning because the dataset is tabular, sparse, and operationally easier to audit.

### Fill and Slippage Model

Separate models should estimate:

- probability of fill;
- expected slippage;
- time to fill;
- probability of adverse selection after fill.

These models decide whether a theoretical edge is executable.

### Evidence and NLP Signals

External evidence can be scored and converted into features, but it should not directly issue trades. NLP outputs must become structured, testable inputs such as:

- evidence direction;
- confidence;
- source quality;
- recency;
- contradiction score.

## Offline Evaluation

Before live promotion, run:

- data lake export;
- synthetic offline fills for dry-run samples without observed fills;
- observed-vs-synthetic fill comparison to measure synthetic fill optimism;
- unfilled reason summary to separate model, market-data, and execution causes;
- dry-run simulator quality summary for fill-rate, slippage, time-to-fill, and terminal status mix;
- backtest report from [data_lake_plan.md](data_lake_plan.md);
- game-theory report from [game_theory_plan.md](game_theory_plan.md);
- walk-forward split by date;
- calibration report;
- pre-live promotion report;
- offline agent advisory report;
- comparison against deterministic baseline.

Minimum promotion gates:

- positive realized edge after slippage;
- acceptable fill-rate;
- minimum real dry-run capture duration and signal count;
- acceptable observed dry-run fill-rate;
- bounded simulator-quality fill-rate delta versus synthetic fills;
- no persistent adverse selection;
- stable calibration;
- bounded drawdown in dry-run.

The pre-live promotion report is implemented as `src.research.pre_live_promotion`.
It combines backtest, game-theory, calibration, stale-data, drawdown, and
reconciliation-divergence metrics into a single versioned offline gate.
It also exports segment-level metrics by `market_id`, `asset_id`, `side`,
`strategy`, and `model_version`, so a weak market can be blocked without hiding
the rest of the strategy. Promotion PnL and drawdown are computed from observed
trades; synthetic fills are used only for simulator-quality comparison.
`blocked_segments.json` is the runtime artifact for those blocks. It restricts
future signals but does not authorize trades, and Rust risk remains the final
execution gate. Normalized segment ratios are exported for diagnosis, not yet as
hard gates.

Synthetic fills are implemented as `src.research.synthetic_fills`. They are
research-only estimates from future orderbook snapshots and are useful when
short `dry_run` samples have no observed fills. They are not exchange fills and
must not be used as live execution evidence.

Backtest exports include `observed_vs_synthetic_fill_summary`. This report is
the guardrail for synthetic fills: it compares observed fills, synthetic fills,
fill-rate deltas, slippage deltas, and realized-edge deltas for the same
signals. Synthetic fills should remain a baseline, not a promotion signal, until
that comparison is stable on real dry-run samples.

Backtest also exports `unfilled_reason_summary`. Use it before changing a model:
if most misses are `future_book_never_touched_limit`, the issue is quote
placement; if they are `observed_delayed` or `observed_unmatched`, the issue is
execution/reconciliation behavior; if synthetic fills are available without
observed fills, dry-run/live execution simulation needs review.

Backtest exports `dry_run_simulator_quality` for real dry-run runs. This report
is the simulator-quality gate: it compares observed dry-run fill-rate against
synthetic fill-rate, simulated slippage, time-to-fill, and the `PARTIAL` vs
`MATCHED` mix before model changes are evaluated.

The offline agent advisory report is implemented as `src.research.agent_advisory`.
It runs deterministic reviewers for edge, execution quality, adverse selection,
calibration, data quality, and reconciliation. These reviewers can flag model
risks and produce auditable artifacts, but `can_execute_trades` is always false.
Any useful reviewer output must become a versioned, testable feature before it
can influence the deterministic live policy.

## Versioning

Every promoted signal or research output should carry:

- `model_version`: model or policy identity;
- `data_version`: source/contract version;
- `feature_version`: feature set version.

The fields are optional in runtime signal contracts for backward compatibility, but new predictors and offline baselines should populate them.
