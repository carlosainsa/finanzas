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

This baseline is interpretable and should be the benchmark for any ML model.

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
- backtest report from [data_lake_plan.md](data_lake_plan.md);
- game-theory report from [game_theory_plan.md](game_theory_plan.md);
- walk-forward split by date;
- calibration report;
- comparison against deterministic baseline.

Minimum promotion gates:

- positive realized edge after slippage;
- acceptable fill-rate;
- no persistent adverse selection;
- stable calibration;
- bounded drawdown in dry-run.
