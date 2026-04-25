# Game Theory Plan

This layer is offline research first. It should inform pricing, sizing, and cancellation policy, but it must not bypass deterministic Rust risk gates.

## Goals

- Detect adverse selection after passive fills.
- Measure whether quote competition is too aggressive for passive market making.
- Quantify fill-rate by distance to the current mid-price.
- Track binary no-arbitrage gaps where paired outcome prices imply `YES + NO != 1`.
- Convert these measurements into features for the predictor only after backtests show stable value.

## Models

### Passive Maker Game

The bot chooses whether to improve a quote, rest at the current level, cancel, or avoid the market. The payoff is:

```text
expected_payoff = expected_edge - adverse_selection_cost - inventory_penalty - execution_cost
```

The first implementation measures the terms rather than optimizing them live.

### Adverse Selection

Passive orders are harmful when fills are followed by price movement against the bot. The offline metric computes mark-to-market PnL after a fill at multiple horizons:

- `pnl_5s`
- `pnl_30s`
- `pnl_300s`

For BUY fills:

```text
pnl_horizon = future_mid_price - filled_price
```

For SELL fills:

```text
pnl_horizon = filled_price - future_mid_price
```

Negative average PnL after passive fills means the strategy is being selected by better-informed flow.

### Quote Competition

Repeated quote changes at the best bid/ask indicate competition for priority. The first metric counts best bid/ask changes per market and asset:

- `snapshots`
- `quote_changes`
- `quote_change_rate`
- `avg_spread`
- `avg_depth`

High quote change rate plus low spread can make passive edge fragile.

### Queue and Fill Game

Fill-rate is grouped by distance from the signal price to the latest mid-price:

- `000_050bps`
- `050_100bps`
- `100_250bps`
- `250_500bps`
- `500bps_plus`

This provides the first approximation of queue competitiveness without pretending to know exact queue position.

### Binary No-Arbitrage

For binary markets, paired outcome mid-prices should sum near one:

```text
no_arbitrage_gap = asset_a_mid + asset_b_mid - 1
```

The offline view is advisory. Live arbitrage requires fee, spread, fill probability, and cancellation-risk checks before execution.

## Implementation

Run after exporting the research data lake:

```bash
PYTHONPATH=python-service python -m src.research.data_lake \
  --root data_lake \
  --duckdb data_lake/research.duckdb \
  --count 1000

PYTHONPATH=python-service python -m src.research.game_theory \
  --duckdb data_lake/research.duckdb \
  --output-dir data_lake/game_theory
```

The command writes:

- `post_fill_pnl_horizons.parquet`
- `fill_rate_by_distance_to_mid.parquet`
- `adverse_selection_by_strategy.parquet`
- `quote_competition.parquet`
- `binary_no_arbitrage.parquet`

## Promotion Criteria

A game-theory feature can enter the live predictor only after:

- walk-forward backtests improve realized edge after slippage;
- adverse-selection metrics are stable by market class;
- feature definitions are deterministic and reproducible from Parquet/DuckDB;
- Rust risk gates continue to enforce size, stale-signal, exposure, kill-switch, and execution-mode limits.
