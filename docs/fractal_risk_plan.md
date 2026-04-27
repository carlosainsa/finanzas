# Fractal Risk Plan

This plan covers Mandelbrot-style market diagnostics: fat tails, volatility
clustering, fractal roughness, drawdown shocks, and black-swan preparedness.
It is an offline research and risk layer first. It does not authorize trades.

## Why It Matters

Prediction-market prices are bounded probabilities, but their short-horizon
microstructure is not Gaussian. Liquidity can vanish, spreads can jump, and
public information can reprice a market discontinuously. Normal variance alone
understates this risk.

The goal is not to predict black swans. The goal is to reduce size, pause
segments, or require stronger evidence when a market shows heavy-tail or
unstable-regime behavior.

## Diagnostics

Initial diagnostics are implemented in `src.research.market_regime`:

- log returns of mid-price by `market_id` and `asset_id`;
- realized volatility and max absolute return;
- mid-price drawdown from local running highs;
- volatility clustering through autocorrelation of absolute returns;
- Hurst proxy and fractal-dimension proxy;
- Hill tail-index estimate over the top decile of absolute returns.

These metrics are deliberately conservative diagnostics. They should be used to
flag regimes and compare strategy behavior, not as standalone alpha.

## Operational Use

Offline:

- identify markets where edge disappears during jumpy regimes;
- compare realized edge and fill-rate across tail-risk buckets;
- stress-test blocklists before applying them to dry-run;
- explain drawdown clusters before changing predictor thresholds.

Risk layer later:

- reduce maximum size in high tail-risk regimes;
- require higher confidence when volatility clustering is high;
- pause segments when drawdown shocks exceed calibrated thresholds;
- feed a deterministic Rust risk gate only after offline validation.

## Acceptance Criteria

Before any fractal-risk signal influences live behavior:

- metrics must be exported in every research run;
- walk-forward analysis must show that risk reduction improves drawdown without
  destroying fill-adjusted realized edge;
- thresholds must be versioned and deterministic;
- changes must pass `research_promotion_decision`;
- Rust remains the final execution gate.

## Current Artifact

Run:

```bash
PYTHONPATH=python-service python -m src.research.market_regime \
  --duckdb data_lake/research.duckdb \
  --output-dir data_lake/market_regime
```

Outputs:

- `market_regime_summary.parquet`;
- `market_tail_risk.parquet`;
- `whale_pressure.parquet`;
- `market_regime_trade_context.parquet`;
- `market_regime_trade_buckets.parquet`;
- `market_regime_bucket_drawdown.parquet`;
- `market_regime_bucket_performance.parquet`;
- `market_regime.json`.

`market_regime_bucket_performance.parquet` joins regime diagnostics with
`backtest_trades` and reports realized edge, fill-rate, slippage, adverse edge
rate, trade-level max drawdown, and PnL per signal by tail-risk,
volatility-cluster, Hurst, and whale-pressure buckets. This is the first
evidence layer for deciding whether a regime metric should become a blocklist
rule, a sizing reducer, or only a diagnostic label.

The current attribution is post-run explanatory. It uses run-level regime
summaries, so it must not be treated as point-in-time training evidence until a
future view joins only features available before `signal_timestamp_ms`.
