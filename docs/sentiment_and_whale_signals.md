# Sentiment and Whale Signals

This document defines how external sentiment and large-flow detection should
enter the platform. Both are research inputs first. Neither should place orders
or bypass deterministic risk controls.

## Sentiment Market Layer

Sentiment is useful only when converted into structured, timestamped, testable
features. It must be aligned by event time to avoid leakage.

Candidate sources:

- news headlines and article embeddings;
- social posts from X, Reddit, Farcaster, Telegram, and relevant forums;
- Google Trends, Wikipedia pageviews, and other attention proxies;
- Polymarket market comments if available through a stable source;
- event calendars and official announcements.

Candidate features:

- `sentiment_score`;
- `sentiment_momentum`;
- `sentiment_disagreement`;
- `source_quality_score`;
- `evidence_recency_ms`;
- `attention_spike_zscore`;
- `narrative_shift_score`;
- divergence between sentiment direction and implied market probability.

Evaluation rules:

- all features must be timestamped before the target event or signal;
- train/test splits must be chronological;
- sentiment must improve Brier score, log loss, realized edge after slippage,
  or market-discovery precision;
- sentiment cannot directly issue trades.

## Data Contracts

Initial contracts are defined in `shared/schemas/external_evidence.json`,
`shared/schemas/sentiment_feature.json`, and `python-service/src/schemas.py`.

`external_evidence` rows contain:

- `evidence_id`;
- `source`;
- `source_type`;
- `published_at_ms`;
- `observed_at_ms`;
- `available_at_ms`;
- `market_id`;
- `asset_id`;
- `raw_reference_hash`;
- `direction`;
- `sentiment_score`;
- `source_quality`;
- `confidence`;
- `data_version`.

`sentiment_features` rows contain:

- `feature_id`;
- `evidence_id`;
- `market_id`;
- `asset_id`;
- `observed_at_ms`;
- `available_at_ms`;
- `feature_timestamp_ms`;
- `direction`;
- `sentiment_score`;
- `net_sentiment`;
- `lookback_ms`;
- `evidence_count`;
- `source_count`;
- `evidence_ids_hash`;
- `source_quality`;
- `confidence`;
- `model_version`;
- `data_version`;
- `feature_version`.

The first anti-leakage rules are enforced at schema validation time:
`observed_at_ms >= published_at_ms` for evidence and
`feature_timestamp_ms >= observed_at_ms` for derived sentiment features. Derived
features also require `available_at_ms >= feature_timestamp_ms`.

The first deterministic builder is `src.research.sentiment_features`. It
aggregates already-loaded `external_evidence` rows into
`sentiment_feature_candidates` over a configurable lookback window. It performs
no scraping and does not enter the live predictor.

## Whale and Flow Detection

Whale detection means large-flow and market-impact diagnostics. It must not try
to identify private people or infer non-public identity. The useful unit is
behavior visible in the orderbook and fills.

Initial signals:

- large top-of-book levels relative to recent level-size distribution;
- depth withdrawals after large quotes appear;
- orderbook imbalance;
- repeated quote changes;
- sweeps inferred from rapid mid-price movement and depth changes;
- price impact after observed fills.

The first implemented artifact is `whale_pressure` in
`src.research.market_regime`. It exports:

- `large_level_threshold`;
- `large_level_updates`;
- `large_order_ratio`;
- `depth_withdrawal_events`;
- `depth_withdrawal_rate`;
- `avg_orderbook_imbalance`;
- `whale_pressure_score`.

## How These Signals Should Be Used

Allowed offline uses:

- diagnose adverse selection;
- classify markets as crowded, thin, unstable, or whale-sensitive;
- propose segment blocks;
- compare fill-rate and drawdown by whale-pressure bucket;
- inform future deterministic risk rules.

Not allowed:

- LLM or sentiment direct execution;
- free-form agent consensus for live trades;
- live size increases without Rust risk gates;
- training on post-signal text, post-resolution labels, or future orderbook data.

## Promotion Path

1. Export sentiment/whale features into DuckDB with explicit timestamps.
2. Add walk-forward reports that prove no leakage.
3. Compare against the deterministic baseline.
4. Pass `research_promotion_decision`.
5. Convert useful features into versioned predictor inputs.
6. Keep Rust as final risk and execution authority.
