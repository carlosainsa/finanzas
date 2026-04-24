# Ideas from external Polymarket repositories

This review extracts implementation ideas that can add value to this trading system without copying code.

## Adopted into roadmap

- [architecture_plan.md](architecture_plan.md) defines the target incremental architecture.
- [interface_plan.md](interface_plan.md) defines the v1 Operator API + CLI interface. A dashboard web is intentionally deferred.
- [implementation_roadmap.md](implementation_roadmap.md) turns these ideas into phased implementation work.

## Highest-value next steps

1. **Research data lake**
   - Inspired by `prediction-market-analysis`.
   - Store normalized markets, orderbooks, trades, signals, and execution reports as Parquet snapshots.
   - Add DuckDB analysis scripts for calibration, realized edge by price bucket, maker/taker performance, and strategy PnL.

2. **Full local orderbook maintenance**
   - Inspired by `poly-maker`.
   - Keep a per-asset local book from `book` snapshots plus `price_change` deltas.
   - Derive best/second-best levels, size-aware best bid/ask, depth near midpoint, and stale-book checks.

3. **User WebSocket reconciliation**
   - Inspired by `poly-maker`.
   - Subscribe to the authenticated user channel and update orders/fills from live `order` and `trade` events.
   - Use this to close the loop faster than polling the CLOB REST API.

4. **CLI/operator controls**
   - Inspired by `polymarket-cli`.
   - Add operator commands for status, open orders, balances, cancel-all, risk snapshot, and JSON output for automation.
   - Support signature type as explicit config: `proxy`, `eoa`, or `gnosis-safe`.

5. **Market discovery and evidence scoring**
   - Inspired by `Polymarket/agents` and `last30days-skill`.
   - Build a research pipeline that combines Gamma market metadata, recent news/search context, social/news signals, and market odds movement.
   - Rank candidate markets before the predictor sees them.

## Useful patterns to adopt later

- Chunked append-only storage with deduplication for large historical datasets.
- Calibration metrics: expected calibration error, maximum calibration error, Brier-style summaries, and win-rate-vs-price buckets.
- Position-aware quoting: quote size should depend on existing inventory, open orders, min order size, and exposure across complementary outcomes.
- Configuration precedence: CLI flag, env var, config file, then defaults.
- Output mode split: human table output for operators and structured JSON for agents/scripts.
- Search-quality style evals: compare baseline vs candidate strategy outputs using deterministic overlap/retention metrics before promoting a new model.

## Not recommended right now

- Google Sheets as production control plane: useful for manual market-making experiments, but weaker than typed config plus API controls.
- Recursive agent retries for trading decisions: failures should go through bounded retry and dead-letter paths.
- LLM direct execution of trades: use LLM/research outputs as inputs to scoring, then enforce deterministic risk gates before execution.
