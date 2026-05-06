# Execution Probe Observation Log

This document records operator-level dry-run observations that should inform the
next execution-probe variant. These entries are research evidence only and never
authorize live trading.

## 2026-05-06 - execution_probe_v5 Multi-Market 60m

- Run id: `execution-probe-v5-multimarket-60m-20260506T211421Z`
- Report root: `.tmp/real-dry-run-data-lake/execution-probe-v5-multimarket-60m-20260506T211421Z/reports/execution-probe-v5-multimarket-60m-20260506T211421Z`
- Mode: `EXECUTION_MODE=dry_run`
- Profile: `execution_probe_v5`
- Universe: 6 market assets from `execution_probe_universe_selection_v1`
- Near-touch fraction: external `execution_probe_v5_fraction_selection.json`, selected fraction `0.75`
- Comparison baseline: `restricted-execution-probe-v5-60m-20260506174655`

### Result

The run is `NO_GO`.

Key metrics:

- Orderbook snapshots: `2717`
- Signals: `454`
- Execution reports: `8`
- Filled signals: `0`
- Fill rate: `0.0`
- Dry-run observed fill rate: `0.0`
- Synthetic fill rate: `0.0`
- Adjusted synthetic fill rate: `0.0`
- Stale data rate: `0.02097902097902098`
- Reconciliation divergence rate: `0.0`
- Drawdown: `0.0`

Readiness blockers:

- `go_no_go_passed`
- `acceptable_dry_run_observed_fill_rate`
- `acceptable_fill_rate`
- `calibration_available`
- `has_fills`
- `positive_realized_edge`

### Interpretation

The multi-market universe improved activity and data freshness versus the prior
restricted v5 run:

- Signals increased from `112` to `454`.
- Execution reports increased from `4` to `8`.
- Stale data rate improved from `0.1` to `0.02097902097902098`.

It did not improve execution quality:

- Observed fills remained `0`.
- Synthetic fills remained `0`.
- The dominant quote diagnostics root cause was `future_book_never_touched_limit`.

### Decision

Do not repeat `execution_probe_v5` unchanged.

The next variant should change the quote/execution policy instead of only
rotating markets or repeating `near_touch_max_spread_fraction=0.75`. A useful
`execution_probe_v6` should test a deliberately more executable quote policy,
still in `dry_run`, while keeping Rust risk gates as the final authority.

Candidate directions:

- quote closer to touch for a bounded subset of high-liquidity markets;
- reduce signal/report mismatch by emitting fewer but more executable signals;
- add per-market quote aggressiveness from observed spread/touch dynamics;
- keep `can_execute_trades=false` until observed fills, realized edge, and
  adverse-selection metrics become measurable.

## Next Variant: execution_probe_v6

`execution_probe_v6` is the next research-only variant. It should test whether
quotes at or very near the current touch can create measurable dry-run fills.

Operator flow:

```bash
scripts/prepare_execution_probe_cycle.sh \
  --universe-duckdb "<prior-wide-run>/research.duckdb" \
  --baseline-report-root "<prior-v5-report-root>" \
  --duration-seconds 3600
```

Then inspect the generated `.tmp/operational/.../execution_probe_observation_plan.json`
before running the emitted command.

Required interpretation:

- If v6 gets fills, compare realized edge, adverse selection, drawdown, and
  reconciliation before any longer repeat.
- If v6 still gets no fills, the blocker is probably market selection or signal
  timing, not just quote distance.
- If v6 gets fills but adverse selection is bad, keep the quote policy but add
  stricter market/side filters.
