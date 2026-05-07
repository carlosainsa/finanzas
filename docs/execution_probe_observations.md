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
scripts/run_execution_probe_v6_cycle.sh \
  --universe-duckdb "<prior-wide-run>/research.duckdb" \
  --baseline-report-root "<prior-v5-report-root>" \
  --duration-seconds 5400 \
  --print-plan
```

Then rerun without `--print-plan` to execute the observation. The wrapper writes
`.tmp/operational/.../execution_probe_v6_cycle_summary.json` with the report
root and the recommendation from `execution_probe_next_decision.json`.

Required interpretation:

- Generate `execution_probe_next_decision.json` from
  `profile_observation_comparison.json` before choosing the next variant.
- If v6 gets fills without synthetic optimism or risk regression, repeat v6 for
  a longer window before any promotion discussion.
- If v6 still gets no fills, use no-fill future-touch diagnostics to distinguish
  filter relaxation from market/timing changes.
- If v6 gets fills but adverse selection or drawdown is bad, keep the profile
  research-only and add stricter market/side filters.

Manual interpretation is allowed only as review of the versioned decision
artifact. The decision engine remains offline and always emits
`can_execute_trades=false`.

## 2026-05-07 - execution_probe_v6 Cycle 90m

- Run id: `execution-probe-v6-cycle-20260507T005153Z`
- Report root: `.tmp/real-dry-run-data-lake/execution-probe-v6-cycle-20260507T005153Z/reports/execution-probe-v6-cycle-20260507T005153Z`
- Mode: `EXECUTION_MODE=dry_run`
- Profile: `execution_probe_v6`
- Universe: 6 market assets selected from `pre-live-execution-probe-45m-20260505T213534Z`
- Baseline: `execution-probe-v5-multimarket-60m-20260506T211421Z`
- Duration: 90 minutes

Key metrics:

- Orderbook snapshots: `12001`
- Signals: `836`
- Execution reports: `8`
- Filled signals: `4`
- Observed fill rate: `0.004784688995215311`
- Synthetic fill rate: `1.0`
- Fill-rate gap: `0.9952153110047847`
- Adverse selection: `0.9905945570761467`
- Realized edge: `0.08249999999999995`
- Stale data rate: `0.004666277810182484`
- Reconciliation divergence rate: `0.0`

Decision artifact:

- `execution_probe_next_decision.json`
- Recommendation: `CREATE_V7_LESS_AGGRESSIVE_QUOTE`
- Next step: create v7 with lower quote aggressiveness or stricter synthetic-fill guards.

Interpretation:

`execution_probe_v6` recovered measurable activity and observed fills, but it
also exposed a major simulator mismatch: 832 signals were synthetic-only, while
only 4 signals had observed dry-run fills. The dominant failure is not missing
touch evidence; no-fill diagnostics report `avg_required_quote_move=0` and
`no_fill_future_touch_rate=1.0`. The next variant must therefore reduce quote
aggression and keep synthetic-only fills as diagnostic evidence, not promotion
evidence.

## Next Variant: execution_probe_v7

`execution_probe_v7` is the dry-run-only response to the v6 decision artifact.
It keeps the v6 market/universe workflow but quotes less aggressively by default:

- `PREDICTOR_EXECUTION_PROBE_V7_NEAR_TOUCH_MAX_SPREAD_FRACTION=0.85`
- `PREDICTOR_EXECUTION_PROBE_V7_OFFSET_TICKS=1`

Operator flow:

```bash
scripts/run_execution_probe_v7_cycle.sh \
  --universe-duckdb "<prior-wide-run>/research.duckdb" \
  --baseline-report-root "<v6-report-root>" \
  --duration-seconds 5400 \
  --print-plan
```

Then rerun without `--print-plan` only after confirming the plan remains
`dry_run` and `can_execute_trades=false`.

## execution_probe_v7 Result And Market/Timing Filter

The 2026-05-07 90-minute `execution_probe_v7` cycle fixed the main v6
diagnostic issue: synthetic fill-rate no longer dominated observed fill-rate.
However, it still produced no observed fills:

- signals: `949`
- observed fill-rate: `0.0`
- synthetic fill-rate: `0.0`
- synthetic-vs-observed gap: `0.0`
- no-fill future-touch rate: `0.0`
- recommendation: `CHANGE_MARKET_OR_TIMING_FILTERS`

That result means the next repeat should not change quote aggressiveness. It
should keep `execution_probe_v7` dry-run-only and select markets with prior
future-touch evidence before launching the observation:

```bash
scripts/run_execution_probe_v7_cycle.sh \
  --universe-duckdb "<prior-wide-run>/research.duckdb" \
  --baseline-report-root "<v7-report-root>" \
  --market-timing-filter future_touch \
  --min-future-touch-rate 0.10 \
  --min-timing-signals 5 \
  --min-assets 3 \
  --duration-seconds 5400 \
  --print-plan
```

The v7 cycle defaults to `future_touch` filtering and
`min_avg_opportunity_spread=0.01` with a three-asset minimum so one-tick offset
quotes are not tested on markets where the spread is too tight to give useful
execution evidence.

After a filtered v7 run, `execution_probe_next_decision.json` includes
`market_timing_filter_decision`. That block is still offline-only
(`can_execute_trades=false`) and decides whether to keep, relax, reject, or
repeat the market/timing filter based on fill-rate lift, synthetic-vs-observed
gap, adverse selection, drawdown, and sample size.

The 2026-05-07 filtered v7 run selected 3 assets and captured 607 signals, but
still produced zero observed fills and zero future-touch evidence. The generated
`market_timing_filter_decision` was `RELAX_MARKET_TIMING_FILTER`, with the next
cycle lowering `min_future_touch_rate` to `0.05` and
`min_avg_opportunity_spread` to `0.005` while keeping the run research-only.
