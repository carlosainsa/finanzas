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

## 2026-05-08 - Relaxed Filtered execution_probe_v7 60m

- Run id: `execution-probe-v7-cycle-20260508T015251Z`
- Report root: `.tmp/real-dry-run-data-lake/execution-probe-v7-cycle-20260508T015251Z/reports/execution-probe-v7-cycle-20260508T015251Z`
- Mode: `EXECUTION_MODE=dry_run`
- Profile: `execution_probe_v7`
- Market/timing filter: `future_touch`
- Filter thresholds: `min_future_touch_rate=0.05`, `min_avg_opportunity_spread=0.005`, `min_timing_signals=5`
- Universe: 4 market assets
- Duration: 60 minutes

Key metrics:

- Orderbook snapshots: `5751`
- Signals: `710`
- Execution reports: `8`
- Observed fill-rate: `0.0`
- Synthetic fill-rate: `0.04366197183098591`
- Synthetic-only signals: `30`
- No-fill future-touch rate: `0.04366197183098591`
- Stale data rate: `0.0027821248478525473`
- Reconciliation divergence rate: `0.0`

Decision artifact:

- `execution_probe_next_decision.json`
- Recommendation: `CHANGE_MARKET_OR_TIMING_FILTERS`
- `market_timing_filter_decision.decision`: `RELAX_MARKET_TIMING_FILTER`
- Next cycle thresholds: `min_future_touch_rate=0.025`, `min_avg_opportunity_spread=0.0025`, `min_timing_signals=5`

Interpretation:

The relaxed filter recovered enough activity and some future-touch evidence, but
still produced no observed dry-run fills. The pre-live gate remains `NO_GO`
because fill-rate is zero, positive realized edge is unavailable, and adverse
selection remains blocked. This is still a market/timing selection problem, not
a live-readiness problem.

The new `fillability_baseline_v1` artifact selected one asset with materially
better timing evidence:

- asset: `88275040060084773376557187972215267513049848642895776801789297917961077894224`
- future-touch rate: `0.24031007751937986`
- signals: `129`
- recommendation: `PROMOTE_TO_OBSERVATION`

Next observation should either use the generated relaxed thresholds across the
universe or run a focused observation on the fillability-selected asset before
changing quote policy again. Both paths remain research-only and must keep
`can_execute_trades=false`.

## 2026-05-08 - Fillability-Focused execution_probe_v7 30m

- Run id: `execution-probe-v7-cycle-20260508T123244Z`
- Report root: `.tmp/real-dry-run-data-lake/execution-probe-v7-cycle-20260508T123244Z/reports/execution-probe-v7-cycle-20260508T123244Z`
- Mode: `EXECUTION_MODE=dry_run`
- Profile: `execution_probe_v7`
- Selection source: `fillability_baseline_v1`
- Universe: 1 market asset
- Asset: `88275040060084773376557187972215267513049848642895776801789297961077894224`
- Duration: 30 minutes

Key metrics:

- Orderbook snapshots: `314`
- Signals: `68`
- Execution reports: `2`
- Observed fill-rate: `0.0`
- Synthetic fill-rate: `0.0`
- No-fill future-touch rate: `0.0`
- Stale data rate: `0.012738853503184714`
- Reconciliation divergence rate: `0.0`

Decision artifact:

- `execution_probe_next_decision.json`
- Recommendation: `CHANGE_MARKET_OR_TIMING_FILTERS`
- `market_timing_filter_decision.decision`: `RELAX_MARKET_TIMING_FILTER`
- Next cycle thresholds: `min_future_touch_rate=0.025`, `min_avg_opportunity_spread=0.0025`, `min_timing_signals=5`

Interpretation:

The fillability-focused run proved the new single-asset observation path works,
but the selected asset did not produce fills or future-touch evidence in this
window. This weakens the hypothesis that one historically better asset is enough
to recover execution activity. The next repeat should use a broader
fillability-first universe or the generated relaxed timing thresholds, rather
than increasing quote aggressiveness immediately.

The new `ml_fill_evaluation_v1` report also ran on this sample. It produced 69
evaluation examples, but the test split still has one-class or insufficient
labels, so it is diagnostic only and not ready for model training.

The follow-up tooling now keeps this fail-closed boundary explicit:
`prepare_ml_training_dataset` only exports `ml_fill_training_examples.parquet`
after `ml_fill_label_quality_gate_v1.can_train_models=true`. It can be enabled
in the research loop with `PREPARE_ML_TRAINING_DATASET=1`, but blocked runs
produce only a preparation report and never a trainable parquet artifact.

Follow-up rule:

`execution_probe_next_decision_v1` now treats narrow fillability runs separately.
If `selection_source=fillability` has no observed fills and the universe is too
small or the signal sample is thin, the decision is
`EXPAND_FILLABILITY_UNIVERSE` instead of immediately relaxing timing thresholds.
The universe selector can now backfill missing `min_assets` coverage from
`KEEP_DIAGNOSTIC` markets with sufficient signals, liquidity, and spread
evidence. Backfilled rows carry
`fallback_reason=fillability_min_assets_backfill_keep_diagnostic_liquidity_spread`,
so they remain diagnostic candidates, not promoted fillability winners. The
next dry-run plan should start from:

```bash
scripts/run_execution_probe_v7_cycle.sh \
  --universe-duckdb ".tmp/real-dry-run-data-lake/execution-probe-v7-cycle-20260508T015251Z/research.duckdb" \
  --baseline-report-root ".tmp/real-dry-run-data-lake/execution-probe-v7-cycle-20260508T123244Z/reports/execution-probe-v7-cycle-20260508T123244Z" \
  --universe-selection-source fillability \
  --universe-limit 5 \
  --min-assets 3 \
  --min-future-touch-rate 0.025 \
  --min-timing-signals 5 \
  --min-avg-opportunity-spread 0.0025 \
  --duration-seconds 1800 \
  --print-plan
```

## 2026-05-08 - Fillability Fallback execution_probe_v7 60m

- Run id: `execution-probe-v7-fillability-fallback-20260508T134000Z`
- Report root: `.tmp/real-dry-run-data-lake/execution-probe-v7-fillability-fallback-20260508T134000Z/reports/execution-probe-v7-fillability-fallback-20260508T134000Z`
- Mode: `EXECUTION_MODE=dry_run`
- Profile: `execution_probe_v7`
- Selection source: `fillability`
- Universe: 3 market assets
- Fallback: 1 primary fillability asset + 2 `KEEP_DIAGNOSTIC` backfill assets
- Duration: 60 minutes
- Universe hash: `93a5e6d312296a012d5a51f4d48126e688dfd74a810e04f77d8a7b309bbdb760`

Key metrics:

- Orderbook stream events: `2714`
- Signals: `415`
- Execution reports: `6`
- Report statuses: `3 DELAYED`, `3 UNMATCHED`
- Observed fill-rate: `0.0`
- Dry-run fill-rate: `0.0`
- Synthetic fill-rate: `0.08674698795180723`
- Synthetic-only signals: `36`
- No-fill future-touch rate: `0.08674698795180723`
- Adjusted synthetic fill-rate: `0.021686746987951807`
- Stale data rate: `0.011786372007366482`
- Reconciliation divergence rate: `0.0`

Decision artifacts:

- `profile_observation_comparison.json`
- `profile_observation_comparison_all_v7.json`
- `execution_probe_next_decision.json`
- `execution_probe_next_decision_all_v7.json`
- Recommendation: `CHANGE_MARKET_OR_TIMING_FILTERS`
- `market_timing_filter_decision.decision`: `EXPAND_FILLABILITY_UNIVERSE`
- Reason: `fillability_universe_too_sparse_for_market_timing_relaxation`

Interpretation:

The fallback selector worked operationally: it produced a ready 3-asset
fillability universe and collected enough signal volume for a meaningful
observation. The result is still not promotable. The run recovered synthetic
future-touch evidence, but did not produce observed fills, and only 3 signal
lifecycles created dry-run orders. Most signals had no observed report, so the
next step should expand the fillability universe before changing quote
aggressiveness. The all-v7 comparison reached the same decision as the immediate
baseline comparison.

Next dry-run plan:

```bash
scripts/run_execution_probe_v7_cycle.sh \
  --universe-duckdb ".tmp/real-dry-run-data-lake/execution-probe-v7-fillability-fallback-20260508T134000Z/research.duckdb" \
  --baseline-report-root ".tmp/real-dry-run-data-lake/execution-probe-v7-fillability-fallback-20260508T134000Z/reports/execution-probe-v7-fillability-fallback-20260508T134000Z" \
  --universe-selection-source fillability \
  --universe-limit 10 \
  --min-assets 5 \
  --min-future-touch-rate 0.025 \
  --min-timing-signals 5 \
  --min-avg-opportunity-spread 0.0025 \
  --duration-seconds 3600 \
  --print-plan
```

## 2026-05-08 - Fillability Expanded execution_probe_v7 60m

- Run id: `execution-probe-v7-fillability-expanded-20260508T151000Z`
- Report root: `.tmp/real-dry-run-data-lake/execution-probe-v7-fillability-expanded-20260508T151000Z/reports/execution-probe-v7-fillability-expanded-20260508T151000Z`
- Mode: `EXECUTION_MODE=dry_run`
- Profile: `execution_probe_v7`
- Selection source: `fillability`
- Universe: 5 market assets
- Universe config: `universe_limit=10`, `min_assets=5`
- Filter thresholds: `min_future_touch_rate=0.025`, `min_avg_opportunity_spread=0.0025`, `min_timing_signals=5`
- Fallback: 1 primary fillability asset + 2 `KEEP_DIAGNOSTIC` backfill assets + 2 market metadata liquidity backfill assets
- Duration: 60 minutes
- Universe hash: `ec55dd32946a0820d39e5c888b2abe4705a9984b333b79cd06e80af2e518fed0`

Key metrics:

- Orderbook stream events: `2637`
- Orderbook snapshots: `2637`
- Signals: `373`
- Execution reports: `6` raw rows, `3` terminal signal reports
- Report statuses: `3 DELAYED`, `3 UNMATCHED`
- Observed fill-rate: `0.0`
- Dry-run fill-rate: `0.0`
- Synthetic fill-rate: `0.03485254691689008`
- Synthetic-only signals: `13`
- No-fill future-touch rate: `0.03485254691689008`
- Adjusted synthetic fill-rate: `0.00871313672922252`
- Stale data rate: `0.02578687902919985`
- Reconciliation divergence rate: `0.0`
- Filled signals: `0`
- Realized edge: unavailable because there were no fills
- Adverse selection: `1.0`
- Drawdown: `0.0`

Signal-to-order conversion:

- `signal_to_order_conversion.report_version`: `signal_to_order_conversion_v1`
- Signals analyzed: `373`
- Signals with terminal report: `3`
- Signals missing execution report: `370`
- Orders created: `3`
- Order creation rate: `0.00804289544235925`
- Missing report rate: `0.9919571045576407`
- Dominant root cause: `missing_execution_report`

Decision artifacts:

- `profile_observation_comparison.json`
- `profile_observation_comparison_all_v7.json`
- `execution_probe_next_decision.json`
- `execution_probe_next_decision_all_v7.json`
- `signal_to_order_conversion.json`
- Recommendation: `CHANGE_MARKET_OR_TIMING_FILTERS`
- `market_timing_filter_decision.decision`: `RELAX_MARKET_TIMING_FILTER`
- Reason: `filtered_universe_still_has_no_observed_fills`
- Next cycle thresholds: `min_future_touch_rate=0.0125`, `min_avg_opportunity_spread=0.00125`, `min_timing_signals=5`

Interpretation:

The 5-asset fillability-expanded run improved universe coverage and reduced the
synthetic-only gap versus the 3-asset fallback run, but it still produced no
observed fills. The new signal-to-order diagnostic shows the main operational
blocker more directly: 370 of 373 signals had no terminal execution report, and
only 3 signals created dry-run orders. That means the current issue is not signal
generation volume. The next research-only loop should relax market/timing
filters and investigate why most accepted strategy signals do not become
executor reports before changing quote aggressiveness.

The run remains `research-only`; `can_execute_trades=false` and the go/no-go
decision is `NO_GO`.

## 2026-05-09 - Relaxed Timing execution_probe_v7 60m

- Run id: `execution-probe-v7-relaxed-timing-20260509T000000Z`
- Report root: `.tmp/real-dry-run-data-lake/execution-probe-v7-relaxed-timing-20260509T000000Z/reports/execution-probe-v7-relaxed-timing-20260509T000000Z`
- Mode: `EXECUTION_MODE=dry_run`
- Profile: `execution_probe_v7`
- Selection source: `fillability`
- Universe: 5 market assets
- Filter thresholds: `min_future_touch_rate=0.0125`, `min_avg_opportunity_spread=0.00125`, `min_timing_signals=5`
- Duration: 60 minutes
- Universe hash: `85e07aab664e24f9e1e01f879974ee8c00e0cc604bf11dca75d2d6dbb05042e2`

Key metrics:

- Orderbook stream events: `4671`
- Orderbook snapshots: `4672`
- Signals: `434`
- Execution reports: `815` raw rows, `434` terminal signal reports
- Report statuses: `381 DELAYED`, `381 UNMATCHED`, `53 ERROR`
- Observed fill-rate: `0.0`
- Dry-run fill-rate: `0.0`
- Synthetic fill-rate: `0.06451612903225806`
- Synthetic-only signals: `0`
- No-fill future-touch rate: `0.06451612903225806`
- Adjusted synthetic fill-rate: `0.04032258064516129`
- Stale data rate: `0.004066780821917808`
- Reconciliation divergence rate: `0.0`
- Filled signals: `0`
- Adverse selection: `1.0`
- Drawdown: `0.0`

Signal-to-order conversion:

- `signal_to_order_conversion.report_version`: `signal_to_order_conversion_v1`
- Signals analyzed: `434`
- Signals consumed: `434`
- Consumption rate: `1.0`
- Signals missing execution report: `0`
- Orders created: `381`
- Order creation rate: `0.8778801843317973`
- Rejected consumed signals: `53`
- Rejection rate: `0.12211981566820276`
- Rejection reason: `market exposure exceeds MAX_MARKET_EXPOSURE`
- Dominant root cause after pipeline fix: `order_created_unfilled`

Decision artifacts:

- `profile_observation_comparison.json`
- `profile_observation_comparison_all_v7.json`
- `execution_probe_next_decision.json`
- `execution_probe_next_decision_all_v7.json`
- `signal_to_order_conversion.json`
- Recommendation: `CHANGE_MARKET_OR_TIMING_FILTERS`
- `market_timing_filter_decision.decision`: `RELAX_MARKET_TIMING_FILTER`
- Reason: `filtered_universe_still_has_no_observed_fills`
- Next cycle thresholds: `min_future_touch_rate=0.00625`, `min_avg_opportunity_spread=0.000625`, `min_timing_signals=5`

Interpretation:

The executor/Redis durability fix changed the problem materially. The previous
expanded run had 370 of 373 signals without an observed report. This run had
zero missing execution reports: every signal was consumed and classified. The
remaining blocker is no longer signal-to-order traceability; it is execution
quality. Most consumed signals created dry-run orders and ended `UNMATCHED`, and
the explicit `ERROR` reports are risk gate rejections from max market exposure.

The run remains `research-only`; `can_execute_trades=false` and the go/no-go
decision is `NO_GO`.
