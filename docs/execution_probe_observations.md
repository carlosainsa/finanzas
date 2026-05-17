# Execution Probe Observation Log

This document records operator-level dry-run observations that should inform the
next execution-probe variant. These entries are research evidence only and never
authorize live trading.

## Current Next Decision After execution_probe_v11

The completed `execution_probe_v11` run confirmed that runtime activity can be
restored on a touch-probability universe, but selected markets still did not
produce fills or future-touch evidence. The next change should target fresh
market/timing selection, not live gates.

V11 differences from v10:

- offline `touch_probability_ranking_v1` ranks assets by observed/future touch
  probability, observed fill evidence, distance to touch, stale rate, depth,
  liquidity, and synthetic-vs-observed gap penalty.
- `execution_probe_universe_selection_v1` supports
  `selection_source=touch_probability` and records
  `touch_probability_filter` in the research-only universe payload.
- runtime uses `execution_probe_v11`, remains `EXECUTION_MODE=dry_run`, and
  preserves the conservative v10 quote policy while testing whether better
  market/timing selection can recover fills.
- `scripts/run_execution_probe_v11_cycle.sh` defaults to 60 minutes,
  `selection_source=touch_probability`, and `market_timing_filter=future_touch`.

Promotion remains blocked. The next candidate should use
`scripts/run_runtime_touch_cycle.sh`, which captures a fresh runtime window,
selects assets with `runtime_touch_ranking_v1`, runs `execution_probe_v11` in
dry-run over that universe, and writes `execution_probe_touch_comparison.json`.
This keeps the predictor profile explicit while changing only market/timing
selection.

## Prior Variant: execution_probe_v10

`execution_probe_v10` is the next research-only variant after the corrected
v9 toxicity filter proved active but produced zero observed fills in the longer
fixed-universe repeat. It should test an evidence-ranked executable segment
policy, not another global quote or toxicity-filter tweak.

Planned differences from v9:

- offline `executable_opportunities_v1` dataset joins signal, book, fill,
  synthetic touch, depth, spread, timing, and markout context.
- offline `segment_opportunity_ranking_v1` promotes only segments with
  sufficient executable opportunities, edge, depth, low synthetic optimism, and
  acceptable adverse selection, and now scores runtime coverage with
  `runtime_opportunities` / `runtime_active_minutes` so offline-good segments
  that never appear at runtime do not dominate the allowlist.
- runtime v10 loads `allowed_segments_v1` and rejects snapshots outside the
  selected market/asset/side/spread/timing buckets.
- `allowed_segment_candidates` may include
  `allowed_reason=RUNTIME_ACTIVITY_BACKFILL` rows when they have runtime
  activity but were not selected by the strict offline executable ranking. These
  are coverage candidates for dry-run observation, not live permissions.
- `scripts/run_execution_probe_v10_cycle.sh` defaults to
  `selection_source=executable_segments` and `EXECUTION_MODE=dry_run`.

Promotion remains blocked unless v10 produces observed fills, positive realized
edge after slippage, low synthetic-vs-observed gap, stable segment evidence, and
materially lower adverse selection.

## 2026-05-13 - execution_probe_v10 Local Preflight

- Run root: `.tmp/operational/execution-probe-v10-preflight-20260513T011527Z`
- Source DuckDB: `data_lake/research.duckdb`
- Baseline report root: `data_lake/reports/real-dry-run-20260426T204208Z`
- Mode: research-only preflight, no services started
- Profile: `execution_probe_v10`
- Selection source: `executable_segments`
- Planned duration: 30 minutes

Generated offline artifacts:

- `executable_opportunities_v1`
- `segment_opportunity_ranking_v1`
- `allowed_segments_v1`
- `run_execution_probe_v10_cycle.print_plan.json`

Key metrics:

- Opportunities: `20`
- Executable opportunities: `0`
- Observed fills: `0`
- Synthetic fills: `0`
- Ranked segments: `20`
- Selected executable segments: `0`
- Universe status: `insufficient_assets`
- Selected market assets: `0`

Decision:

- Observation was not started.
- `scripts/run_execution_probe_v10_cycle.sh --print-plan` resolved correctly
  with `selection_source=executable_segments` and `toxicity_filter=none`.
- `scripts/prepare_execution_probe_cycle.sh` correctly failed closed because
  the generated universe had `selected_segments=0` and
  `market_asset_ids_count=0`.
- Live remains blocked.

Interpretation:

The v10 pipeline works as a fail-closed research gate, but the only local
DuckDB left after cleanup is too small and too old to infer executable
segments. Running a 30-90 minute v10 observation now would be empty or
misleading because the predictor would have no allowed segments. The next
operational step is to collect a fresh dry-run data lake window with orderbook,
signals, reports, and synthetic/observed execution diagnostics, then regenerate
`executable_opportunities_v1` and `segment_opportunity_ranking_v1`.

## 2026-05-13 - execution_probe_v10 Fresh Base And Sparse Observation

- Base run id: `pre-v10-base-20260513T012123Z`
- Base report root: `.tmp/real-dry-run-data-lake/pre-v10-base-20260513T012123Z/reports/pre-v10-base-20260513T012123Z`
- V10 cycle id: `execution-probe-v10-complete-from-pre-v10-base-20260513T012123Z`
- V10 report root: `.tmp/real-dry-run-data-lake/execution-probe-v10-complete-from-pre-v10-base-20260513T012123Z/reports/execution-probe-v10-complete-from-pre-v10-base-20260513T012123Z`
- Mode: `EXECUTION_MODE=dry_run`
- Profile: `execution_probe_v10`
- Selection source: `executable_segments`
- Planned duration: 30 minutes
- Selected market assets: `2`

Base dry-run evidence:

- Orderbook stream length: `10786`
- Signals stream length: `10786`
- Reports stream length: `21570`
- Recent report statuses: `MATCHED=10743`, `DELAYED=10786`, `UNMATCHED=41`
- Base run note: the capture produced a usable DuckDB and evidence artifacts,
  but the full post-run readiness wrapper hit memory pressure in
  `signal_to_order_conversion`. The exported DuckDB and reports were still
  sufficient for v10 offline selection.

Offline v10 selection:

- Opportunities: `10802`
- Executable opportunities: `10785`
- Observed fills: `10761`
- Synthetic fills: `10742`
- Ranked segments: `69`
- Selected executable segments: `2`
- Selected assets:
  - `53831553061883006530739877284105938919721408776239639687877978808906551086026`
  - `98022490269692409998126496127597032490334070080325855126491859374983463996227`

Runtime v10 observation:

- Preflight contract was tightened for sparse research probes:
  `execution_probe_v10` can pass preflight with orderbook progress and zero
  signal progress, while the normal pre-live dry-run path still requires signal
  progress.
- Final stream lengths: `orderbook=977`, `signals=0`, `reports=0`
- Observation classification: `sparse_probe_no_signals`
- Cycle recommendation: `DO_NOT_PROMOTE`
- Next step emitted by the cycle: expand or re-rank the v10 segment selection by
  runtime activity before repeating observation.

Decision:

- Live remains blocked.
- Do not tune global quote aggressiveness from this result.
- Do not promote the two selected segments: they looked executable offline but
  generated no runtime signals under the v10 allowlist during the 30-minute
  observation.
- The next v10 improvement should add an activity-aware selector or widen the
  allowlist with explicit runtime-coverage evidence before repeating.
- Follow-up implementation note: ranking and universe selection should use
  `runtime_opportunities` and `runtime_active_minutes`; the selector may add
  `RUNTIME_ACTIVITY_BACKFILL` candidates to avoid choosing universes that looked
  executable offline but had no live market activity during the probe window.
  Live remains blocked.

## 2026-05-13 - execution_probe_v10 Runtime-Backfill 60m

- Run id: `execution-probe-v10-runtime-backfill-20260513T134541Z`
- Report root: `.tmp/real-dry-run-data-lake/execution-probe-v10-runtime-backfill-20260513T134541Z/reports/execution-probe-v10-runtime-backfill-20260513T134541Z`
- Source DuckDB: `.tmp/real-dry-run-data-lake/pre-v10-base-20260513T012123Z/research.duckdb`
- Mode: `EXECUTION_MODE=dry_run`
- Profile: `execution_probe_v10`
- Selection source: `executable_segments`
- Runtime activity backfill: enabled
- Planned duration: 60 minutes

Offline v10 selection:

- Universe status: `ready`
- Unique market assets: `7`
- Allowed segment candidates: `8`
- Strict promoted segments: `2`
- Backfill candidates: `6`
- Allowed reasons: `PROMOTE_TO_OBSERVATION`,
  `RUNTIME_ACTIVITY_BACKFILL`

Runtime observation:

- Final stream lengths: `orderbook=2890`, `signals=118`, `reports=236`
- Recent report statuses: `DELAYED=118`, `UNMATCHED=118`
- Signal-to-order consumption rate: `1.0`
- Order creation rate: `1.0`
- Missing report rate: `0.0`
- Observed fill rate: `0.0`
- Synthetic fill rate: `0.0`
- Average no-fill distance to touch: `0.0010000000000000009`
- No-fill future-touch rate: `0.0`
- Pre-live decision: `NO_GO`
- Cycle recommendation: `CHANGE_MARKET_OR_TIMING_FILTERS`

Decision:

- Live remains blocked.
- The runtime backfill solved the sparse-runtime failure: v10 no longer produced
  `signals=0`.
- The remaining blocker moved to market/timing quality, not infrastructure:
  118 orders were created and reconciled, but none filled and none had synthetic
  future-touch evidence.
- Do not increase quote aggression from this run alone. The automatic next step
  is to keep v10 research-only and retune market/timing selection before quote
  aggression.
- The selector now deduplicates `market_asset_ids` when multiple allowed
  segment buckets point to the same asset, so universe coverage is not inflated
  by spread/timing variants.

## 2026-05-13 - execution_probe_v11 Touch-Probability 60m

- Run id: `execution-probe-v11-touch-probability-20260513T160121Z`
- Report root: `.tmp/real-dry-run-data-lake/execution-probe-v11-touch-probability-20260513T160121Z/reports/execution-probe-v11-touch-probability-20260513T160121Z`
- Source DuckDB: `.tmp/real-dry-run-data-lake/pre-v10-base-20260513T012123Z/research.duckdb`
- Mode: `EXECUTION_MODE=dry_run`
- Profile: `execution_probe_v11`
- Selection source: `touch_probability`
- Planned duration: 60 minutes

Offline v11 selection:

- Default `min_assets=5` failed closed because the source evidence only had 2
  unique assets after deduplicating strategy/model rows.
- The actual run used explicit `min_assets=2` to observe the two assets with
  strongest touch/fill evidence instead of inflating coverage with duplicate
  rows.
- Selected assets:
  - `88275040060084773376557187972215267513049848642895776801789297917961077894224`
  - `53831553061883006530739877284105938919721408776239639687877978808906551086026`

Runtime observation:

- Final stream lengths: `orderbook=4264`, `signals=241`, `reports=482`
- Recent report statuses: `DELAYED=241`, `UNMATCHED=241`
- Signal-to-order consumption rate: `1.0`
- Order creation rate: `1.0`
- Missing report rate: `0.0`
- Observed fill rate: `0.0`
- Synthetic fill rate: `0.0`
- No-fill future-touch rate: `0.0`
- Average no-fill distance to touch: `0.0016431535269709559`
- Pre-live decision: `NO_GO`
- Cycle recommendation: `CHANGE_MARKET_OR_TIMING_FILTERS`

Decision:

- Live remains blocked.
- V11 confirmed the stack works end-to-end on a touch-probability universe, but
  it did not recover fills.
- The two historically fillable assets were close to touch in this window, but
  their quotes were not touched within the synthetic future window. This points
  to market/timing regime drift or stale offline selection, not a Rust/API
  execution failure.
- Do not increase quote aggression from this single run. The next work should
  re-rank by fresh runtime touch evidence, widen the candidate universe with
  current-market activity, or run a short at-touch diagnostic before changing
  the default quote policy.

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

## 2026-05-17 - Runtime-Touch Auto Route Fresh Capture 60m

- Run id: `runtime-touch-ab-auto-route-20260517T180537Z`
- Fresh data lake: `.tmp/real-dry-run-data-lake/runtime-touch-ab-auto-route-20260517T180537Z-fresh`
- Fresh report root: `.tmp/real-dry-run-data-lake/runtime-touch-ab-auto-route-20260517T180537Z-fresh/reports/runtime-touch-ab-auto-route-20260517T180537Z-fresh`
- Retry ladder: `.tmp/operational/runtime-touch-ab-auto-route-20260517T180537Z/runtime_touch_ab_retry_ladder/runtime_touch_ab_retry_ladder.json`
- Mode: `EXECUTION_MODE=dry_run`
- Route: `scripts/run_runtime_touch_ab_auto_route.sh`
- Capture duration: 60 minutes
- A/B execution: not launched because the ladder did not emit `next_command`

Fresh capture metrics:

- Market assets observed: `20`
- Orderbook snapshots: `21541`
- Orderbook levels: `1514326`
- Signals: `21561`
- Execution reports: `43124`
- Market metadata rows: `50`
- Deadletters: `0` orderbook, `0` signals
- Runtime stream report status counts during capture: `21538 DELAYED`, `21526 MATCHED`, `12 UNMATCHED`

Retry ladder result:

- Recommendation: `COLLECT_FRESH_RUNTIME_SAMPLE_OR_CHANGE_MARKET_TIMING`
- `selected_attempt`: `null`
- `next_command`: `null`
- `strict_signalable`: `insufficient_assets`, `1` asset
- `wider_universe`: `insufficient_assets`, `1` asset
- `lower_signalable_density`: `insufficient_assets`, `1` asset
- `lower_signalable_snapshots`: `insufficient_assets`, `1` asset
- `runtime_hybrid_backfill`: `insufficient_assets`, `1` asset

Interpretation:

The auto-route path worked operationally: preflight passed, the 60-minute
capture produced a large clean data lake, and the retry ladder ran against the
fresh DuckDB. The result still blocks A/B because every ladder level found only
one signalable runtime-touch asset, below the minimum comparable universe of
two assets.

This means the next blocker is market/timing selection, not Rust execution,
Redis durability, or A/B orchestration. Do not relax the minimum universe size
or infer v11/v12 quote-policy quality from this run. Live remains blocked until
a route can produce a comparable universe and then an A/B observation with
observed fills, low synthetic optimism, and acceptable adverse selection.

Follow-up selection-only validation on the same DuckDB:

- Signalability diagnostic: `.tmp/operational/runtime-touch-selection-probe-validation/runtime_touch_signalability_diagnostic/runtime_touch_signalability_diagnostic.json`
- Candidate expansion: `.tmp/operational/runtime-touch-selection-probe-validation/runtime_touch_candidate_expansion/runtime_touch_candidate_expansion.json`
- Summary: `.tmp/operational/runtime-touch-selection-probe-validation/runtime_touch_selection_probe_summary.json`
- Signalable assets: `1`
- Candidate expansion assets: `1`
- Dominant blockers: `19` assets failed `signalable_snapshots`, `19` failed `signalable_density`, `13` failed `touch_change_rate`, `2` failed `stale_rate`, and `1` failed `snapshots`
- Next action: `CHANGE_MARKET_TIMING_OR_DISCOVERY`

The follow-up confirms that the 2026-05-17 blocker is not simply the strict A/B
ladder. Even a selection-only candidate expansion on the fresh DuckDB still
finds only one asset that clears the predictor-aligned signalability floor.

## Current Diagnostic Loop

After the v10/v11 observations, the current blocker is not Redis, signal
consumption, or executor reconciliation. The blocker is that selected assets
generate orders but the runtime market does not fill or later touch those quotes.

New research-only artifacts:

- `at_touch_asset_diagnostic_v1`: per-asset diagnosis from existing quote
  execution reports. It decides whether to retune to at-touch, drop stale
  runtime assets, repeat, or collect more sample.
- `runtime_touch_ranking_v1`: selector input based on fresh top-of-book changes,
  active minutes, stale-rate, spread opportunity, and depth.
- `execution_probe_touch_comparison_v1`: compares v10/v11/at-touch diagnostics
  and emits the next research action while keeping `can_execute_trades=false`.

Operational command order:

Preferred command:

```bash
scripts/run_runtime_touch_cycle.sh \
  --comparison-report-roots <v10-root>,<v11-root> \
  --fresh-capture-seconds 1800 \
  --duration-seconds 3600
```

Manual command order:

1. `scripts/run_pre_live_dry_run.sh --duration-seconds 1800`
2. `scripts/rank_runtime_touch_assets.sh --duckdb <fresh-research.duckdb> --output-dir <report-root>/runtime_touch_ranking`
3. `PYTHONPATH=python-service python3 -m src.research.execution_probe_universe_selection --selection-source runtime_touch ...`
4. `scripts/run_runtime_touch_observation.sh --universe-selection <execution_probe_universe_selection.json>`
5. `scripts/analyze_at_touch_assets.sh --report-root <runtime-touch-observation-root>`
6. `PYTHONPATH=python-service python3 -m src.research.execution_probe_touch_comparison --report-root <v10-root> --report-root <v11-root> --report-root <runtime-touch-observation-root> --output <runtime-touch-observation-root>/execution_probe_touch_comparison.json`

Live remains blocked until observed fills, synthetic optimism, adverse selection,
and realized edge all pass the go/no-go contract.

First validation on the 2026-05-13 v11 report:

- `at_touch_asset_diagnostic_v1`: 2 assets, 241 signals, both
  `DROP_RUNTIME_STALE`, future-touch rate `0.0`, observed fill-rate `0.0`.
- `runtime_touch_ranking_v1`: 2 ranked assets, 0 selected runtime-touch assets.
- `execution_probe_touch_comparison_v1`: diagnosis
  `runtime_touch_stale_or_quotes_not_reachable`, next action
  `CHANGE_MARKET_OR_TIMING_FILTERS`.

## 2026-05-13 - Runtime-Touch Cycle 30m + 30m

Command path:

- `scripts/run_runtime_touch_cycle.sh`
- Fresh capture: `runtime-touch-cycle-20260513T175000Z-fresh`
- Observation: `runtime-touch-cycle-20260513T175000Z-resume-observation`
- Comparison roots: v10 runtime-backfill, v11 touch-probability, runtime-touch observation

Fresh capture result:

- Duration: 30 minutes
- Stream lengths: `orderbook=6465`, `signals=6465`, `reports=12930`
- Report statuses: `6465 DELAYED`, `6464 MATCHED`, `1 UNMATCHED`
- The full research loop was too heavy for this selection-only step and hit exit
  code `137` in `ml_fill_evaluation`; the runtime-touch cycle now uses
  `REAL_DRY_RUN_RESEARCH_MODE=data_lake_only` for fresh capture so future runs
  export the DuckDB needed for selection without running heavy ML reports.

Runtime-touch universe:

- `runtime_touch_ranking_v1`: 20 ranked assets, 4 selected assets
- Universe status: `ready`
- Selection source: `runtime_touch`
- Profile used for observation: `execution_probe_v11`

Observation result:

- Duration: 30 minutes
- Stream lengths: `orderbook=495`, `signals=36`, `reports=68`
- Report statuses: `32 DELAYED`, `32 UNMATCHED`, `4 ERROR`
- Observed fill-rate: `0.0`
- Synthetic fill-rate: `0.0`
- At-touch diagnostic: 1 active asset, `DROP_RUNTIME_STALE`, future-touch rate `0.0`
- Touch comparison diagnosis: `runtime_touch_stale_or_quotes_not_reachable`
- Next action: `CHANGE_MARKET_OR_TIMING_FILTERS`

Interpretation:

The runtime-touch selector improved universe freshness enough to start a valid
dry-run observation, but the selected runtime-active markets still did not touch
or fill the strategy quotes. This is stronger evidence that the blocker is not
just stale v11 asset selection; the next iteration should change market/timing
filters or quote policy in a versioned research profile. Live remains blocked.

## 2026-05-09 - Exposure Release execution_probe_v7 60m

- Run id: `execution-probe-v7-exposure-release-20260509T020000Z`
- Report root: `.tmp/real-dry-run-data-lake/execution-probe-v7-exposure-release-20260509T020000Z/reports/execution-probe-v7-exposure-release-20260509T020000Z`
- Mode: `EXECUTION_MODE=dry_run`
- Profile: `execution_probe_v7`
- Selection source: `fillability`
- Universe: 5 market assets
- Filter thresholds: `min_future_touch_rate=0.0125`, `min_avg_opportunity_spread=0.00125`, `min_timing_signals=5`
- Duration: 60 minutes
- Universe hash: `156296cf24500d8b310f7f967aed12fa47b1f9ba79738493be2bb99b7ee6d7da`

Key metrics:

- Orderbook stream events: `5343`
- Signals: `574`
- Execution reports: `1148` raw rows, `574` terminal signal reports
- Report statuses: `572 DELAYED`, `572 UNMATCHED`
- Observed fill-rate: `0.0`
- Dry-run fill-rate: `0.0`
- Synthetic fill-rate: `0.0017421602787456446`
- Synthetic-only signals: `0`
- No-fill future-touch rate: `0.0017421602787456446`
- Adjusted synthetic fill-rate: `0.0017421602787456446`
- Filled signals: `0`
- Reconciliation divergence rate: `0.0`

Signal-to-order conversion:

- Signals analyzed: `574`
- Signals consumed: `574`
- Consumption rate: `1.0`
- Signals missing execution report: `0`
- Orders created: `574`
- Order creation rate: `1.0`
- Rejected consumed signals: `0`
- Rejection rate: `0.0`
- Dominant root cause: `order_created_unfilled`

Unmatched diagnostics:

- Top root cause: `dry_run_created_unmatched`
- Top asset: `53831553061883006530739877284105938919721408776239639687877978808906551086026`
- Top asset signals: `262`
- Average distance to touch: approximately `0.01`
- Future touch rate on top unmatched buckets: `0.0`
- Average required quote move: approximately `0.01`

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

The dry-run exposure release fixed the risk-gate artifact from the previous
run. Rejections dropped from `53` to `0`, and order creation increased from
`381` to `574`. This isolates the remaining blocker: accepted near-touch orders
are being placed, but they do not fill. The top unmatched diagnostics show the
orders are still about one tick away from the touch and the future market rarely
touches that limit. The next research-only decision should focus on market/timing
filters or quote placement, not signal consumption or exposure accounting.

The run remains `research-only`; `can_execute_trades=false` and the go/no-go
decision is `NO_GO`.

Next probe:

- `execution_probe_v8` is the reproducible follow-up for this blocker.
- It keeps `EXECUTION_MODE=dry_run`, `PREDICTOR_QUOTE_PLACEMENT=near_touch`, `PREDICTOR_EXECUTION_PROBE_V8_NEAR_TOUCH_MAX_SPREAD_FRACTION=1.0`, and `PREDICTOR_EXECUTION_PROBE_V8_OFFSET_TICKS=0`.
- It must be launched through `scripts/run_execution_probe_v8_cycle.sh` so the at-touch quote test is constrained by explicit market/timing filters and produces `quote_aggressiveness_decision`.
- Passing v8 requires observed fills without synthetic optimism, adverse selection, or drawdown regression; it does not enable live execution.

## 2026-05-11 - At-Touch execution_probe_v8 60m

- Run id: `execution-probe-v8-at-touch-20260511T135000Z`
- Report root: `.tmp/real-dry-run-data-lake/execution-probe-v8-at-touch-20260511T135000Z/reports/execution-probe-v8-at-touch-20260511T135000Z`
- Mode: `EXECUTION_MODE=dry_run`
- Profile: `execution_probe_v8`
- Quote policy: `near_touch`, at touch with `near_touch_max_spread_fraction=1.0`, `offset_ticks=0`
- Selection source: fillability/timing universe from `execution_probe_universe_selection_v1`
- Universe: 5 selected market assets, 4 assets with signals and reports
- Duration: 60 minutes
- Universe hash: `1f46b7e79e4da09f9b41e2ad307bbcfdc8844e50dc80f77b36b05df7e836b524`

Key metrics:

- Orderbook stream events: `3754`
- Signals: `450`
- Execution reports: `900` raw rows, `450` terminal signal reports
- Report statuses: `450 DELAYED`, `450 MATCHED`
- Observed fill-rate: `1.0`
- Dry-run fill-rate: `1.0`
- Synthetic fill-rate: `1.0`
- Synthetic-only signals: `0`
- Synthetic-vs-observed fill-rate gap: `0.0`
- Stale data rate: `0.011720831113478956`
- Reconciliation divergence rate: `0.0`
- Realized edge: `0.12971111111111147`
- Test Brier score: `0.1681851851851853`
- Adverse selection: `0.9856051166792855`
- Drawdown: `0.0`

Signal-to-order conversion:

- Signals analyzed: `450`
- Signals consumed: `450`
- Consumption rate: `1.0`
- Signals missing execution report: `0`
- Orders created: `450`
- Order creation rate: `1.0`
- Rejected consumed signals: `0`
- Rejection rate: `0.0`
- Dominant root cause: `order_created_filled`

Decision artifacts:

- `pre_live_readiness.json`
- `pre_live_promotion.json`
- `quote_execution_diagnostics.json`
- `signal_to_order_conversion.json`
- `execution_probe_next_decision.json`
- `asset_execution_decision/asset_execution_decision.json`
- `.tmp/operational/execution-probe-v8-at-touch-20260511T135000Z/execution_probe_v8_cycle_summary.json`

Decision:

- Pre-live status: `blocked`
- Go/no-go: `NO_GO`
- Main blocker: `no_persistent_adverse_selection`
- `execution_probe_next_decision.recommendation`: `ADD_MARKET_SIDE_RISK_FILTERS`
- Next step: keep the `execution_probe_v8` quote policy, add market/side filters, then repeat.
- `quote_aggressiveness_decision.decision`: `HOLD_QUOTE_POLICY`
- `market_timing_filter_decision.decision`: `REJECT_MARKET_TIMING_FILTER`
- `asset_execution_decision` summary: `4 REPEAT_ASSET`, `0 RETUNE_ASSET`, `0 BLOCK_ASSET`

Interpretation:

`execution_probe_v8` solved the previous execution-quality blocker. At-touch
dry-run orders were created for every signal and all terminal reports were
`MATCHED`; the synthetic-vs-observed gap also dropped to zero. That is useful
evidence that the pipeline can produce executable orders when quote placement is
at touch.

The run is still not live-ready. Adverse selection remains far above the
pre-live threshold, so the correct next action is not to increase quote
aggressiveness or loosen global risk. The next research-only run should keep the
v8 quote policy but filter market/side segments using the segment and asset
diagnostics. Asset-level evidence did not justify blocking a specific asset from
one run, so the next loop should add explicit market/side risk filters and
repeat before any promotion discussion.

## 2026-05-11 - Market/Side Filtered execution_probe_v8 60m

- Run id: `execution-probe-v8-market-side-filter-20260511T162000Z`
- Report root: `.tmp/real-dry-run-data-lake/execution-probe-v8-market-side-filter-20260511T162000Z/reports/execution-probe-v8-market-side-filter-20260511T162000Z`
- Mode: `EXECUTION_MODE=dry_run`
- Profile: `execution_probe_v8`
- Quote policy: `near_touch`, at touch with `near_touch_max_spread_fraction=1.0`, `offset_ticks=0`
- Selection source: `fillability`
- Adverse-selection filter: `market_side`, `max_adverse_30s_rate=0.9855`, `min_adverse_filled_events=10`
- Universe: 2 market assets
- Universe hash: `dc3d63fc35c22d8d288bb0ac95b58390a85c9dbf2dcbcbb753c75d78f34c7ae6`
- Duration: 60 minutes

Filter exclusions:

- `0x1fad72fae204143ff1c3035e99e7c0f65ea8d5cd9bd1070987bd1a3316f772be` / `BUY`: adverse 30s rate `0.986013986013986`
- `0x50ddb9cd80d5c271664a2ebb7fcaed1d0a148d82c8e8d314d830f75a944c3dcc` / `BUY`: adverse 30s rate `0.9855072463768116`

Key metrics:

- Orderbook stream events: `606`
- Signals: `182`
- Execution reports: `364` raw rows, `182` terminal signal reports
- Report statuses: `182 DELAYED`, `182 MATCHED`
- Observed fill-rate: `1.0`
- Dry-run fill-rate: `1.0`
- Synthetic fill-rate: `1.0`
- Synthetic-vs-observed fill-rate gap: `0.0`
- Stale data rate: `0.04785478547854786`
- Reconciliation divergence rate: `0.0`
- Realized edge: `0.04846153846153857`
- Test Brier score: `0.20250000000000032`
- Adverse selection: `0.9807692307692307`
- Drawdown: `0.0`

Decision:

- Pre-live status: `blocked`
- Go/no-go: `NO_GO`
- Main blockers: `has_signals`, `no_persistent_adverse_selection`, and stricter go/no-go `fresh_market_data`
- `execution_probe_next_decision.recommendation`: `REJECT_MARKET_SIDE_RISK_FILTER`
- Next step: do not repeat the same market/side filter; it reduced coverage without resolving adverse selection.
- `asset_execution_decision` summary: `2 REPEAT_ASSET`, `0 RETUNE_ASSET`, `0 BLOCK_ASSET`

Interpretation:

The filter preserved execution quality: every signal was consumed, every order
was created, and every terminal report was `MATCHED`. It did not solve the
trading-quality blocker. Adverse selection improved only from
`0.9856051166792855` to `0.9807692307692307`, while signal volume dropped from
`450` to `182`. That is not enough to justify repeating the same filter or
promoting the profile.

The useful outcome is architectural: the platform can now apply a
research-only market/side adverse-selection filter during universe selection,
and it refuses to treat zero-evidence metadata backfill as a ready filtered
universe. The next research step should design stronger adverse-selection
features, such as post-fill mark movement buckets, side-specific microstructure
regimes, or time-of-market filters, before another 60-90 minute v8 observation.

## 2026-05-11 - Toxicity-Aware execution_probe_v9 90m

- Run id: `execution-probe-v9-cycle-20260511T181136Z`
- Report root: `.tmp/real-dry-run-data-lake/execution-probe-v9-cycle-20260511T181136Z/reports/execution-probe-v9-cycle-20260511T181136Z`
- Mode: `EXECUTION_MODE=dry_run`
- Profile: `execution_probe_v9`
- Quote policy: `near_touch`, `near_touch_max_spread_fraction=0.90`, `offset_ticks=0`
- Minimum confidence: `0.55`
- Selection source: `fillability`
- Universe: 2 market assets
- Universe hash: `dc3d63fc35c22d8d288bb0ac95b58390a85c9dbf2dcbcbb753c75d78f34c7ae6`
- Duration: 90 minutes

Operational note:

- The first v9 attempt at `2026-05-11T180800Z` failed preflight with `missing_signals_stream_progress`.
- Root cause: `execution_probe_v9` defaulted to `min_confidence=0.57`, while the selected 1-cent spread assets produce confidence near `0.55`.
- The observation wrapper now pins `PREDICTOR_EXECUTION_PROBE_V9_MIN_CONFIDENCE=0.55` for this research profile so the run can test quote placement instead of silently rejecting every snapshot.

Key metrics:

- Orderbook stream events: `1019`
- Signals: `287`
- Execution reports: `574` raw rows, `287` terminal signal reports
- Report statuses: `287 DELAYED`, `287 UNMATCHED`
- Observed fill-rate: `0.0`
- Dry-run fill-rate: `0.0`
- Synthetic fill-rate: `0.0`
- Synthetic-vs-observed fill-rate gap: `0.0`
- Signals without observed report: `0`
- Stale data rate: `0.03042198233562316`
- Reconciliation divergence rate: `0.0`
- Drawdown: `0.0`

Quote diagnostics:

- Average no-fill distance to touch: `0.0010000000000000009`
- Average no-fill distance to mid: `0.0040000000000000036`
- Average required quote move: `0.0010000000000000009`
- No-fill future-touch rate: `0.0`
- Dominant root cause: `dry_run_created_unmatched`

Fill toxicity:

- Segments: `2`
- Signals: `287`
- Filled events: `0`
- Fill toxicity decision: both segments are `INSUFFICIENT_SAMPLE`
- `fill_toxicity` cannot evaluate adverse markout because there were no fills.

Decision:

- Pre-live status: `blocked`
- Go/no-go: `NO_GO`
- `execution_probe_next_decision.recommendation`: `CHANGE_MARKET_OR_TIMING_FILTERS`
- Next step: keep `execution_probe_v9` research-only and retune market/timing selection before quote aggression.
- `market_timing_filter_decision.decision`: `EXPAND_FILLABILITY_UNIVERSE`
- Next cycle: `scripts/run_execution_probe_v9_cycle.sh` with `--selection-source fillability`, `--universe-limit 20`, `--min-assets 5`, and the same timing thresholds.
- `quote_aggressiveness_decision.decision`: `HOLD_QUOTE_POLICY`
- `asset_execution_decision` summary: `0 REPEAT_ASSET`, `2 RETUNE_ASSET`, `0 BLOCK_ASSET`

Interpretation:

`execution_probe_v9` removed the adverse-selection measurement problem by
removing fills entirely. That is not an improvement. The one-tick-behind-touch
policy avoided synthetic optimism and preserved signal-to-order traceability,
but it also produced no observed fills and no future touch evidence. The
correct next step is not another quote-aggression threshold guess; it is a
broader fillability universe so v9 can be tested on enough markets where its
slightly-behind-touch quote can actually be touched.

Live remains blocked. The run is useful as negative evidence: v9 is safer than
v8 in the sense that it does not generate toxic fills, but it is not executable
on this two-asset universe.

## 2026-05-12 - Expanded Fillability execution_probe_v9 60m

- Run id: `execution-probe-v9-cycle-20260512T003801Z`
- Report root: `.tmp/real-dry-run-data-lake/execution-probe-v9-cycle-20260512T003801Z/reports/execution-probe-v9-cycle-20260512T003801Z`
- Mode: `EXECUTION_MODE=dry_run`
- Profile: `execution_probe_v9`
- Quote policy: `near_touch`, `near_touch_max_spread_fraction=0.90`, `offset_ticks=0`
- Minimum confidence: `0.55`
- Selection source: `fillability`
- Universe: 20 primary market assets, no fallback assets
- Universe hash: `2d7aefafa43b7084c0b34c87cbd8818056e2372335421a65d97f2eb5454b97f0`
- Duration: 60 minutes
- Comparison baselines: v8 at-touch, v8 market/side filtered, and narrow v9.

Key metrics:

- Orderbook stream events: `24180`
- Orderbook snapshots: `24183`
- Signals: `1285`
- Execution reports: `2564` raw rows, `1285` terminal signal reports
- Report statuses: `1282 DELAYED`, `1277 UNMATCHED`, `5 MATCHED`
- Observed fill-rate: `0.0038910505836575876`
- Synthetic fill-rate: `0.052140077821011675`
- Synthetic-vs-observed fill-rate gap: `0.04824902723735409`
- Adjusted synthetic fill-rate: `0.03385214007782101`
- Signals without observed report: `0`
- Stale data rate: `0.005747839391307944`
- Reconciliation divergence rate: `0.0`
- Realized edge: `0.10840000000000005`
- Adverse selection: `1.0`
- Drawdown: `0.09899999999999987`
- Test Brier score: `0.2122727272727273`

Fill toxicity:

- Segments: `16`
- Filled events: `5`
- Fill toxicity fill-rate: `0.0038910505836575876`
- Adverse 30s rate: `1.0`
- Average PnL 30s: `-0.004111111111111096`
- Rejected segments: `1`
- Insufficient-sample segments: `15`
- Promoted segments: `0`
- Most toxic observed segment: asset `101738487887518832481587379955535423775326921556438741919099866785354159699479`, fill-rate `0.08333333333333333`, `3` fills, adverse 30s rate `1.0`, decision `REJECT_TOXICITY`.

Decision:

- Pre-live status: `blocked`
- Go/no-go: `NO_GO`
- Go/no-go blockers: `acceptable_dry_run_observed_fill_rate`, `acceptable_fill_rate`, `no_persistent_adverse_selection`
- `execution_probe_next_decision.recommendation`: `HOLD_RESEARCH`
- Next step: do not tune quote aggression further until fill toxicity improves on observed segments.
- `asset_execution_decision` summary: `5 REPEAT_ASSET`, `11 RETUNE_ASSET`, `0 BLOCK_ASSET`
- `can_execute_trades=false`

Interpretation:

The expanded universe fixed the prior underdetermined two-asset sample and
proved that v9 can occasionally fill when run across broader fillability
markets. It did not make the profile tradable. The fill-rate remains below the
minimum dry-run threshold, synthetic fills still overstate observed execution,
and every observed fill was adverse at the 30-second markout horizon.

The next research step should stop changing global quote aggression. The useful
path is segment-level filtering or feature work from `fill_toxicity_v1`: block
or isolate the rejected toxic segment, require more fill evidence before trusting
insufficient-sample segments, and rerun only after the decision policy can
exclude toxic fills deterministically. Live remains blocked.

## 2026-05-12 - Toxicity-Filtered execution_probe_v9 60m

- Run id: `execution-probe-v9-cycle-20260512T143600Z`
- Report root: `.tmp/real-dry-run-data-lake/execution-probe-v9-cycle-20260512T143600Z/reports/execution-probe-v9-cycle-20260512T143600Z`
- Baseline: `execution-probe-v9-cycle-20260512T003801Z`
- Mode: `EXECUTION_MODE=dry_run`
- Profile: `execution_probe_v9`
- Selection source: `fillability`
- Toxicity filter: `segment`
- Universe: 20 primary market assets
- Universe hash: `2d7aefafa43b7084c0b34c87cbd8818056e2372335421a65d97f2eb5454b97f0`
- Duration: 60 minutes

Key metrics:

- Signals: `1097`
- Stream signals: `1095`
- Report statuses: `1095 DELAYED`, `1091 UNMATCHED`, `1 MATCHED`
- Observed fill-rate: `0.0009115770282588879`
- Synthetic fill-rate: `0.028258887876025523`
- Synthetic-vs-observed fill-rate gap: `0.027347310847766634`
- Adverse selection: `0.9761904761904763`
- Realized edge: `0.1700000000000001`
- Drawdown: `0.0`
- Fill toxicity filled events: `1`
- Fill toxicity rejected segments after the run: `0`
- Insufficient-sample segments: `14`

Toxicity filter impact:

- Candidate blocklist segments: `32`
- Runtime snapshots: `12722`
- Accepted snapshots: `1096`
- Runtime `blocked_segment` rejections: `0`
- Corrected impact decision: `REPAIR_FILTER_CONTRACT`
- Reason: `blocked_segments_never_matched_runtime`

Interpretation:

This run is not valid evidence that the toxicity filter worked. The filter was
enabled and the pre-run blocklist contained 32 blocked segments, but none of
those segments matched runtime decisions. The root cause was an over-specific
blocklist contract: fill-toxicity segments learned from an earlier near-touch
model version were exported with that source `strategy/model_version`, while
the runtime v9 predictor matched blocklists exactly by current
`strategy/model_version`.

The implementation now keeps source strategy/model as evidence fields while
making fill-toxicity blocklists match by market, asset, side, spread bucket, and
timing bucket. Do not repeat a 90-120 minute toxicity-filtered observation until
the regenerated filter shows nonzero runtime `blocked_segment` rejections on a
fixed universe. Live remains blocked.

## 2026-05-12 - Corrected Toxicity-Filtered execution_probe_v9 60m

- Run id: `execution-probe-v9-cycle-20260512T155855Z`
- Report root: `.tmp/real-dry-run-data-lake/execution-probe-v9-cycle-20260512T155855Z/reports/execution-probe-v9-cycle-20260512T155855Z`
- Baseline: `execution-probe-v9-cycle-20260512T003801Z`
- Mode: `EXECUTION_MODE=dry_run`
- Profile: `execution_probe_v9`
- Selection source: `fillability`
- Toxicity filter: `segment`
- Universe: 20 primary market assets
- Universe hash: `2d7aefafa43b7084c0b34c87cbd8818056e2372335421a65d97f2eb5454b97f0`
- Duration: 60 minutes

Key metrics:

- Signals: `745`
- Execution reports: `1490` raw rows, `745` terminal signal reports
- Report statuses: `744 DELAYED`, `740 UNMATCHED`, `4 MATCHED`
- Observed fill-rate: `0.005369127516778523`
- Synthetic fill-rate: `0.05906040268456376`
- Synthetic-vs-observed fill-rate gap: `0.053691275167785234`
- Realized edge: `0.02750000000000008`
- Adverse selection: `0.8317099567099567`
- Drawdown: `0.04999999999999993`
- Stale data rate: `0.015498817195529814`
- Signal-to-order conversion: `consumption_rate=1.0`, `order_creation_rate=1.0`, `missing_report_rate=0.0`

Toxicity filter impact:

- Candidate blocklist segments: `32`
- Runtime snapshots: `12259`
- Accepted snapshots: `744`
- Runtime `blocked_segment` rejections: `1777`
- Runtime blocked segment rate: `0.14495472713924465`
- Decision: `KEEP_FILTER_FOR_RESEARCH`
- Reason: `toxicity_filter_improved_relative_metrics_but_failed_absolute_research_thresholds`

Baseline deltas:

- Observed fill-rate: `+0.0014780769331209356`
- Adverse selection: `-0.16829004329004327`
- Drawdown: `-0.04899999999999993`
- Fill toxicity adverse 30s rate: `-0.8333333333333334`
- Filled signals: `-1`
- Signals: `-540`
- Realized edge: `-0.08089999999999997`
- Synthetic fill-rate: `+0.0069203248635520825`

Decision:

- Pre-live status: `blocked`
- Go/no-go: `NO_GO`
- Go/no-go blockers: `go_no_go_passed`, `acceptable_dry_run_observed_fill_rate`, `acceptable_fill_rate`, `bounded_simulator_fill_rate_delta`, `no_persistent_adverse_selection`
- `execution_probe_next_decision.recommendation`: `HOLD_RESEARCH`
- Next step from toxicity impact: repeat a longer 90-120 minute observation, but keep it research-only and do not treat it as promotion evidence yet.
- `asset_execution_decision` summary: `6 REPEAT_ASSET`, `6 RETUNE_ASSET`, `0 BLOCK_ASSET`
- `can_execute_trades=false`

Interpretation:

The corrected fill-toxicity blocklist contract worked. The runtime rejected
1,777 snapshots as `blocked_segment`, so this run is valid evidence that the
filter is active. Compared with the expanded v9 baseline, observed fill-rate
improved slightly and adverse selection fell materially, while the filter did
not kill activity.

This is not a live-trading signal. Fill-rate is still below the pre-live gate,
adverse selection is still high in absolute terms, and the synthetic-vs-observed
gap remains above threshold. The correct next step is to keep the same corrected
filter for research only and run a longer fixed-universe observation. Live
remains blocked until that longer run passes go/no-go style evidence and
adverse selection improves further.

## 2026-05-12 - Long Corrected Toxicity-Filtered execution_probe_v9 90m

- Run id: `execution-probe-v9-cycle-20260512T190549Z`
- Report root: `.tmp/real-dry-run-data-lake/execution-probe-v9-cycle-20260512T190549Z/reports/execution-probe-v9-cycle-20260512T190549Z`
- Baseline: `execution-probe-v9-cycle-20260512T003801Z`
- Mode: `EXECUTION_MODE=dry_run`
- Profile: `execution_probe_v9`
- Selection source: `fillability`
- Toxicity filter: `segment`
- Universe: 20 primary market assets
- Universe hash: `2d7aefafa43b7084c0b34c87cbd8818056e2372335421a65d97f2eb5454b97f0`
- Duration: 90 minutes

Key metrics:

- Signals: `1521`
- Execution reports: `3042` raw rows, `1521` terminal signal reports
- Report statuses: `1520 DELAYED`, `1520 UNMATCHED`, `0 MATCHED`
- Observed fill-rate: `0.0`
- Synthetic fill-rate: `0.024326101249178174`
- Synthetic-vs-observed fill-rate gap: `0.024326101249178174`
- Realized edge: `null`
- Adverse selection: `1.0`
- Drawdown: `0.0`
- Stale data rate: `0.01512493193780628`
- Signal-to-order conversion: `consumption_rate=1.0`, `order_creation_rate=1.0`, `missing_report_rate=0.0`

Toxicity filter impact:

- Candidate blocklist segments: `32`
- Runtime snapshots: `16529`
- Accepted snapshots: `1520`
- Runtime `blocked_segment` rejections: `1199`
- Runtime blocked segment rate: `0.07253917357371892`
- Fill toxicity filled events: `0`
- Fill toxicity rejected segments after the run: `0`
- Fill toxicity insufficient-sample segments: `13`
- Decision: `REJECT_FILTER`
- Reason: `toxicity_filter_did_not_reduce_adverse_selection`

Baseline deltas:

- Observed fill-rate: `-0.0038910505836575876`
- Adverse selection: `0.0`
- Drawdown: `-0.09899999999999987`
- Filled signals: `-5`
- Signals: `+236`
- Synthetic fill-rate: `-0.0278139765718335`
- Runtime `blocked_segment` rejections: `+1199` versus the unfiltered baseline runtime

Decision:

- Pre-live status: `blocked`
- Go/no-go: `NO_GO`
- Go/no-go blockers: `acceptable_dry_run_observed_fill_rate`, `acceptable_fill_rate`, `has_fills`, `no_persistent_adverse_selection`, `positive_realized_edge`
- `execution_probe_next_decision.recommendation`: `CHANGE_MARKET_OR_TIMING_FILTERS`
- Next step from toxicity impact: move to feature/model redesign instead of repeating this filter unchanged.
- `asset_execution_decision` summary: `3 REPEAT_ASSET`, `10 RETUNE_ASSET`, `0 BLOCK_ASSET`
- `can_execute_trades=false`

Interpretation:

The 90-minute fixed-universe repeat confirms that the corrected blocklist is
active in runtime, but it does not produce executable evidence. It generated
more signals than the expanded v9 baseline and blocked 1,199 snapshots, yet it
produced zero observed fills. With no fills, realized edge cannot be measured,
adverse selection remains failed, and the strategy cannot be promoted.

This invalidates repeating the same toxicity filter unchanged. The next work
should change market/timing selection or feature/model scoring so the bot can
find fillable quotes without returning to synthetic optimism. Live remains
blocked.

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
