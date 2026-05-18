
This roadmap converts [repo_ideas.md](repo_ideas.md) and [architecture_plan.md](architecture_plan.md) into implementation phases.

## Phase 1: Local Orderbook

- Maintain an in-memory orderbook per `asset_id`.
- Apply `book` snapshots and `price_change` deltas.
- Publish normalized orderbooks only after sorting, liquidity, and stale-book checks.
- Add tests for snapshots, deltas, deletion at size zero, invalid side, and stale timestamps.

## Phase 2: User WebSocket Reconciliation

- Subscribe to the authenticated Polymarket user WebSocket.
- Process `order` and `trade` events.
- Persist lifecycle updates idempotently by `order_id`, `trade_id`, and `signal_id`.
- Track partial fills by accumulating `filled_size` and `remaining_size` per `order_id`.
- Use live user events to update open orders, fills, and positions.
- Keep `dry_run` usable without credentials by simulating the lifecycle from `DELAYED` to `UNMATCHED`.
- Use `USER_MARKET_IDS` condition IDs for the user channel; do not reuse `MARKET_ASSET_IDS` token IDs.

## Phase 3: Operator API + CLI

- Extend FastAPI from informational endpoints to operator controls described in [interface_plan.md](interface_plan.md).
- Add CLI commands for `status`, `risk`, `streams`, `orders`, `cancel-all`, and `kill-switch on|off`.
- Keep CLI output dual-mode: `table` and `json`.
- Use command-specific table columns for orders, positions, metrics, and control results.
- Keep dashboard work in Phase 6 so controls are API-first.
- Runtime kill switch is backed by Redis key `operator:kill_switch` and read by Rust before each signal is accepted.
- `cancel-all` publishes `cancel_all` to `operator:commands:stream`; Rust consumes it through `rust-control` and calls CLOB `cancel_all_orders()` in live mode.
- `cancel-bot-open` is the preferred control path and only cancels orders known by this bot.
- `cancel-all` is emergency-only and requires `confirmation_phrase = "CANCEL ALL OPEN ORDERS"`.
- Cancellation requests move through `SENT`, `CONFIRMED`, `DIVERGED`, or `FAILED`; HTTP acceptance alone is not treated as final cancellation.
- Operator routes support optional role-based bearer auth through `OPERATOR_READ_TOKEN`, `OPERATOR_CONTROL_TOKEN`, and legacy `OPERATOR_API_TOKEN`.

## Phase 4: Research Data Lake

- Write normalized market, orderbook, signal, execution, order, and position snapshots to Parquet.
- Use DuckDB for calibration, realized edge, maker/taker style analysis, and strategy PnL reports.
- Keep Postgres as operational state; use Parquet/DuckDB for research and backtesting.
- Initial implementation exports Redis Streams to partitioned Parquet and creates DuckDB views as described in [data_lake_plan.md](data_lake_plan.md).
- Financial, learning, and game-theory model plans are documented in [modeling_plan.md](modeling_plan.md) and [game_theory_plan.md](game_theory_plan.md).

## Phase 5: Market Discovery and Evidence Scoring

- Add Gamma market metadata ingestion.
- Rank candidate markets using liquidity, spread, activity, odds movement, and external evidence.
- Use social/news/search evidence as a scoring input, never as direct trade execution authority.
- Feed ranked markets into the predictor only after deterministic filters pass.
- Initial implementation exposes read-only Gamma discovery via API/CLI and deterministic metadata scoring as described in [market_discovery_plan.md](market_discovery_plan.md).

## Phase 6: Optional Web Dashboard

- Build a dashboard only after the Operator API is stable.
- Show status, streams, risk, orders, positions, execution reports, and strategy metrics.
- All dashboard actions must call the same Operator API used by the CLI.
- Initial implementation is a TypeScript React dashboard in `frontend/` that consumes only Operator API endpoints.
- FastAPI serves the built dashboard at `/`, while `/api/*` aliases keep the browser client and standalone API compatible.
- Frontend API types and the typed OpenAPI client are generated from OpenAPI with `npm run generate:types`.
- Local verification is consolidated in `scripts/check_all.sh`.
- Local integration smoke testing uses `docker-compose.test.yml` plus `scripts/integration_smoke.py`.
- Managed local integration testing uses `scripts/run_integration_smoke.sh` and disables the market WebSocket with `DISABLE_MARKET_WS=true`.
- GitHub Actions runs `scripts/check_all.sh` on push and pull request, with Cargo, pip, and npm caches.
- CI rejects stale generated OpenAPI/TypeScript artifacts.
- The dashboard separates read/control tokens and shows recent `/control/results`.

## Platform-First Next Steps

These steps improve the trading platform before introducing heavier models. The order is intentional: live trading should wait until state, controls, observability, and replayability are solid.

1. Production state authority
   - Make Postgres the required source for orders, trades, positions, balances, cancel requests, and control results when `APP_ENV=production`.
   - Remove Redis fallback from production API reads; Redis Streams remain the transport boundary, not the long-term operational state.
   - Add versioned migrations for every operational table and keep Python startup validation aligned with Rust migrations.

2. Time-series storage strategy
   - Use normal Postgres tables for canonical operational state that needs strict constraints and idempotency: orders, trades, fills, positions, balances, cancel requests, control results, and risk snapshots.
   - Use partitioned Postgres tables or optional TimescaleDB hypertables for high-volume time-series: orderbook snapshots, price changes, runtime metrics, latency samples, signal history, and fill history.
   - Keep TimescaleDB optional at first; promote it when retention, compression, continuous aggregates, or query volume justify the operational dependency.
   - Keep Parquet/DuckDB as the offline research and backtesting store for reproducible historical analysis.
   - Add retention policies by dataset class: short retention for raw orderbook ticks, longer retention for normalized snapshots, and permanent retention for orders/fills/control audit records.

3. Operator safety and controls
   - Keep `cancel-bot-open` as the default cancellation action and reserve `cancel-all` for emergencies.
   - Show command lifecycle clearly in CLI and dashboard: `QUEUED`, `SENT`, `CONFIRMED`, `DIVERGED`, `FAILED`.
   - Add operator audit fields everywhere: `command_id`, `operator`, `reason`, `created_at`, `completed_at`, and final error.
   - Add a dry-run command preview endpoint before dangerous control actions.

4. Reconciliation hardening
   - Treat User WebSocket events as the preferred confirmation path for orders, fills, and cancellations.
   - Use CLOB polling only as fallback with timeout and divergence tracking.
   - Persist partial fills idempotently by `trade_id` and reject duplicate fill accounting.
   - Add reconciliation reports that compare local Postgres state against CLOB open orders and recent fills.

5. Observability and runbooks
   - Expand `/metrics/prometheus` with bounded labels for command type, report status, rejection reason, and CLOB error type.
   - Add latency histograms for WS to signal, signal to order, order to report, and command to confirmation.
   - Keep structured JSON logs with `signal_id`, `order_id`, `command_id`, `market_id`, and `asset_id`.
   - Add runbook steps for degraded WebSocket, Redis outage, Postgres outage, CLOB API errors, and emergency cancellation.

6. Dashboard and CLI ergonomics
   - Make dashboard pages task-based: Overview, Orders, Positions, Controls, Streams, Metrics, Research.
   - Add filtering by market, asset, status, strategy, command type, and time window.
   - Add clear stale-data indicators when API state is older than expected.
   - Keep CLI parity with dashboard controls and ensure every command supports `--json`.

7. Integration and CI confidence
   - Promote the managed smoke test to a scheduled or manually required pre-release job.
   - Add integration coverage for Redis restart, Postgres restart, stale signal rejection, duplicate fill events, and cancel divergence.
   - Add local fixtures for representative Polymarket WebSocket and User WebSocket messages.
   - Keep `scripts/check_all.sh` as the fast required gate and leave heavier end-to-end checks opt-in until runtime is acceptable.

8. Data quality foundations
   - Incremental data lake export state is implemented with `_export_state.json`; next work is operational monitoring for exporter lag.
   - Market metadata snapshots now export asset/outcome mapping; next work is richer grouping by category, end date, market type, and liquidity regime.
   - Generate time-windowed datasets for orderbook, signals, execution reports, fills, and control events.
   - Add explicit model/data version fields to signal and research outputs, even before ML models exist.
   - Research run manifests are implemented as persistent, versioned run indexes under `data_lake/research_runs/`.

9. Research and model readiness
   - Offline deterministic baseline `deterministic_microstructure_baseline_v1` is implemented with spread, depth, orderbook imbalance, short-horizon momentum, stale-market, and adverse-selection filters.
   - Offline synthetic fills `conservative_orderbook_fill_v1` are implemented for research-only fill-rate, slippage, and realized-edge estimation from future orderbook snapshots.
   - Observed-vs-synthetic fill comparison is implemented in backtest outputs to detect optimistic offline fill assumptions before promotion.
   - Unfilled reason reports are implemented in backtest outputs to explain whether missing observed fills came from execution state, missing market data, or lack of limit touch.
   - Dry-run simulator quality reports are implemented in backtest outputs to compare observed dry-run fill-rate, synthetic fill-rate, slippage, time-to-fill, and `PARTIAL`/`MATCHED` mix.
   - Market-regime diagnostics are implemented offline for fractal/tail-risk metrics and whale-pressure features.
   - Sentiment inputs are implemented as timestamped external evidence features; they are not in the live signal path.
   - Sentiment lift evaluation is implemented offline with point-in-time joins against `backtest_trades` and reports realized edge, fill-rate, adverse edge rate, drawdown, and baseline lift.
   - Feature blocklist candidates are implemented offline from regime/sentiment buckets and exported as candidate-only diagnostics, not runtime rules.
   - Fillability baseline `fillability_baseline_v1` is implemented offline to rank market/assets by observed timing evidence, spread opportunity, stale rate, and liquidity context before scheduling another execution probe.
   - ML fill dataset `ml_fill_targets_v1` is implemented offline with point-in-time execution features and explicit training flags for `will_fill_within_5m`, `future_touch`, and `adverse_selection_after_fill`; unlabeled fills stay null instead of becoming false negatives.
   - ML fill evaluation `ml_fill_evaluation_v1` is implemented as an offline scorecard over `ml_fill_targets_v1`; it measures train/test label quality, Brier/log loss, buckets, lift, and AUC where labels are usable, but it does not train or authorize a model.
   - Minimum label quality gate `ml_fill_label_quality_gate_v1` is implemented inside `ml_fill_evaluation.json`; model training must fail closed unless `can_train_models=true`.
   - ML training dataset preparation is implemented as a fail-closed offline step; it writes `ml_fill_training_examples.parquet` only when `ml_fill_label_quality_gate_v1.can_train_models=true`, and exported training columns exclude the heuristic `prediction_score` to avoid target leakage.
   - A committee of agents is acceptable only as an offline/advisory layer for model review, bias detection, feature proposals, and signal audits.
   - Live trading decisions must not depend on free-form agent consensus; they must remain deterministic, versioned, reproducible, and gated by Rust risk controls.
   - Agent outputs can become scores or diagnostics only after they are converted into versioned, testable inputs with clear promotion metrics.
   - NVIDIA NIM is acceptable only as an optional offline/advisory inference layer for evidence summaries, contradiction checks, feature proposals, and research diagnostics; it must not enter the live predictor or publish Redis signals.
   - NIM advisory artifacts are implemented as optional research outputs and manifest diagnostics; they do not affect promotion gates or research exit codes.
   - Run [game_theory_plan.md](game_theory_plan.md) reports over real dry-run/live-like data before promoting any strategy.
   - Use the pre-live gate and calibration reports as promotion checks; walk-forward splits, Brier score, log loss, reliability buckets, and realized edge by confidence bucket are now generated offline.
   - Pre-live promotion report `pre_live_promotion_v1` is implemented offline and combines realized edge, fill-rate, slippage, adverse selection, drawdown, stale-data rate, reconciliation divergence rate, and calibration quality.
   - Go/no-go report `go_no_go_v1` is implemented as the single read-only quantitative decision artifact. It is exposed through API, CLI, dashboard, manifests, and `scripts/run_research_loop.sh`, but always keeps `can_execute_trades=false`.
   - Go/no-go thresholds are versioned as `go_no_go_thresholds_v1` with explicit `dev`, `paper`, `pre_live`, and `live_candidate` profiles. `GO_NO_GO_PROFILE=pre_live` is the default production research posture until enough long-run evidence exists for `live_candidate`.
   - Agent advisory report `agent_advisory_offline_v1` is implemented as auditable offline reviewers; it does not authorize live trades.
   - Evaluate gradient boosting only after the deterministic baseline is reproducible, calibrated, and better than `passive_spread_capture_v1`.

10. Live promotion gates
   - Use [pre_live_dry_run_runbook.md](pre_live_dry_run_runbook.md) as the operator procedure for real-market pre-live evidence runs.
   - Run `scripts/run_pre_live_dry_run.sh` for managed isolated pre-live captures; use `--print-plan` before scheduled runs to verify resolved gates.
   - Treat `pre_live_readiness.json` as the single readiness summary for a run; it is advisory and always keeps `can_execute_trades=false`.
   - Feed the pre-live promotion report and advisory report with isolated long runs from `scripts/run_real_dry_run_research.sh`, not only unit-test fixtures.
   - Require `observed_vs_synthetic_fill_summary` review for real dry-run samples before treating synthetic fills as a reliable baseline.
   - Require `unfilled_reason_summary` review before tuning predictor thresholds or risk limits.
   - Require `dry_run_simulator_quality` review before using dry-run results as execution-quality evidence.
   - Enforce explicit pre-live thresholds for capture duration, minimum signals, observed dry-run fill-rate, reconciliation divergence, and simulator-quality fill-rate delta.
   - Use `pre_live_promotion_segments` to identify failing markets/assets before changing global thresholds.
   - Export `blocked_segments.json` from promotion and load it explicitly with `PREDICTOR_BLOCKED_SEGMENTS_PATH` when running a restricted dry-run.
   - Compare unrestricted vs restricted dry-runs with `compare_runs --baseline-report-root ... --candidate-report-root ...` before accepting a blocklist.
   - Use the fixed `MARKET_ASSET_IDS` command emitted by `pre_live_blocker_diagnostics.json` for restricted follow-up runs; `compare_runs` must reject candidates whose recorded market-universe hash does not match the blocklist evaluation contract.
   - Use `scripts/run_restricted_blocklist_observation.sh` to run or evaluate restricted candidates and persist both `comparison.json` and `research_promotion_decision.json`.
   - Finalize every restricted observation with `restricted_blocklist_decision.json`; `PROMOTE` from the generic research gate only becomes `REPEAT_OBSERVATION` for blocklists until repeated evidence is available.
   - Review `top_explanatory_buckets` and the defensive blocklist candidate before repeating a run so drawdown/adverse-selection fixes target concrete segments, not global thresholds.
   - Review segment-level improved/worsened/new/removed counts plus newly blocked/unblocked segment keys as the objective promotion evidence.
   - Treat `compare_runs` verdict `no_comparable` as a hard research blocker until both runs meet `segment_comparability_v2`: expected blocklist removals only, minimum shared segment coverage, minimum shared signal coverage, and minimum shared fill coverage.
   - Require `comparison.restricted_blocklist_assessment.status` to be `accepted_for_observation` before repeating a candidate blocklist; reject it immediately when protected metrics regress even if aggregate `verdict` says `candidate_improved`.
   - Keep every generated blocker blocklist candidate research-only with an embedded evaluation contract; never apply it live from one run.
   - When a multi-segment blocklist is rejected, test a narrower generated variant such as `blocked_segments_candidate_top_1.json` before changing model or risk thresholds.
   - Use simulator regression diagnostics to identify whether protected metric regressions come from observed errors, missing future orderbook evidence, no dry-run reports, or synthetic-only fills.
   - Use `research_promotion_decision` to convert a comparable comparison into `PROMOTE`, `REJECT`, or `NEED_MORE_DATA`; do not promote from aggregate metrics alone.
   - Rotate execution-probe observations across an offline-selected multi-market universe before tuning another global quote threshold. Repeating the same two assets after `execution_probe_v4/v5` produced no fills is underdetermined.
   - Use `execution_probe_universe_selection_v1` to select 5-10 candidate assets from `candidate_market_ranking`; the artifact is research-only, hashed, and consumed by `scripts/run_execution_probe_v5_observation.sh`.
   - Calibrate near-touch fractions across multiple DuckDB runs with minimum market coverage before accepting a fraction; a single-window boundary candidate is not enough evidence.
   - Run the next `execution_probe_v5` observation for 60-90 minutes with both a universe-selection artifact and an `execution_probe_v5_fraction_selection.json`, then compare it against v3/v4/v5 roots.
   - The 2026-05-06 multi-market `execution_probe_v5` observation is recorded in [execution_probe_observations.md](execution_probe_observations.md). It improved activity and stale-data rate but still produced zero observed or synthetic fills, so the next step is an `execution_probe_v6` quote-policy change rather than another unchanged v5 repeat.
   - Implement `execution_probe_v6` as a dry-run-only, at-touch research profile and use `scripts/prepare_execution_probe_cycle.sh` to automate universe selection, observation planning, and post-run comparison commands.
   - Use `quote_execution_no_fill_diagnostics` before each new variant decision; required quote move, distance to touch, and future touch rate should explain whether the blocker is quote aggressiveness, market choice, or timing.
   - Generate `execution_probe_next_decision.json` after each profile observation comparison so v6/v7 tuning is deterministic, versioned, and research-only instead of an ad hoc manual threshold discussion.
   - Use `scripts/run_execution_probe_v6_cycle.sh` for the full v6 loop: universe selection, observation, profile comparison, next decision, and cycle summary with known report paths.
   - The 2026-05-07 `execution_probe_v6` cycle produced observed fills but failed on low fill-rate, synthetic-vs-observed gap, and adverse selection; implement `execution_probe_v7` as a less aggressive dry-run profile with one-tick offset and a lower near-touch fraction before any repeat.
   - The 2026-05-07 `execution_probe_v7` 90-minute cycle removed synthetic optimism but produced zero observed fills and zero future-touch evidence; repeat v7 with market/timing selection (`future_touch`, minimum timing sample, and spread floor) instead of changing quote aggressiveness again.
   - Add `market_timing_filter_decision` to `execution_probe_next_decision.json` so filtered v7 runs deterministically keep, relax, reject, or repeat market/timing filters without manual interpretation.
   - The first filtered v7 run produced 607 signals and zero fills; next repeat should relax the filter thresholds (`min_future_touch_rate=0.05`, `min_avg_opportunity_spread=0.005`) before changing quote policy again.
   - Integrate `fillability_baseline_v1` and `ml_fill_targets_v1` in every research loop so future market selection and ML candidate design are based on comparable offline artifacts, not one-off manual threshold searches.
   - The 2026-05-08 relaxed filtered v7 run produced 710 signals, zero observed fills, synthetic fill-rate `0.04366197183098591`, and one fillability-selected asset with future-touch rate `0.24031007751937986`; keep v7 research-only and either relax market/timing thresholds again (`0.025`/`0.0025`) or run a focused observation on the fillability-selected asset before changing quote policy.
   - The 2026-05-08 fillability-focused v7 run used one asset for 30 minutes and produced 68 signals, zero observed fills, and zero synthetic fills; keep single-asset observation as a supported diagnostic path, but next evidence should use a broader fillability-first universe or the generated relaxed timing thresholds before changing quote aggressiveness.
   - `execution_probe_next_decision_v1` now emits `EXPAND_FILLABILITY_UNIVERSE` for narrow fillability runs with no fills, so the next planned run should use `--universe-selection-source fillability --universe-limit 5 --min-assets 3` before further relaxing quote policy.
   - `execution_probe_universe_selection_v1` now supports a traceable fillability fallback: if positive future-touch assets are fewer than `min_assets`, it can backfill from `KEEP_DIAGNOSTIC` markets with sufficient signals, liquidity, and spread evidence while marking each added row with `fallback_reason`.
   - The 2026-05-08 60-minute fillability fallback v7 run produced a ready 3-asset universe, 415 signals, zero observed fills, and synthetic fill-rate `0.08674698795180723`; the all-v7 comparison still recommends `EXPAND_FILLABILITY_UNIVERSE`, now with `--universe-limit 10 --min-assets 5`, before changing quote aggressiveness.
   - The 2026-05-08 60-minute fillability-expanded v7 run used a 5-asset fillability-first universe, produced 373 signals, zero observed fills, synthetic fill-rate `0.03485254691689008`, and only 3 terminal signal reports; keep v7 research-only, use `signal_to_order_conversion_v1` on every run, and relax timing thresholds to `min_future_touch_rate=0.0125` / `min_avg_opportunity_spread=0.00125` before changing quote aggressiveness.
   - The 2026-05-09 relaxed-timing v7 run validated the signal-to-order contract: 434 signals, 434 consumed signals, zero missing reports, 381 dry-run orders, 53 explicit risk rejections, and zero fills. The next work should stop treating missing reports as the main blocker and focus on why near-touch dry-run orders remain unmatched, while keeping live blocked.
   - The 2026-05-09 exposure-release v7 run removed the dry-run exposure artifact: 574 signals, 574 consumed signals, 574 dry-run orders, zero risk rejections, and zero fills. The blocker is now isolated to quote/timing/market selection; `profile_observation_comparison_v1` includes `unmatched_diagnostics` from quote execution reports so the next variant must target the top unmatched assets and one-tick distance-to-touch problem.
   - `execution_probe_v8` is the next research-only quote/timing probe: it quotes at touch (`near_touch_max_spread_fraction=1.0`, `offset_ticks=0`) but only through explicit market/timing universe selection. It must remain `dry_run`, produce `quote_aggressiveness_decision`, and prove observed fills without synthetic optimism or adverse selection before any further promotion discussion.
   - The 2026-05-11 at-touch `execution_probe_v8` 60-minute run solved the immediate execution-quality blocker: 450 signals, 450 terminal `MATCHED` reports, observed fill-rate `1.0`, synthetic-vs-observed gap `0.0`, and zero signal-to-order loss. It remains `NO_GO` because adverse selection was `0.9856051166792855`; the next loop must keep v8 quote placement, add market/side risk filters, and repeat before any promotion discussion.
   - `asset_execution_decision_v1` is implemented as an offline asset/segment decision report under each v8 report root. It classifies assets as `REPEAT_ASSET`, `RETUNE_ASSET`, or `BLOCK_ASSET` from observed execution, signal-to-order quality, quote diagnostics, and pre-live blocked segments; generated blocklist candidates remain research-only and `can_apply_live=false`.
   - `execution_probe_universe_selection_v1` now supports research-only market/side adverse-selection filtering from `adverse_selection_by_strategy`. The filter is applied before services start, records exclusions in `execution_probe_universe_adverse_exclusions.parquet`, and refuses to backfill filtered universes with zero-evidence metadata-only assets.
   - The 2026-05-11 market/side filtered v8 run preserved execution quality but did not solve adverse selection: 182 signals, observed fill-rate `1.0`, synthetic-vs-observed gap `0.0`, adverse selection `0.9807692307692307`, and lower coverage than the unfiltered run. The historical result rejected repeating that filter; the deterministic next decision now routes this case to `execution_probe_v9` instead of another market/side repeat.
   - `fill_toxicity_v1` is implemented as an offline report that measures fill-level post-fill PnL, adverse markout, pre-fill quote churn, fill-rate, and segment decisions by market/asset/side/strategy. It is exported by the research loop and remains `can_execute_trades=false`.
   - `execution_probe_v9` is implemented as the next dry-run-only quote profile after the failed market/side v8 filter. It quotes slightly behind touch, requires higher confidence/depth, and uses stricter top-of-book rotation and cooldown filters. It is meant to test lower toxic-fill exposure, not to enable live trading.
   - The 2026-05-11 90-minute `execution_probe_v9` observation produced 287 signals, 287 terminal `UNMATCHED` reports, zero observed fills, zero synthetic fills, and zero missing signal-to-order reports. The first preflight failed at `min_confidence=0.57`; the v9 observation wrapper now pins `PREDICTOR_EXECUTION_PROBE_V9_MIN_CONFIDENCE=0.55` so 1-cent spread candidates can emit research signals. The deterministic next decision is `CHANGE_MARKET_OR_TIMING_FILTERS`, with `EXPAND_FILLABILITY_UNIVERSE` for `scripts/run_execution_probe_v9_cycle.sh --selection-source fillability --universe-limit 20 --min-assets 5`.
   - The 2026-05-12 expanded-fillability `execution_probe_v9` observation used 20 primary assets, produced 1,285 signals and 5 observed fills, but remained `NO_GO`: observed fill-rate `0.0038910505836575876`, synthetic fill-rate `0.052140077821011675`, adverse selection `1.0`, drawdown `0.09899999999999987`, and `fill_toxicity_v1` rejected one segment with adverse 30s rate `1.0`. The deterministic next decision is `HOLD_RESEARCH`; do not tune global quote aggression again until toxic-fill segments can be excluded or scored deterministically.
   - `execution_probe_next_decision_v1` now emits runnable `--universe-limit` arguments for fillability expansion, and the v9 observation wrapper sets a capture-duration threshold with startup/preflight slack so long dry-runs do not fail solely because service startup consumed part of the wall-clock window.
   - `fill_toxicity_v1` now exports bucketed toxic segments by market, asset, side, spread bucket, and timing bucket. Source `strategy/model_version` are preserved as evidence fields, but they are not used as hard runtime match keys for fill-toxicity blocklists so filters learned from prior near-touch versions can be tested against the current v9 predictor without silently missing every segment.
   - `execution_probe_universe_selection_v1` now supports `toxicity_filter=segment` for fillability-first v9 cycles. It generates a toxicity-adjusted universe score from fillability, future-touch evidence, liquidity, and observed toxic-fill penalties, then records `execution_probe_universe_toxicity_quality.parquet` and the applied blocklist path. The selector should optimize quality-adjusted market coverage before any more quote-aggression tuning.
   - The 2026-05-12 toxicity-filtered `execution_probe_v9` 60-minute run produced 1,097 signals and one observed fill, but the impact report is `REPAIR_FILTER_CONTRACT`: the pre-run blocklist had 32 segments and runtime accepted 1,096 snapshots with zero `blocked_segment` rejections. The next step is to regenerate the fill-toxicity blocklist under the corrected match scope and rerun a fixed-universe observation; do not extend to 90-120 minutes until the filter actually blocks in runtime.
   - The corrected 2026-05-12 toxicity-filtered `execution_probe_v9` 60-minute run validated the runtime contract: 1,777 `blocked_segment` rejections, 745 signals, 4 observed fills, observed fill-rate `0.005369127516778523`, adverse selection `0.8317099567099567`, and toxicity impact `KEEP_FILTER_FOR_RESEARCH`. The next step was a longer fixed-universe repeat with the same corrected filter; it remained research-only because go/no-go stayed `NO_GO` on fill-rate, synthetic gap, and adverse selection.
   - The 2026-05-12 long corrected toxicity-filtered `execution_probe_v9` 90-minute run rejected repeating that filter unchanged: 1,199 runtime `blocked_segment` rejections, 1,521 signals, zero observed fills, observed fill-rate `0.0`, synthetic fill-rate `0.024326101249178174`, and toxicity impact `REJECT_FILTER`. The deterministic next decision is `CHANGE_MARKET_OR_TIMING_FILTERS`; move to feature/model or market/timing redesign instead of another longer run with the same filter. Live remains blocked.
   - `executable_opportunities_v1` is now the offline dataset for inferring strategies from executable evidence. It joins signal context, quote relation, depth, distance to touch, synthetic/observed fill evidence, timing buckets, expected edge, and post-fill markout context into `executable_opportunities.parquet`.
   - `segment_opportunity_ranking_v1` ranks market/asset/side/spread/timing buckets from executable opportunities and exports `allowed_segments_v1`. Ranking now includes runtime-activity coverage through `runtime_opportunities` and `runtime_active_minutes` so selected segments are not only offline-good but observable in the current runtime stream. This separates research evidence from execution: the artifact remains `can_execute_trades=false`, while `execution_probe_v10` can use it in dry-run to reject snapshots outside selected segments.
   - `execution_probe_v10` is implemented as the next research-only probe. It defaults to `selection_source=executable_segments`, loads `PREDICTOR_ALLOWED_SEGMENTS_PATH`, keeps near-touch execution gated to `EXECUTION_MODE=dry_run`, and should be run only after a ranking artifact has at least one promoted segment. `allowed_segment_candidates` may include `allowed_reason=RUNTIME_ACTIVITY_BACKFILL` rows to avoid universes that looked good offline but had no runtime activity; these candidates are dry-run coverage aids, not live permissions. Its purpose is to test an inferred segment policy, not to enable live.
   - The 2026-05-13 local v10 preflight was intentionally fail-closed: the only available `data_lake/research.duckdb` produced 20 opportunities, zero executable opportunities, zero selected segments, and `execution_probe_universe_selection.status=insufficient_assets`. Do not run a v10 observation from this local DuckDB; collect a fresh dry-run window first, then regenerate `executable_opportunities_v1` and `segment_opportunity_ranking_v1`.
   - The 2026-05-13 fresh v10 cycle generated a strong base window (`10786` signals, `21570` reports), promoted two executable segments offline, then produced a 30-minute sparse runtime observation with `orderbook=977`, `signals=0`, and `reports=0`. The cycle now records this as `sparse_probe_no_signals` / `DO_NOT_PROMOTE` instead of a generic crash. Next: re-rank or widen v10 selection using `runtime_opportunities` / `runtime_active_minutes`, with `RUNTIME_ACTIVITY_BACKFILL` allowed for candidate coverage before repeating; do not change live gates or global quote aggression from this result.
   - The 2026-05-13 runtime-backfill v10 60-minute run fixed the sparse-runtime failure but did not make the strategy tradable: `orderbook=2890`, `signals=118`, `reports=236`, signal-to-order consumption `1.0`, order creation `1.0`, observed fill-rate `0.0`, synthetic fill-rate `0.0`, and no future-touch evidence. The deterministic decision is `CHANGE_MARKET_OR_TIMING_FILTERS`; keep v10 research-only, deduplicate market assets in universe reporting, and retune market/timing selection before changing quote aggression.
   - `touch_probability_ranking_v1` is implemented as the next market-selection artifact. It ranks market/assets by observed/future touch probability, observed fill evidence, distance-to-touch, stale rate, liquidity/depth, and synthetic optimism penalty.
   - `execution_probe_v11` is the next dry-run-only probe. It keeps the conservative v10 quote policy but changes universe selection to `selection_source=touch_probability`, so the next 60-minute observation tests fillability of market/timing selection before changing quote aggression again.
   - Run `scripts/run_execution_probe_v11_cycle.sh --print-plan` before any v11 observation. If the generated universe has insufficient assets or weak touch evidence, do not start services; collect a fresher DuckDB or relax selection thresholds through the versioned cycle arguments.
   - The 2026-05-13 `execution_probe_v11` touch-probability 60-minute run selected 2 unique assets, produced `signals=241`, consumed and reconciled every signal, but produced zero observed or synthetic fills. The decision remains `CHANGE_MARKET_OR_TIMING_FILTERS`; next work should use fresh runtime touch evidence/current-market activity before changing global quote aggressiveness.
   - Add `at_touch_asset_diagnostic_v1` to explain whether the v11 assets were simply quoted too far from touch or whether the runtime book never touched those prices. It reads existing quote diagnostics, emits per-asset decisions (`RETUNE_TO_AT_TOUCH`, `DROP_RUNTIME_STALE`, `REPEAT_AT_TOUCH`, `NEEDS_SAMPLE`), and remains advisory-only.
   - Add `runtime_touch_ranking_v1` and `selection_source=runtime_touch` so the next candidate universe can be ranked by recent top-of-book movement, spread opportunity density, stale-rate, depth, and active minutes from the current orderbook stream. This avoids repeating assets that looked good historically but were inactive during the observation window.
   - Add `execution_probe_touch_comparison_v1` to compare v10/v11/at-touch diagnostics in one report. The comparator must keep `can_execute_trades=false` and output only the next research action: change market/timing filters, retune to at-touch, reduce synthetic dependence, or repeat a longer observation.
   - The first post-v11 diagnostic over the 2026-05-13 v11 root classified both v11 assets as `DROP_RUNTIME_STALE`; fresh runtime-touch ranking found 2 ranked assets but 0 selected assets, and `execution_probe_touch_comparison_v1` diagnosed `runtime_touch_stale_or_quotes_not_reachable` with next action `CHANGE_MARKET_OR_TIMING_FILTERS`.
   - `execution_probe_v12` is the next dry-run-only at-touch runtime probe. It uses the same runtime-touch universe contract as v11 but quotes at touch (`near_touch_max_spread_fraction=1.0`, `offset_ticks=0`) while preserving v11-style depth, top-rotation, and cooldown controls.
   - Add `execution_failure_diagnostics_v1` to keep executor/risk `ERROR` evidence separate from order-created `UNMATCHED` evidence. Quote/touch diagnostics should guide quote or market timing only after errors are excluded from the unmatched family.
   - Add `scripts/run_runtime_touch_ab_cycle.sh` to compare v11 versus v12 on one runtime-touch universe, emit failure diagnostics for each arm, and generate profile/touch comparisons. The A/B cycle remains research-only and must be run with `--print-plan` before any 60-90 minute observation.
   - Add `runtime_touch_ab_decision_v1` as the final A/B decision artifact. It must validate that v11 and v12 share the same universe hash, asset count, duration, and research-only contract before recommending repeat, rejection, market/timing changes, or risk/executor fixes.
   - The 2026-05-13 A/B attempt generated a strong fresh capture (`7187` signals, `14357` reports) and a ready 2-asset runtime-touch universe, but v11 failed preflight with `missing_signals_stream_progress` on that universe. The generated decision is `RERANK_RUNTIME_TOUCH_UNIVERSE`; do not run v12 or infer quote-policy improvement until the baseline profile emits signals on the same universe.
   - Runtime-touch universe selection now includes `signalable_snapshots` and `signalable_density` filters. The A/B path defaults to requiring snapshots with spread/depth sufficient for the predictor before starting services, so assets with book movement but zero expected signal emissions are rejected before preflight.
   - Add `runtime_touch_ab_retry_ladder_v1` and `scripts/run_runtime_touch_ab_retry_ladder.sh` before the next long A/B. The ladder evaluates the same fresh DuckDB through a deterministic sequence: strict signalable universe, wider universe, lower signalable density, lower signalable snapshots, then a runtime-hybrid market backfill. It does not start services or execute trades; it emits the first reproducible `scripts/run_runtime_touch_ab_cycle.sh` command whose universe is ready.
   - The retry ladder must keep `runtime_signal_min_spread` aligned with the predictor floor (`0.03`) to avoid selecting assets that look active in orderbook snapshots but cannot emit runtime signals. The hybrid fallback is only allowed after normal signalable attempts fail and must be marked with `runtime_touch_min_assets_backfill_signalable_market_liquidity`.
   - Add `scripts/run_runtime_touch_ab_auto_route.sh` as the operator wrapper for the next pre-live attempt: fresh runtime capture for 60-90 minutes, retry ladder evaluation, and optional selected A/B execution. Operators must run `--print-plan` first; live remains blocked regardless of the route result.
   - The 2026-05-17 auto-route fresh capture is recorded in [execution_probe_observations.md](execution_probe_observations.md). It produced `21561` signals and `43124` reports, but every retry-ladder level found only `1` signalable asset versus the minimum `2`, so no A/B was launched. The next implementation should improve market/timing selection coverage rather than relax comparable-universe requirements.
   - Add `runtime_touch_signalability_diagnostic_v1` to explain why runtime-touch assets fail before A/B selection. The diagnostic must report blocker counts for snapshots, active minutes, touch-change rate, spread, signalable snapshots, signalable density, stale rate, and metadata status.
   - Add `runtime_touch_candidate_expansion_v1` plus `scripts/run_runtime_touch_selection_probe.sh` for short 30-45 minute selection-only captures. This path is research-only, never launches A/B, and should prove that a candidate expansion can reach at least two signalable assets before another long v11/v12 comparison.
   - Add `runtime_touch_opportunity_windows_v1` to rank assets by recent signalable time windows before retrying A/B. This keeps the next universe tied to actual runtime spread/depth opportunities instead of one isolated snapshot, and the wrapper now requires both candidate expansion and opportunity-window selection to be ready before recommending a manual A/B retry ladder.
   - Add `runtime_touch_market_timing_scout_v1` to rank short active market windows across the fresh DuckDB before spending 60-90 minutes on another observation. The scout is research-only, emits `can_execute_trades=false`, and identifies whether the next move is a better time window or broader market discovery.
   - Add `runtime_touch_route_decision_v1` as the deterministic route decision for selection probes. It maps probe evidence to `READY_FOR_AB_RETRY`, `CHANGE_TIME_WINDOW`, `EXPAND_MARKET_DISCOVERY`, or `BLOCK_LIVE`, while keeping `live_gate=BLOCK_LIVE` until a later go/no-go process explicitly clears live trading.
   - Add `runtime_touch_discovery_batches_v1` and `scripts/run_runtime_touch_discovery_loop.sh` to widen market discovery before another long observation. The loop partitions ranked Gamma discovery into asset batches, runs the existing selection probe per batch in dry-run, and keeps every batch as an auditable research-only run root.
   - Add `runtime_touch_discovery_batch_comparison_v1` to compare discovery batches by markets, assets, eligible scout windows, selected assets, route decision, and rejection reason. The comparator recommends either the best batch for another selection probe or `EXPAND_MARKET_DISCOVERY`; it never promotes live trading.
   - Add `market_fillability_score_v1` as the market/asset evidence layer for discovery ranking. It combines observed dry-run fills, synthetic fills, future-touch evidence, quote relation, spread/depth, stale-rate, liquidity, and synthetic optimism penalties without producing executable signals.
   - Add `market_family_memory_v1` to remember which market families repeatedly produce usable windows or fail with no eligible runtime windows. Discovery batching now supports an epsilon-style explore/exploit policy using fillability scores and family memory while preserving a deterministic exploration quota for new families.
   - Add `runtime_touch_discovery_loop_control_v1` as the early-stop gate for the discovery loop. After each batch probe, the loop can stop when the route is `READY_FOR_AB_RETRY` or when `CHANGE_TIME_WINDOW` already has enough selected assets and eligible windows. It still writes a comparison and loop summary, so skipped batches remain explicit instead of hidden.
   - Extend `runtime_touch_discovery_batch_comparison_v1` with `recommended_next_run` and `recommended_next_command` only when the selected batch is `READY_FOR_AB_RETRY`. The recommendation points to `scripts/run_runtime_touch_ab_retry_ladder.sh` with `EXECUTION_MODE=dry_run`; it does not call the A/B cycle directly and cannot promote live.
   - Add bounded fillability scoring for large DuckDB captures. `market_fillability_score_v1` now limits candidate signals/snapshots before future-touch joins and exports rankings with DuckDB `COPY` instead of loading full rankings into pandas.
   - Add `--max-batches` to `scripts/run_runtime_touch_discovery_loop.sh` so operators can run a real, finite discovery probe even when early-stop does not trigger. The cap is recorded in `runtime_touch_discovery_loop_summary_v1`.
   - Upgrade discovery batching to `epsilon_family_fillability_diversified_batches_v2`. The selector still ranks by fillability and family memory, but now forms batches with family diversity when alternatives exist, records batch-level diversity metadata, and supports `--no-diversify-batches` for controlled comparisons against the previous sequential packing.
   - Add `runtime_touch_discovery_batch_diagnostics_v1` to explain why a discovery loop did or did not produce an A/B-ready batch. It reports per-batch readiness, primary blockers, stage status, source artifacts, selector-adjustment recommendations, and distinguishes unprocessed batches from missing artifacts.
   - The 2026-05-18 diversified discovery loop processed two bounded batches and found no A/B-ready universe: `batch-01` had `0` signalable assets and `batch-02` had `1`, below the comparable-universe minimum of `2`. The diagnostic recommendation is `EXPAND_DISCOVERY_WITH_SIGNALABILITY_AWARE_FILTER`; do not run the retry ladder until discovery finds at least two signalable assets.
   - Upgrade discovery batching to `epsilon_family_fillability_signalability_diversified_batches_v3`. The selector now consumes fillability assets as richer signalability priors, penalizes families with repeated signalability failures, and refuses to classify negative family memory as exploit just because a family has observations.
   - `scripts/run_runtime_touch_discovery_loop.sh` now emits `market_family_memory_update/market_family_memory.json` from discovery batches, comparison, diagnostics, and fillability evidence. This consolidates failed families after no-ready-batch loops while preserving the research-only contract.
   - Validate the next broad discovery run with `--print-plan --discovery-limit 40 --batch-size 6 --max-batches 6 --exploration-rate 0.35` before launching a long dry-run. Only run the retry ladder if the comparison emits a non-null `recommended_next_run`.
   - The 2026-05-18 broad signalability-aware loop found an A/B-ready `batch-04` after three failed batches. It emitted a valid memory update with `58` observations and a retry-ladder recommendation for a strict-signalable 2-asset universe. The next step is to run the emitted `scripts/run_runtime_touch_ab_cycle.sh --skip-fresh-capture` command in `dry_run`, not to change selector thresholds again.
   - The 2026-05-18 A/B attempt failed in v11 preflight with `missing_signals_stream_progress`: orderbook moved, but `signals` and `execution:reports` stayed at zero. Do not infer v11/v12 quote-policy quality from that run. The next implementation should re-rank or widen the runtime-touch universe with an immediate preflight-signalability requirement before retrying A/B.
   - Runtime-touch A/B now has an explicit signalability gate before ranking and observations. It writes `runtime_touch_signalability_gate/runtime_touch_signalability_diagnostic.json`, blocks with status `blocked` and exit code `20` when the fresh DuckDB cannot produce at least the required signalable assets, and only continues to A/B when the gate is ready.
   - Runtime-touch ranking now records freshness fields (`current_is_signalable`, `last_signalable_timestamp_ms`, recent signalable density, book age, and freshness score) and the A/B path uses `freshest_first` ordering so universes prioritize assets that are signalable now, not only historically active inside the lookback.
   - Runtime-touch A/B blocked runs now also write the canonical `runtime_touch_ab_decision.json`, so operators and auto-route tooling read one decision artifact for preflight failures, gate failures, and completed A/B comparisons.
   - The retry ladder now emits commands that reproduce the selected freshness and hybrid-backfill contract. Auto-route accepts research-only fresh-capture exit code `20` when artifacts exist and passes the actual fresh-capture duration into the ladder report.
   - `scripts/run_runtime_touch_cycle.sh` remains the single-profile runtime-touch operator path: fresh dry-run capture, `runtime_touch_ranking_v1`, `selection_source=runtime_touch`, dry-run observation, at-touch diagnostic, failure diagnostics, and `execution_probe_touch_comparison_v1`.
   - The 2026-05-13 runtime-touch cycle captured a fresh 30-minute window with `6465` signals and selected 4 runtime-touch assets, then ran a 30-minute observation with `36` signals, `32 UNMATCHED`, `4 ERROR`, zero observed fills, and zero synthetic fills. `execution_probe_touch_comparison_v1` diagnosed `runtime_touch_stale_or_quotes_not_reachable`; live remains blocked.
   - Next execution step: change market/timing filters or quote policy in a versioned research profile before repeating. Do not enable live until the comparison shows observed fills, low synthetic optimism, and acceptable adverse selection.
   - Require positive realized edge after slippage and no persistent adverse selection before enabling `EXECUTION_MODE=live`.
   - Require clean operator controls, confirmed cancellation behavior, and passing integration smoke before any live deployment.
   - Operator command intents are persisted in Postgres `control_commands` before Redis Stream publication when Postgres is configured, and production/control-required mode fails closed if that audit store is unavailable.
   - Keep Rust risk limits as the final authority for size, exposure, stale signals, kill switch, and cancellation behavior.
   - In `APP_ENV=production` or `REQUIRE_POSTGRES_STATE=true`, API reads for orders, positions, reports, control results, reconciliation, strategy metrics, runtime metrics, and Prometheus metrics must come from Postgres or fail closed with `503`.

## Acceptance Criteria

- Every phase has tests before live trading is enabled.
- `EXECUTION_MODE=dry_run` remains default.
- Any live execution must pass Rust risk gates.
- Redis Streams remain the internal service boundary.
- Postgres is recommended for operation, even if optional for local dry-run development.
- `APP_ENV=production` must fail startup unless required production settings are present.
