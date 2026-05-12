
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
   - `segment_opportunity_ranking_v1` ranks market/asset/side/spread/timing buckets from executable opportunities and exports `allowed_segments_v1`. This separates research evidence from execution: the artifact remains `can_execute_trades=false`, while `execution_probe_v10` can use it in dry-run to reject snapshots outside selected segments.
   - `execution_probe_v10` is implemented as the next research-only probe. It defaults to `selection_source=executable_segments`, loads `PREDICTOR_ALLOWED_SEGMENTS_PATH`, keeps near-touch execution gated to `EXECUTION_MODE=dry_run`, and should be run only after a ranking artifact has at least one promoted segment. Its purpose is to test an inferred segment policy, not to enable live.
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
