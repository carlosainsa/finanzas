# Pre-Live Dry-Run Runbook

This runbook defines the repeatable pre-live evidence run. It is still
`EXECUTION_MODE=dry_run`; it does not authorize live trading.

## Goal

Create one isolated research bundle from real public market data:

- `real_dry_run_evidence.json`
- `research_summary.json`
- `pre_live_promotion.json`
- `go_no_go.json`
- `agent_advisory.json`
- `pre_live_readiness.json`

The only acceptable final live posture from this run is advisory evidence. The
readiness report always keeps `can_execute_trades=false`.

## Command

Use the wrapper, not the lower-level script, for operator runs:

```bash
scripts/run_pre_live_dry_run.sh
```

For a shorter rehearsal that still uses the same gates:

```bash
scripts/run_pre_live_dry_run.sh --duration-seconds 900
```

To inspect the resolved plan without starting Docker or services:

```bash
scripts/run_pre_live_dry_run.sh --print-plan
```

The wrapper delegates to `scripts/run_real_dry_run_research.sh` and pins:

- `EXECUTION_MODE=dry_run`
- `DISABLE_MARKET_WS=false`
- `GO_NO_GO_PROFILE=pre_live`
- `REAL_DRY_RUN_ISOLATED=1`
- `REQUIRE_POSTGRES_STATE=true`
- `PRE_LIVE_MIN_CAPTURE_DURATION_MS=duration * 1000`
- `PRE_LIVE_MIN_SIGNALS=250`

## Inputs

Required:

- Docker available on the VM.
- Network access to Polymarket public market data and Gamma discovery.
- Free local ports for the managed test services, by default Redis `6382`,
  Postgres `5434`, and Operator API `18001`.

Optional:

- `MARKET_ASSET_IDS=token1,token2,...` to bypass Gamma discovery.
- `PREDICTOR_BLOCKED_SEGMENTS_PATH=/path/blocked_segments.json` for a restricted
  follow-up run.
- `REPORT_TIMESTAMP=<stable-run-id>` when an external scheduler owns run IDs.

Do not set `EXECUTION_MODE=live`. The scripts refuse to run live.

## Outputs

By default, outputs are isolated under:

```text
.tmp/real-dry-run-data-lake/<run_id>/
```

The most important file is:

```text
.tmp/real-dry-run-data-lake/<run_id>/reports/<run_id>/pre_live_readiness.json
```

The script prints a `pre_live_readiness_summary` block with:

- `status`
- `run_id`
- `profile`
- `decision`
- `blockers`
- `path`

For manual inspection:

```bash
scripts/summarize_pre_live_readiness.sh "$RESEARCH_REPORT_ROOT/pre_live_readiness.json"
```

For machine-readable automation:

```bash
PYTHONPATH=python-service python3 -m src.research.pre_live_readiness \
  --input "$RESEARCH_REPORT_ROOT/pre_live_readiness.json" \
  --format summary-json
```

## Interpreting `pre_live_readiness.json`

Top-level `status`:

- `ready`: all required pre-live artifacts, dry-run evidence, go/no-go, and
  Postgres audit checks passed.
- `blocked`: the run completed enough to produce a report, but one or more
  checks failed.
- `missing`: no valid research run was found.

Required checks:

- `research_manifest_available`: manifest index exists and has a latest run.
- `research_summary_available`: research summary artifact exists.
- `real_dry_run_evidence_available`: real dry-run evidence artifact exists.
- `dry_run_execution_mode`: evidence confirms `dry_run`.
- `go_no_go_report_available`: go/no-go artifact exists.
- `go_no_go_profile_pre_live`: profile is `pre_live` or stricter.
- `go_no_go_passed`: quantitative gate passed.
- `pre_live_promotion_available`: pre-live promotion artifact exists.
- `postgres_audit_available`: Postgres operational tables are readable.

Any item in `blockers` must be treated as a hard blocker before repeating the
run or considering `live_candidate`.

## Blocker Diagnostics And Restricted Retry

When readiness is blocked by quantitative checks, generate a segment-level
diagnostic report before changing thresholds:

```bash
scripts/analyze_pre_live_blockers.sh "$RESEARCH_REPORT_ROOT"
```

This writes:

- `blocker_diagnostics/pre_live_blocker_diagnostics.json`
- `blocker_diagnostics/blocked_segments_candidate.json`
- `blocker_diagnostics/blocked_segments_candidate_top_1.json` when the full
  candidate has more than one segment

The candidate blocklist is compatible with `PREDICTOR_BLOCKED_SEGMENTS_PATH`,
but it is still research-only. It should be used only for a restricted follow-up
dry-run:

```bash
MARKET_ASSET_IDS="<asset ids from fixed_market_universe.market_asset_ids_csv>" \
PREDICTOR_BLOCKED_SEGMENTS_PATH="$RESEARCH_REPORT_ROOT/blocker_diagnostics/blocked_segments_candidate.json" \
scripts/run_pre_live_dry_run.sh --duration-seconds 900
```

`pre_live_blocker_diagnostics.json` includes a ready-to-run command under
`next_restricted_run.command`. Use that command when available so the restricted
run reuses the same fixed market universe. `compare_runs` rejects restricted
comparisons when the candidate run's recorded `MARKET_ASSET_IDS` hash does not
match the blocklist evaluation contract.

For a repeatable operator flow, prefer the wrapper:

```bash
scripts/run_restricted_blocklist_observation.sh \
  --baseline-report-root "$RESEARCH_REPORT_ROOT" \
  --blocklist-kind top_1 \
  --duration-seconds 900
```

The wrapper reads the fixed market universe from
`pre_live_blocker_diagnostics.json`, runs the restricted dry-run, then writes
`comparison.json`, `research_promotion_decision.json`, and
`restricted_blocklist_observation_summary.json` into the restricted report root.
Use `--candidate-report-root <restricted-report-root>` to evaluate an already
completed restricted run without starting services.

Close the restricted decision explicitly:

```bash
scripts/finalize_restricted_blocklist_decision.sh \
  --observation-root "<restricted-report-root>"
```

This writes `restricted_blocklist_decision.json` with one of:

- `REJECT`: protected metrics or the research promotion gate rejected the run;
- `NEED_MORE_DATA`: the restricted run was not comparable or lacks evidence;
- `REPEAT_OBSERVATION`: the candidate was accepted only for another observation.

`REPEAT_OBSERVATION` is not live approval. All restricted blocklist artifacts
remain research-only and keep `can_execute_trades=false`.

The diagnostics also export:

- `blocker_diagnostics/fixed_market_universe.json`;
- `blocker_diagnostics/blocked_segments_defensive_candidate.json`;
- `top_explanatory_buckets`, which attributes drawdown and adverse-selection
  pressure by market, market/asset, strategy, and full segment.

Compare unrestricted versus restricted runs before accepting the blocklist:

```bash
PYTHONPATH=python-service python3 -m src.research.compare_runs \
  --baseline-report-root "$RESEARCH_REPORT_ROOT" \
  --candidate-report-root "<restricted-report-root>"
```

Do not promote a blocklist from one run. A candidate is useful only if the
restricted run improves drawdown/adverse-selection without degrading realized
edge, fill-rate, reconciliation, or simulator-quality metrics.

`compare_runs` now applies `segment_comparability_v2` for restricted runs. A
restricted run may lose only the segments explicitly listed in the candidate
blocklist used by `PREDICTOR_BLOCKED_SEGMENTS_PATH`. It must still meet minimum
coverage across shared segments, signals, and fills. Treat any unexpected
segment loss, unexpected new segment, or insufficient shared coverage as a hard
research blocker; repeat with a narrower blocklist or a longer capture before
changing thresholds.

For restricted runs, read `comparison.restricted_blocklist_assessment` before
looking at the aggregate verdict. `candidate_improved` is not enough. A
candidate blocklist remains rejected if protected metrics regress, including
realized edge, fill-rate, simulator-quality delta, or reconciliation divergence.
When rejected for simulator-quality regression, inspect
`comparison.restricted_blocklist_assessment.simulator_regression_diagnostics`;
it lists the worst market/asset/side segments and attaches dominant unfilled
reasons from the candidate run.

## Exit Codes

- `0`: research infra and gates passed.
- `20`: infrastructure completed but promotion/readiness gates failed. This is
  common for early samples and still produces useful artifacts.
- `64`: invalid operator input or unsafe environment.
- `69`: Docker is missing for the managed wrapper.
- `70`: one of the managed services exited during capture.

By default `ALLOW_RESEARCH_GATE_FAILURE=1`, so early research runs can keep
their artifacts even when readiness is blocked. Set `ALLOW_RESEARCH_GATE_FAILURE=0`
for scheduled pre-release runs where a blocked readiness report must fail the
procedure.

## Promotion Rule

A single `ready` run is not enough for live. Minimum next evidence:

1. At least two comparable isolated pre-live runs with `status=ready`.
2. Stable `compare_runs` result without `no_comparable`.
3. No persistent adverse selection in `game_theory.json` and backtest segments.
4. Clean operator controls and reconciliation, with Postgres audit available.
5. Manual review of `observed_vs_synthetic_fill_summary`,
   `unfilled_reason_summary`, and `dry_run_simulator_quality`.

Only after that should a separate `live_candidate` profile run be scheduled.
