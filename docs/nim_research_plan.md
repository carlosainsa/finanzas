# NVIDIA NIM Research Plan

NVIDIA NIM is an optional offline/advisory inference layer for research. It is
not part of the live predictor, executor, Redis Streams transport, or Rust risk
gate.

## Allowed Uses

- Summarize external evidence already available before a signal timestamp.
- Classify evidence direction, uncertainty, contradiction, and source quality.
- Produce advisory notes for model review, feature proposals, and bias checks.
- Explain research reports such as calibration, adverse selection, and
  feature-blocklist candidates.

## Prohibited Uses

- Publishing to `signals:stream`.
- Calling Operator API control endpoints.
- Modifying runtime blocklists.
- Choosing live `side`, `price`, `size`, or market exposure.
- Being imported by `python-service/src/ml/predictor.py`.
- Replacing deterministic promotion gates or Rust risk controls.

Every NIM artifact must carry:

- `decision_policy = offline_advisory_only`;
- `can_execute_trades = false`;
- model and prompt version fields;
- input/output hashes when persisted;
- point-in-time evidence timestamps.

## Configuration

Use environment variables, a secret manager, or an ignored local `.env`. Never
commit real NVIDIA API keys. Before committing NIM changes, verify that `.env`
is not staged and that secret scans do not match `nvapi-` tokens.

```bash
ENABLE_NIM_ADVISORY=false
NVIDIA_NIM_API_KEY=
NIM_BASE_URL=https://integrate.api.nvidia.com/v1
NIM_MODEL=deepseek-ai/deepseek-v3.2
NIM_TIMEOUT_SECONDS=30
NIM_MAX_EVIDENCE_PER_RUN=25
NIM_INPUT_COST_PER_MILLION_TOKENS=0
NIM_OUTPUT_COST_PER_MILLION_TOKENS=0
NIM_COST_CURRENCY=USD
```

The default is disabled. Enabling NIM only allows explicit research callers to
make advisory requests.

## Implementation Boundary

The client lives under:

```text
python-service/src/research/llm/nim_client.py
```

This path is intentionally under `research/llm/`, not `ml/` or `data/`, so it
does not enter runtime signal generation. The client uses the OpenAI-compatible
`/chat/completions` endpoint through `httpx`.

The offline advisory exporter lives under:

```text
python-service/src/research/nim_advisory.py
```

It can be run directly:

```bash
PYTHONPATH=python-service python -m src.research.nim_advisory \
  --duckdb data_lake/research.duckdb \
  --output-dir data_lake/nim_advisory
```

By default, it writes disabled/empty artifacts and makes no NVIDIA API call.
Set `ENABLE_NIM_ADVISORY=true` or pass `--enabled` to run real advisory
inference. The research loop emits `nim_advisory.json` for manifest/audit
consistency, but NIM does not affect `research_exit_code.txt` or promotion gates.

Persisted outputs are separate research artifacts:

```text
nim_advisory.json
nim_advisory_annotations.parquet
nim_advisory_summary.parquet
nim_advisory_cost_summary.parquet
nim_advisory_cost_summary.json
```

These artifacts are registered in research manifests as diagnostics. They must
remain advisory until converted into deterministic, timestamped, versioned
features and promoted through existing backtest, calibration, comparison, and
pre-live gates.

## Real Smoke And Model Inventory

After rotating the NVIDIA key and storing it in a secret manager or ignored
local `.env`, run a real advisory smoke against temporary evidence. The script
writes only to a temporary directory and does not print the key:

```bash
PYTHONPATH=python-service ENABLE_NIM_ADVISORY=true scripts/run_nim_advisory_smoke.py
```

To inspect currently available models for the account:

```bash
PYTHONPATH=python-service python -m src.research.llm.nim_models \
  --output data_lake/nim_model_inventory.json
```

Use the generated inventory instead of hardcoding model names. NVIDIA may
deprecate or add models over time. If an old local `.env` overrides
`NIM_MODEL`, rerun the smoke with an explicit current model:

```bash
PYTHONPATH=python-service ENABLE_NIM_ADVISORY=true \
  NIM_MODEL=deepseek-ai/deepseek-v3.2 \
  scripts/run_nim_advisory_smoke.py
```
