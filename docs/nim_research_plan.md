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

Use environment variables or a secret manager. Never commit real NVIDIA API
keys.

```bash
ENABLE_NIM_ADVISORY=false
NVIDIA_NIM_API_KEY=
NIM_BASE_URL=https://integrate.api.nvidia.com/v1
NIM_MODEL=deepseek-ai/deepseek-r1
NIM_TIMEOUT_SECONDS=30
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
```

These artifacts are registered in research manifests as diagnostics. They must
remain advisory until converted into deterministic, timestamped, versioned
features and promoted through existing backtest, calibration, comparison, and
pre-live gates.
