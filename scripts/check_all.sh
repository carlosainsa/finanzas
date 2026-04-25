#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR/rust-engine"
cargo fmt --check
cargo check
cargo test
cargo clippy -- -D warnings

cd "$ROOT_DIR"
PYTHONPATH=python-service pytest -q python-service/tests
python3 -m compileall python-service/src python-service/tests
PYTHONPATH=python-service mypy python-service/src python-service/tests

cd "$ROOT_DIR/frontend"
npm run generate:types
npm run typecheck
npm run build
