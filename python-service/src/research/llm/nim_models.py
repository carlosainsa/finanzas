from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import httpx

from src.config import settings
from src.research.llm.nim_client import NIMConfigurationError, NIMResearchConfig


def fetch_nim_models(
    config: NIMResearchConfig | None = None,
    http_client: httpx.Client | None = None,
) -> list[dict[str, object]]:
    resolved = config or NIMResearchConfig.from_settings()
    if not resolved.api_key:
        raise NIMConfigurationError("NVIDIA_NIM_API_KEY is required to list models")
    client = http_client or httpx.Client(
        base_url=resolved.base_url.rstrip("/"),
        timeout=resolved.timeout_seconds,
    )
    response = client.get("/models", headers={"Authorization": f"Bearer {resolved.api_key}"})
    response.raise_for_status()
    payload = response.json()
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, list):
        return []
    return [normalize_model(item) for item in data if isinstance(item, dict)]


def normalize_model(item: dict[str, Any]) -> dict[str, object]:
    model_id = item.get("id") or item.get("model") or item.get("name")
    owned_by = item.get("owned_by") or item.get("owner")
    return {
        "id": str(model_id) if model_id is not None else "",
        "owned_by": str(owned_by) if owned_by is not None else None,
        "object": item.get("object"),
        "created": item.get("created"),
    }


def classify_models(models: list[dict[str, object]]) -> dict[str, list[str]]:
    ids = sorted(str(model.get("id")) for model in models if model.get("id"))
    domain_tokens = ("palmyra-med", "bionemo", "protein", "bio", "chem", "health")
    return {
        "reasoning_or_general": [
            model_id
            for model_id in ids
            if any(
                key in model_id.lower()
                for key in (
                    "deepseek",
                    "qwen",
                    "llama",
                    "nemotron",
                    "kimi",
                    "minimax",
                    "glm",
                    "mistral",
                )
            )
        ],
        "embeddings_or_retrieval": [
            model_id
            for model_id in ids
            if any(key in model_id.lower() for key in ("embed", "retrieval", "rerank"))
        ],
        "domain_specific": [
            model_id
            for model_id in ids
            if any(key in model_id.lower() for key in domain_tokens)
        ],
    }


def create_model_inventory(models: list[dict[str, object]]) -> dict[str, object]:
    return {
        "report_version": "nim_model_inventory_v1",
        "base_url": settings.nim_base_url,
        "model_count": len(models),
        "recommended_for_finanzas": classify_models(models),
        "models": models,
    }


def main() -> int:
    parser = argparse.ArgumentParser(prog="nim-model-inventory")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    inventory = create_model_inventory(fetch_nim_models())
    text = json.dumps(inventory, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
