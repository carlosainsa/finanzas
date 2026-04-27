import httpx

from src.research.llm.nim_client import NIMResearchConfig
from src.research.llm.nim_models import (
    classify_models,
    create_model_inventory,
    fetch_nim_models,
)


def test_fetch_nim_models_uses_openai_compatible_models_endpoint() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "data": [
                    {"id": "deepseek-ai/deepseek-v4-flash", "owned_by": "deepseek-ai"},
                    {"id": "nvidia/nv-embedqa-e5-v5", "owned_by": "nvidia"},
                ]
            },
        )

    client = httpx.Client(
        base_url="https://integrate.api.nvidia.com/v1",
        transport=httpx.MockTransport(handler),
    )

    models = fetch_nim_models(
        NIMResearchConfig(api_key="nvapi-test", base_url="https://integrate.api.nvidia.com/v1"),
        client,
    )

    assert [model["id"] for model in models] == [
        "deepseek-ai/deepseek-v4-flash",
        "nvidia/nv-embedqa-e5-v5",
    ]
    assert requests[0].url.path == "/v1/models"
    assert requests[0].headers["authorization"] == "Bearer nvapi-test"


def test_model_inventory_classifies_candidate_model_families() -> None:
    models: list[dict[str, object]] = [
        {"id": "deepseek-ai/deepseek-v4-flash"},
        {"id": "moonshotai/kimi-k2-instruct"},
        {"id": "nvidia/nv-embedqa-e5-v5"},
        {"id": "writer/palmyra-med-70b"},
    ]

    groups = classify_models(models)
    inventory = create_model_inventory(models)

    assert "deepseek-ai/deepseek-v4-flash" in groups["reasoning_or_general"]
    assert "moonshotai/kimi-k2-instruct" in groups["reasoning_or_general"]
    assert groups["embeddings_or_retrieval"] == ["nvidia/nv-embedqa-e5-v5"]
    assert groups["domain_specific"] == ["writer/palmyra-med-70b"]
    assert inventory["model_count"] == 4
