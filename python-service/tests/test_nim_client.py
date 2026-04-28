import json

import httpx
import pytest

from src.research.llm.nim_client import (
    NIM_ADVISORY_MODEL_VERSION,
    NIMConfigurationError,
    NIMRequestError,
    NIMResearchClient,
    NIMResearchConfig,
)


def test_healthcheck_reports_disabled_without_requiring_key() -> None:
    client = NIMResearchClient(NIMResearchConfig(enabled=False))

    result = client.healthcheck()

    assert result["status"] == "disabled"
    assert result["can_execute_trades"] is False


def test_generate_refuses_when_disabled() -> None:
    client = NIMResearchClient(NIMResearchConfig(enabled=False, api_key="test"))

    with pytest.raises(NIMConfigurationError):
        client.generate("system", "user")


def test_generate_posts_openai_compatible_payload() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "model": "deepseek-ai/deepseek-v3.2",
                "choices": [
                    {
                        "message": {"content": "diagnostic only"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"prompt_tokens": 10, "completion_tokens": 3, "total_tokens": 13},
            },
        )

    http_client = httpx.Client(
        base_url="https://integrate.api.nvidia.com/v1",
        transport=httpx.MockTransport(handler),
    )
    client = NIMResearchClient(
        NIMResearchConfig(
            enabled=True,
            api_key="nvapi-test",
            model="deepseek-ai/deepseek-v3.2",
        ),
        http_client=http_client,
    )

    result = client.generate("You are offline only.", "Review this feature.")

    assert result.text == "diagnostic only"
    assert result.model == "deepseek-ai/deepseek-v3.2"
    assert result.model_version == NIM_ADVISORY_MODEL_VERSION
    assert result.decision_policy == "offline_advisory_only"
    assert result.can_execute_trades is False
    assert result.usage["prompt_tokens"] == 10
    assert result.usage["total_tokens"] == 13
    assert result.latency_ms >= 0
    assert requests[0].url.path == "/v1/chat/completions"
    assert requests[0].headers["authorization"] == "Bearer nvapi-test"
    assert requests[0].read()
    payload = requests[0].content.decode()
    parsed_payload = json.loads(payload)
    assert parsed_payload["model"] == "deepseek-ai/deepseek-v3.2"
    assert parsed_payload["messages"][0]["role"] == "system"


def test_generate_maps_http_errors_to_request_error() -> None:
    http_client = httpx.Client(
        base_url="https://integrate.api.nvidia.com/v1",
        transport=httpx.MockTransport(lambda _: httpx.Response(500, json={})),
    )
    client = NIMResearchClient(
        NIMResearchConfig(enabled=True, api_key="nvapi-test"),
        http_client=http_client,
    )

    with pytest.raises(NIMRequestError):
        client.generate("system", "user")


def test_generate_requires_api_key_when_enabled() -> None:
    client = NIMResearchClient(NIMResearchConfig(enabled=True, api_key=None))

    with pytest.raises(NIMConfigurationError):
        client.generate("system", "user")
