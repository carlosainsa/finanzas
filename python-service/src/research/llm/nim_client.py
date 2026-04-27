from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter
from typing import Any

import httpx

from src.config import settings


NIM_ADVISORY_MODEL_VERSION = "nvidia_nim_research_client_v1"
NIM_DECISION_POLICY = "offline_advisory_only"


class NIMConfigurationError(RuntimeError):
    pass


class NIMRequestError(RuntimeError):
    pass


@dataclass(frozen=True)
class NIMResearchConfig:
    api_key: str | None = None
    base_url: str = "https://integrate.api.nvidia.com/v1"
    model: str = "deepseek-ai/deepseek-v3.2"
    timeout_seconds: float = 30.0
    enabled: bool = False

    @classmethod
    def from_settings(cls) -> "NIMResearchConfig":
        return cls(
            api_key=settings.nvidia_nim_api_key,
            base_url=settings.nim_base_url,
            model=settings.nim_model,
            timeout_seconds=settings.nim_timeout_seconds,
            enabled=settings.enable_nim_advisory,
        )


@dataclass(frozen=True)
class NIMAdvisoryResult:
    text: str
    model: str
    model_version: str
    decision_policy: str
    can_execute_trades: bool
    usage: dict[str, object]
    finish_reason: str | None
    latency_ms: float

    def as_dict(self) -> dict[str, object]:
        return {
            "text": self.text,
            "model": self.model,
            "model_version": self.model_version,
            "decision_policy": self.decision_policy,
            "can_execute_trades": self.can_execute_trades,
            "usage": self.usage,
            "finish_reason": self.finish_reason,
            "latency_ms": self.latency_ms,
        }


class NIMResearchClient:
    """OpenAI-compatible NVIDIA NIM client for offline research only."""

    def __init__(
        self,
        config: NIMResearchConfig | None = None,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.config = config or NIMResearchConfig.from_settings()
        self._client = http_client

    def healthcheck(self) -> dict[str, object]:
        if not self.config.enabled:
            return {
                "status": "disabled",
                "enabled": False,
                "model": self.config.model,
                "decision_policy": NIM_DECISION_POLICY,
                "can_execute_trades": False,
            }
        self._require_api_key()
        return {
            "status": "ok",
            "enabled": True,
            "model": self.config.model,
            "base_url": self.config.base_url,
            "decision_policy": NIM_DECISION_POLICY,
            "can_execute_trades": False,
        }

    def generate(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        temperature: float = 0.0,
        max_tokens: int = 512,
    ) -> NIMAdvisoryResult:
        if not self.config.enabled:
            raise NIMConfigurationError("NVIDIA NIM advisory is disabled")
        if not 0.0 <= temperature <= 2.0:
            raise ValueError("temperature must be between 0 and 2")
        if max_tokens <= 0:
            raise ValueError("max_tokens must be positive")
        self._require_api_key()
        payload = {
            "model": self.config.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        started = perf_counter()
        response = self._http_client().post(
            "/chat/completions",
            headers={"Authorization": f"Bearer {self.config.api_key}"},
            json=payload,
        )
        latency_ms = round((perf_counter() - started) * 1000, 3)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise NIMRequestError(
                f"NVIDIA NIM request failed with status {response.status_code} after {latency_ms}ms"
            ) from exc
        return parse_chat_completion(response.json(), self.config.model, latency_ms)

    def _require_api_key(self) -> None:
        if not self.config.api_key:
            raise NIMConfigurationError("NVIDIA_NIM_API_KEY is required when NIM is enabled")

    def _http_client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(
                base_url=self.config.base_url.rstrip("/"),
                timeout=self.config.timeout_seconds,
            )
        return self._client


def parse_chat_completion(
    payload: dict[str, Any], fallback_model: str, latency_ms: float
) -> NIMAdvisoryResult:
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise NIMRequestError("NVIDIA NIM response did not contain choices")
    first = choices[0]
    if not isinstance(first, dict):
        raise NIMRequestError("NVIDIA NIM response choice is invalid")
    message = first.get("message")
    if not isinstance(message, dict):
        raise NIMRequestError("NVIDIA NIM response choice did not contain a message")
    content = message.get("content")
    if not isinstance(content, str):
        raise NIMRequestError("NVIDIA NIM response message did not contain text content")
    usage = payload.get("usage")
    return NIMAdvisoryResult(
        text=content,
        model=str(payload.get("model") or fallback_model),
        model_version=NIM_ADVISORY_MODEL_VERSION,
        decision_policy=NIM_DECISION_POLICY,
        can_execute_trades=False,
        usage=usage if isinstance(usage, dict) else {},
        finish_reason=first.get("finish_reason")
        if isinstance(first.get("finish_reason"), str)
        else None,
        latency_ms=latency_ms,
    )
