from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import pytest

from src.agent.provider_registry import (
    ZAI_DEFAULT_BASE_URL,
    ZAI_GENERAL_BASE_URL,
    ProviderRuntimeConfig,
)
from src.config import AppConfig
from src.services.agent_provider_service import AgentProviderService as DbAgentProviderService
from src.services.provider_service import AgentProviderService as RuntimeProviderService


@dataclass
class RecordedRequest:
    method: str
    url: str
    headers: dict[str, str] | None
    payload: dict[str, Any] | None


class FakeAiohttpResponse:
    def __init__(self, status: int, payload: dict[str, Any], text: str = "") -> None:
        self.status = status
        self._payload = payload
        self._text = text or str(payload)

    async def __aenter__(self) -> "FakeAiohttpResponse":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def text(self) -> str:
        return self._text

    async def json(self) -> dict[str, Any]:
        return self._payload


class FakeAiohttpClientSession:
    requests: list[RecordedRequest] = []

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        del args, kwargs

    async def __aenter__(self) -> "FakeAiohttpClientSession":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    def post(
        self,
        url: str,
        *,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> FakeAiohttpResponse:
        del kwargs
        self.requests.append(RecordedRequest("POST", url, headers, json))
        if url.startswith(ZAI_GENERAL_BASE_URL.rstrip("/") + "/chat/completions"):
            return FakeAiohttpResponse(429, {"error": "rate limited on general endpoint"})
        return FakeAiohttpResponse(
            200,
            {"choices": [{"message": {"content": "runtime-ok"}}]},
        )


def _zai_cfg(base_url: str = "") -> ProviderRuntimeConfig:
    return ProviderRuntimeConfig(
        provider="zai",
        enabled=True,
        priority=0,
        selected_model="glm-5-turbo",
        plain_fields={"base_url": base_url},
        secret_fields={"api_key": "zai-test-key"},
    )


async def _save_zai_config(db, base_url: str = "") -> AppConfig:
    config = AppConfig()
    config.security.session_encryption_key = "provider-runtime-secret"
    await DbAgentProviderService(db, config).save_provider_configs([_zai_cfg(base_url)])
    return config


@pytest.mark.anyio
async def test_zai_db_config_builds_runtime_adapter_and_calls_coding_chat_endpoint(
    db,
    monkeypatch,
):
    config = await _save_zai_config(db, ZAI_DEFAULT_BASE_URL)
    FakeAiohttpClientSession.requests = []
    monkeypatch.setattr(
        "src.services.provider_service.aiohttp.ClientSession",
        FakeAiohttpClientSession,
    )

    service = RuntimeProviderService(db, config)
    assert await service.load_db_providers() == 1

    result = await service.get_provider_callable("zai")(
        prompt="hello",
        model="glm-5-turbo",
        max_tokens=16,
        temperature=0.2,
    )

    assert result == "runtime-ok"
    request = FakeAiohttpClientSession.requests[-1]
    assert request.method == "POST"
    assert request.url == f"{ZAI_DEFAULT_BASE_URL}/chat/completions"
    assert request.headers == {
        "Content-Type": "application/json",
        "Authorization": "Bearer zai-test-key",
    }
    assert request.payload == {
        "model": "glm-5-turbo",
        "messages": [{"role": "user", "content": "hello"}],
        "max_tokens": 16,
        "temperature": 0.2,
    }


@pytest.mark.anyio
@pytest.mark.parametrize(
    "legacy_base_url",
    [
        "https://api.z.ai/api/anthropic",
        "https://api.z.ai/api/anthropic/v1",
    ],
)
async def test_zai_legacy_anthropic_base_url_is_normalized_before_runtime_call(
    db,
    monkeypatch,
    legacy_base_url,
):
    config = await _save_zai_config(db, legacy_base_url)
    FakeAiohttpClientSession.requests = []
    monkeypatch.setattr(
        "src.services.provider_service.aiohttp.ClientSession",
        FakeAiohttpClientSession,
    )

    service = RuntimeProviderService(db, config)
    await service.load_db_providers()
    result = await service.get_provider_callable("zai")(prompt="hello", model="glm-5-turbo")

    assert result == "runtime-ok"
    assert FakeAiohttpClientSession.requests[-1].url == f"{ZAI_DEFAULT_BASE_URL}/chat/completions"


@pytest.mark.anyio
async def test_zai_models_success_does_not_mask_wrong_general_chat_endpoint(db, monkeypatch):
    config = await _save_zai_config(db, ZAI_GENERAL_BASE_URL)
    db_service = DbAgentProviderService(db, config)

    fetches: list[tuple[str, dict[str, str] | None]] = []

    async def fake_fetch_json(url: str, headers: dict[str, str] | None = None) -> dict[str, Any]:
        fetches.append((url, headers))
        return {"data": [{"id": "glm-5-turbo"}]}

    monkeypatch.setattr(db_service, "_fetch_json", fake_fetch_json)
    entry = await db_service.refresh_models_for_provider("zai", _zai_cfg(ZAI_GENERAL_BASE_URL))

    assert entry.error == ""
    assert entry.models == ["glm-5-turbo"]
    assert fetches == [(f"{ZAI_DEFAULT_BASE_URL}/models", {"Authorization": "Bearer zai-test-key"})]

    FakeAiohttpClientSession.requests = []
    monkeypatch.setattr(
        "src.services.provider_service.aiohttp.ClientSession",
        FakeAiohttpClientSession,
    )
    runtime_service = RuntimeProviderService(db, config)
    await runtime_service.load_db_providers()

    with pytest.raises(RuntimeError, match="Provider error 429"):
        await runtime_service.get_provider_callable("zai")(prompt="hello", model="glm-5-turbo")

    assert FakeAiohttpClientSession.requests[-1].url == (
        f"{ZAI_GENERAL_BASE_URL}/chat/completions"
    )


@pytest.mark.real_provider_smoke
@pytest.mark.skipif(
    os.environ.get("RUN_REAL_PROVIDER_SMOKE") != "1" or not os.environ.get("ZAI_API_KEY"),
    reason="Set RUN_REAL_PROVIDER_SMOKE=1 and ZAI_API_KEY to run live provider smoke.",
)
@pytest.mark.anyio
async def test_live_zai_default_adapter_smoke(monkeypatch):
    monkeypatch.setenv("ZAI_API_KEY", os.environ["ZAI_API_KEY"])
    service = RuntimeProviderService()
    provider = service.get_provider_callable("zai")

    response = await provider(
        prompt="Reply with exactly: ok",
        model="glm-5-turbo",
        max_tokens=8,
        temperature=0,
    )

    assert isinstance(response, str)
    assert response.strip()
