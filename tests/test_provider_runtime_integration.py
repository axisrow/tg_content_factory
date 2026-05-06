from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from typing import Any

import httpx
import pytest

from src.agent.provider_registry import (
    ZAI_CODING_BASE_URL,
    ZAI_GENERAL_BASE_URL,
    ProviderRuntimeConfig,
)
from src.config import AppConfig
from src.services.agent_provider_service import ProviderConfigService as DbProviderConfigService
from src.services.provider_service import RuntimeProviderRegistry as RuntimeProviderService


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
        return FakeAiohttpResponse(
            200,
            {"choices": [{"message": {"content": "runtime-ok"}}]},
        )


class FakeLangChainChatModel:
    def __init__(self, response_text: str = "runtime-ok") -> None:
        self.response_text = response_text
        self.prompts: list[str] = []

    async def ainvoke(self, prompt: str):
        self.prompts.append(prompt)
        return type("FakeLangChainResponse", (), {"content": self.response_text})()


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
    await DbProviderConfigService(db, config).save_provider_configs([_zai_cfg(base_url)])
    return config


@pytest.mark.anyio
async def test_zai_db_config_builds_runtime_adapter_and_calls_general_chat_endpoint(
    db,
    monkeypatch,
):
    config = await _save_zai_config(db, ZAI_GENERAL_BASE_URL)
    captured: dict[str, Any] = {}
    fake_model = FakeLangChainChatModel()

    def fake_init_chat_model(**kwargs):
        captured.update(kwargs)
        return fake_model

    monkeypatch.setattr("langchain.chat_models.init_chat_model", fake_init_chat_model)

    service = RuntimeProviderService(db, config)
    assert await service.load_db_providers() == 1

    result = await service.get_provider_callable("zai")(
        prompt="hello",
        model="glm-5-turbo",
        max_tokens=16,
        temperature=0.2,
    )

    assert result == "runtime-ok"
    assert fake_model.prompts == ["hello"]
    assert captured["model_provider"] == "openai"
    assert captured["model"] == "glm-5-turbo"
    assert captured["base_url"] == ZAI_GENERAL_BASE_URL
    assert captured["api_key"] == "zai-test-key"
    assert captured["max_tokens"] == 16
    assert captured["temperature"] == 0.2


@pytest.mark.anyio
async def test_zai_db_config_with_empty_base_url_defaults_to_coding_endpoint(db, monkeypatch):
    config = await _save_zai_config(db, "")
    captured: dict[str, Any] = {}

    def fake_init_chat_model(**kwargs):
        captured.update(kwargs)
        return FakeLangChainChatModel()

    monkeypatch.setattr("langchain.chat_models.init_chat_model", fake_init_chat_model)

    service = RuntimeProviderService(db, config)
    assert await service.load_db_providers() == 1

    result = await service.get_provider_callable("zai")(
        prompt="hello",
        model="glm-5-turbo",
        max_tokens=16,
        temperature=0.2,
    )

    assert result == "runtime-ok"
    assert captured["base_url"] == ZAI_CODING_BASE_URL


@pytest.mark.anyio
async def test_zai_explicit_coding_endpoint_is_honored_by_runtime_adapter(db, monkeypatch):
    config = await _save_zai_config(db, ZAI_CODING_BASE_URL)
    captured: dict[str, Any] = {}

    def fake_init_chat_model(**kwargs):
        captured.update(kwargs)
        return FakeLangChainChatModel()

    monkeypatch.setattr("langchain.chat_models.init_chat_model", fake_init_chat_model)

    service = RuntimeProviderService(db, config)
    assert await service.load_db_providers() == 1

    result = await service.get_provider_callable("zai")(prompt="hello", model="glm-5-turbo")

    assert result == "runtime-ok"
    assert captured["base_url"] == ZAI_CODING_BASE_URL


@pytest.mark.anyio
@pytest.mark.parametrize(
    "legacy_base_url",
    [
        "https://api.z.ai/api/anthropic",
        "https://api.z.ai/api/anthropic/v1",
    ],
)
async def test_zai_legacy_anthropic_base_url_is_rejected_before_runtime_registration(
    db,
    legacy_base_url,
):
    config = await _save_zai_config(db, legacy_base_url)
    db_service = DbProviderConfigService(db, config)
    loaded = await db_service.load_provider_configs()

    validation_error = db_service.validate_provider_config(loaded[0])
    assert "Anthropic-compatible proxy" in validation_error
    assert "anthropic provider" in validation_error
    assert ZAI_GENERAL_BASE_URL in validation_error

    service = RuntimeProviderService(db, config)
    assert await service.load_db_providers() == 0
    assert not service.has_providers()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "configured_base_url,expected_models_url",
    [
        (ZAI_GENERAL_BASE_URL, f"{ZAI_GENERAL_BASE_URL}/models"),
        (ZAI_CODING_BASE_URL, f"{ZAI_CODING_BASE_URL}/models"),
    ],
)
async def test_zai_model_refresh_uses_configured_base_url(
    db,
    monkeypatch,
    configured_base_url,
    expected_models_url,
):
    config = await _save_zai_config(db, configured_base_url)
    db_service = DbProviderConfigService(db, config)

    fetches: list[tuple[str, dict[str, str] | None]] = []

    async def fake_fetch_json(url: str, headers: dict[str, str] | None = None) -> dict[str, Any]:
        fetches.append((url, headers))
        return {"data": [{"id": "glm-5-turbo"}]}

    monkeypatch.setattr(db_service, "_fetch_json", fake_fetch_json)
    entry = await db_service.refresh_models_for_provider("zai", _zai_cfg(configured_base_url))

    assert entry.error == ""
    assert entry.models == ["glm-5-turbo"]
    assert fetches == [(expected_models_url, {"Authorization": "Bearer zai-test-key"})]


@pytest.mark.anyio
async def test_zai_model_refresh_rejects_legacy_anthropic_models_endpoint(db, monkeypatch):
    config = await _save_zai_config(db, "https://api.z.ai/api/anthropic/v1")
    db_service = DbProviderConfigService(db, config)

    async def fake_fetch_json(url: str, headers: dict[str, str] | None = None) -> dict[str, Any]:
        raise AssertionError(f"legacy URL must be rejected before HTTP fetch: {url} {headers}")

    monkeypatch.setattr(db_service, "_fetch_json", fake_fetch_json)
    entry = await db_service.refresh_models_for_provider(
        "zai",
        _zai_cfg("https://api.z.ai/api/anthropic/v1"),
    )

    assert entry.error
    assert "Anthropic-compatible proxy" in entry.error


@pytest.mark.parametrize(
    "provider,base_url,model,expected_url",
    [
        # Expected URLs are hardcoded so the test catches changes to the
        # configured endpoint flowing through LangChain.
        (
            "zai",
            "https://api.z.ai/api/paas/v4",
            "glm-5-turbo",
            "https://api.z.ai/api/paas/v4/chat/completions",
        ),
        (
            "zai",
            "https://api.z.ai/api/coding/paas/v4",
            "glm-5-turbo",
            "https://api.z.ai/api/coding/paas/v4/chat/completions",
        ),
        (
            "openai",
            "https://api.openai.com/v1",
            "gpt-4o-mini",
            "https://api.openai.com/v1/chat/completions",
        ),
    ],
)
@pytest.mark.anyio
async def test_deepagents_chat_runs_real_init_chat_model_and_create_deep_agent(
    db,
    monkeypatch,
    provider,
    base_url,
    model,
    expected_url,
):
    """Build a real langchain model + real deepagents agent and verify the
    actual outbound HTTP call hits the correct OpenAI-compat endpoint.

    Catches regressions on the AgentManager → langchain → openai → httpx path
    that mocking init_chat_model / create_deep_agent would silently miss
    (e.g. the Z.AI Coding endpoint regression from #516).
    """
    config = AppConfig()
    config.security.session_encryption_key = "deepagents-runtime-secret"
    cfg = ProviderRuntimeConfig(
        provider=provider,
        enabled=True,
        priority=0,
        selected_model=model,
        plain_fields={"base_url": base_url},
        secret_fields={"api_key": f"{provider}-test-key"},
    )
    await DbProviderConfigService(db, config).save_provider_configs([cfg])

    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    captured_requests: list[dict[str, Any]] = []

    def _build_response(request: httpx.Request) -> httpx.Response:
        body = {
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "created": 0,
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "deepagents-runtime-ok",
                    },
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
        }
        return httpx.Response(
            status_code=200,
            headers={"Content-Type": "application/json"},
            content=json.dumps(body).encode("utf-8"),
            request=request,
        )

    def _capture(request: httpx.Request) -> None:
        try:
            payload = json.loads(request.content) if request.content else None
        except json.JSONDecodeError:
            payload = None
        captured_requests.append(
            {
                "method": request.method,
                "url": str(request.url),
                "headers": {k.lower(): v for k, v in request.headers.items()},
                "payload": payload,
            }
        )

    def fake_sync_send(self, request, **kwargs):
        del self, kwargs
        _capture(request)
        return _build_response(request)

    async def fake_async_send(self, request, **kwargs):
        del self, kwargs
        _capture(request)
        return _build_response(request)

    monkeypatch.setattr(httpx.Client, "send", fake_sync_send)
    monkeypatch.setattr(httpx.AsyncClient, "send", fake_async_send)

    # Build the real DeepagentsBackend → real init_chat_model → real
    # create_deep_agent. The backend's _build_agent reads cfg, normalizes
    # the Z.AI base URL and selects model_provider="openai" — exactly the
    # production path that the #516 regression broke.
    from src.agent.manager import DeepagentsBackend

    backend = DeepagentsBackend(db, config)
    await backend.refresh_settings_cache()
    agent = backend._build_agent(cfg, tools=[])

    result = await asyncio.to_thread(
        agent.invoke,
        {"messages": [{"role": "user", "content": "hello"}]},
    )

    assert captured_requests, "Expected at least one HTTP request from deepagents agent"
    first = captured_requests[0]
    assert first["method"] == "POST"
    assert first["url"] == expected_url
    assert first["headers"].get("authorization") == f"Bearer {provider}-test-key"
    assert first["payload"] is not None
    assert first["payload"]["model"] == model
    user_messages = [m for m in first["payload"]["messages"] if m.get("role") == "user"]
    assert any("hello" in (m.get("content") or "") for m in user_messages)

    text = backend._extract_result_text(result)
    assert "deepagents-runtime-ok" in text


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
