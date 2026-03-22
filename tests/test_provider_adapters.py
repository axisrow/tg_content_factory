"""Tests for provider adapters."""

import pytest

from src.services.provider_adapters import (
    _parse_json_for_text,
    make_cohere,
    make_cohere_adapter,
    make_context7_adapter,
    make_generic_http_adapter,
    make_huggingface,
    make_huggingface_adapter,
    make_ollama,
    make_ollama_adapter,
)


class FakeResp:
    def __init__(self, status=200, json_data=None, text_data=None):
        self.status = status
        self._json = json_data or {}
        self._text = text_data or ""

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def text(self):
        return self._text or ""

    async def json(self):
        return self._json


class FakeSession:
    def __init__(self, resp: FakeResp):
        self._resp = resp

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def post(self, *args, **kwargs):
        return self._resp


def fake_client_session_factory(resp):
    class _Factory:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return FakeSession(resp)

        async def __aexit__(self, exc_type, exc, tb):
            return False

    return _Factory


# === _parse_json_for_text tests ===


@pytest.mark.asyncio
async def test_parse_json_none_returns_empty():
    assert await _parse_json_for_text(None) == ""


@pytest.mark.asyncio
async def test_parse_json_string_returns_as_is():
    assert await _parse_json_for_text("hello world") == "hello world"


@pytest.mark.asyncio
async def test_parse_json_openai_choices_with_message():
    data = {"choices": [{"message": {"content": "OpenAI reply"}}]}
    assert await _parse_json_for_text(data) == "OpenAI reply"


@pytest.mark.asyncio
async def test_parse_json_openai_choices_with_text():
    data = {"choices": [{"text": "OpenAI text"}]}
    assert await _parse_json_for_text(data) == "OpenAI text"


@pytest.mark.asyncio
async def test_parse_json_cohere_generations():
    data = {"generations": [{"text": "Cohere response"}]}
    assert await _parse_json_for_text(data) == "Cohere response"


@pytest.mark.asyncio
async def test_parse_json_huggingface_generated_text():
    data = {"generated_text": "HF output"}
    assert await _parse_json_for_text(data) == "HF output"


@pytest.mark.asyncio
async def test_parse_json_outputs_with_content():
    data = {"outputs": [{"content": "Output content"}]}
    assert await _parse_json_for_text(data) == "Output content"


@pytest.mark.asyncio
async def test_parse_json_outputs_with_text():
    data = {"outputs": [{"text": "Output text"}]}
    assert await _parse_json_for_text(data) == "Output text"


@pytest.mark.asyncio
async def test_parse_json_outputs_string():
    data = {"outputs": ["string output"]}
    assert "string output" in await _parse_json_for_text(data)


@pytest.mark.asyncio
async def test_parse_json_result_string():
    data = {"result": "Result string"}
    assert await _parse_json_for_text(data) == "Result string"


@pytest.mark.asyncio
async def test_parse_json_result_dict_with_text():
    data = {"result": {"text": "Result text"}}
    assert await _parse_json_for_text(data) == "Result text"


@pytest.mark.asyncio
async def test_parse_json_result_dict_with_content():
    data = {"result": {"content": "Result content"}}
    assert await _parse_json_for_text(data) == "Result content"


@pytest.mark.asyncio
async def test_parse_json_result_dict_with_generated_text():
    data = {"result": {"generated_text": "Generated"}}
    assert await _parse_json_for_text(data) == "Generated"


@pytest.mark.asyncio
async def test_parse_json_result_dict_fallback():
    data = {"result": {"unknown_key": "value"}}
    assert "unknown_key" in await _parse_json_for_text(data)


@pytest.mark.asyncio
async def test_parse_json_ollama_results_nested():
    data = {"results": [{"content": {"text": "Ollama nested"}}]}
    assert await _parse_json_for_text(data) == "Ollama nested"


@pytest.mark.asyncio
async def test_parse_json_ollama_results_flat():
    data = {"results": [{"text": "Ollama flat"}]}
    assert await _parse_json_for_text(data) == "Ollama flat"


@pytest.mark.asyncio
async def test_parse_json_fallback_first_string():
    data = {"key1": 123, "key2": "first string", "key3": "second"}
    assert await _parse_json_for_text(data) == "first string"


@pytest.mark.asyncio
async def test_parse_json_fallback_str():
    data = {"key1": 123, "key2": [1, 2]}
    result = await _parse_json_for_text(data)
    assert "key1" in result


@pytest.mark.asyncio
async def test_parse_json_list():
    data = [{"text": "list item"}]
    assert await _parse_json_for_text(data) == "list item"


@pytest.mark.asyncio
async def test_parse_json_unknown_type():
    result = await _parse_json_for_text(42)
    assert result == "42"


# === Cohere adapter tests ===


@pytest.mark.asyncio
async def test_cohere_adapter_parses_generations(monkeypatch):
    resp = FakeResp(
        status=200, json_data={"generations": [{"text": "Hello from Cohere"}]}, text_data="ok"
    )
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_cohere_adapter("fakekey")
    out = await adapter("my prompt")
    assert "Hello from Cohere" in out


@pytest.mark.asyncio
async def test_cohere_adapter_with_model(monkeypatch):
    resp = FakeResp(status=200, json_data={"generations": [{"text": "Model response"}]})
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_cohere_adapter("fakekey")
    out = await adapter("prompt", model="command")
    assert "Model response" in out


@pytest.mark.asyncio
async def test_cohere_adapter_error_status(monkeypatch):
    resp = FakeResp(status=500, text_data="Internal error")
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_cohere_adapter("fakekey")
    with pytest.raises(RuntimeError) as exc_info:
        await adapter("prompt")
    assert "500" in str(exc_info.value)


@pytest.mark.asyncio
async def test_cohere_adapter_custom_base_url(monkeypatch):
    resp = FakeResp(status=200, json_data={"generations": [{"text": "OK"}]})
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_cohere_adapter("fakekey", base_url="https://custom.api/endpoint")
    out = await adapter("prompt")
    assert "OK" in out


@pytest.mark.asyncio
async def test_cohere_shim():
    adapter = make_cohere("test_key")
    assert callable(adapter)


# === Ollama adapter tests ===


@pytest.mark.asyncio
async def test_ollama_adapter_parses_results(monkeypatch):
    resp = FakeResp(status=200, json_data={"results": [{"content": {"text": "Ollama says hi"}}]})
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_ollama_adapter("http://example.org", api_key=None)
    out = await adapter("p")
    assert "Ollama says hi" in out


@pytest.mark.asyncio
async def test_ollama_adapter_with_api_key(monkeypatch):
    resp = FakeResp(status=200, json_data={"results": [{"text": "Auth response"}]})
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_ollama_adapter("http://example.org", api_key="secret")
    out = await adapter("prompt")
    assert "Auth response" in out


@pytest.mark.asyncio
async def test_ollama_adapter_error_status(monkeypatch):
    resp = FakeResp(status=400, text_data="Bad request")
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_ollama_adapter("http://example.org")
    with pytest.raises(RuntimeError) as exc_info:
        await adapter("prompt")
    assert "400" in str(exc_info.value)


@pytest.mark.asyncio
async def test_ollama_shim():
    adapter = make_ollama("http://localhost:11434")
    assert callable(adapter)


# === HuggingFace adapter tests ===


@pytest.mark.asyncio
async def test_hf_adapter_parses_generated_text(monkeypatch):
    resp = FakeResp(status=200, json_data={"generated_text": "HF reply"})
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_huggingface_adapter("fakehf")
    out = await adapter("q", model="gpt-like")
    assert "HF reply" in out


@pytest.mark.asyncio
async def test_hf_adapter_without_model(monkeypatch):
    resp = FakeResp(status=200, json_data={"generated_text": "No model response"})
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_huggingface_adapter("fakehf", base_url="https://custom.hf.api")
    out = await adapter("q")
    assert "No model response" in out


@pytest.mark.asyncio
async def test_hf_adapter_error_status(monkeypatch):
    resp = FakeResp(status=403, text_data="Forbidden")
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_huggingface_adapter("fakehf")
    with pytest.raises(RuntimeError) as exc_info:
        await adapter("prompt")
    assert "403" in str(exc_info.value)


@pytest.mark.asyncio
async def test_huggingface_shim():
    adapter = make_huggingface("test_token")
    assert callable(adapter)


# === Generic HTTP adapter tests ===


@pytest.mark.asyncio
async def test_generic_http_adapter_success(monkeypatch):
    resp = FakeResp(status=200, json_data={"text": "Generic response"})
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_generic_http_adapter("https://api.example.com/generate")
    out = await adapter("test prompt")
    assert "Generic response" in out


@pytest.mark.asyncio
async def test_generic_http_adapter_with_model(monkeypatch):
    resp = FakeResp(status=200, json_data={"result": "Model output"})
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_generic_http_adapter("https://api.example.com/generate")
    out = await adapter("prompt", model="custom-model")
    assert "Model output" in out


@pytest.mark.asyncio
async def test_generic_http_adapter_with_api_key(monkeypatch):
    resp = FakeResp(status=200, json_data={"text": "Authenticated"})
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_generic_http_adapter("https://api.example.com/generate", api_key="secret")
    out = await adapter("prompt")
    assert "Authenticated" in out


@pytest.mark.asyncio
async def test_generic_http_adapter_custom_header(monkeypatch):
    resp = FakeResp(status=200, json_data={"text": "Custom header"})
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_generic_http_adapter(
        "https://api.example.com/generate",
        api_key="secret",
        api_key_header="X-API-Key",
    )
    out = await adapter("prompt")
    assert "Custom header" in out


@pytest.mark.asyncio
async def test_generic_http_adapter_error(monkeypatch):
    resp = FakeResp(status=502, text_data="Bad gateway")
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_generic_http_adapter("https://api.example.com/generate")
    with pytest.raises(RuntimeError) as exc_info:
        await adapter("prompt")
    assert "502" in str(exc_info.value)


# === Context7 adapter tests ===


@pytest.mark.asyncio
async def test_context7_adapter_success(monkeypatch):
    resp = FakeResp(status=200, json_data={"text": "Context7 response"})
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_context7_adapter("test_key")
    out = await adapter("test prompt")
    assert "Context7 response" in out


@pytest.mark.asyncio
async def test_context7_adapter_custom_base_url(monkeypatch):
    resp = FakeResp(status=200, json_data={"text": "Custom base"})
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_context7_adapter("test_key", base_url="https://custom.context7.api")
    out = await adapter("prompt")
    assert "Custom base" in out
