import pytest

from src.services.provider_adapters import (
    make_cohere_adapter,
    make_huggingface_adapter,
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
async def test_ollama_adapter_parses_results(monkeypatch):
    resp = FakeResp(status=200, json_data={"results": [{"content": {"text": "Ollama says hi"}}]})
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_ollama_adapter("http://example.org", api_key=None)
    out = await adapter("p")
    assert "Ollama says hi" in out


@pytest.mark.asyncio
async def test_hf_adapter_parses_generated_text(monkeypatch):
    resp = FakeResp(status=200, json_data={"generated_text": "HF reply"})
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_huggingface_adapter("fakehf")
    out = await adapter("q", model="gpt-like")
    assert "HF reply" in out
