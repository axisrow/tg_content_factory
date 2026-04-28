import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.image_generation_service import ImageGenerationService


def _make_clean_service():
    """Create service without env-based auto-registration."""
    svc = ImageGenerationService.__new__(ImageGenerationService)
    svc._adapters = {}
    return svc


# ── search_models() huggingface ──


@pytest.mark.anyio
async def test_search_models_huggingface_with_token(monkeypatch):
    """Test HuggingFace model search with API token."""
    monkeypatch.delenv("HUGGINGFACE_API_KEY", raising=False)
    monkeypatch.delenv("HUGGINGFACE_TOKEN", raising=False)

    svc = _make_clean_service()

    # Mock model objects
    class MockModel:
        def __init__(self, model_id, downloads, description):
            self.id = model_id
            self.downloads = downloads
            self.cardData = {"description": description}

    mock_models = [
        MockModel("stabilityai/sdxl", 1000, "SDXL model"),
        MockModel("black-forest-labs/flux", 500, "FLUX model"),
    ]

    # Create mock HfApi class
    mock_hf_api = MagicMock()
    mock_hf_api.return_value.list_models.return_value = mock_models

    # Mock the huggingface_hub module
    mock_hf_module = MagicMock()
    mock_hf_module.HfApi = mock_hf_api

    with patch.dict(sys.modules, {"huggingface_hub": mock_hf_module}):
        result = await svc.search_models("huggingface", query="flux", api_key="hf_test_token")

        assert len(result) == 2
        assert result[0]["id"] == "stabilityai/sdxl"
        assert result[0]["model_string"] == "huggingface:stabilityai/sdxl"
        assert result[0]["run_count"] == 1000
        mock_hf_api.return_value.list_models.assert_called_once_with(
            filter="text-to-image",
            search="flux",
            sort="downloads",
            limit=20,
            token="hf_test_token",
        )


@pytest.mark.anyio
async def test_search_models_huggingface_no_token_returns_empty(monkeypatch):
    """Test HuggingFace search returns empty list when no token available."""
    monkeypatch.delenv("HUGGINGFACE_API_KEY", raising=False)
    monkeypatch.delenv("HUGGINGFACE_TOKEN", raising=False)

    svc = _make_clean_service()

    result = await svc.search_models("huggingface", query="flux", api_key="")
    assert result == []


@pytest.mark.anyio
async def test_search_models_huggingface_exception_returns_empty():
    """Test HuggingFace search handles exceptions gracefully."""
    svc = _make_clean_service()

    # Create mock HfApi class that raises an error
    mock_hf_api = MagicMock()
    mock_hf_api.return_value.list_models.side_effect = RuntimeError("API error")

    # Mock the huggingface_hub module
    mock_hf_module = MagicMock()
    mock_hf_module.HfApi = mock_hf_api

    with patch.dict(sys.modules, {"huggingface_hub": mock_hf_module}):
        result = await svc.search_models("huggingface", query="test", api_key="hf_token")
        assert result == []


# ── generate() ──


@pytest.mark.anyio
async def test_generate_returns_url_with_adapter():
    svc = _make_clean_service()

    async def fake_adapter(prompt: str, model: str) -> str:
        return f"https://img.example.com/{model}.png"

    svc.register_adapter("fake", fake_adapter)
    result = await svc.generate("fake:my-model", "A sunset")
    assert result == "https://img.example.com/my-model.png"


@pytest.mark.anyio
async def test_generate_returns_none_no_adapters():
    svc = _make_clean_service()
    result = await svc.generate("some-model", "A sunset")
    assert result is None


@pytest.mark.anyio
async def test_generate_empty_text_returns_none():
    svc = _make_clean_service()

    async def fake_adapter(prompt: str, model: str) -> str:
        return "url"

    svc.register_adapter("fake", fake_adapter)
    result = await svc.generate("fake:m", "")
    assert result is None


@pytest.mark.anyio
async def test_generate_catches_adapter_error():
    svc = _make_clean_service()

    async def broken_adapter(prompt: str, model: str) -> str:
        raise RuntimeError("boom")

    svc.register_adapter("broken", broken_adapter)
    result = await svc.generate("broken:m", "test prompt")
    assert result is None


# ── is_available() ──


@pytest.mark.anyio
async def test_is_available_false_when_empty():
    svc = _make_clean_service()
    assert await svc.is_available() is False


@pytest.mark.anyio
async def test_is_available_true_with_adapter():
    svc = _make_clean_service()

    async def noop(prompt: str, model: str) -> str:
        return ""

    svc.register_adapter("x", noop)
    assert await svc.is_available() is True


# ── _parse_model_string() ──


def test_parse_model_string_with_prefix():
    provider, model_id = ImageGenerationService._parse_model_string("together:flux-schnell")
    assert provider == "together"
    assert model_id == "flux-schnell"


def test_parse_model_string_without_prefix():
    provider, model_id = ImageGenerationService._parse_model_string("flux-schnell")
    assert provider is None
    assert model_id == "flux-schnell"


def test_parse_model_string_none():
    provider, model_id = ImageGenerationService._parse_model_string(None)
    assert provider is None
    assert model_id == ""


def test_parse_model_string_with_slashes():
    provider, model_id = ImageGenerationService._parse_model_string(
        "together:black-forest-labs/FLUX.1-schnell"
    )
    assert provider == "together"
    assert model_id == "black-forest-labs/FLUX.1-schnell"


# ── fallback ──


@pytest.mark.anyio
async def test_fallback_to_first_adapter():
    svc = _make_clean_service()
    calls = []

    async def first_adapter(prompt: str, model: str) -> str:
        calls.append(("first", prompt, model))
        return "first-url"

    async def second_adapter(prompt: str, model: str) -> str:
        calls.append(("second", prompt, model))
        return "second-url"

    svc.register_adapter("alpha", first_adapter)
    svc.register_adapter("beta", second_adapter)

    result = await svc.generate("no-prefix-model", "prompt")
    assert result == "first-url"
    assert calls[0][0] == "first"


# ── adapter_names ──


def test_adapter_names():
    svc = _make_clean_service()

    async def noop(prompt: str, model: str) -> str:
        return ""

    svc.register_adapter("a", noop)
    svc.register_adapter("b", noop)
    assert svc.adapter_names == ["a", "b"]


# ── constructor with adapters param ──


@pytest.mark.anyio
async def test_init_with_prebuilt_adapters():
    async def fake(prompt: str, model: str) -> str:
        return "prebuilt-url"

    svc = ImageGenerationService(adapters={"test": fake})
    assert svc.adapter_names == ["test"]
    result = await svc.generate("test:m", "prompt")
    assert result == "prebuilt-url"


def test_init_with_empty_adapters():
    svc = ImageGenerationService(adapters={})
    assert svc.adapter_names == []


def test_init_with_none_calls_register_from_env(monkeypatch):
    monkeypatch.delenv("TOGETHER_API_KEY", raising=False)
    monkeypatch.delenv("HUGGINGFACE_API_KEY", raising=False)
    monkeypatch.delenv("HUGGINGFACE_TOKEN", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("REPLICATE_API_TOKEN", raising=False)
    svc = ImageGenerationService(adapters=None)
    assert svc.adapter_names == []  # no env vars set


# ── _init_s3 tests ──


def test_init_s3_logs_when_configured(monkeypatch, caplog):
    """S3 storage is initialized when env vars are present."""
    import logging

    class FakeS3Store:
        @classmethod
        def from_env(cls):
            return cls()

    monkeypatch.setattr("src.services.s3_store.S3Store", FakeS3Store)
    monkeypatch.delenv("TOGETHER_API_KEY", raising=False)
    monkeypatch.delenv("HUGGINGFACE_API_KEY", raising=False)
    monkeypatch.delenv("HUGGINGFACE_TOKEN", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("REPLICATE_API_TOKEN", raising=False)

    with caplog.at_level(logging.INFO):
        ImageGenerationService(adapters=None)
    assert any("S3 image storage configured" in r.message for r in caplog.records)


def test_init_s3_no_log_when_not_configured(monkeypatch, caplog):
    """S3 storage logs nothing when env vars are absent."""
    import logging

    class FakeS3StoreNone:
        @classmethod
        def from_env(cls):
            return None

    monkeypatch.setattr("src.services.s3_store.S3Store", FakeS3StoreNone)
    monkeypatch.delenv("TOGETHER_API_KEY", raising=False)
    monkeypatch.delenv("HUGGINGFACE_API_KEY", raising=False)
    monkeypatch.delenv("HUGGINGFACE_TOKEN", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("REPLICATE_API_TOKEN", raising=False)

    with caplog.at_level(logging.INFO):
        ImageGenerationService(adapters=None)
    assert not any("S3 image storage" in r.message for r in caplog.records)


# ── _resolve_adapter tests ──


def test_resolve_adapter_returns_none_when_no_adapters():
    svc = _make_clean_service()
    assert svc._resolve_adapter("anything") is None
    assert svc._resolve_adapter(None) is None


# ── generate() S3 upload ──


@pytest.mark.anyio
async def test_generate_uploads_to_s3_when_local_path():
    """When adapter returns a non-http path and S3 is configured, uploads to S3."""
    svc = _make_clean_service()

    async def local_adapter(prompt: str, model: str) -> str:
        return "/tmp/image.png"

    svc.register_adapter("local", local_adapter)

    # Mock S3 store - need to set it before _s3 check
    svc._s3 = MagicMock()
    svc._s3.upload_file = AsyncMock(return_value="https://s3.example.com/image.png")

    result = await svc.generate("local:m", "a cat")
    assert result == "https://s3.example.com/image.png"
    svc._s3.upload_file.assert_called_once_with("/tmp/image.png")


@pytest.mark.anyio
async def test_generate_skips_s3_when_adapter_returns_url():
    """When adapter returns a URL, S3 upload is skipped."""
    svc = _make_clean_service()

    async def url_adapter(prompt: str, model: str) -> str:
        return "https://already.a.url/image.png"

    svc.register_adapter("url", url_adapter)

    svc._s3 = MagicMock()
    svc._s3.upload_file = AsyncMock(return_value="https://s3.example.com/image.png")

    result = await svc.generate("url:m", "a cat")
    assert result == "https://already.a.url/image.png"
    svc._s3.upload_file.assert_not_called()


@pytest.mark.anyio
async def test_generate_s3_upload_fails_returns_local_path():
    """When S3 upload fails, returns local path as fallback."""
    svc = _make_clean_service()

    async def local_adapter(prompt: str, model: str) -> str:
        return "/tmp/local.png"

    svc.register_adapter("local", local_adapter)

    svc._s3 = MagicMock()
    svc._s3.upload_file = AsyncMock(return_value=None)  # S3 upload returns None

    result = await svc.generate("local:m", "a cat")
    assert result == "/tmp/local.png"


# ── generate() OSError and TimeoutError ──


@pytest.mark.anyio
async def test_generate_catches_os_error():
    svc = _make_clean_service()

    async def oserr_adapter(prompt: str, model: str) -> str:
        raise OSError("connection refused")

    svc.register_adapter("oserr", oserr_adapter)
    result = await svc.generate("oserr:m", "test")
    assert result is None


@pytest.mark.anyio
async def test_generate_catches_timeout_error():
    svc = _make_clean_service()

    async def timeout_adapter(prompt: str, model: str) -> str:
        raise asyncio.TimeoutError("timed out")

    svc.register_adapter("timeout", timeout_adapter)
    result = await svc.generate("timeout:m", "test")
    assert result is None


# ── generate() no adapter for provider ──


@pytest.mark.anyio
async def test_generate_no_adapter_for_named_provider():
    """When no adapters are registered at all, returns None."""
    svc = _make_clean_service()
    # No adapters at all — _resolve_adapter returns None
    result = await svc.generate("beta:m", "test")
    assert result is None


# ── _register_from_env with env vars ──


def test_register_from_env_together(monkeypatch):
    monkeypatch.setenv("TOGETHER_API_KEY", "test-key")
    monkeypatch.delenv("HUGGINGFACE_API_KEY", raising=False)
    monkeypatch.delenv("HUGGINGFACE_TOKEN", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("REPLICATE_API_TOKEN", raising=False)
    svc = ImageGenerationService(adapters=None)
    assert "together" in svc.adapter_names


def test_register_from_env_huggingface(monkeypatch):
    monkeypatch.delenv("TOGETHER_API_KEY", raising=False)
    monkeypatch.setenv("HUGGINGFACE_API_KEY", "test-key")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("REPLICATE_API_TOKEN", raising=False)
    svc = ImageGenerationService(adapters=None)
    assert "huggingface" in svc.adapter_names


def test_register_from_env_openai(monkeypatch):
    monkeypatch.delenv("TOGETHER_API_KEY", raising=False)
    monkeypatch.delenv("HUGGINGFACE_API_KEY", raising=False)
    monkeypatch.delenv("HUGGINGFACE_TOKEN", raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.delenv("REPLICATE_API_TOKEN", raising=False)
    svc = ImageGenerationService(adapters=None)
    assert "openai" in svc.adapter_names


def test_register_from_env_replicate(monkeypatch):
    monkeypatch.delenv("TOGETHER_API_KEY", raising=False)
    monkeypatch.delenv("HUGGINGFACE_API_KEY", raising=False)
    monkeypatch.delenv("HUGGINGFACE_TOKEN", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("REPLICATE_API_TOKEN", "test-token")
    svc = ImageGenerationService(adapters=None)
    assert "replicate" in svc.adapter_names


# ── search_models static catalogs ──


@pytest.mark.anyio
async def test_search_models_together_catalog():
    svc = _make_clean_service()
    models = await svc.search_models("together", query="flux")
    assert len(models) == 2
    assert all("together:" in m["model_string"] for m in models)


@pytest.mark.anyio
async def test_search_models_openai_catalog():
    svc = _make_clean_service()
    models = await svc.search_models("openai")
    assert len(models) == 2
    assert any("dall-e-3" in m["id"] for m in models)


@pytest.mark.anyio
async def test_search_models_unknown_provider():
    svc = _make_clean_service()
    models = await svc.search_models("unknown_provider")
    assert models == []


@pytest.mark.anyio
async def test_search_models_with_query_filter():
    svc = _make_clean_service()
    models = await svc.search_models("together", query="dev")
    assert len(models) == 1
    assert "dev" in models[0]["id"]


# ── search_models Replicate (mocked HTTP) ──


@pytest.mark.anyio
async def test_search_models_replicate_no_token(monkeypatch):
    monkeypatch.delenv("REPLICATE_API_TOKEN", raising=False)
    svc = _make_clean_service()
    models = await svc.search_models("replicate", api_key="")
    assert models == []


@pytest.mark.anyio
async def test_search_models_replicate_with_token(monkeypatch):
    class FakeAiohttpSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        def get(self, *args, **kwargs):
            return self._FakeResp(
                200,
                {"results": [
                    {"owner": "testowner", "name": "test-model", "description": "desc", "run_count": 5},
                ]},
            )

        class _FakeResp:
            def __init__(self, status, json_data):
                self.status = status
                self._json = json_data

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def json(self):
                return self._json

    monkeypatch.setattr("aiohttp.ClientSession", FakeAiohttpSession)
    svc = _make_clean_service()
    models = await svc.search_models("replicate", api_key="fake-token")
    assert len(models) == 1
    assert models[0]["id"] == "testowner/test-model"
