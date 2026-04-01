import sys
from unittest.mock import MagicMock, patch

import pytest

from src.services.image_generation_service import ImageGenerationService


def _make_clean_service():
    """Create service without env-based auto-registration."""
    svc = ImageGenerationService.__new__(ImageGenerationService)
    svc._adapters = {}
    return svc


# ── search_models() huggingface ──


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_search_models_huggingface_no_token_returns_empty():
    """Test HuggingFace search returns empty list when no token available."""
    svc = _make_clean_service()

    result = await svc.search_models("huggingface", query="flux", api_key="")
    assert result == []


@pytest.mark.asyncio
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
    """Create service without env-based auto-registration."""
    svc = ImageGenerationService.__new__(ImageGenerationService)
    svc._adapters = {}
    return svc


# ── generate() ──


@pytest.mark.asyncio
async def test_generate_returns_url_with_adapter():
    svc = _make_clean_service()

    async def fake_adapter(prompt: str, model: str) -> str:
        return f"https://img.example.com/{model}.png"

    svc.register_adapter("fake", fake_adapter)
    result = await svc.generate("fake:my-model", "A sunset")
    assert result == "https://img.example.com/my-model.png"


@pytest.mark.asyncio
async def test_generate_returns_none_no_adapters():
    svc = _make_clean_service()
    result = await svc.generate("some-model", "A sunset")
    assert result is None


@pytest.mark.asyncio
async def test_generate_empty_text_returns_none():
    svc = _make_clean_service()

    async def fake_adapter(prompt: str, model: str) -> str:
        return "url"

    svc.register_adapter("fake", fake_adapter)
    result = await svc.generate("fake:m", "")
    assert result is None


@pytest.mark.asyncio
async def test_generate_catches_adapter_error():
    svc = _make_clean_service()

    async def broken_adapter(prompt: str, model: str) -> str:
        raise RuntimeError("boom")

    svc.register_adapter("broken", broken_adapter)
    result = await svc.generate("broken:m", "test prompt")
    assert result is None


# ── is_available() ──


@pytest.mark.asyncio
async def test_is_available_false_when_empty():
    svc = _make_clean_service()
    assert await svc.is_available() is False


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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
