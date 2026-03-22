import pytest

from src.services.image_generation_service import ImageGenerationService


def _make_clean_service():
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
