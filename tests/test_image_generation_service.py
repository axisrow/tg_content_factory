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
    assert svc.last_failure is not None
    assert svc.last_failure.kind == "error"
    assert svc.last_failure.model == "broken:m"

    async def fixed_adapter(prompt: str, model: str) -> str:
        return "https://img.example.com/fixed.png"

    svc.register_adapter("broken", fixed_adapter)
    result = await svc.generate("broken:m", "test prompt")
    assert result == "https://img.example.com/fixed.png"
    assert svc.last_failure is None


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


def test_init_with_none_calls_register_from_env(monkeypatch, pin_codex):
    monkeypatch.delenv("TOGETHER_API_KEY", raising=False)
    monkeypatch.delenv("HUGGINGFACE_API_KEY", raising=False)
    monkeypatch.delenv("HUGGINGFACE_TOKEN", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("REPLICATE_API_TOKEN", raising=False)
    # codex is keyless and registers on detection; pin it off so this test stays
    # about env-keyed providers regardless of whether the SDK is installed.
    pin_codex(False)
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


def test_resolve_adapter_skips_codex_in_implicit_fallback():
    """Codex spawns a blocking subprocess, so it must never be the default adapter.

    An unqualified generate() (no provider) must fall through to a non-codex
    adapter; codex is reachable only by explicit ``codex:model`` selection.
    """

    async def _keyed(prompt: str, model: str) -> str:
        return "keyed.png"

    async def _codex(prompt: str, model: str) -> str:  # pragma: no cover - must not run
        raise AssertionError("codex must not be picked as the implicit fallback")

    svc = _make_clean_service()
    # codex first in insertion order — the old `next(iter(...))` would pick it.
    svc._adapters = {"codex": _codex, "together": _keyed}
    assert svc._resolve_adapter(None) is _keyed
    # but explicit selection still resolves codex
    assert svc._resolve_adapter("codex") is _codex


def test_resolve_adapter_none_when_only_codex():
    """With codex the sole adapter, an implicit request resolves to nothing."""

    async def _codex(prompt: str, model: str) -> str:  # pragma: no cover
        raise AssertionError("must not run")

    svc = _make_clean_service()
    svc._adapters = {"codex": _codex}
    assert svc._resolve_adapter(None) is None
    assert svc._resolve_adapter("codex") is _codex


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
    assert svc.last_failure is not None
    assert svc.last_failure.kind == "error"
    assert svc.last_failure.provider == "oserr"
    assert svc.last_failure.retryable is True


@pytest.mark.anyio
async def test_generate_catches_timeout_error():
    svc = _make_clean_service()

    async def timeout_adapter(prompt: str, model: str) -> str:
        raise asyncio.TimeoutError("timed out")

    svc.register_adapter("timeout", timeout_adapter)
    result = await svc.generate("timeout:m", "test")
    assert result is None
    assert svc.last_failure is not None
    assert svc.last_failure.kind == "timeout"
    assert svc.last_failure.provider == "timeout"
    assert svc.last_failure.model == "timeout:m"
    assert svc.last_failure.retryable is True


# ── generate() no adapter for provider ──


@pytest.mark.anyio
async def test_generate_no_adapter_for_named_provider():
    """When no adapters are registered at all, returns None."""
    svc = _make_clean_service()
    # No adapters at all — _resolve_adapter returns None
    result = await svc.generate("beta:m", "test")
    assert result is None
    assert svc.last_failure is None


@pytest.mark.anyio
async def test_generate_records_no_adapter_failure_for_explicit_only_default():
    svc = _make_clean_service()

    async def codex_adapter(prompt: str, model: str) -> str:
        return "should-not-run"

    svc.register_adapter("codex", codex_adapter)
    result = await svc.generate(None, "test")
    assert result is None
    assert svc.last_failure is not None
    assert svc.last_failure.kind == "no_adapter"
    assert svc.last_failure.provider is None
    assert svc.last_failure.model is None
    assert svc.last_failure.retryable is False


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


# ── codex provider (keyless, detection-based) ──


def test_register_codex_when_available(monkeypatch, pin_codex):
    monkeypatch.delenv("TOGETHER_API_KEY", raising=False)
    monkeypatch.delenv("HUGGINGFACE_API_KEY", raising=False)
    monkeypatch.delenv("HUGGINGFACE_TOKEN", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("REPLICATE_API_TOKEN", raising=False)
    pin_codex(True)
    svc = ImageGenerationService(adapters=None)
    assert "codex" in svc.adapter_names


def test_no_codex_when_unavailable(monkeypatch, pin_codex):
    monkeypatch.delenv("TOGETHER_API_KEY", raising=False)
    monkeypatch.delenv("HUGGINGFACE_API_KEY", raising=False)
    monkeypatch.delenv("HUGGINGFACE_TOKEN", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("REPLICATE_API_TOKEN", raising=False)
    pin_codex(False)
    svc = ImageGenerationService(adapters=None)
    assert "codex" not in svc.adapter_names


def test_codex_available_false_without_sdk(monkeypatch):
    """The detection predicate returns False when the SDK is not importable."""
    import importlib.util

    from src.services import provider_adapters

    real_find_spec = importlib.util.find_spec

    def fake_find_spec(name, *a, **k):
        if name == "openai_codex":
            return None
        return real_find_spec(name, *a, **k)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)
    provider_adapters.codex_available.cache_clear()
    assert provider_adapters.codex_available() is False
    provider_adapters.codex_available.cache_clear()


def test_codex_available_true_with_writable_codex_home(monkeypatch, tmp_path):
    """A usable Codex home needs SDK, auth, and writable runtime state."""
    from src.services import provider_adapters

    codex_home = tmp_path / "codex-home"
    codex_home.mkdir()
    (codex_home / "auth.json").write_text("{}", encoding="utf-8")
    (codex_home / "state_5.sqlite").write_bytes(b"")

    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    monkeypatch.setattr(provider_adapters, "_codex_sdk_installed", lambda: True)
    provider_adapters.codex_available.cache_clear()
    try:
        assert provider_adapters.codex_available() is True
    finally:
        provider_adapters.codex_available.cache_clear()


def test_codex_available_false_without_auth_in_codex_home(monkeypatch, tmp_path):
    """CODEX_HOME is authoritative; missing auth there means unavailable."""
    from src.services import provider_adapters

    codex_home = tmp_path / "codex-home"
    codex_home.mkdir()

    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    monkeypatch.setattr(provider_adapters, "_codex_sdk_installed", lambda: True)
    provider_adapters.codex_available.cache_clear()
    try:
        assert provider_adapters.codex_available() is False
    finally:
        provider_adapters.codex_available.cache_clear()


def test_codex_available_false_when_codex_home_not_writable(monkeypatch, tmp_path):
    """Sandboxed readonly Codex homes should not register the provider."""
    from src.services import provider_adapters

    codex_home = tmp_path / "codex-home"
    codex_home.mkdir()
    (codex_home / "auth.json").write_text("{}", encoding="utf-8")

    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    monkeypatch.setattr(provider_adapters, "_codex_sdk_installed", lambda: True)
    monkeypatch.setattr(provider_adapters, "_codex_path_writable", lambda path: path != codex_home)
    provider_adapters.codex_available.cache_clear()
    try:
        assert provider_adapters.codex_available() is False
    finally:
        provider_adapters.codex_available.cache_clear()


def test_codex_available_false_when_state_db_not_writable(monkeypatch, tmp_path):
    """Existing Codex state DB files must be writable before live SDK use."""
    from src.services import provider_adapters

    codex_home = tmp_path / "codex-home"
    codex_home.mkdir()
    state_db = codex_home / "state_5.sqlite"
    (codex_home / "auth.json").write_text("{}", encoding="utf-8")
    state_db.write_bytes(b"")

    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    monkeypatch.setattr(provider_adapters, "_codex_sdk_installed", lambda: True)
    monkeypatch.setattr(provider_adapters, "_codex_path_writable", lambda path: path != state_db)
    provider_adapters.codex_available.cache_clear()
    try:
        assert provider_adapters.codex_available() is False
    finally:
        provider_adapters.codex_available.cache_clear()


@pytest.mark.anyio
async def test_search_models_codex_static_catalog():
    svc = _make_clean_service()
    models = await svc.search_models("codex")
    ids = [m["id"] for m in models]
    assert "gpt-5.4" in ids
    assert all(m["model_string"].startswith("codex:") for m in models)


@pytest.mark.anyio
async def test_search_models_codex_refresh_uses_sdk(monkeypatch):
    svc = _make_clean_service()

    async def _fake_fetch():
        return [
            {"id": "gpt-5.5", "model_string": "codex:gpt-5.5", "description": "x", "run_count": 0},
            {"id": "gpt-5.4", "model_string": "codex:gpt-5.4", "description": "y", "run_count": 0},
        ]

    monkeypatch.setattr(svc, "_fetch_codex_models", staticmethod(_fake_fetch))
    models = await svc.search_models("codex", refresh=True)
    ids = [m["id"] for m in models]
    assert ids == ["gpt-5.5", "gpt-5.4"]


@pytest.mark.anyio
async def test_search_models_codex_refresh_falls_back_to_static(monkeypatch):
    """When the SDK listing is empty/unavailable, refresh falls back to the static catalog."""
    svc = _make_clean_service()

    async def _empty():
        return []

    monkeypatch.setattr(svc, "_fetch_codex_models", staticmethod(_empty))
    models = await svc.search_models("codex", refresh=True)
    assert any(m["id"] == "gpt-5.4" for m in models)


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
    ids = [m["id"] for m in models]
    # gpt-image-1 is the current default and listed first; legacy dall-e-* still present.
    # Unconfirmed variants (gpt-image-1-mini/1.5) are intentionally NOT in the static
    # fallback — they only surface via the live refresh path.
    assert ids[0] == "gpt-image-1"
    assert "dall-e-3" in ids  # legacy kept for backward compatibility
    assert "gpt-image-1-mini" not in ids


@pytest.mark.anyio
async def test_search_models_openai_refresh_uses_live_list(monkeypatch):
    svc = _make_clean_service()

    async def _fake_fetch(api_key):
        assert api_key == "sk-test"
        return [
            {"id": "gpt-image-1", "model_string": "openai:gpt-image-1", "description": "x", "run_count": 0},
            {"id": "gpt-image-9", "model_string": "openai:gpt-image-9", "description": "x", "run_count": 0},
        ]

    monkeypatch.setattr(svc, "_fetch_openai_image_models", staticmethod(_fake_fetch))
    models = await svc.search_models("openai", api_key="sk-test", refresh=True)
    ids = [m["id"] for m in models]
    assert ids == ["gpt-image-1", "gpt-image-9"]


@pytest.mark.anyio
async def test_search_models_openai_refresh_falls_back_on_empty(monkeypatch):
    svc = _make_clean_service()

    async def _fake_fetch(api_key):
        return []  # fetch failure / no image models

    monkeypatch.setattr(svc, "_fetch_openai_image_models", staticmethod(_fake_fetch))
    models = await svc.search_models("openai", api_key="sk-test", refresh=True)
    # falls through to static catalog
    assert any(m["id"] == "gpt-image-1" for m in models)
    assert any(m["id"] == "dall-e-3" for m in models)


@pytest.mark.anyio
async def test_fetch_openai_image_models_filters_image_only(monkeypatch):
    """The image fetch helper keeps only gpt-image*/dall-e* ids from /v1/models."""
    from src.services import provider_model_cache

    async def _fake_ids(base_url, api_key):
        return ["gpt-4o", "gpt-image-1", "dall-e-3", "text-embedding-3-small", "gpt-image-1-mini"]

    monkeypatch.setattr(provider_model_cache, "fetch_openai_model_ids", _fake_ids)
    models = await ImageGenerationService._fetch_openai_image_models("sk-test")
    ids = [m["id"] for m in models]
    assert ids == ["dall-e-3", "gpt-image-1", "gpt-image-1-mini"]  # sorted, image-only


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


# === audit #835/11 explicit-provider routing + #836/4 S3 mirror ===


@pytest.mark.anyio
async def test_explicit_unknown_provider_does_not_fall_back():
    """An explicit provider must fail cleanly, not route to another adapter (#835/11)."""
    svc = _make_clean_service()

    async def together_adapter(prompt: str, model: str) -> str:
        return "https://together/img.png"

    svc.register_adapter("together", together_adapter)
    result = await svc.generate("openai:dall-e-3", "x")
    assert result is None


@pytest.mark.anyio
async def test_http_result_mirrored_to_s3():
    """Ephemeral provider URLs (Replicate) are mirrored into durable S3 (#836/4)."""
    svc = _make_clean_service()

    async def replicate_adapter(prompt: str, model: str) -> str:
        return "https://replicate.delivery/x.png"

    svc.register_adapter("replicate", replicate_adapter)
    s3 = MagicMock()
    s3.owns_url = MagicMock(return_value=False)
    s3.upload_url = AsyncMock(return_value="https://s3.example.com/presigned")
    svc._s3 = s3

    result = await svc.generate("replicate:m", "x")
    assert result == "https://s3.example.com/presigned"
    s3.upload_url.assert_awaited_once()


@pytest.mark.anyio
async def test_local_path_uploaded_via_upload_file():
    svc = _make_clean_service()

    async def local_adapter(prompt: str, model: str) -> str:
        return "/tmp/generated.png"

    svc.register_adapter("hf", local_adapter)
    s3 = MagicMock()
    s3.upload_file = AsyncMock(return_value="https://s3.example.com/presigned")
    svc._s3 = s3

    result = await svc.generate("hf:m", "x")
    assert result == "https://s3.example.com/presigned"
    s3.upload_file.assert_awaited_once()
