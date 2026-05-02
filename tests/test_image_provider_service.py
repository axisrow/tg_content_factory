"""Tests for ImageProviderService — DB-backed image provider management."""

import logging

import pytest

from src.config import AppConfig
from src.services.image_provider_service import (
    ImageProviderConfig,
    ImageProviderService,
)


def _make_service(db, *, encryption_key: str = "test-secret") -> ImageProviderService:
    config = AppConfig()
    config.security.session_encryption_key = encryption_key
    return ImageProviderService(db, config)


def _make_readonly_service(db) -> ImageProviderService:
    config = AppConfig()
    config.security.session_encryption_key = ""
    return ImageProviderService(db, config)


# ── load / save ──


@pytest.mark.anyio
async def test_load_empty(db):
    svc = _make_service(db)
    configs = await svc.load_provider_configs()
    assert configs == []


@pytest.mark.anyio
async def test_save_and_load_roundtrip(db):
    svc = _make_service(db)
    cfg = ImageProviderConfig(provider="together", enabled=True, api_key="sk-test-key")
    await svc.save_provider_configs([cfg])
    loaded = await svc.load_provider_configs()
    assert len(loaded) == 1
    assert loaded[0].provider == "together"
    assert loaded[0].enabled is True
    assert loaded[0].api_key == "sk-test-key"


@pytest.mark.anyio
async def test_save_multiple_providers(db):
    svc = _make_service(db)
    configs = [
        ImageProviderConfig(provider="together", enabled=True, api_key="key-1"),
        ImageProviderConfig(provider="openai", enabled=False, api_key="key-2"),
    ]
    await svc.save_provider_configs(configs)
    loaded = await svc.load_provider_configs()
    assert len(loaded) == 2
    providers = {c.provider for c in loaded}
    assert providers == {"together", "openai"}


@pytest.mark.anyio
async def test_save_without_key_omits_encrypted_field(db):
    svc = _make_service(db)
    cfg = ImageProviderConfig(provider="replicate", enabled=True, api_key="")
    await svc.save_provider_configs([cfg])
    loaded = await svc.load_provider_configs()
    assert len(loaded) == 1
    assert loaded[0].api_key == ""


@pytest.mark.anyio
async def test_save_preserves_encrypted_on_decrypt_failure(db, caplog):
    """When api_key is empty but _api_key_enc_preserved has a value, it's written to DB."""
    svc = _make_service(db)
    # First save a real key
    cfg = ImageProviderConfig(provider="together", enabled=True, api_key="real-key")
    await svc.save_provider_configs([cfg])

    # Load with wrong key (simulates decrypt failure)
    svc2 = _make_service(db, encryption_key="wrong-key")
    with caplog.at_level(logging.DEBUG, logger="src.services.image_provider_service"):
        loaded = await svc2.load_provider_configs()
    assert len(loaded) == 1
    assert loaded[0].api_key == ""  # decrypt failed
    assert loaded[0]._api_key_enc_preserved != ""  # raw preserved
    assert loaded[0].secret_status == "decrypt_failed"
    assert any(
        record.levelno == logging.DEBUG
        and "decrypt failed: resource=image_provider identifier=together status=key_mismatch" in record.message
        for record in caplog.records
    )
    assert not any(record.levelno >= logging.ERROR for record in caplog.records)

    # Save back — should keep the preserved encrypted value
    await _make_service(db).save_provider_configs(loaded)

    # Load with correct key — key is still there
    svc3 = _make_service(db)
    reloaded = await svc3.load_provider_configs()
    assert len(reloaded) == 1
    assert reloaded[0].api_key == "real-key"


@pytest.mark.anyio
async def test_writes_disabled_without_cipher(db):
    svc = _make_readonly_service(db)
    assert svc.writes_enabled is False
    with pytest.raises(RuntimeError, match="SESSION_ENCRYPTION_KEY"):
        await svc.save_provider_configs([])


@pytest.mark.anyio
async def test_load_ignores_unknown_providers(db):
    svc = _make_service(db)
    cfg = ImageProviderConfig(provider="together", enabled=True, api_key="key")
    await svc.save_provider_configs([cfg])
    # Corrupt: add unknown provider
    import json

    raw = await db.get_setting("image_providers_v1")
    data = json.loads(raw)
    data.append({"provider": "nonexistent", "enabled": True})
    await db.set_setting("image_providers_v1", json.dumps(data))
    loaded = await svc.load_provider_configs()
    assert len(loaded) == 1
    assert loaded[0].provider == "together"


# ── parse_provider_form ──


@pytest.mark.anyio
async def test_parse_provider_form_new_key(db):
    svc = _make_service(db)
    form = {
        "img_provider_present__together": "1",
        "img_provider_enabled__together": "1",
        "img_provider_secret__together__api_key": "new-key",
    }
    configs = svc.parse_provider_form(form, [])
    assert len(configs) == 1
    assert configs[0].api_key == "new-key"
    assert configs[0].enabled is True


@pytest.mark.anyio
async def test_parse_provider_form_keep_old_key(db):
    svc = _make_service(db)
    existing = [ImageProviderConfig(provider="together", enabled=True, api_key="old-key")]
    form = {
        "img_provider_present__together": "1",
        "img_provider_enabled__together": "1",
        "img_provider_secret__together__api_key": "",
    }
    configs = svc.parse_provider_form(form, existing)
    assert configs[0].api_key == "old-key"


@pytest.mark.anyio
async def test_parse_provider_form_preserves_enc(db):
    svc = _make_service(db)
    existing = [
        ImageProviderConfig(
            provider="together", enabled=True, api_key="", _api_key_enc_preserved="enc-blob"
        )
    ]
    form = {
        "img_provider_present__together": "1",
        "img_provider_enabled__together": "1",
        "img_provider_secret__together__api_key": "",
    }
    configs = svc.parse_provider_form(form, existing)
    assert configs[0]._api_key_enc_preserved == "enc-blob"


# ── build_provider_views ──


def test_build_provider_views():
    svc_config = AppConfig()
    svc_config.security.session_encryption_key = "x"
    configs = [
        ImageProviderConfig(provider="together", enabled=True, api_key="secret"),
        ImageProviderConfig(provider="openai", enabled=False, api_key=""),
    ]
    # build_provider_views doesn't need db, just the service instance
    svc = ImageProviderService.__new__(ImageProviderService)
    views = svc.build_provider_views(configs)
    assert len(views) == 2
    assert views[0]["provider"] == "together"
    assert views[0]["has_key"] is True
    assert views[0]["display_name"] == "Together AI"
    assert views[1]["has_key"] is False
    assert views[1]["enabled"] is False


# ── build_adapters ──


@pytest.mark.anyio
async def test_build_adapters_from_db(db):
    svc = _make_service(db)
    configs = [ImageProviderConfig(provider="together", enabled=True, api_key="test-key")]
    adapters = svc.build_adapters(configs)
    assert "together" in adapters
    assert callable(adapters["together"])


@pytest.mark.anyio
async def test_build_adapters_disabled_provider_skipped(db):
    svc = _make_service(db)
    configs = [ImageProviderConfig(provider="together", enabled=False, api_key="test-key")]
    adapters = svc.build_adapters(configs)
    assert "together" not in adapters


@pytest.mark.anyio
async def test_build_adapters_env_fallback(db, monkeypatch):
    svc = _make_service(db)
    monkeypatch.setenv("REPLICATE_API_TOKEN", "env-token")
    # No DB config for replicate — should fall back to env
    configs = [ImageProviderConfig(provider="together", enabled=True, api_key="key")]
    adapters = svc.build_adapters(configs)
    assert "together" in adapters
    assert "replicate" in adapters  # from env


@pytest.mark.anyio
async def test_build_adapters_disabled_blocks_env(db, monkeypatch):
    """Disabled DB config should block env-var fallback for that provider."""
    svc = _make_service(db)
    monkeypatch.setenv("TOGETHER_API_KEY", "env-key")
    configs = [ImageProviderConfig(provider="together", enabled=False, api_key="")]
    adapters = svc.build_adapters(configs)
    assert "together" not in adapters


# ── create_empty_config ──


def test_create_empty_config():
    svc = ImageProviderService.__new__(ImageProviderService)
    cfg = svc.create_empty_config("openai")
    assert cfg.provider == "openai"
    assert cfg.enabled is True
    assert cfg.api_key == ""
