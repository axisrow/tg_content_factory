"""Tests for image generation routes."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture
async def client(route_client):
    return route_client


@pytest.mark.asyncio
async def test_images_page(client, monkeypatch):
    mock_svc = MagicMock()
    mock_svc.adapter_names = ["test_provider"]
    monkeypatch.setattr(
        "src.web.routes.images._get_image_service",
        AsyncMock(return_value=mock_svc),
    )
    resp = await client.get("/images/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]


@pytest.mark.asyncio
async def test_generate_no_prompt(client, monkeypatch):
    resp = await client.post("/images/generate", data={})
    assert resp.status_code == 400
    body = resp.json()
    assert body["ok"] is False
    assert "Prompt" in body["error"]


@pytest.mark.asyncio
async def test_generate_no_providers(client, monkeypatch):
    mock_svc = MagicMock()
    mock_svc.is_available = AsyncMock(return_value=False)
    monkeypatch.setattr(
        "src.web.routes.images._get_image_service",
        AsyncMock(return_value=mock_svc),
    )
    resp = await client.post("/images/generate", data={"prompt": "a cat"})
    assert resp.status_code == 409
    body = resp.json()
    assert "No image providers" in body["error"]


@pytest.mark.asyncio
async def test_generate_success(client, monkeypatch):
    mock_svc = MagicMock()
    mock_svc.is_available = AsyncMock(return_value=True)
    mock_svc.generate = AsyncMock(return_value="https://img.example.com/1.png")
    monkeypatch.setattr(
        "src.web.routes.images._get_image_service",
        AsyncMock(return_value=mock_svc),
    )
    resp = await client.post("/images/generate", data={"prompt": "a cat", "model": "test:model"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["url"] == "https://img.example.com/1.png"


@pytest.mark.asyncio
async def test_generate_failure(client, monkeypatch):
    mock_svc = MagicMock()
    mock_svc.is_available = AsyncMock(return_value=True)
    mock_svc.generate = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.routes.images._get_image_service",
        AsyncMock(return_value=mock_svc),
    )
    resp = await client.post("/images/generate", data={"prompt": "a cat"})
    assert resp.status_code == 500
    body = resp.json()
    assert body["ok"] is False
    assert "Generation failed" in body["error"]


@pytest.mark.asyncio
async def test_search_models_no_provider(client, monkeypatch):
    resp = await client.get("/images/models/search?provider=")
    assert resp.status_code == 400
    body = resp.json()
    assert "provider" in body["error"]


@pytest.mark.asyncio
async def test_search_models_success(client, monkeypatch):
    monkeypatch.setattr(
        "src.web.routes.images._get_provider_api_key",
        AsyncMock(return_value="fake-key"),
    )
    mock_models = [{"id": "model-1", "name": "Test Model"}]
    with patch("src.web.routes.images.ImageGenerationService") as mock_cls:
        instance = MagicMock()
        instance.search_models = AsyncMock(return_value=mock_models)
        mock_cls.return_value = instance

        resp = await client.get("/images/models/search?provider=together&q=flux")
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True
        assert len(body["models"]) == 1


@pytest.mark.asyncio
async def test_search_models_no_api_key(client, monkeypatch):
    """Test search models when no API key is found."""
    monkeypatch.setattr(
        "src.web.routes.images._get_provider_api_key",
        AsyncMock(return_value=""),
    )
    resp = await client.get("/images/models/search?provider=unknown&q=test")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True


@pytest.mark.asyncio
async def test_images_page_with_db_error(client, monkeypatch):
    """Test images page when DB provider loading fails."""
    with patch("src.services.image_provider_service.ImageProviderService", side_effect=Exception("DB error")):
        resp = await client.get("/images/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_generate_with_model(client, monkeypatch):
    """Test generate with specific model selection."""
    mock_svc = MagicMock()
    mock_svc.is_available = AsyncMock(return_value=True)
    mock_svc.generate = AsyncMock(return_value="https://img.example.com/2.png")
    monkeypatch.setattr(
        "src.web.routes.images._get_image_service",
        AsyncMock(return_value=mock_svc),
    )
    resp = await client.post("/images/generate", data={"prompt": "a dog", "model": "test:model"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["model"] == "test:model"


@pytest.mark.asyncio
async def test_get_provider_api_key_from_db_config(monkeypatch):
    """API key returned from DB provider config."""
    mock_config = MagicMock(provider="together", api_key="db-key-123")
    mock_svc = MagicMock()
    mock_svc.load_provider_configs = AsyncMock(return_value=[mock_config])

    from src.web.routes.images import _get_provider_api_key

    with patch("src.services.image_provider_service.ImageProviderService", return_value=mock_svc):
        result = await _get_provider_api_key(MagicMock(), "together")
    assert result == "db-key-123"


@pytest.mark.asyncio
async def test_get_provider_api_key_env_fallback(monkeypatch):
    """Falls back to env var when DB config has no matching key."""
    mock_config = MagicMock(provider="other", api_key="other-key")
    mock_svc = MagicMock()
    mock_svc.load_provider_configs = AsyncMock(return_value=[mock_config])

    from src.services.image_provider_service import IMAGE_PROVIDER_SPECS

    first_provider = next(iter(IMAGE_PROVIDER_SPECS), None)
    if not first_provider:
        pytest.skip("No IMAGE_PROVIDER_SPECS")

    spec = IMAGE_PROVIDER_SPECS[first_provider]
    for var in spec.env_vars:
        monkeypatch.setenv(var, "env-key-456")

    from src.web.routes.images import _get_provider_api_key

    with patch("src.services.image_provider_service.ImageProviderService", return_value=mock_svc):
        result = await _get_provider_api_key(MagicMock(), first_provider)
    assert result == "env-key-456"


@pytest.mark.asyncio
async def test_get_provider_api_key_exception_returns_empty():
    """Returns empty string on any exception."""
    from src.web.routes.images import _get_provider_api_key

    with patch("src.services.image_provider_service.ImageProviderService", side_effect=Exception("boom")):
        result = await _get_provider_api_key(MagicMock(), "anything")
    assert result == ""
