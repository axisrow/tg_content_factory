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
