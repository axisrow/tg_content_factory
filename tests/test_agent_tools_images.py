"""Tests for agent tools: images.py MCP tools."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.agent_tools_helpers import _get_tool_handlers, _text


class TestImagesToolGenerateImage:
    @pytest.mark.anyio
    async def test_missing_prompt(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["generate_image"]({"prompt": ""})
        assert "prompt обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_not_available(self, mock_db):
        with patch("src.services.image_generation_service.ImageGenerationService") as mock_svc:
            mock_svc.return_value.is_available = AsyncMock(return_value=False)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["generate_image"]({"prompt": "a cat"})
        assert "не настроена" in _text(result)

    @pytest.mark.anyio
    async def test_local_path_result(self, mock_db):
        with patch("src.services.image_generation_service.ImageGenerationService") as mock_svc:
            mock_svc.return_value.is_available = AsyncMock(return_value=True)
            mock_svc.return_value.generate = AsyncMock(return_value="/local/path/image.png")
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["generate_image"]({"prompt": "a dog"})
        text = _text(result)
        assert "/local/path/image.png" in text

    @pytest.mark.anyio
    async def test_no_result(self, mock_db):
        with patch("src.services.image_generation_service.ImageGenerationService") as mock_svc:
            mock_svc.return_value.is_available = AsyncMock(return_value=True)
            mock_svc.return_value.generate = AsyncMock(return_value=None)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["generate_image"]({"prompt": "something"})
        assert "не вернула результат" in _text(result)

    @pytest.mark.anyio
    async def test_error_returns_text(self, mock_db):
        with patch("src.services.image_generation_service.ImageGenerationService") as mock_svc:
            mock_svc.return_value.is_available = AsyncMock(side_effect=Exception("provider down"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["generate_image"]({"prompt": "test"})
        assert "Ошибка" in _text(result)


class TestImagesToolListImageModels:
    @pytest.mark.anyio
    async def test_missing_provider(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_image_models"]({"provider": ""})
        assert "provider обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_empty_models(self, mock_db):
        with patch("src.services.image_generation_service.ImageGenerationService") as mock_svc:
            mock_svc.return_value.search_models = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["list_image_models"]({"provider": "together"})
        assert "не найдены" in _text(result)

    @pytest.mark.anyio
    async def test_with_models(self, mock_db):
        models = [
            {"id": "flux-schnell", "run_count": 10000, "rank": 1},
            {"id": "flux-dev", "run_count": 5000},
        ]
        with patch("src.services.image_generation_service.ImageGenerationService") as mock_svc:
            mock_svc.return_value.search_models = AsyncMock(return_value=models)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["list_image_models"]({"provider": "together", "query": "flux"})
        text = _text(result)
        assert "flux-schnell" in text
        assert "10,000 runs" in text
        assert "rank 1" in text


class TestImagesToolListImageProviders:
    @pytest.mark.anyio
    async def test_no_providers(self, mock_db):
        with patch("src.services.image_generation_service.ImageGenerationService") as mock_svc:
            mock_svc.return_value.adapter_names = []
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["list_image_providers"]({})
        assert "не настроены" in _text(result)

    @pytest.mark.anyio
    async def test_with_providers(self, mock_db):
        with patch("src.services.image_generation_service.ImageGenerationService") as mock_svc:
            mock_svc.return_value.adapter_names = ["together", "hf", "replicate"]
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["list_image_providers"]({})
        text = _text(result)
        assert "together" in text
        assert "hf" in text
        assert "replicate" in text
        assert "Провайдеры изображений (3)" in text

    @pytest.mark.anyio
    async def test_error(self, mock_db):
        with patch("src.services.image_generation_service.ImageGenerationService") as mock_svc:
            mock_svc.side_effect = Exception("provider fail")
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["list_image_providers"]({})
        assert "Ошибка" in _text(result)


class TestImagesToolListGeneratedImages:
    @pytest.mark.anyio
    async def test_empty(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.generated_images.list_recent = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_generated_images"]({})
        assert "Нет сгенерированных" in _text(result)

    @pytest.mark.anyio
    async def test_with_images(self, mock_db):
        img = SimpleNamespace(
            id=1,
            prompt="a beautiful cat",
            model="together:flux",
            local_path="/data/img/abc.png",
            created_at="2026-01-01",
        )
        mock_db.repos = MagicMock()
        mock_db.repos.generated_images.list_recent = AsyncMock(return_value=[img])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_generated_images"]({"limit": 5})
        text = _text(result)
        assert "a beautiful cat" in text
        assert "together:flux" in text
        assert "/data/img/abc.png" in text

    @pytest.mark.anyio
    async def test_long_prompt_truncated(self, mock_db):
        long_prompt = "x" * 100
        img = SimpleNamespace(
            id=2,
            prompt=long_prompt,
            model=None,
            local_path=None,
            created_at="2026-01-01",
        )
        mock_db.repos = MagicMock()
        mock_db.repos.generated_images.list_recent = AsyncMock(return_value=[img])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_generated_images"]({})
        text = _text(result)
        assert "..." in text
        # truncated to 60 chars + "..."
        assert "x" * 60 in text
        assert "x" * 61 not in text

    @pytest.mark.anyio
    async def test_error(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.generated_images.list_recent = AsyncMock(side_effect=Exception("db err"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_generated_images"]({})
        assert "Ошибка" in _text(result)
