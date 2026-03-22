import pytest

from src.services.image_generation_service import ImageGenerationService


@pytest.mark.asyncio
async def test_generate_returns_none():
    service = ImageGenerationService()
    result = await service.generate("dall-e-3", "A beautiful sunset over mountains")
    assert result is None


@pytest.mark.asyncio
async def test_generate_with_none_model_returns_none():
    service = ImageGenerationService()
    result = await service.generate(None, "Test prompt")
    assert result is None


@pytest.mark.asyncio
async def test_generate_empty_text_returns_none():
    service = ImageGenerationService()
    result = await service.generate("test-model", "")
    assert result is None


@pytest.mark.asyncio
async def test_is_available_returns_false():
    service = ImageGenerationService()
    result = await service.is_available()
    assert result is False
