import pytest

from src.services.image_generation_service import ImageGenerationService


@pytest.mark.asyncio
async def test_image_generation_service_raises_not_implemented():
    service = ImageGenerationService()
    
    with pytest.raises(NotImplementedError) as exc_info:
        await service.generate("dall-e-3", "A beautiful sunset over mountains")
    
    assert "not yet implemented" in str(exc_info.value)
    assert "dall-e-3" in str(exc_info.value)


@pytest.mark.asyncio
async def test_image_generation_service_is_available_returns_false():
    service = ImageGenerationService()
    
    result = await service.is_available()
    
    assert result is False


@pytest.mark.asyncio
async def test_image_generation_service_with_none_model():
    service = ImageGenerationService()
    
    with pytest.raises(NotImplementedError) as exc_info:
        await service.generate(None, "Test prompt")
    
    assert "default" in str(exc_info.value)


@pytest.mark.asyncio
async def test_image_generation_service_empty_text():
    service = ImageGenerationService()
    
    with pytest.raises(NotImplementedError) as exc_info:
        await service.generate("test-model", "")
    
    assert "0 chars" in str(exc_info.value)
