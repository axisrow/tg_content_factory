from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class ImageGenerationService:
    """Service for generating images from text.
    
    This is currently a stub implementation that raises NotImplementedError.
    Future implementations will integrate with image generation providers.
    """

    def __init__(self) -> None:
        pass

    async def generate(self, model: str | None, text: str) -> str:
        """Generate an image from text and return the image URL.
        
        Args:
            model: The image model to use (e.g., "dall-e-3", "stable-diffusion-xl")
            text: The text prompt for image generation
            
        Returns:
            URL of the generated image
            
        Raises:
            NotImplementedError: Always, as this is a stub implementation
        """
        raise NotImplementedError(
            "Image generation is not yet implemented. "
            "This feature will be added in a future release. "
            f"Requested model: {model or 'default'}, prompt length: {len(text)} chars"
        )

    async def is_available(self) -> bool:
        """Check if image generation is available.
        
        Returns:
            False, as this is a stub implementation
        """
        return False
