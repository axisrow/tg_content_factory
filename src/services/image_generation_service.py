from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class ImageGenerationService:
    """Stub image generation service. Returns None until a real provider is wired up."""

    def __init__(self) -> None:
        pass

    async def generate(self, model: str | None, text: str) -> str | None:
        """Return None — image generation is not yet implemented."""
        logger.warning(
            "ImageGenerationService.generate() called but not implemented "
            "(model=%s, prompt_len=%d); returning None",
            model or "default",
            len(text),
        )
        return None

    async def is_available(self) -> bool:
        return False
