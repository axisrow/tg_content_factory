from __future__ import annotations

import logging
from typing import Any

from src.agent.tools._registry import ToolInputError
from src.services.pipeline_refs import parse_pipeline_target_refs
from src.services.pipeline_service import PipelineValidationError

logger = logging.getLogger(__name__)


def parse_agent_target_refs(raw: str):
    try:
        return parse_pipeline_target_refs(
            raw,
            missing_separator_message="Неверный формат target_ref: '{part}'. Ожидается 'phone|dialog_id'.",
            invalid_dialog_id_message="dialog_id в target_ref '{part}' должен быть целым числом.",
        )
    except PipelineValidationError as exc:
        raise ToolInputError(str(exc)) from exc


async def build_image_service(db: Any, config: Any):
    """Build ImageGenerationService with DB providers + env fallback."""
    from src.services.image_generation_service import ImageGenerationService

    if db and config:
        try:
            from src.services.image_provider_service import ImageProviderService

            svc = ImageProviderService(db, config)
            configs = await svc.load_provider_configs()
            adapters = svc.build_adapters(configs)
            if adapters:
                return ImageGenerationService(adapters=adapters)
        except Exception:
            logger.warning("Failed to load image providers from DB", exc_info=True)
    return ImageGenerationService()
