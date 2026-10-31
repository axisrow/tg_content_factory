"""Explicit factories shared by pipeline operation handlers."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tg_agent.search.engine import SearchEngine
    from tg_agent.services.content_generation_service import ContentGenerationService
    from tg_agent.services.publish_service import PublishService


@dataclass(frozen=True)
class PipelineServices:
    """Keep service overrides explicit while sharing dependencies between handlers."""

    generation_service: Callable[..., ContentGenerationService]
    publish_service: Callable[..., PublishService]
    search_engine: Callable[..., SearchEngine]


def default_services() -> PipelineServices:
    from tg_agent.search.engine import SearchEngine
    from tg_agent.services.content_generation_service import ContentGenerationService
    from tg_agent.services.publish_service import PublishService

    return PipelineServices(ContentGenerationService, PublishService, SearchEngine)
