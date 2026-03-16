from __future__ import annotations

import logging

from src.database import Database
from src.database.bundles import PipelineBundle
from src.models import Pipeline, PipelinePublishMode, PipelineTarget

logger = logging.getLogger(__name__)


class PipelineService:
    def __init__(self, bundle: PipelineBundle | Database):
        if isinstance(bundle, Database):
            bundle = PipelineBundle.from_database(bundle)
        self._bundle = bundle

    async def add(
        self,
        name: str,
        phone: str,
        *,
        source_channel_ids: list[int] | None = None,
        targets: list[PipelineTarget] | None = None,
        prompt_template: str | None = None,
        llm_model: str | None = None,
        publish_mode: PipelinePublishMode = PipelinePublishMode.DRAFT,
    ) -> int:
        pipeline = Pipeline(
            name=name,
            phone=phone,
            source_channel_ids=source_channel_ids or [],
            targets=targets or [],
            prompt_template=prompt_template,
            llm_model=llm_model,
            publish_mode=publish_mode,
        )
        return await self._bundle.add(pipeline)

    async def list(self, active_only: bool = False) -> list[Pipeline]:
        return await self._bundle.get_all(active_only)

    async def get(self, pipeline_id: int) -> Pipeline | None:
        return await self._bundle.get_by_id(pipeline_id)

    async def toggle(self, pipeline_id: int) -> None:
        p = await self._bundle.get_by_id(pipeline_id)
        if p:
            await self._bundle.set_active(pipeline_id, not p.is_active)

    async def update(
        self,
        pipeline_id: int,
        name: str,
        phone: str,
        *,
        source_channel_ids: list[int] | None = None,
        targets: list[PipelineTarget] | None = None,
        prompt_template: str | None = None,
        llm_model: str | None = None,
        publish_mode: PipelinePublishMode = PipelinePublishMode.DRAFT,
    ) -> bool:
        existing = await self._bundle.get_by_id(pipeline_id)
        if not existing:
            return False
        pipeline = Pipeline(
            name=name,
            phone=phone,
            source_channel_ids=source_channel_ids or [],
            targets=targets or [],
            prompt_template=prompt_template,
            llm_model=llm_model,
            publish_mode=publish_mode,
            is_active=existing.is_active,
        )
        await self._bundle.update(pipeline_id, pipeline)
        return True

    async def delete(self, pipeline_id: int) -> None:
        await self._bundle.delete(pipeline_id)
