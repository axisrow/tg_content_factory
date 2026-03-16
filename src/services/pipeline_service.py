from __future__ import annotations

from src.database import Database
from src.database.bundles import PipelineBundle
from src.models import Channel, Pipeline


class PipelineService:
    def __init__(self, bundle: PipelineBundle | Database):
        if isinstance(bundle, Database):
            bundle = PipelineBundle.from_database(bundle)
        self._bundle = bundle

    async def list_for_page(
        self,
        *,
        phone: str | None = None,
    ) -> tuple[list[Pipeline], list[Channel]]:
        pipelines = await self._bundle.list(phone)
        channels = await self._bundle.list_channels(include_filtered=False)
        return pipelines, channels

    async def get_by_id(self, pipeline_id: int) -> Pipeline | None:
        return await self._bundle.get_by_id(pipeline_id)

    async def add(self, pipeline: Pipeline) -> int:
        return await self._bundle.add(pipeline)

    async def update(self, pipeline_id: int, pipeline: Pipeline) -> None:
        await self._bundle.update(pipeline_id, pipeline)

    async def delete(self, pipeline_id: int) -> None:
        await self._bundle.delete(pipeline_id)
