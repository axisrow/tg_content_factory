from __future__ import annotations

from src.database import Database
from src.database.bundles import PipelineBundle
from src.models import Pipeline


class PipelineService:
    def __init__(self, bundle: PipelineBundle | Database):
        if isinstance(bundle, Database):
            bundle = PipelineBundle.from_database(bundle)
        self._bundle = bundle

    async def add(self, name: str) -> int:
        return await self._bundle.add(Pipeline(name=name))

    async def list(self, active_only: bool = False) -> list[Pipeline]:
        return await self._bundle.get_all(active_only)

    async def get(self, pipeline_id: int) -> Pipeline | None:
        return await self._bundle.get_by_id(pipeline_id)

    async def toggle(self, pipeline_id: int) -> None:
        pipeline = await self._bundle.get_by_id(pipeline_id)
        if pipeline:
            await self._bundle.set_active(pipeline_id, not pipeline.is_active)

    async def update(self, pipeline_id: int, name: str) -> bool:
        existing = await self._bundle.get_by_id(pipeline_id)
        if not existing:
            return False
        await self._bundle.update(
            pipeline_id,
            Pipeline(name=name, is_active=existing.is_active),
        )
        return True

    async def delete(self, pipeline_id: int) -> None:
        await self._bundle.delete(pipeline_id)
