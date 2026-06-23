"""Moderation publish command handler (#1047).

Domain: ``moderation.publish_run`` — publishes a moderated generation run to its
pipeline target.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.services.dispatcher._base import _DispatcherProtocol

    _Base = _DispatcherProtocol
else:
    _Base = object


class ModerationCommandsMixin(_Base):
    """``moderation.*`` command handlers."""

    async def _handle_moderation_publish_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        from src.services.pipeline_service import PipelineService
        from src.services.publish_service import PublishService

        run_id = int(payload["run_id"])
        run = await self._db.repos.generation_runs.get(run_id)
        if run is None:
            raise RuntimeError("run_not_found")
        pipeline = await PipelineService(self._db).get(int(payload["pipeline_id"]))
        if pipeline is None:
            raise RuntimeError("pipeline_invalid")
        results = await PublishService(self._db, self._pool).publish_run(run, pipeline)
        if not results or not all(result.success for result in results):
            raise RuntimeError("pipeline_run_failed")
        return {"run_id": run_id, "published": len(results)}
