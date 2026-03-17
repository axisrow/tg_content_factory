from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from src.database import Database
from src.models import ContentPipeline, GenerationRun, PipelineTarget

logger = logging.getLogger(__name__)


@dataclass
class PublishResult:
    success: bool
    message_id: int | None = None
    error: str | None = None


class PublishService:
    """Service for publishing generated content to Telegram targets.
    
    Handles:
    - Fetching pipeline targets (phone + dialog_id pairs)
    - Sending text and optional image to each target
    - Updating generation_runs.published_at on success
    """

    def __init__(self, db: Database, client_pool: object) -> None:
        self._db = db
        self._client_pool = client_pool

    async def publish_run(
        self,
        run: GenerationRun,
        pipeline: ContentPipeline,
    ) -> list[PublishResult]:
        """Publish a generation run to all pipeline targets.
        
        Args:
            run: The generation run to publish
            pipeline: The pipeline with target definitions
            
        Returns:
            List of PublishResult for each target
        """
        if not run.generated_text:
            logger.warning("Run %s has no generated_text, skipping publish", run.id)
            return [PublishResult(success=False, error="No generated text")]

        targets = await self._db.repos.content_pipelines.list_targets(pipeline.id)
        if not targets:
            logger.warning("Pipeline %s has no targets", pipeline.id)
            return [PublishResult(success=False, error="No targets configured")]

        results: list[PublishResult] = []
        for target in targets:
            result = await self._publish_to_target(run, target)
            results.append(result)

        if all(r.success for r in results):
            await self._db.repos.generation_runs.set_published_at(run.id)
            logger.info("Published run %s to %d targets", run.id, len(targets))

        return results

    async def _publish_to_target(
        self,
        run: GenerationRun,
        target: PipelineTarget,
    ) -> PublishResult:
        """Publish to a single target."""
        from src.telegram.client_pool import ClientPool

        pool: ClientPool = self._client_pool

        try:
            result = await pool.get_client_by_phone(target.phone)
            if result is None:
                return PublishResult(
                    success=False,
                    error=f"No client for phone {target.phone}",
                )
            client, _phone = result

            entity = await self._resolve_entity(client, target.dialog_id)
            if entity is None:
                return PublishResult(
                    success=False,
                    error=f"Could not resolve dialog_id={target.dialog_id}",
                )

            raw_client = client.raw_client

            if run.image_url:
                msg = await asyncio.wait_for(
                    raw_client.send_message(
                        entity,
                        run.generated_text,
                        file=run.image_url,
                    ),
                    timeout=60.0,
                )
            else:
                msg = await asyncio.wait_for(
                    raw_client.send_message(entity, run.generated_text),
                    timeout=60.0,
                )

            return PublishResult(
                success=True,
                message_id=msg.id if hasattr(msg, "id") else None,
            )

        except asyncio.TimeoutError:
            logger.error("Timeout publishing to %s:%s", target.phone, target.dialog_id)
            return PublishResult(success=False, error="Timeout")
        except Exception as e:
            logger.exception("Failed to publish to %s:%s", target.phone, target.dialog_id)
            return PublishResult(success=False, error=str(e))

    async def _resolve_entity(self, client, dialog_id: int):
        """Resolve dialog_id to entity."""
        try:
            from telethon.tl.types import PeerChannel, PeerChat, PeerUser

            if dialog_id < 0:
                if dialog_id < -1000000000:
                    peer = PeerChannel(channel_id=-1000000000000 - dialog_id)
                else:
                    peer = PeerChannel(channel_id=-dialog_id)
            else:
                peer = PeerUser(user_id=dialog_id)

            entity = await asyncio.wait_for(client.get_entity(peer), timeout=30.0)
            return entity
        except Exception as e:
            logger.warning("Could not resolve dialog_id %s: %s", dialog_id, e)
            return None

    async def preview_targets(self, pipeline_id: int) -> list[dict]:
        """Get preview info about pipeline targets."""
        targets = await self._db.repos.content_pipelines.list_targets(pipeline_id)
        result = []
        for t in targets:
            result.append({
                "phone": t.phone,
                "dialog_id": t.dialog_id,
                "title": t.title,
                "type": t.dialog_type,
            })
        return result
