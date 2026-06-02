from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from src.database import Database
from src.models import ContentPipeline, GenerationRun, PipelinePublishMode, PipelineTarget
from src.telegram.backends import adapt_transport_session

logger = logging.getLogger(__name__)


@dataclass
class PublishResult:
    success: bool
    message_id: int | None = None
    error: str | None = None
    phone: str | None = None
    dialog_id: int | None = None


def _target_key(target: PipelineTarget) -> str:
    """Stable identity for a publish target — (phone, dialog_id) pair."""
    return f"{target.phone}:{target.dialog_id}"


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
        """Publish a generation run to all pipeline targets."""
        if run.id is None or pipeline.id is None:
            return [PublishResult(success=False, error="Missing run or pipeline id")]

        if not (run.generated_text or "").strip():
            logger.warning("Run %s has no generated_text, skipping publish", run.id)
            return [PublishResult(success=False, error="No generated text")]

        effective_mode = (
            (run.metadata or {}).get("effective_publish_mode", pipeline.publish_mode.value)
        )
        if (
            effective_mode == PipelinePublishMode.MODERATED.value
            and run.moderation_status not in {"approved", "published"}
        ):
            logger.warning(
                "Run %s is not eligible for publish: moderation_status=%s publish_mode=%s",
                run.id,
                run.moderation_status,
                effective_mode,
            )
            return [PublishResult(success=False, error="Run is not approved for publish")]

        targets = await self._db.repos.content_pipelines.list_targets(pipeline.id)
        if not targets:
            logger.warning("Pipeline %s has no targets", pipeline.id)
            return [PublishResult(success=False, error="No targets configured")]

        # Track per-target delivery across attempts: on a partial failure the run
        # stays eligible for retry, so without this a re-publish would re-send to
        # targets that already succeeded, duplicating messages (issue #633).
        metadata = dict(run.metadata or {})
        delivered: set[str] = set(metadata.get("published_targets") or [])

        results: list[PublishResult] = []
        newly_delivered: list[str] = []
        for target in targets:
            key = _target_key(target)
            if key in delivered:
                # Already published on a previous attempt — skip to avoid a duplicate.
                results.append(PublishResult(success=True))
                continue
            result = await self._publish_to_target(run, target)
            results.append(result)
            if result.success:
                newly_delivered.append(key)

        # Persist incremental progress so a later retry resumes only the failed targets.
        if newly_delivered:
            delivered.update(newly_delivered)
            metadata["published_targets"] = sorted(delivered)
            await self._db.repos.generation_runs.set_metadata(run.id, metadata)

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
        pool = self._client_pool
        if pool is None:
            return PublishResult(success=False, error="client_pool not configured")
        acquired_phone: str | None = None
        try:
            result = await pool.get_client_by_phone(target.phone)
            if result is None:
                return PublishResult(
                    success=False,
                    error=f"No client for phone {target.phone}",
                )
            client, acquired_phone = result
            session = adapt_transport_session(client, disconnect_on_close=False)

            entity = await self._resolve_entity(session, acquired_phone, target)
            if entity is None:
                return PublishResult(
                    success=False,
                    error=f"Could not resolve dialog_id={target.dialog_id}",
                )

            reply_to = None
            if run.metadata and run.metadata.get("publish_reply"):
                reply_to = run.metadata.get("reply_to_message_id")

            if run.image_url:
                msg = await asyncio.wait_for(
                    session.publish_files(
                        entity,
                        run.image_url,
                        caption=run.generated_text,
                    ),
                    timeout=60.0,
                )
            else:
                send_kwargs: dict = {}
                if reply_to is not None:
                    send_kwargs["reply_to"] = reply_to
                msg = await asyncio.wait_for(
                    session.send_message(entity, run.generated_text, **send_kwargs),
                    timeout=60.0,
                )

            return PublishResult(
                success=True,
                message_id=msg.id if hasattr(msg, "id") else None,
                phone=acquired_phone,
                dialog_id=target.dialog_id,
            )

        except asyncio.TimeoutError:
            logger.error("Timeout publishing to %s:%s", target.phone, target.dialog_id)
            return PublishResult(success=False, error="Timeout")
        except Exception as e:
            logger.exception("Failed to publish to %s:%s", target.phone, target.dialog_id)
            return PublishResult(success=False, error=str(e))
        finally:
            if acquired_phone is not None:
                await pool.release_client(acquired_phone)

    async def _resolve_entity(
        self,
        session,
        phone: str,
        target: PipelineTarget,
    ):
        """Resolve dialog_id to entity."""
        try:
            resolver = getattr(self._client_pool, "resolve_dialog_entity", None)
            if callable(resolver):
                resolved = resolver(session, phone, target.dialog_id, target.dialog_type)
                return await asyncio.wait_for(resolved, timeout=30.0)

            return await asyncio.wait_for(session.resolve_input_entity(target.dialog_id), timeout=30.0)
        except Exception as e:
            logger.warning("Could not resolve dialog_id %s: %s", target.dialog_id, e)
            return None

    async def preview_targets(self, pipeline_id: int) -> list[dict]:
        """Get preview info about pipeline targets."""
        targets = await self._db.repos.content_pipelines.list_targets(pipeline_id)
        return [
            {
                "phone": target.phone,
                "dialog_id": target.dialog_id,
                "title": target.title,
                "type": target.dialog_type,
            }
            for target in targets
        ]
