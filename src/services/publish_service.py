from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Protocol

from src.database import Database
from src.models import ContentPipeline, GenerationRun, PipelinePublishMode, PipelineTarget
from src.telegram.backends import adapt_transport_session

logger = logging.getLogger(__name__)


class _PublishClientPool(Protocol):
    async def get_client_by_phone(
        self,
        phone: str,
        *,
        wait_for_flood: bool = False,
    ) -> tuple[Any, str] | None: ...

    async def release_client(self, phone: str) -> None: ...

    async def resolve_dialog_entity(
        self,
        session: Any,
        phone: str,
        dialog_id: int,
        target_type: str | None = None,
    ) -> Any: ...


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

    def __init__(self, db: Database, client_pool: _PublishClientPool | None) -> None:
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

        # Single, service-level guard against publishing a run whose generation
        # did not complete (issue #1036 review, Codex). publish_run is the one
        # path every publish entrypoint funnels through — CONTENT_GENERATE
        # auto-publish, the CONTENT_PUBLISH batch, the web/CLI moderation
        # "publish" button (via the dispatcher), and the agent publish tool. A
        # run can carry generated_text (saved before a later step failed) yet end
        # at status='failed'; or a human/agent could approve a failed run. Gating
        # delivery on status='completed' here blocks every such case at the one
        # irreversible boundary, not just in CONTENT_PUBLISH's SQL filter.
        if run.status != "completed":
            logger.warning(
                "Run %s is not eligible for publish: status=%s (must be 'completed')",
                run.id,
                run.status,
            )
            return [PublishResult(success=False, error="Run generation is not completed")]

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
        for target in targets:
            key = _target_key(target)
            if key in delivered:
                # Already published on a previous attempt — skip to avoid a duplicate.
                results.append(PublishResult(success=True))
                continue
            result = await self._publish_to_target(run, target)
            results.append(result)
            if not result.success:
                continue
            # Persist progress immediately after EACH delivery — never batched to a
            # single end-of-loop write (issue #1116). A send to Telegram is
            # irreversible and there is no transaction spanning send + DB write, so
            # if this write fails the run goes FAILED and is retried. Recording the
            # delivered target right now bounds the worst case to re-sending the one
            # in-flight target on retry; a batched write would instead lose every
            # target delivered in this attempt and duplicate them all. The write is
            # deliberately NOT wrapped in try/except: a failed write means we can no
            # longer remember what we delivered, so the raised exception must stop
            # the loop rather than keep sending to targets we cannot track.
            delivered.add(key)
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
            result = await pool.get_client_by_phone(target.phone, wait_for_flood=True)
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

            # The actual send is awaited directly — NEVER wrapped in
            # asyncio.wait_for (issue #1239). A publish is irreversible: once the
            # MTProto request leaves for Telegram's servers the message WILL be
            # delivered, and a client-side timeout cancels only the local wait,
            # not the delivery. Timing out here would return success=False so the
            # target is left out of metadata.published_targets, the run stays
            # retry-eligible, and an operator retry re-sends the already-delivered
            # post → duplicate. This mirrors the #795 decision that removed
            # wait_for from resolve_entity for the same orphaned-request reason.
            # Telethon owns the transport-level timeout/retry for the send itself.
            if run.image_url:
                # Re-sign at publish time: a run that sat in moderation/schedule
                # longer than the 7-day presigned TTL would otherwise send a dead
                # S3 link (#869/#873/#874). Non-S3 URLs pass through unchanged.
                from src.services.s3_store import refresh_s3_url

                image_url = await refresh_s3_url(run.image_url)
                msg = await session.publish_files(
                    entity,
                    image_url,
                    caption=run.generated_text,
                )
            else:
                send_kwargs: dict = {}
                if reply_to is not None:
                    send_kwargs["reply_to"] = reply_to
                msg = await session.send_message(entity, run.generated_text, **send_kwargs)

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
                return await resolver(session, phone, target.dialog_id, target.dialog_type)

            return await session.resolve_input_entity(target.dialog_id)
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
