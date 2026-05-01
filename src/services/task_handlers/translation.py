from __future__ import annotations

import logging

from src.models import CollectionTask, CollectionTaskStatus, CollectionTaskType, TranslateBatchTaskPayload
from src.services.task_handlers.base import TaskHandlerContext

logger = logging.getLogger(__name__)


class TranslationTaskHandler:
    task_types = (CollectionTaskType.TRANSLATE_BATCH,)

    def __init__(self, context: TaskHandlerContext):
        self._context = context

    async def handle(self, task: CollectionTask) -> None:
        await self.handle_translate_batch(task)

    async def handle_translate_batch(self, task: CollectionTask) -> None:
        ctx = self._context
        if task.id is None:
            return

        payload = task.payload
        if not isinstance(payload, TranslateBatchTaskPayload):
            await ctx.tasks.update_collection_task(
                task.id, CollectionTaskStatus.FAILED, error="Invalid TRANSLATE_BATCH payload"
            )
            return

        if not ctx.db:
            await ctx.tasks.update_collection_task(
                task.id, CollectionTaskStatus.FAILED, error="Database not configured"
            )
            return

        try:
            from src.services.provider_service import build_provider_service
            from src.services.translation_service import TranslationService

            provider_name = await ctx.db.get_setting("translation_provider")
            model = await ctx.db.get_setting("translation_model")

            provider_service = await build_provider_service(ctx.db, ctx.config)

            resolved = provider_service.get_provider_callable(provider_name)
            if resolved is provider_service._registry.get("default"):
                await ctx.tasks.update_collection_task(
                    task.id,
                    CollectionTaskStatus.FAILED,
                    error="No translation provider configured (only stub default available)",
                )
                return

            svc = TranslationService(ctx.db, provider_service=provider_service)

            target_lang = payload.target_lang
            source_filter = payload.source_filter or []
            batch_size = payload.batch_size or 20
            last_id = payload.last_processed_id or 0

            msgs = await ctx.db.repos.messages.get_untranslated_messages(
                target=target_lang,
                source_langs=source_filter or None,
                limit=batch_size,
                after_id=last_id,
            )

            if not msgs:
                await ctx.tasks.update_collection_task(
                    task.id,
                    CollectionTaskStatus.COMPLETED,
                    note="No more messages to translate",
                )
                return

            results = await svc.translate_batch(msgs, target_lang, provider_name=provider_name, model=model)

            for msg_id, translated in results:
                await ctx.db.repos.messages.update_translation(msg_id, target_lang, translated)

            new_last_id = max(m.id for m in msgs if m.id is not None) if msgs else last_id

            remaining = await ctx.db.repos.messages.get_untranslated_messages(
                target=target_lang,
                source_langs=source_filter or None,
                limit=1,
                after_id=new_last_id,
            )

            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                messages_collected=len(results),
                note=f"Translated {len(results)}/{len(msgs)} messages",
            )

            if remaining:
                follow_up = TranslateBatchTaskPayload(
                    target_lang=target_lang,
                    source_filter=source_filter,
                    batch_size=batch_size,
                    last_processed_id=new_last_id,
                )
                await ctx.tasks.create_generic_task(
                    CollectionTaskType.TRANSLATE_BATCH,
                    title=f"Translation batch ({target_lang}) cont.",
                    payload=follow_up,
                )

        except Exception as exc:
            logger.exception("Translate batch handler failed")
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error=str(exc)[:500],
            )
