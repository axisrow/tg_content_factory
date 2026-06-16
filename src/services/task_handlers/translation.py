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

            # Advance the cursor past a contiguous prefix of messages that are either
            # successfully translated OR legitimately need no work this pass. The old
            # max(all selected) skipped genuinely-failed rows, losing them forever
            # (audit #835/12). But advancing ONLY over translated rows stalls the whole
            # chain when a no-work row (source==target, which translate_batch excludes
            # by design, or empty text) sits at the head — every later row is then
            # starved (#866 review). Treat no-work rows as skippable so the cursor moves
            # past them; only a row that SHOULD translate but got no result is a genuine
            # non-progress stall, and even then we step one id past it so the tail is not
            # starved (the failed row stays translation NULL and is re-selected on a full
            # re-run / explicit retry, not silently dropped).
            written_ids = {msg_id for msg_id, _ in results}
            # Mirror translate_batch's eligibility predicate: a row needs translation only
            # if it has a detected source language different from the target and has text.
            def _needs_translation(m) -> bool:
                detected = getattr(m, "detected_lang", None)
                return bool(detected and detected != target_lang and getattr(m, "text", None))

            no_work_ids = {m.id for m in msgs if not _needs_translation(m)}
            skippable = written_ids | no_work_ids
            ordered = sorted(msgs, key=lambda x: x.id or 0)
            new_last_id = last_id
            for m in ordered:
                if m.id is None:
                    continue
                if m.id in skippable:
                    new_last_id = m.id
                else:
                    # Genuine failure: an eligible row the provider returned no result for.
                    # Step the cursor one past it so later rows are not starved; the row
                    # itself stays untranslated and re-selectable on a later full pass.
                    new_last_id = m.id
                    break
            # Every selected row has id > last_id (query filter `m.id > ?`), and the loop
            # always lands on some real row's id, so new_last_id > last_id whenever any
            # row was processed — the cursor strictly advances. This deliberately favours
            # "never starve the tail" over a strict contiguous prefix: a poison row is
            # stepped past (re-selectable on a full re-run), not allowed to stall the
            # whole queue (#835/12, #866 review; locked by test_handler_cursor_advances_
            # past_translated_then_stops_at_genuine_failure).

            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                messages_collected=len(results),
                note=f"Translated {len(results)}/{len(msgs)} messages",
            )

            remaining = await ctx.db.repos.messages.get_untranslated_messages(
                target=target_lang,
                source_langs=source_filter or None,
                limit=1,
                after_id=new_last_id,
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
