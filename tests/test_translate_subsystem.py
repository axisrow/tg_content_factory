"""Tests for translate subsystem fixes (audit #836/1, #836/5, #836/6, #835/12)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.config import SchedulerConfig
from src.database import Database
from src.models import CollectionTaskType, Message
from src.services.translation_service import TranslationService
from src.telegram.collector import Collector

# ── 836#1: backfill returns rows considered + 'und' sentinel ──────────────────


@pytest.mark.anyio
async def test_backfill_returns_rows_considered_and_marks_und(tmp_path):
    db = Database(str(tmp_path / "t.db"))
    await db.initialize()
    try:
        for i in range(3):
            await db.insert_message(
                Message(channel_id=100, message_id=i, text="hi", date="2025-01-01T00:00:00")
            )
        # batch_size 2 -> considers 2 rows (returns 2, not "detected" count).
        first = await db.repos.messages.backfill_language_detection(batch_size=2)
        assert first == 2
        # Second pass considers the remaining 1; third pass finds none.
        assert await db.repos.messages.backfill_language_detection(batch_size=2) == 1
        assert await db.repos.messages.backfill_language_detection(batch_size=2) == 0

        # All rows were stamped (sentinel 'und' for the too-short text) — no NULLs.
        cur = await db.execute("SELECT COUNT(*) AS c FROM messages WHERE detected_lang IS NULL")
        assert (await cur.fetchone())["c"] == 0
    finally:
        await db.close()


@pytest.mark.anyio
async def test_get_untranslated_excludes_und_sentinel(tmp_path):
    db = Database(str(tmp_path / "t.db"))
    await db.initialize()
    try:
        await db.insert_message(
            Message(channel_id=100, message_id=1, text="hello", date="2025-01-01T00:00:00")
        )
        await db.insert_message(
            Message(channel_id=100, message_id=2, text="world", date="2025-01-01T00:00:00")
        )
        await db.execute("UPDATE messages SET detected_lang='und' WHERE message_id=1")
        await db.execute("UPDATE messages SET detected_lang='ru' WHERE message_id=2")
        await db.db.commit()

        msgs = await db.repos.messages.get_untranslated_messages(target="en", limit=10)
        ids = {m.message_id for m in msgs}
        assert 2 in ids  # 'ru' is translatable
        assert 1 not in ids  # 'und' is excluded
    finally:
        await db.close()


# ── 836#5: translate_batch caps very long input (and logs) ────────────────────


class _FakeRegistry:
    def get(self, name):  # default provider is None, so our provider isn't "default"
        return None


class _FakeProviderService:
    def __init__(self, provider):
        self._provider = provider
        self._registry = _FakeRegistry()

    def get_provider_callable(self, name):
        return self._provider


@pytest.mark.anyio
async def test_translate_batch_caps_long_input():
    captured: list[str] = []

    async def _provider(prompt, model=None, max_tokens=0, temperature=0.0):
        captured.append(prompt)
        return "1: translated"

    svc = TranslationService(MagicMock(), provider_service=_FakeProviderService(_provider))
    long_msg = Message(
        id=1, channel_id=1, message_id=1, text="a" * 9000, detected_lang="ru", date="2025-01-01T00:00:00"
    )
    await svc.translate_batch([long_msg], "en")

    assert captured, "provider should be called"
    assert "a" * 8000 in captured[0]
    assert "a" * 8001 not in captured[0]  # capped


# ── 836#6: collector enqueues auto-translate when enabled ─────────────────────


def _collector_with_settings(settings: dict[str, str]) -> Collector:
    db = MagicMock()
    db.get_setting = AsyncMock(side_effect=lambda k: settings.get(k))
    tasks = MagicMock()
    tasks.has_active_task = AsyncMock(return_value=False)
    tasks.create_generic_task = AsyncMock(return_value=1)
    db.repos = MagicMock()
    db.repos.tasks = tasks
    collector = Collector(MagicMock(), db, SchedulerConfig())
    return collector


@pytest.mark.anyio
async def test_auto_translate_enqueues_when_enabled():
    c = _collector_with_settings({"translation_auto_on_collect": "1", "translation_target_lang": "en"})
    await c._maybe_enqueue_auto_translate()
    c._db.repos.tasks.create_generic_task.assert_awaited_once()
    assert c._db.repos.tasks.create_generic_task.await_args.args[0] == CollectionTaskType.TRANSLATE_BATCH


@pytest.mark.anyio
async def test_auto_translate_noop_when_disabled():
    c = _collector_with_settings({"translation_auto_on_collect": "0"})
    await c._maybe_enqueue_auto_translate()
    c._db.repos.tasks.create_generic_task.assert_not_awaited()


@pytest.mark.anyio
async def test_auto_translate_dedup_when_active_task_exists():
    c = _collector_with_settings({"translation_auto_on_collect": "1"})
    c._db.repos.tasks.has_active_task = AsyncMock(return_value=True)
    await c._maybe_enqueue_auto_translate()
    c._db.repos.tasks.create_generic_task.assert_not_awaited()


# ── 835#12: handler advances cursor only past a translated prefix ─────────────


def _translate_ctx(untranslated_first, remaining):
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from src.services.task_handlers import TaskHandlerContext

    tasks = MagicMock()
    tasks.update_collection_task = AsyncMock()
    tasks.create_generic_task = AsyncMock(return_value=99)
    db = MagicMock()
    db.get_setting = AsyncMock(return_value="someprovider")
    db.repos = MagicMock()
    db.repos.messages.get_untranslated_messages = AsyncMock(side_effect=[untranslated_first, remaining])
    db.repos.messages.update_translation = AsyncMock()
    ctx = TaskHandlerContext(
        collector=MagicMock(),
        channel_bundle=MagicMock(),
        tasks=tasks,
        stop_event=MagicMock(),
        db=db,
        config=MagicMock(),
    )
    return ctx, SimpleNamespace


async def _run_handler(ctx, translate_results):
    from unittest.mock import patch

    from src.models import CollectionTask, CollectionTaskStatus, CollectionTaskType, TranslateBatchTaskPayload
    from src.services.task_handlers import TranslationTaskHandler

    provider_service = MagicMock()
    provider_service.get_provider_callable = MagicMock(return_value=object())
    provider_service._registry = MagicMock()
    provider_service._registry.get = MagicMock(return_value=None)

    svc = MagicMock()
    svc.translate_batch = AsyncMock(return_value=translate_results)

    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.TRANSLATE_BATCH,
        status=CollectionTaskStatus.RUNNING,
        payload=TranslateBatchTaskPayload(target_lang="en", last_processed_id=0),
    )
    with (
        patch("src.services.provider_service.build_provider_service", AsyncMock(return_value=provider_service)),
        patch("src.services.translation_service.TranslationService", return_value=svc),
    ):
        await TranslationTaskHandler(ctx).handle_translate_batch(task)


def _tmsg(mid, detected_lang="ru", text="x"):
    """A message that NEEDS translation (foreign source, has text) unless overridden."""
    from types import SimpleNamespace

    return SimpleNamespace(id=mid, detected_lang=detected_lang, text=text)


@pytest.mark.anyio
async def test_handler_cursor_advances_past_translated_then_stops_at_genuine_failure():
    # All three need translation; provider returns only 10,11; id=12 is a GENUINE failure.
    msgs = [_tmsg(10), _tmsg(11), _tmsg(12)]
    ctx, _ = _translate_ctx(msgs, [_tmsg(20)])
    await _run_handler(ctx, [(10, "x"), (11, "y")])

    # Cursor steps one past the failed head (12) so the tail is never starved; a follow-up
    # is enqueued from 12 (the failed row stays untranslated, re-selectable on a full re-run).
    ctx.tasks.create_generic_task.assert_awaited_once()
    follow_up = ctx.tasks.create_generic_task.await_args.kwargs["payload"]
    assert follow_up.last_processed_id == 12


@pytest.mark.anyio
async def test_handler_no_work_head_does_not_starve_tail():
    # Regression (#866 review): a source==target head row (detected_lang == target 'en')
    # is excluded by translate_batch — it must NOT stall the chain. The cursor advances past
    # it and the genuinely-foreign tail rows still get translated.
    msgs = [_tmsg(10, detected_lang="en"), _tmsg(11, detected_lang="ru"), _tmsg(12, detected_lang="ru")]
    ctx, _ = _translate_ctx(msgs, [_tmsg(20)])
    # translate_batch returns the two foreign rows (it filters out the en head).
    await _run_handler(ctx, [(11, "y"), (12, "z")])

    # Chain continues (not stalled at the en head), cursor at 12, follow-up enqueued.
    ctx.tasks.create_generic_task.assert_awaited_once()
    follow_up = ctx.tasks.create_generic_task.await_args.kwargs["payload"]
    assert follow_up.last_processed_id == 12


@pytest.mark.anyio
async def test_handler_failed_head_still_enqueues_followup_when_work_remains():
    """The cursor strictly advances even when the FIRST row is a genuine failure, so the
    chain always makes progress and a follow-up is enqueued while work remains — there is
    no dead "no-progress stop" branch that could strand the tail (#866 review cleanup)."""
    # id=10 is eligible but the provider returns nothing for it (failure at the head).
    msgs = [_tmsg(10), _tmsg(11)]
    ctx, _ = _translate_ctx(msgs, [_tmsg(20)])
    await _run_handler(ctx, [])  # provider returned no results at all

    # Cursor steps past the failed head (10) instead of stalling; follow-up from 10.
    ctx.tasks.create_generic_task.assert_awaited_once()
    follow_up = ctx.tasks.create_generic_task.await_args.kwargs["payload"]
    assert follow_up.last_processed_id == 10


@pytest.mark.anyio
async def test_handler_no_followup_when_nothing_remains():
    """When the remaining-probe finds no more untranslated rows, the chain ends — no
    follow-up task is created."""
    msgs = [_tmsg(10), _tmsg(11)]
    ctx, _ = _translate_ctx(msgs, [])  # remaining probe returns empty
    await _run_handler(ctx, [(10, "x"), (11, "y")])

    ctx.tasks.create_generic_task.assert_not_awaited()
