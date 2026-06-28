"""Tests for photo task cancellation, batch status and per-file progress.

Covers audit #835/3 (cancel a server-scheduled item), #837/11 (batch terminal
status with CANCELLED), and #835/4 (per-file send progress).
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import PhotoBatchStatus, PhotoSendMode
from src.services.photo_publish_service import PhotoPublishService
from src.services.photo_task_service import PhotoTaskService
from tests.helpers import FakeCliTelethonClient

_NOW = datetime.now(timezone.utc)


# ── 837#11: _sync_batch_status terminal handling ──────────────────────────────


async def _run_sync(item_statuses: list[PhotoBatchStatus]) -> PhotoBatchStatus:
    bundle = MagicMock()
    bundle.list_items_for_batch = AsyncMock(
        return_value=[SimpleNamespace(status=s) for s in item_statuses]
    )
    bundle.update_batch = AsyncMock()
    svc = PhotoTaskService(bundle, MagicMock())
    await svc._sync_batch_status(1, last_run_at=_NOW)
    return bundle.update_batch.await_args.kwargs["status"]


@pytest.mark.anyio
async def test_sync_batch_completed_plus_cancelled_is_completed():
    status = await _run_sync([PhotoBatchStatus.COMPLETED, PhotoBatchStatus.CANCELLED])
    assert status == PhotoBatchStatus.COMPLETED  # not stuck RUNNING (#837/11)


@pytest.mark.anyio
async def test_sync_batch_all_cancelled_is_cancelled():
    status = await _run_sync([PhotoBatchStatus.CANCELLED, PhotoBatchStatus.CANCELLED])
    assert status == PhotoBatchStatus.CANCELLED


@pytest.mark.anyio
async def test_sync_batch_any_failed_is_failed():
    status = await _run_sync([PhotoBatchStatus.COMPLETED, PhotoBatchStatus.FAILED])
    assert status == PhotoBatchStatus.FAILED


@pytest.mark.anyio
async def test_sync_batch_with_pending_stays_running():
    status = await _run_sync([PhotoBatchStatus.COMPLETED, PhotoBatchStatus.PENDING])
    assert status == PhotoBatchStatus.RUNNING


@pytest.mark.anyio
async def test_sync_batch_all_held_stays_held():
    status = await _run_sync([PhotoBatchStatus.HELD, PhotoBatchStatus.HELD])
    assert status == PhotoBatchStatus.HELD


# ── 835#3: cancel_item unschedules a server-scheduled item ────────────────────


@pytest.mark.anyio
async def test_cancel_scheduled_item_unschedules_on_telegram():
    item = SimpleNamespace(
        id=5,
        status=PhotoBatchStatus.SCHEDULED,
        telegram_message_ids=[101, 102],
        phone="+7",
        target_dialog_id=-100,
        target_type="channel",
        batch_id=9,
    )
    bundle = MagicMock()
    bundle.get_item = AsyncMock(return_value=item)
    bundle.cancel_item = AsyncMock(return_value=True)
    bundle.list_items_for_batch = AsyncMock(
        return_value=[SimpleNamespace(status=PhotoBatchStatus.CANCELLED)]
    )
    bundle.update_batch = AsyncMock()
    publish = MagicMock()
    publish.unschedule = AsyncMock()

    svc = PhotoTaskService(bundle, publish)
    result = await svc.cancel_item(5)

    assert result is True
    publish.unschedule.assert_awaited_once()
    assert publish.unschedule.await_args.kwargs["message_ids"] == [101, 102]


@pytest.mark.anyio
async def test_cancel_pending_item_does_not_unschedule():
    item = SimpleNamespace(
        id=5,
        status=PhotoBatchStatus.PENDING,
        telegram_message_ids=None,
        phone="+7",
        target_dialog_id=-100,
        target_type="channel",
        batch_id=None,
    )
    bundle = MagicMock()
    bundle.get_item = AsyncMock(return_value=item)
    bundle.cancel_item = AsyncMock(return_value=True)
    publish = MagicMock()
    publish.unschedule = AsyncMock()

    svc = PhotoTaskService(bundle, publish)
    result = await svc.cancel_item(5)

    assert result is True
    publish.unschedule.assert_not_awaited()


@pytest.mark.anyio
async def test_cancel_scheduled_item_not_marked_when_unschedule_fails():
    """Regression (#864 review): if the server-side unschedule fails for a SCHEDULED item
    with telegram_message_ids, the item must NOT be marked CANCELLED — otherwise the UI
    reports success while Telegram still publishes the post. cancel_item returns False and
    records the error; the item stays SCHEDULED so cancellation can be retried."""
    item = SimpleNamespace(
        id=5,
        status=PhotoBatchStatus.SCHEDULED,
        telegram_message_ids=[101, 102],
        phone="+7",
        target_dialog_id=-100,
        target_type="channel",
        batch_id=9,
    )
    bundle = MagicMock()
    bundle.get_item = AsyncMock(return_value=item)
    bundle.cancel_item = AsyncMock(return_value=True)
    bundle.update_item = AsyncMock()
    publish = MagicMock()
    publish.unschedule = AsyncMock(side_effect=RuntimeError("flood wait"))

    svc = PhotoTaskService(bundle, publish)
    result = await svc.cancel_item(5)

    assert result is False, "cancel must report failure when the server-side unschedule fails"
    bundle.cancel_item.assert_not_awaited()  # item NOT marked CANCELLED
    bundle.update_item.assert_awaited_once()  # error recorded
    assert "unschedule failed" in bundle.update_item.await_args.kwargs["error"]


# ── 835#4: per-file progress callback ─────────────────────────────────────────


@pytest.mark.anyio
async def test_send_now_separate_marks_each_file_via_callback(real_pool_harness_factory):
    harness = real_pool_harness_factory()
    counter = {"n": 0}

    def _send_file(*args, **kwargs):
        counter["n"] += 1
        return SimpleNamespace(id=200 + counter["n"])

    harness.queue_cli_client(
        phone="+7000",
        client=FakeCliTelethonClient(
            input_entity_resolver=lambda peer: SimpleNamespace(
                id=getattr(peer, "channel_id", getattr(peer, "user_id", peer))
            ),
            send_file_side_effect=_send_file,
        ),
    )
    await harness.add_account("+7000", session_string="s", is_primary=True)
    await harness.initialize_connected_accounts()

    marked: list[tuple[str, list[int]]] = []

    async def _on_file_sent(path: str, ids: list[int]) -> None:
        marked.append((path, ids))

    service = PhotoPublishService(harness.pool)
    result = await service.send_now(
        phone="+7000",
        target_dialog_id=-1001,
        target_type="channel",
        file_paths=["/a.jpg", "/b.jpg"],
        send_mode=PhotoSendMode.SEPARATE,
        on_file_sent=_on_file_sent,
    )

    assert result == [201, 202]
    assert marked == [("/a.jpg", [201]), ("/b.jpg", [202])]
