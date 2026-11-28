"""Tests for photo task cancellation, batch status and per-file progress.

Covers audit #835/3 (cancel a server-scheduled item), #837/11 (batch terminal
status with CANCELLED), and #835/4 (per-file send progress).
"""

from __future__ import annotations

import asyncio
import errno
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.helpers import FakeCliTelethonClient
from tg_agent.database.bundles import PhotoLoaderBundle
from tg_agent.models import PhotoAutoUploadJob, PhotoBatchStatus, PhotoSendMode
from tg_agent.services.photo_auto_upload_service import PhotoAutoUploadService
from tg_agent.services.photo_publish_service import PhotoPublishService
from tg_agent.services.photo_task_service import PhotoTarget, PhotoTaskService

_NOW = datetime.now(timezone.utc)


def _due_item(item_id: int) -> SimpleNamespace:
    return SimpleNamespace(
        id=item_id,
        batch_id=None,
        phone="+7",
        target_dialog_id=-100,
        target_type="channel",
        file_paths=[f"/tmp/{item_id}.jpg"],
        send_mode=PhotoSendMode.SEPARATE,
        caption=None,
    )


@pytest.mark.anyio
async def test_run_due_reports_progress_after_each_item():
    bundle = MagicMock()
    bundle.count_due_items = AsyncMock(return_value=2)
    bundle.claim_next_due_item = AsyncMock(side_effect=[_due_item(1), _due_item(2), None])
    bundle.update_item = AsyncMock()
    publish = MagicMock()
    publish.send_now = AsyncMock(return_value=[101])
    svc = PhotoTaskService(bundle, publish)

    progress: list[tuple[int, int]] = []
    processed = await svc.run_due(on_progress=lambda done, total: progress.append((done, total)))

    assert processed == 2
    assert progress == [(1, 2), (2, 2)]


@pytest.mark.anyio
async def test_run_due_without_progress_callback_still_processes():
    bundle = MagicMock()
    bundle.count_due_items = AsyncMock(return_value=1)
    bundle.claim_next_due_item = AsyncMock(side_effect=[_due_item(1), None])
    bundle.update_item = AsyncMock()
    publish = MagicMock()
    publish.send_now = AsyncMock(return_value=[101])
    svc = PhotoTaskService(bundle, publish)

    assert await svc.run_due() == 1
    assert svc.last_run_failed == 0
    assert svc.last_run_errors == []


def _service_whose_second_send_fails():
    bundle = MagicMock()
    bundle.count_due_items = AsyncMock(return_value=2)
    bundle.claim_next_due_item = AsyncMock(side_effect=[_due_item(1), _due_item(2), None])
    bundle.update_item = AsyncMock()
    publish = MagicMock()
    publish.send_now = AsyncMock(side_effect=[[101], RuntimeError("send failed")])
    return PhotoTaskService(bundle, publish), bundle


@pytest.mark.anyio
async def test_run_due_remembers_an_item_that_was_not_sent():
    """#1969: a claimed item whose send failed is FAILED on its row; the run must
    not hand that count to its callers as work done without saying so."""
    svc, bundle = _service_whose_second_send_fails()

    processed = await svc.run_due()

    assert processed == 2
    assert svc.last_run_failed == 1
    assert svc.last_run_errors == ["item #2: send failed"]
    assert bundle.update_item.await_args.kwargs["status"] == PhotoBatchStatus.FAILED


@pytest.mark.anyio
async def test_run_due_single_item_that_was_not_sent():
    """#1969: `--item-id` path — the same accounting, the reason never empty."""
    bundle = MagicMock()
    bundle.count_due_items = AsyncMock(return_value=1)
    bundle.claim_next_due_item = AsyncMock(return_value=_due_item(7))
    bundle.update_item = AsyncMock()
    publish = MagicMock()
    publish.send_now = AsyncMock(side_effect=asyncio.TimeoutError())
    svc = PhotoTaskService(bundle, publish)

    assert await svc.run_due(item_id=7) == 1
    assert svc.last_run_failed == 1
    assert svc.last_run_errors == ["item #7: TimeoutError"]


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


# ── #1981: an album longer than one Telegram group ────────────────────────────

#: How `TelegramClient.send_file` treats an album: at most ten files per
#: `messages.sendMultiMedia`, the groups sent one after another, an exception
#: from the group that fails — after the groups before it went out.
TELETHON_ALBUM_GROUP = 10


def _telethon_like_album_transport(state: dict):
    """A ``send_file`` stand-in with Telethon's album behaviour.

    Every group it sends lands in ``state["groups"]`` as ``(files, caption)``;
    the first group holding ``state["unreadable"]`` raises ``PermissionError``,
    like a file the upload cannot open.
    """

    def _send_file(entity, files, **kwargs):
        if not isinstance(files, (list, tuple)):
            return _send_file(entity, [files], **kwargs)[0]
        sent: list = []
        for start in range(0, len(files), TELETHON_ALBUM_GROUP):
            group = list(files[start : start + TELETHON_ALBUM_GROUP])
            if state["unreadable"] is not None and state["unreadable"] in group:
                raise PermissionError(errno.EACCES, "Permission denied", state["unreadable"])
            state["groups"].append((group, kwargs.get("caption")))
            base = 1000 + 10 * len(state["groups"])
            sent.extend(SimpleNamespace(id=base + offset) for offset in range(len(group)))
        return sent

    return _send_file


def _album_files(folder: Path, count: int) -> list[Path]:
    folder.mkdir(parents=True, exist_ok=True)
    paths = [folder / f"{index:02d}.png" for index in range(1, count + 1)]
    for path in paths:
        path.write_bytes(b"\x89PNG\r\n\x1a\n")
    return paths


def _eleven_files_with_the_last_unreadable(folder: Path) -> tuple[list[Path], dict]:
    files = _album_files(folder, 10) + [folder / "11.png"]
    files[-1].write_bytes(b"\x89PNG\r\n\x1a\n")
    files[-1].chmod(0o000)
    return files, {"groups": [], "unreadable": str(files[-1])}


async def _services_over_album_transport(harness_factory, db, state: dict):
    """Real task and auto-upload services over the real bundle and the
    Telethon-like transport."""
    from telethon import types

    harness = harness_factory()
    harness.queue_cli_client(
        phone="+7000",
        client=FakeCliTelethonClient(
            # a real TL peer: the registry rows read the chat id off the entity
            input_entity_resolver=lambda peer: types.PeerChannel(channel_id=1001),
            send_file_side_effect=_telethon_like_album_transport(state),
        ),
    )
    await harness.add_account("+7000", session_string="s", is_primary=True)
    await harness.initialize_connected_accounts()
    publish = PhotoPublishService(harness.pool, db=db)
    bundle = PhotoLoaderBundle.from_database(db)
    return PhotoTaskService(bundle, publish), PhotoAutoUploadService(bundle, publish)


@pytest.mark.anyio
async def test_album_keeps_the_ids_of_the_group_that_went_out(db, tmp_path, real_pool_harness_factory):
    """#1981: eleven files, the last one unreadable. Telegram has the first ten —
    the item and the registry must know their ids, or a rerun publishes them twice."""
    files, state = _eleven_files_with_the_last_unreadable(tmp_path)
    service, _auto = await _services_over_album_transport(real_pool_harness_factory, db, state)

    item = await service.send_now(
        phone="+7000",
        target=PhotoTarget(dialog_id=-1001),
        file_paths=[str(path) for path in files],
        mode=PhotoSendMode.ALBUM,
    )

    assert [len(group) for group, _caption in state["groups"]] == [10]
    assert item.status is PhotoBatchStatus.COMPLETED  # what went out is live (#864)
    assert item.telegram_message_ids == list(range(1010, 1020))
    assert item.error is not None
    assert "partial send failure" in item.error and "11.png" in item.error
    rows = await db.execute_fetchall("SELECT message_id FROM sent_messages ORDER BY message_id")
    assert [row["message_id"] for row in rows] == list(range(1010, 1020))


@pytest.mark.anyio
async def test_scheduled_album_keeps_the_cancel_handle(db, tmp_path, real_pool_harness_factory):
    """#1981: the first group is already queued on Telegram when the second one
    fails — its ids stay on the SCHEDULED item, so `cancel_item` reaches them."""
    files, state = _eleven_files_with_the_last_unreadable(tmp_path)
    service, _auto = await _services_over_album_transport(real_pool_harness_factory, db, state)

    item = await service.schedule_send(
        phone="+7000",
        target=PhotoTarget(dialog_id=-1001),
        file_paths=[str(path) for path in files],
        mode=PhotoSendMode.ALBUM,
        schedule_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )

    assert item.status is PhotoBatchStatus.SCHEDULED
    assert item.telegram_message_ids == list(range(1010, 1020))
    assert item.error is not None
    assert "partial schedule failure" in item.error
    assert item.id is not None
    with patch.object(service._publish, "unschedule", new_callable=AsyncMock) as unschedule:
        assert await service.cancel_item(item.id) is True
    assert list(unschedule.await_args.kwargs["message_ids"]) == list(range(1010, 1020))


@pytest.mark.anyio
async def test_auto_upload_resends_only_the_files_that_did_not_go_out(db, tmp_path, real_pool_harness_factory):
    """#1981: a due run that failed on the eleventh file marked nothing, so the
    next run published the first ten a second time."""
    folder = tmp_path / "folder"
    files, state = _eleven_files_with_the_last_unreadable(folder)
    _service, auto = await _services_over_album_transport(real_pool_harness_factory, db, state)
    job_id = await auto.create_job(
        PhotoAutoUploadJob(
            phone="+7000",
            target_dialog_id=-1001,
            target_title="Target",
            target_type="channel",
            folder_path=str(folder),
            send_mode=PhotoSendMode.ALBUM,
            caption=None,
            interval_minutes=60,
        )
    )
    with pytest.raises(PermissionError):
        await auto.run_job(job_id)

    # The unreadable file becomes readable: the next run sends it alone.
    state["unreadable"] = None
    files[-1].chmod(0o644)
    assert await auto.run_job(job_id) == 1

    assert [len(group) for group, _caption in state["groups"]] == [10, 1]
    assert state["groups"][1][0] == [str(files[-1])]


@pytest.mark.anyio
async def test_long_album_goes_out_in_groups_with_the_caption_on_the_first(db, tmp_path, real_pool_harness_factory):
    """Groups of ten, the caption on the first one only — what Telegram showed
    for a long album before the groups were sent one by one."""
    files = _album_files(tmp_path, 12)
    state = {"groups": [], "unreadable": None}
    service, _auto = await _services_over_album_transport(real_pool_harness_factory, db, state)

    item = await service.send_now(
        phone="+7000",
        target=PhotoTarget(dialog_id=-1001),
        file_paths=[str(path) for path in files],
        mode=PhotoSendMode.ALBUM,
        caption="cap",
    )

    assert item.telegram_message_ids == list(range(1010, 1022))
    assert [(len(group), caption) for group, caption in state["groups"]] == [(10, "cap"), (2, None)]
