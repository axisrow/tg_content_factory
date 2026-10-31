from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.helpers import (
    FakeCliTelethonClient,
)
from tg_agent.config import AppConfig
from tg_agent.database import Database
from tg_agent.database.bundles import PhotoLoaderBundle
from tg_agent.models import (
    PhotoAutoUploadJob,
    PhotoBatchStatus,
    PhotoSendMode,
)
from tg_agent.scheduler.service import SchedulerManager
from tg_agent.services.photo_auto_upload_service import PhotoAutoUploadService
from tg_agent.services.photo_publish_service import PhotoPublishService
from tg_agent.services.photo_task_service import PhotoTarget, PhotoTaskService


@pytest.mark.anyio
async def test_photo_task_send_now_uses_send_file(db, tmp_path, real_pool_harness_factory):
    image = tmp_path / "one.jpg"
    image.write_bytes(b"x")
    harness = real_pool_harness_factory()
    client = harness.queue_cli_client(
        phone="+7000",
        client=FakeCliTelethonClient(
            input_entity_resolver=lambda peer: SimpleNamespace(
                id=getattr(peer, "channel_id", getattr(peer, "user_id", peer))
            ),
            send_file_side_effect=lambda *args, **kwargs: SimpleNamespace(id=101),
        ),
    )
    await harness.connect_account("+7000", session_string="s", is_primary=True)
    await db.repos.dialog_cache.replace_dialogs(
        "+7000",
        [
            {"channel_id": -1001, "title": "Target Channel", "channel_type": "channel"},
            {"channel_id": -1002, "title": "Target Group", "channel_type": "supergroup"},
            {"channel_id": 42, "title": "Target DM", "channel_type": "dm"},
            {"channel_id": 99, "title": "Target Bot", "channel_type": "bot"},
        ],
    )

    service = PhotoTaskService(
        PhotoLoaderBundle.from_database(db), PhotoPublishService(harness.pool)
    )
    item = await service.send_now(
        phone="+7000",
        target=PhotoTarget(dialog_id=-1001),
        file_paths=[str(image)],
        mode=PhotoSendMode.ALBUM,
    )

    assert item.status == PhotoBatchStatus.COMPLETED
    client.send_file.assert_awaited_once()
    assert client.send_file.await_args.kwargs["schedule"] is None


@pytest.mark.anyio
async def test_photo_task_schedule_send_passes_schedule(db, tmp_path, real_pool_harness_factory):
    image = tmp_path / "one.jpg"
    image.write_bytes(b"x")
    harness = real_pool_harness_factory()
    client = harness.queue_cli_client(
        phone="+7000",
        client=FakeCliTelethonClient(
            input_entity_resolver=lambda peer: SimpleNamespace(
                id=getattr(peer, "channel_id", getattr(peer, "user_id", peer))
            ),
            send_file_side_effect=lambda *args, **kwargs: SimpleNamespace(id=202),
        ),
    )
    await harness.connect_account("+7000", session_string="s", is_primary=True)

    service = PhotoTaskService(
        PhotoLoaderBundle.from_database(db), PhotoPublishService(harness.pool)
    )
    schedule_at = datetime.now(timezone.utc) + timedelta(hours=1)
    item = await service.schedule_send(
        phone="+7000",
        target=PhotoTarget(dialog_id=-1001),
        file_paths=[str(image)],
        mode=PhotoSendMode.SEPARATE,
        schedule_at=schedule_at,
    )

    assert item.status == PhotoBatchStatus.SCHEDULED
    assert client.send_file.await_args.kwargs["schedule"] == schedule_at


@pytest.mark.anyio
async def test_photo_task_create_batch_holds_until_publish(db, tmp_path, real_pool_harness_factory):
    image = tmp_path / "one.jpg"
    image.write_bytes(b"x")
    harness = real_pool_harness_factory()
    harness.queue_cli_client(
        phone="+7000",
        client=FakeCliTelethonClient(
            input_entity_resolver=lambda peer: SimpleNamespace(
                id=getattr(peer, "channel_id", getattr(peer, "user_id", peer))
            ),
            send_file_side_effect=lambda *args, **kwargs: SimpleNamespace(id=303),
        ),
    )
    await harness.connect_account("+7000", session_string="s", is_primary=True)

    service = PhotoTaskService(
        PhotoLoaderBundle.from_database(db), PhotoPublishService(harness.pool)
    )
    batch_id = await service.create_batch(
        phone="+7000",
        target=PhotoTarget(dialog_id=-1001),
        entries=[
            {
                "at": (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
                "files": [str(image)],
                "mode": "album",
            }
        ],
    )

    claimed = await db.repos.photo_loader.claim_next_due_item(datetime.now(timezone.utc))
    assert claimed is None
    processed_before_publish = await service.run_due()
    assert processed_before_publish == 0
    item = (await db.repos.photo_loader.list_items_for_batch(batch_id))[0]
    assert item.status == PhotoBatchStatus.HELD

    published = await service.publish_batch(batch_id)
    assert published == 1
    claimed = await db.repos.photo_loader.claim_next_due_item(datetime.now(timezone.utc))
    assert claimed is not None
    assert claimed.status == PhotoBatchStatus.RUNNING
    await db.repos.photo_loader.update_item(claimed.id or 0, status=PhotoBatchStatus.PENDING)

    processed = await service.run_due()
    assert processed == 1
    items = await service.list_items()
    item = next(item for item in items if item.batch_id == batch_id)
    assert item.status == PhotoBatchStatus.COMPLETED


@pytest.mark.anyio
async def test_photo_task_create_batch_error_leaves_no_partial_batch(db, tmp_path):
    """#1601: ошибка валидации любого entry не оставляет в БД батч и элементы."""
    image = tmp_path / "one.jpg"
    image.write_bytes(b"x")
    service = PhotoTaskService(PhotoLoaderBundle.from_database(db), MagicMock())

    with pytest.raises(ValueError, match="File not found"):
        await service.create_batch(
            phone="+7000",
            target=PhotoTarget(dialog_id=-1001),
            entries=[
                {
                    "at": "2026-12-01T10:00:00+00:00",
                    "files": [str(image)],
                    "caption": "Первый пост",
                },
                {
                    "at": "2026-12-02T10:00:00+00:00",
                    "files": [str(tmp_path / "missing.jpg")],
                    "caption": "Второй пост",
                },
            ],
        )

    assert await db.repos.photo_loader.list_batches() == []
    assert await db.repos.photo_loader.list_items() == []


@pytest.mark.anyio
async def test_photo_task_create_batch_write_error_rolls_back_batch(db, tmp_path, monkeypatch):
    """#1601: сбой записи после валидации откатывает и батч, и уже вставленные элементы."""
    from tg_agent.database.repositories import photo_loader as photo_loader_repo_module

    image = tmp_path / "one.jpg"
    image.write_bytes(b"x")
    service = PhotoTaskService(PhotoLoaderBundle.from_database(db), MagicMock())

    def _boom(*args, **kwargs):
        raise RuntimeError("insert failed")

    monkeypatch.setattr(photo_loader_repo_module, "safe_json_dumps", _boom)

    with pytest.raises(RuntimeError, match="insert failed"):
        await service.create_batch(
            phone="+7000",
            target=PhotoTarget(dialog_id=-1001),
            entries=[
                {"at": "2026-12-01T10:00:00+00:00", "files": [str(image)]},
                {"at": "2026-12-02T10:00:00+00:00", "files": [str(image)]},
            ],
        )

    assert await db.repos.photo_loader.list_batches() == []
    assert await db.repos.photo_loader.list_items() == []


@pytest.mark.anyio
async def test_photo_auto_upload_sends_only_new_files(db, tmp_path, real_pool_harness_factory):
    folder = tmp_path / "photos"
    folder.mkdir()
    first = folder / "1.jpg"
    second = folder / "2.jpg"
    first.write_bytes(b"1")
    second.write_bytes(b"2")

    harness = real_pool_harness_factory()
    client = harness.queue_cli_client(
        phone="+7000",
        client=FakeCliTelethonClient(
            input_entity_resolver=lambda peer: SimpleNamespace(
                id=getattr(peer, "channel_id", getattr(peer, "user_id", peer))
            ),
            send_file_side_effect=lambda *args, **kwargs: [
                SimpleNamespace(id=1),
                SimpleNamespace(id=2),
            ],
        ),
    )
    await harness.connect_account("+7000", session_string="s", is_primary=True)

    service = PhotoAutoUploadService(
        PhotoLoaderBundle.from_database(db), PhotoPublishService(harness.pool)
    )
    job_id = await service.create_job(
        PhotoAutoUploadJob(
            phone="+7000",
            target_dialog_id=-1001,
            folder_path=str(folder),
            send_mode=PhotoSendMode.ALBUM,
            interval_minutes=1,
        )
    )

    sent = await service.run_job(job_id)
    assert sent == 2
    client.send_file.assert_awaited_once()

    client.send_file.reset_mock()
    sent_again = await service.run_job(job_id)
    assert sent_again == 0
    client.send_file.assert_not_called()


@pytest.mark.anyio
async def test_scheduler_registers_photo_jobs():
    collector = MagicMock()
    collector.collect_all_channels = AsyncMock(return_value={"channels": 0})
    collector.is_running = False
    bundle = MagicMock()
    bundle.get_setting = AsyncMock(return_value=None)
    bundle.list_notification_queries = AsyncMock(return_value=[])
    photo_tasks = MagicMock()
    photo_tasks.run_due = AsyncMock(return_value=1)
    photo_auto = MagicMock()
    photo_auto.run_due = AsyncMock(return_value=2)

    task_enqueuer = MagicMock()
    task_enqueuer.enqueue_photo_due = AsyncMock()
    task_enqueuer.enqueue_photo_auto = AsyncMock()

    manager = SchedulerManager(
        AppConfig().scheduler,
        scheduler_bundle=bundle,
        task_enqueuer=task_enqueuer,
    )
    await manager.start()
    job_ids = {job.id for job in manager._scheduler.get_jobs()}
    assert "photo_due" in job_ids
    assert "photo_auto" in job_ids
    await manager.stop()


def test_photo_loader_cli_parser():
    from typer.testing import CliRunner

    from tg_agent.cli.typer_app import app

    mock_impl = MagicMock()
    with (
        patch("tg_agent.cli.typer_commands.photo_loader_cmd.schedule_send_impl", mock_impl),
        patch("tg_agent.cli.typer_commands.run_async"),
    ):
        result = CliRunner().invoke(
            app,
            [
                "photo-loader",
                "schedule-send",
                "--phone",
                "+7000",
                "--target",
                "-1001",
                "--files",
                "/tmp/a.jpg",
                "--files",
                "/tmp/b.jpg",
                "--mode",
                "album",
                "--at",
                "2026-03-11T18:30:00+00:00",
            ],
        )
    assert result.exit_code == 0
    kwargs = mock_impl.call_args.kwargs
    assert kwargs["files"] == ["/tmp/a.jpg", "/tmp/b.jpg"]
    assert kwargs["mode"] == "album"


@pytest.mark.aiosqlite_serial
def test_photo_loader_cli_send_command(tmp_path, cli_init_patch, capsys):
    image = tmp_path / "one.jpg"
    image.write_bytes(b"x")
    db = Database(str(tmp_path / "cli.db"))
    asyncio.run(db.initialize())

    fake_pool = AsyncMock()
    fake_pool.disconnect_all = AsyncMock()
    fake_pool.release_client = AsyncMock()
    send_client = SimpleNamespace(send_file=AsyncMock(return_value=SimpleNamespace(id=1)))
    fake_pool.get_client_by_phone = AsyncMock(return_value=(send_client, "+7000"))

    async def fake_init_pool(config, db):
        from tg_shared.auth import TelegramAuth

        return TelegramAuth(0, ""), fake_pool

    with (
        cli_init_patch(db, "tg_agent.cli.runtime.init_db", config=AppConfig()),
        patch("tg_agent.cli.runtime.init_pool", side_effect=fake_init_pool),
    ):
        from tg_agent.cli.commands.photo_loader import run

        run(
            argparse.Namespace(
                config="config.yaml",
                photo_loader_action="send",
                phone="+7000",
                target="-1001",
                files=[str(image)],
                mode="album",
                caption=None,
            )
        )
    assert "Sent photo item" in capsys.readouterr().out
    asyncio.run(db.close())
