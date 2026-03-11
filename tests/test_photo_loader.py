from __future__ import annotations

import argparse
import asyncio
import base64
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.cli.parser import build_parser
from src.config import AppConfig
from src.database import Database
from src.database.bundles import PhotoLoaderBundle
from src.models import (
    Account,
    PhotoAutoUploadJob,
    PhotoBatch,
    PhotoBatchItem,
    PhotoBatchStatus,
    PhotoSendMode,
)
from src.scheduler.manager import SchedulerManager
from src.services.photo_auto_upload_service import PhotoAutoUploadService
from src.services.photo_publish_service import PhotoPublishService
from src.services.photo_task_service import PhotoTarget, PhotoTaskService
from src.telegram.collector import Collector
from src.web.app import create_app


async def _build_photo_loader_app(tmp_path, dialogs=None, dialogs_error=None):
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db

    mock_client = MagicMock()

    async def _get_dialogs_for_phone(self, phone, include_dm=False, mode="full", refresh=False):
        if dialogs_error is not None:
            raise dialogs_error
        return dialogs or [
            {"channel_id": -1001, "title": "Target Channel", "channel_type": "channel"}
        ]

    async def _get_client_by_phone(self, phone):
        return mock_client, phone

    async def _release_client(self, phone):
        return None

    app.state.pool = type(
        "Pool",
        (),
        {
            "clients": {"+7000": mock_client},
            "get_dialogs_for_phone": _get_dialogs_for_phone,
            "get_client_by_phone": _get_client_by_phone,
            "release_client": _release_client,
        },
    )()
    from src.telegram.auth import TelegramAuth

    app.state.auth = TelegramAuth(12345, "hash")
    app.state.collector = Collector(app.state.pool, db, config.scheduler)
    app.state.search_engine = MagicMock()
    app.state.ai_search = MagicMock()
    app.state.scheduler = SchedulerManager(app.state.collector, config.scheduler)
    app.state.session_secret = "secret"
    await db.add_account(Account(phone="+7000", session_string="s"))

    return app, db


@pytest.mark.asyncio
async def test_photo_task_send_now_uses_send_file(db, tmp_path):
    image = tmp_path / "one.jpg"
    image.write_bytes(b"x")
    client = MagicMock()
    client.send_file = AsyncMock(return_value=SimpleNamespace(id=101))
    pool = MagicMock()
    pool.get_client_by_phone = AsyncMock(return_value=(client, "+7000"))
    pool.release_client = AsyncMock()

    service = PhotoTaskService(PhotoLoaderBundle.from_database(db), PhotoPublishService(pool))
    item = await service.send_now(
        phone="+7000",
        target=PhotoTarget(dialog_id=-1001),
        file_paths=[str(image)],
        mode=PhotoSendMode.ALBUM,
    )

    assert item.status == PhotoBatchStatus.COMPLETED
    client.send_file.assert_awaited_once()
    assert client.send_file.await_args.kwargs["schedule"] is None


@pytest.mark.asyncio
async def test_photo_task_schedule_send_passes_schedule(db, tmp_path):
    image = tmp_path / "one.jpg"
    image.write_bytes(b"x")
    client = MagicMock()
    client.send_file = AsyncMock(return_value=SimpleNamespace(id=202))
    pool = MagicMock()
    pool.get_client_by_phone = AsyncMock(return_value=(client, "+7000"))
    pool.release_client = AsyncMock()

    service = PhotoTaskService(PhotoLoaderBundle.from_database(db), PhotoPublishService(pool))
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


@pytest.mark.asyncio
async def test_photo_task_run_due_processes_pending_items(db, tmp_path):
    image = tmp_path / "one.jpg"
    image.write_bytes(b"x")
    client = MagicMock()
    client.send_file = AsyncMock(return_value=SimpleNamespace(id=303))
    pool = MagicMock()
    pool.get_client_by_phone = AsyncMock(return_value=(client, "+7000"))
    pool.release_client = AsyncMock()

    service = PhotoTaskService(PhotoLoaderBundle.from_database(db), PhotoPublishService(pool))
    batch_id = await service.create_batch(
        phone="+7000",
        target=PhotoTarget(dialog_id=-1001),
        entries=[{
            "at": (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
            "files": [str(image)],
            "mode": "album",
        }],
    )

    processed = await service.run_due()
    assert processed == 1
    items = await service.list_items()
    item = next(item for item in items if item.batch_id == batch_id)
    assert item.status == PhotoBatchStatus.COMPLETED


@pytest.mark.asyncio
async def test_photo_auto_upload_sends_only_new_files(db, tmp_path):
    folder = tmp_path / "photos"
    folder.mkdir()
    first = folder / "1.jpg"
    second = folder / "2.jpg"
    first.write_bytes(b"1")
    second.write_bytes(b"2")

    client = MagicMock()
    client.send_file = AsyncMock(return_value=[SimpleNamespace(id=1), SimpleNamespace(id=2)])
    pool = MagicMock()
    pool.get_client_by_phone = AsyncMock(return_value=(client, "+7000"))
    pool.release_client = AsyncMock()

    service = PhotoAutoUploadService(PhotoLoaderBundle.from_database(db), PhotoPublishService(pool))
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


@pytest.mark.asyncio
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

    manager = SchedulerManager(
        collector,
        AppConfig().scheduler,
        scheduler_bundle=bundle,
        photo_task_service=photo_tasks,
        photo_auto_upload_service=photo_auto,
    )
    await manager.start()
    job_ids = {job.id for job in manager._scheduler.get_jobs()}
    assert "photo_due" in job_ids
    assert "photo_auto" in job_ids
    await manager.stop()


@pytest.mark.asyncio
async def test_photo_loader_page_renders(tmp_path):
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db

    mock_client = MagicMock()
    mock_client.send_file = AsyncMock(return_value=SimpleNamespace(id=1))

    async def _get_dialogs_for_phone(self, phone, include_dm=False, mode="full", refresh=False):
        return [
            {"channel_id": -1001, "title": "Target Channel", "channel_type": "channel"},
            {"channel_id": -1002, "title": "Target Group", "channel_type": "supergroup"},
            {"channel_id": 42, "title": "Target DM", "channel_type": "dm"},
            {"channel_id": 99, "title": "Target Bot", "channel_type": "bot"},
        ]

    async def _get_client_by_phone(self, phone):
        return mock_client, phone

    async def _release_client(self, phone):
        return None

    app.state.pool = type(
        "Pool",
        (),
        {
            "clients": {"+7000": mock_client},
            "get_dialogs_for_phone": _get_dialogs_for_phone,
            "get_client_by_phone": _get_client_by_phone,
            "release_client": _release_client,
        },
    )()
    from src.telegram.auth import TelegramAuth

    app.state.auth = TelegramAuth(12345, "hash")
    app.state.collector = Collector(app.state.pool, db, config.scheduler)
    app.state.search_engine = MagicMock()
    app.state.ai_search = MagicMock()
    app.state.scheduler = SchedulerManager(app.state.collector, config.scheduler)
    app.state.session_secret = "secret"
    await db.add_account(Account(phone="+7000", session_string="s"))

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}"},
    ) as client:
        resp = await client.get("/my-telegram/photos?phone=%2B7000")
        assert resp.status_code == 200
        assert "Photo Loader" in resp.text
        assert "Автозагрузка из папки" in resp.text
        assert 'action="/my-telegram/photos/refresh"' in resp.text
        assert "Обновить диалоги" in resp.text
        assert "Используется сохранённый список диалогов" in resp.text
        assert resp.text.count('option value="separate" selected') >= 3
        assert 'data-target-picker' in resp.text
        assert 'data-target-search' in resp.text
        assert 'data-target-filter="channel"' in resp.text
        assert 'data-target-filter="group"' in resp.text
        assert 'data-target-filter="dm"' in resp.text
        assert resp.text.count('name="target_dialog_id"') == 4
        assert 'select name="target_dialog_id"' not in resp.text
        assert resp.text.count('data-photo-submit-form') >= 4
        assert resp.text.count('data-photo-submit-button') >= 4
        assert resp.text.count('data-photo-submit-status') >= 4
        assert "Цель не выбрана" in resp.text
        assert "Выбор сохранится на текущую браузерную сессию." in resp.text
        assert 'data-target-phone="+7000"' in resp.text
        assert 'data-initial-target-id=' not in resp.text
        assert 'name="target_title" value="Target Channel"' not in resp.text
        assert 'name="target_type" value="channel"' not in resp.text
        assert "sessionStorage.setItem(storageKey" in resp.text
        assert "sessionStorage.getItem(storageKey)" in resp.text
        assert "photo_loader_target:" in resp.text
        assert "resetSelectionUI();" in resp.text
        assert "clearSelection();" not in resp.text
        assert "Запрос отправлен, ожидаем ответ сервера..." in resp.text
        assert "Target Channel" in resp.text
        assert "Target Group" in resp.text
        assert "Target DM" in resp.text
        assert "Target Bot" not in resp.text
        assert "summary.innerHTML" not in resp.text
        assert "summary.replaceChildren()" in resp.text
        assert "Последние batches" in resp.text
        assert 'role="alert"\n             data-photo-feedback' not in resp.text

    await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("query", "expected_title", "expected_text", "highlight_kind"),
    [
        (
            "msg=photo_sent",
            "Отправка завершена",
            "Фото успешно отправлены в Target Channel.",
            "item",
        ),
        (
            "msg=photo_scheduled",
            "Отложка создана",
            "Отложенная отправка создана для Target Channel.",
            "item",
        ),
        (
            "msg=photo_batch_created",
            "Batch создан",
            "Batch photo tasks создан для Target Channel.",
            "batch",
        ),
        (
            "msg=photo_auto_created",
            "Авто-загрузка настроена",
            "Авто-джоб создан для Target Channel.",
            "auto",
        ),
        ("error=photo_send_failed", "Отправка не выполнена", "Не удалось отправить фото.", ""),
    ],
)
async def test_photo_loader_page_feedback_panel_and_highlight_hooks(
    tmp_path,
    query,
    expected_title,
    expected_text,
    highlight_kind,
):
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db

    mock_client = MagicMock()

    async def _get_dialogs_for_phone(self, phone, include_dm=False, mode="full", refresh=False):
        return [{"channel_id": -1001, "title": "Target Channel", "channel_type": "channel"}]

    async def _get_client_by_phone(self, phone):
        return mock_client, phone

    async def _release_client(self, phone):
        return None

    app.state.pool = type(
        "Pool",
        (),
        {
            "clients": {"+7000": mock_client},
            "get_dialogs_for_phone": _get_dialogs_for_phone,
            "get_client_by_phone": _get_client_by_phone,
            "release_client": _release_client,
        },
    )()
    from src.telegram.auth import TelegramAuth

    app.state.auth = TelegramAuth(12345, "hash")
    app.state.collector = Collector(app.state.pool, db, config.scheduler)
    app.state.search_engine = MagicMock()
    app.state.ai_search = MagicMock()
    app.state.scheduler = SchedulerManager(app.state.collector, config.scheduler)
    app.state.session_secret = "secret"
    await db.add_account(Account(phone="+7000", session_string="s"))

    bundle = PhotoLoaderBundle.from_database(db)
    batch_id = await bundle.create_batch(
        PhotoBatch(
            phone="+7000",
            target_dialog_id=-1001,
            target_title="Target Channel",
            target_type="channel",
            send_mode=PhotoSendMode.SEPARATE,
            status=PhotoBatchStatus.COMPLETED,
        )
    )
    await bundle.create_item(
        PhotoBatchItem(
            batch_id=batch_id,
            phone="+7000",
            target_dialog_id=-1001,
            target_title="Target Channel",
            target_type="channel",
            file_paths=["/tmp/a.jpg"],
            send_mode=PhotoSendMode.SEPARATE,
            status=PhotoBatchStatus.COMPLETED,
        )
    )
    await bundle.create_auto_job(
        PhotoAutoUploadJob(
            phone="+7000",
            target_dialog_id=-1001,
            target_title="Target Channel",
            target_type="channel",
            folder_path="/tmp/photos",
            send_mode=PhotoSendMode.SEPARATE,
            interval_minutes=60,
            is_active=True,
        )
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}"},
    ) as client:
        resp = await client.get(f"/my-telegram/photos?phone=%2B7000&{query}")

    assert resp.status_code == 200
    assert expected_title in resp.text
    assert expected_text in resp.text
    assert 'data-photo-feedback' in resp.text
    assert f'data-highlight-kind="{highlight_kind}"' in resp.text
    assert 'data-photo-result-row="item"' in resp.text
    assert 'data-photo-result-row="batch"' in resp.text
    assert 'data-photo-result-row="auto"' in resp.text

    await db.close()


@pytest.mark.asyncio
async def test_photo_loader_page_without_phone_selects_first_account(tmp_path):
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db

    mock_client_a = MagicMock()
    mock_client_b = MagicMock()
    seen_phones: list[str] = []

    async def _get_dialogs_for_phone(self, phone, include_dm=False, mode="full", refresh=False):
        seen_phones.append(phone)
        return [{"channel_id": -1001, "title": f"Target {phone}", "channel_type": "channel"}]

    async def _get_client_by_phone(self, phone):
        return mock_client_a, phone

    async def _release_client(self, phone):
        return None

    app.state.pool = type(
        "Pool",
        (),
        {
            "clients": {"+7999": mock_client_b, "+7000": mock_client_a},
            "get_dialogs_for_phone": _get_dialogs_for_phone,
            "get_client_by_phone": _get_client_by_phone,
            "release_client": _release_client,
        },
    )()
    from src.telegram.auth import TelegramAuth

    app.state.auth = TelegramAuth(12345, "hash")
    app.state.collector = Collector(app.state.pool, db, config.scheduler)
    app.state.search_engine = MagicMock()
    app.state.ai_search = MagicMock()
    app.state.scheduler = SchedulerManager(app.state.collector, config.scheduler)
    app.state.session_secret = "secret"
    await db.add_account(Account(phone="+7000", session_string="a"))
    await db.add_account(Account(phone="+7999", session_string="b"))

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}"},
    ) as client:
        resp = await client.get("/my-telegram/photos")
        assert resp.status_code == 200
        assert "Photo Loader" in resp.text
        assert 'option value="+7000" selected' in resp.text
        assert "Target +7000" in resp.text
        assert 'data-target-phone="+7000"' in resp.text
        assert 'data-initial-target-id=' not in resp.text
        assert 'name="target_title" value="Target +7000"' not in resp.text
        assert 'name="target_type" value="channel"' not in resp.text
        assert "Цель не выбрана" in resp.text

    assert seen_phones == ["+7000"]
    await db.close()


@pytest.mark.asyncio
async def test_photo_loader_page_without_selectable_targets_disables_forms(tmp_path):
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db

    mock_client = MagicMock()

    async def _get_dialogs_for_phone(self, phone, include_dm=False, mode="full", refresh=False):
        return [{"channel_id": 77, "title": "Only Bot", "channel_type": "bot"}]

    async def _get_client_by_phone(self, phone):
        return mock_client, phone

    async def _release_client(self, phone):
        return None

    app.state.pool = type(
        "Pool",
        (),
        {
            "clients": {"+7000": mock_client},
            "get_dialogs_for_phone": _get_dialogs_for_phone,
            "get_client_by_phone": _get_client_by_phone,
            "release_client": _release_client,
        },
    )()
    from src.telegram.auth import TelegramAuth

    app.state.auth = TelegramAuth(12345, "hash")
    app.state.collector = Collector(app.state.pool, db, config.scheduler)
    app.state.search_engine = MagicMock()
    app.state.ai_search = MagicMock()
    app.state.scheduler = SchedulerManager(app.state.collector, config.scheduler)
    app.state.session_secret = "secret"
    await db.add_account(Account(phone="+7000", session_string="s"))

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}"},
    ) as client:
        resp = await client.get("/my-telegram/photos?phone=%2B7000")
        assert resp.status_code == 200
        assert "нет доступных целей отправки" in resp.text.lower()
        assert 'id="photo-target-picker"' not in resp.text
        assert resp.text.count("disabled") >= 4

    await db.close()


@pytest.mark.asyncio
async def test_photo_loader_page_without_accounts_renders_empty_state(tmp_path):
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db
    app.state.pool = type("Pool", (), {"clients": {}})()
    from src.telegram.auth import TelegramAuth

    app.state.auth = TelegramAuth(12345, "hash")
    app.state.collector = Collector(app.state.pool, db, config.scheduler)
    app.state.search_engine = MagicMock()
    app.state.ai_search = MagicMock()
    app.state.scheduler = SchedulerManager(app.state.collector, config.scheduler)
    app.state.session_secret = "secret"

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}"},
    ) as client:
        resp = await client.get("/my-telegram/photos")
        assert resp.status_code == 200
        assert "Нет подключённых аккаунтов." in resp.text
        assert "Добавьте аккаунт" in resp.text

    await db.close()


@pytest.mark.asyncio
async def test_photo_loader_refresh_warms_dialog_cache(tmp_path):
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db

    mock_client = MagicMock()

    seen_refresh_values: list[bool] = []

    async def _get_dialogs_for_phone(self, phone, include_dm=False, mode="full", refresh=False):
        seen_refresh_values.append(refresh)
        return [{"channel_id": -1001, "title": "Target Channel", "channel_type": "channel"}]

    async def _get_client_by_phone(self, phone):
        return mock_client, phone

    async def _release_client(self, phone):
        return None

    app.state.pool = type(
        "Pool",
        (),
        {
            "clients": {"+7000": mock_client},
            "get_dialogs_for_phone": _get_dialogs_for_phone,
            "get_client_by_phone": _get_client_by_phone,
            "release_client": _release_client,
        },
    )()
    from src.telegram.auth import TelegramAuth

    app.state.auth = TelegramAuth(12345, "hash")
    app.state.collector = Collector(app.state.pool, db, config.scheduler)
    app.state.search_engine = MagicMock()
    app.state.ai_search = MagicMock()
    app.state.scheduler = SchedulerManager(app.state.collector, config.scheduler)
    app.state.session_secret = "secret"
    await db.add_account(Account(phone="+7000", session_string="s"))

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}"},
    ) as client:
        resp = await client.post("/my-telegram/photos/refresh", data={"phone": "+7000"})
        assert resp.status_code == 200
        assert "Photo Loader" in resp.text
        assert "Target Channel" in resp.text

    assert seen_refresh_values == [True, False]
    await db.close()


def test_photo_loader_cli_parser():
    parser = build_parser()
    args = parser.parse_args(
        [
            "photo-loader",
            "schedule-send",
            "--phone", "+7000",
            "--target", "-1001",
            "--files", "/tmp/a.jpg", "/tmp/b.jpg",
            "--mode", "album",
            "--at", "2026-03-11T18:30:00+00:00",
        ]
    )
    assert args.command == "photo-loader"
    assert args.photo_loader_action == "schedule-send"
    assert args.mode == "album"


def test_photo_loader_cli_send_command(tmp_path, capsys):
    image = tmp_path / "one.jpg"
    image.write_bytes(b"x")
    db = Database(str(tmp_path / "cli.db"))
    asyncio.run(db.initialize())

    fake_pool = AsyncMock()
    fake_pool.disconnect_all = AsyncMock()
    fake_pool.release_client = AsyncMock()
    send_client = SimpleNamespace(
        send_file=AsyncMock(return_value=SimpleNamespace(id=1))
    )
    fake_pool.get_client_by_phone = AsyncMock(
        return_value=(send_client, "+7000")
    )

    async def fake_init_pool(config, db):
        from src.telegram.auth import TelegramAuth
        return TelegramAuth(0, ""), fake_pool

    async def fake_init_db(config_path):
        return AppConfig(), db

    with (
        patch("src.cli.runtime.init_db", side_effect=fake_init_db),
        patch("src.cli.runtime.init_pool", side_effect=fake_init_pool),
    ):
        from src.cli.commands.photo_loader import run

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


@pytest.mark.asyncio
async def test_photo_schedule_logs_exception(tmp_path, caplog):
    app, db = await _build_photo_loader_app(tmp_path)
    app.state.photo_task_service = SimpleNamespace(
        schedule_send=AsyncMock(side_effect=RuntimeError("boom")),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}"},
    ) as client:
        with caplog.at_level(logging.ERROR, logger="src.web.routes.photo_loader"):
            resp = await client.post(
                "/my-telegram/photos/schedule",
                data={
                    "phone": "+7000",
                    "target_dialog_id": "-1001",
                    "target_title": "Target Channel",
                    "target_type": "channel",
                    "send_mode": "separate",
                    "caption": "caption",
                    "schedule_at": "2026-03-11T18:30:00+00:00",
                },
                files={"photos": ("one.jpg", b"x", "image/jpeg")},
                follow_redirects=False,
            )

    assert resp.status_code == 303
    assert "error=photo_schedule_failed" in resp.headers["location"]
    assert "Photo schedule failed" in caplog.text
    await db.close()


@pytest.mark.asyncio
async def test_photo_schedule_redirects_when_target_validation_raises(tmp_path, caplog):
    app, db = await _build_photo_loader_app(tmp_path, dialogs_error=RuntimeError("dialogs boom"))
    app.state.photo_task_service = SimpleNamespace(
        schedule_send=AsyncMock(),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}"},
    ) as client:
        with caplog.at_level(logging.ERROR, logger="src.web.routes.photo_loader"):
            resp = await client.post(
                "/my-telegram/photos/schedule",
                data={
                    "phone": "+7000",
                    "target_dialog_id": "-1001",
                    "target_title": "Target Channel",
                    "target_type": "channel",
                    "send_mode": "separate",
                    "caption": "caption",
                    "schedule_at": "2026-03-11T18:30:00+00:00",
                },
                files={"photos": ("one.jpg", b"x", "image/jpeg")},
                follow_redirects=False,
            )

    assert resp.status_code == 303
    assert "error=photo_schedule_failed" in resp.headers["location"]
    app.state.photo_task_service.schedule_send.assert_not_awaited()
    assert "Photo schedule failed" in caplog.text
    await db.close()


@pytest.mark.asyncio
async def test_photo_schedule_requires_target_selection(tmp_path):
    app, db = await _build_photo_loader_app(tmp_path)
    app.state.photo_task_service = SimpleNamespace(
        schedule_send=AsyncMock(),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}"},
    ) as client:
        resp = await client.post(
            "/my-telegram/photos/schedule",
            data={
                "phone": "+7000",
                "target_dialog_id": "",
                "target_title": "",
                "target_type": "",
                "send_mode": "separate",
                "caption": "caption",
                "schedule_at": "2026-03-11T18:30:00+00:00",
            },
            files={"photos": ("one.jpg", b"x", "image/jpeg")},
            follow_redirects=False,
        )

    assert resp.status_code == 303
    assert "error=photo_target_required" in resp.headers["location"]
    app.state.photo_task_service.schedule_send.assert_not_awaited()
    await db.close()


@pytest.mark.asyncio
async def test_photo_schedule_rejects_unknown_target(tmp_path):
    app, db = await _build_photo_loader_app(tmp_path)
    app.state.photo_task_service = SimpleNamespace(
        schedule_send=AsyncMock(),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}"},
    ) as client:
        resp = await client.post(
            "/my-telegram/photos/schedule",
            data={
                "phone": "+7000",
                "target_dialog_id": "9999",
                "target_title": "Ghost",
                "target_type": "channel",
                "send_mode": "separate",
                "caption": "caption",
                "schedule_at": "2026-03-11T18:30:00+00:00",
            },
            files={"photos": ("one.jpg", b"x", "image/jpeg")},
            follow_redirects=False,
        )

    assert resp.status_code == 303
    assert "error=photo_target_invalid" in resp.headers["location"]
    app.state.photo_task_service.schedule_send.assert_not_awaited()
    await db.close()


@pytest.mark.asyncio
async def test_photo_send_logs_exception(tmp_path, caplog):
    app, db = await _build_photo_loader_app(tmp_path)
    app.state.photo_task_service = SimpleNamespace(
        send_now=AsyncMock(side_effect=RuntimeError("boom")),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}"},
    ) as client:
        with caplog.at_level(logging.ERROR, logger="src.web.routes.photo_loader"):
            resp = await client.post(
                "/my-telegram/photos/send",
                data={
                    "phone": "+7000",
                    "target_dialog_id": "-1001",
                    "target_title": "Target Channel",
                    "target_type": "channel",
                    "send_mode": "separate",
                    "caption": "caption",
                },
                files={"photos": ("one.jpg", b"x", "image/jpeg")},
                follow_redirects=False,
            )

    assert resp.status_code == 303
    assert "error=photo_send_failed" in resp.headers["location"]
    assert "Photo send failed" in caplog.text
    await db.close()


@pytest.mark.asyncio
async def test_photo_send_redirects_when_target_validation_raises(tmp_path, caplog):
    app, db = await _build_photo_loader_app(tmp_path, dialogs_error=RuntimeError("dialogs boom"))
    app.state.photo_task_service = SimpleNamespace(
        send_now=AsyncMock(),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}"},
    ) as client:
        with caplog.at_level(logging.ERROR, logger="src.web.routes.photo_loader"):
            resp = await client.post(
                "/my-telegram/photos/send",
                data={
                    "phone": "+7000",
                    "target_dialog_id": "-1001",
                    "target_title": "Target Channel",
                    "target_type": "channel",
                    "send_mode": "separate",
                    "caption": "caption",
                },
                files={"photos": ("one.jpg", b"x", "image/jpeg")},
                follow_redirects=False,
            )

    assert resp.status_code == 303
    assert "error=photo_send_failed" in resp.headers["location"]
    app.state.photo_task_service.send_now.assert_not_awaited()
    assert "Photo send failed" in caplog.text
    await db.close()


@pytest.mark.asyncio
async def test_photo_send_requires_target_selection(tmp_path):
    app, db = await _build_photo_loader_app(tmp_path)
    app.state.photo_task_service = SimpleNamespace(
        send_now=AsyncMock(),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}"},
    ) as client:
        resp = await client.post(
            "/my-telegram/photos/send",
            data={
                "phone": "+7000",
                "target_dialog_id": "",
                "target_title": "",
                "target_type": "",
                "send_mode": "separate",
                "caption": "caption",
            },
            files={"photos": ("one.jpg", b"x", "image/jpeg")},
            follow_redirects=False,
        )

    assert resp.status_code == 303
    assert "error=photo_target_required" in resp.headers["location"]
    app.state.photo_task_service.send_now.assert_not_awaited()
    await db.close()


@pytest.mark.asyncio
async def test_photo_batch_logs_exception(tmp_path, caplog):
    app, db = await _build_photo_loader_app(tmp_path)
    app.state.photo_task_service = SimpleNamespace(
        create_batch=AsyncMock(side_effect=RuntimeError("boom")),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}"},
    ) as client:
        with caplog.at_level(logging.ERROR, logger="src.web.routes.photo_loader"):
            resp = await client.post(
                "/my-telegram/photos/batch",
                data={
                    "phone": "+7000",
                    "target_dialog_id": "-1001",
                    "target_title": "Target Channel",
                    "target_type": "channel",
                    "caption": "caption",
                    "manifest_text": '[{"files":["/tmp/a.jpg"],"at":"2026-03-11T18:30:00+00:00"}]',
                },
                follow_redirects=False,
            )

    assert resp.status_code == 303
    assert "error=photo_batch_failed" in resp.headers["location"]
    assert "Photo batch creation failed" in caplog.text
    await db.close()


@pytest.mark.asyncio
async def test_photo_batch_redirects_when_target_validation_raises(tmp_path, caplog):
    app, db = await _build_photo_loader_app(tmp_path, dialogs_error=RuntimeError("dialogs boom"))
    app.state.photo_task_service = SimpleNamespace(
        create_batch=AsyncMock(),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}"},
    ) as client:
        with caplog.at_level(logging.ERROR, logger="src.web.routes.photo_loader"):
            resp = await client.post(
                "/my-telegram/photos/batch",
                data={
                    "phone": "+7000",
                    "target_dialog_id": "-1001",
                    "target_title": "Target Channel",
                    "target_type": "channel",
                    "caption": "caption",
                    "manifest_text": '[{"files":["/tmp/a.jpg"],"at":"2026-03-11T18:30:00+00:00"}]',
                },
                follow_redirects=False,
            )

    assert resp.status_code == 303
    assert "error=photo_batch_failed" in resp.headers["location"]
    app.state.photo_task_service.create_batch.assert_not_awaited()
    assert "Photo batch creation failed" in caplog.text
    await db.close()


@pytest.mark.asyncio
async def test_photo_batch_rejects_unknown_target(tmp_path):
    app, db = await _build_photo_loader_app(tmp_path)
    app.state.photo_task_service = SimpleNamespace(
        create_batch=AsyncMock(),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}"},
    ) as client:
        resp = await client.post(
            "/my-telegram/photos/batch",
            data={
                "phone": "+7000",
                "target_dialog_id": "9999",
                "target_title": "Ghost",
                "target_type": "channel",
                "caption": "caption",
                "manifest_text": '[{"files":["/tmp/a.jpg"],"at":"2026-03-11T18:30:00+00:00"}]',
            },
            follow_redirects=False,
        )

    assert resp.status_code == 303
    assert "error=photo_target_invalid" in resp.headers["location"]
    app.state.photo_task_service.create_batch.assert_not_awaited()
    await db.close()


@pytest.mark.asyncio
async def test_photo_send_rejects_bot_target(tmp_path):
    app, db = await _build_photo_loader_app(
        tmp_path,
        dialogs=[
            {"channel_id": -1001, "title": "Target Channel", "channel_type": "channel"},
            {"channel_id": 99, "title": "Target Bot", "channel_type": "bot"},
        ],
    )
    app.state.photo_task_service = SimpleNamespace(
        send_now=AsyncMock(),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}"},
    ) as client:
        resp = await client.post(
            "/my-telegram/photos/send",
            data={
                "phone": "+7000",
                "target_dialog_id": "99",
                "target_title": "Target Bot",
                "target_type": "bot",
                "send_mode": "separate",
                "caption": "caption",
            },
            files={"photos": ("one.jpg", b"x", "image/jpeg")},
            follow_redirects=False,
        )

    assert resp.status_code == 303
    assert "error=photo_target_invalid" in resp.headers["location"]
    app.state.photo_task_service.send_now.assert_not_awaited()
    await db.close()


@pytest.mark.asyncio
async def test_photo_auto_logs_exception(tmp_path, caplog):
    app, db = await _build_photo_loader_app(tmp_path)
    app.state.photo_auto_upload_service = SimpleNamespace(
        create_job=AsyncMock(side_effect=RuntimeError("boom")),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}"},
    ) as client:
        with caplog.at_level(logging.ERROR, logger="src.web.routes.photo_loader"):
            resp = await client.post(
                "/my-telegram/photos/auto",
                data={
                    "phone": "+7000",
                    "target_dialog_id": "-1001",
                    "target_title": "Target Channel",
                    "target_type": "channel",
                    "folder_path": "/tmp/photos",
                    "send_mode": "separate",
                    "caption": "caption",
                    "interval_minutes": "30",
                },
                follow_redirects=False,
            )

    assert resp.status_code == 303
    assert "error=photo_auto_failed" in resp.headers["location"]
    assert "Photo auto job creation failed" in caplog.text
    await db.close()


@pytest.mark.asyncio
async def test_photo_auto_redirects_when_target_validation_raises(tmp_path, caplog):
    app, db = await _build_photo_loader_app(tmp_path, dialogs_error=RuntimeError("dialogs boom"))
    app.state.photo_auto_upload_service = SimpleNamespace(
        create_job=AsyncMock(),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}"},
    ) as client:
        with caplog.at_level(logging.ERROR, logger="src.web.routes.photo_loader"):
            resp = await client.post(
                "/my-telegram/photos/auto",
                data={
                    "phone": "+7000",
                    "target_dialog_id": "-1001",
                    "target_title": "Target Channel",
                    "target_type": "channel",
                    "folder_path": "/tmp/photos",
                    "send_mode": "separate",
                    "caption": "caption",
                    "interval_minutes": "30",
                },
                follow_redirects=False,
            )

    assert resp.status_code == 303
    assert "error=photo_auto_failed" in resp.headers["location"]
    app.state.photo_auto_upload_service.create_job.assert_not_awaited()
    assert "Photo auto job creation failed" in caplog.text
    await db.close()


@pytest.mark.asyncio
async def test_photo_auto_requires_target_selection(tmp_path):
    app, db = await _build_photo_loader_app(tmp_path)
    app.state.photo_auto_upload_service = SimpleNamespace(
        create_job=AsyncMock(),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}"},
    ) as client:
        resp = await client.post(
            "/my-telegram/photos/auto",
            data={
                "phone": "+7000",
                "target_dialog_id": "",
                "target_title": "",
                "target_type": "",
                "folder_path": "/tmp/photos",
                "send_mode": "separate",
                "caption": "caption",
                "interval_minutes": "30",
            },
            follow_redirects=False,
        )

    assert resp.status_code == 303
    assert "error=photo_target_required" in resp.headers["location"]
    app.state.photo_auto_upload_service.create_job.assert_not_awaited()
    await db.close()


@pytest.mark.asyncio
async def test_photo_run_due_logs_exception(tmp_path, caplog):
    app, db = await _build_photo_loader_app(tmp_path)
    app.state.photo_task_service = SimpleNamespace(
        run_due=AsyncMock(side_effect=RuntimeError("boom")),
    )
    app.state.photo_auto_upload_service = SimpleNamespace(
        run_due=AsyncMock(return_value=0),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}"},
    ) as client:
        with caplog.at_level(logging.ERROR, logger="src.web.routes.photo_loader"):
            resp = await client.post(
                "/my-telegram/photos/run-due",
                data={"phone": "+7000"},
                follow_redirects=False,
            )

    assert resp.status_code == 303
    assert "error=photo_run_due_failed" in resp.headers["location"]
    assert "Photo run_due failed" in caplog.text
    await db.close()
