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
    PhotoAutoUploadJob,
    PhotoBatch,
    PhotoBatchItem,
    PhotoBatchStatus,
    PhotoSendMode,
)
from src.scheduler.service import SchedulerManager
from src.services.photo_auto_upload_service import PhotoAutoUploadService
from src.services.photo_publish_service import PhotoPublishService
from src.services.photo_task_service import PhotoTarget, PhotoTaskService
from src.telegram.collector import Collector
from src.web.app import create_app
from tests.helpers import (
    AsyncIterMessages,
    FakeCliTelethonClient,
    RealPoolHarness,
    make_test_config,
)


def _photo_dialog_from_spec(spec: dict) -> MagicMock:
    entity = SimpleNamespace(
        id=spec["channel_id"],
        username=spec.get("username"),
        creator=False,
        bot=spec.get("channel_type") == "bot",
        broadcast=spec.get("channel_type") == "channel",
        megagroup=spec.get("channel_type") == "supergroup",
        gigagroup=spec.get("channel_type") == "gigagroup",
        forum=spec.get("channel_type") == "forum",
        monoforum=spec.get("channel_type") == "monoforum",
        scam=spec.get("channel_type") == "scam",
        fake=spec.get("channel_type") == "fake",
        restricted=spec.get("channel_type") == "restricted",
    )
    dialog = MagicMock()
    dialog.entity = entity
    dialog.title = spec["title"]
    dialog.is_channel = spec.get("channel_type") not in ("dm", "bot")
    dialog.is_group = spec.get("channel_type") in ("group", "supergroup", "gigagroup", "forum")
    return dialog


def _make_photo_dialog_client(
    dialogs: list[dict] | None,
    *,
    dialogs_error: Exception | None = None,
) -> FakeCliTelethonClient:
    prepared = [
        _photo_dialog_from_spec(dialog)
        for dialog in (
            dialogs or [{"channel_id": -1001, "title": "Target Channel", "channel_type": "channel"}]
        )
    ]

    def _iter_dialogs():
        if dialogs_error is None:
            return AsyncIterMessages(prepared)

        async def _raise():
            raise dialogs_error
            yield  # pragma: no cover

        return _raise()

    return FakeCliTelethonClient(iter_dialogs_factory=_iter_dialogs)


async def _build_photo_loader_app(
    tmp_path,
    telethon_cli_spy,
    native_auth_spy,
    dialogs=None,
    dialogs_error=None,
):
    config = make_test_config(tmp_path)
    config.telegram.api_hash = "hash"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db

    telethon_cli_spy.default_client = _make_photo_dialog_client(
        dialogs,
        dialogs_error=dialogs_error,
    )
    harness = RealPoolHarness.build(
        db=db,
        telethon_cli_spy=telethon_cli_spy,
        native_auth_spy=native_auth_spy,
        session_cache_dir=str(tmp_path / "sessions"),
    )
    await harness.connect_account("+7000", session_string="s", is_primary=True)
    app.state.auth = harness.auth
    app.state.pool = harness.pool
    app.state.collector = Collector(app.state.pool, db, config.scheduler)
    app.state.search_engine = MagicMock()
    app.state.ai_search = MagicMock()
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = "secret"

    return app, db


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_photo_task_run_due_processes_pending_items(db, tmp_path, real_pool_harness_factory):
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

    processed = await service.run_due()
    assert processed == 1
    items = await service.list_items()
    item = next(item for item in items if item.batch_id == batch_id)
    assert item.status == PhotoBatchStatus.COMPLETED


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_photo_loader_page_renders(tmp_path, telethon_cli_spy, native_auth_spy):
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db

    telethon_cli_spy.default_client = _make_photo_dialog_client(
        [
            {"channel_id": -1001, "title": "Target Channel", "channel_type": "channel"},
            {"channel_id": -1002, "title": "Target Group", "channel_type": "supergroup"},
            {"channel_id": 42, "title": "Target DM", "channel_type": "dm"},
            {"channel_id": 99, "title": "Target Bot", "channel_type": "bot"},
        ]
    )
    harness = RealPoolHarness.build(
        db=db,
        telethon_cli_spy=telethon_cli_spy,
        native_auth_spy=native_auth_spy,
        session_cache_dir=str(tmp_path / "sessions"),
    )
    await harness.connect_account("+7000", session_string="s", is_primary=True)
    app.state.auth = harness.auth
    app.state.pool = harness.pool
    app.state.collector = Collector(app.state.pool, db, config.scheduler)
    app.state.search_engine = MagicMock()
    app.state.ai_search = MagicMock()
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = "secret"

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as client:
        resp = await client.get("/dialogs/photos?phone=%2B7000")
        assert resp.status_code == 200
        assert "Photo Loader" in resp.text
        assert "Автозагрузка из папки" in resp.text
        assert 'action="/dialogs/photos/refresh"' in resp.text
        assert "Обновить диалоги" in resp.text
        assert "Используется сохранённый список диалогов" in resp.text
        assert resp.text.count('option value="separate" selected') >= 3
        assert "data-target-picker" in resp.text
        assert "data-target-search" in resp.text
        assert 'data-target-filter="channel"' in resp.text
        assert 'data-target-filter="group"' in resp.text
        assert 'data-target-filter="dm"' in resp.text
        assert resp.text.count('name="target_dialog_id"') == 4
        assert 'select name="target_dialog_id"' not in resp.text
        assert resp.text.count("data-photo-submit-form") >= 4
        assert resp.text.count("data-photo-submit-button") >= 4
        assert resp.text.count("data-photo-submit-status") >= 4
        assert "Цель не выбрана" in resp.text
        assert "Выбор сохранится на текущую браузерную сессию." in resp.text
        assert 'data-target-phone="+7000"' in resp.text
        assert "data-initial-target-id=" not in resp.text
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
    telethon_cli_spy,
    native_auth_spy,
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

    telethon_cli_spy.default_client = _make_photo_dialog_client(
        [{"channel_id": -1001, "title": "Target Channel", "channel_type": "channel"}]
    )
    harness = RealPoolHarness.build(
        db=db,
        telethon_cli_spy=telethon_cli_spy,
        native_auth_spy=native_auth_spy,
        session_cache_dir=str(tmp_path / "sessions"),
    )
    await harness.connect_account("+7000", session_string="s", is_primary=True)
    app.state.auth = harness.auth
    app.state.pool = harness.pool
    app.state.collector = Collector(app.state.pool, db, config.scheduler)
    app.state.search_engine = MagicMock()
    app.state.ai_search = MagicMock()
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = "secret"

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
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as client:
        resp = await client.get(f"/dialogs/photos?phone=%2B7000&{query}")

    assert resp.status_code == 200
    assert expected_title in resp.text
    assert expected_text in resp.text
    assert "data-photo-feedback" in resp.text
    assert f'data-highlight-kind="{highlight_kind}"' in resp.text
    assert 'data-photo-result-row="item"' in resp.text
    assert 'data-photo-result-row="batch"' in resp.text
    assert 'data-photo-result-row="auto"' in resp.text

    await db.close()


@pytest.mark.asyncio
async def test_photo_loader_page_without_phone_selects_first_account(
    tmp_path,
    telethon_cli_spy,
    native_auth_spy,
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

    telethon_cli_spy.default_client = _make_photo_dialog_client(
        [{"channel_id": -1001, "title": "Target +7000", "channel_type": "channel"}]
    )
    harness = RealPoolHarness.build(
        db=db,
        telethon_cli_spy=telethon_cli_spy,
        native_auth_spy=native_auth_spy,
        session_cache_dir=str(tmp_path / "sessions"),
    )
    await harness.connect_account("+7999", session_string="b")
    await harness.connect_account("+7000", session_string="a")
    app.state.auth = harness.auth
    app.state.pool = harness.pool
    app.state.collector = Collector(app.state.pool, db, config.scheduler)
    app.state.search_engine = MagicMock()
    app.state.ai_search = MagicMock()
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = "secret"

    created_before = len(telethon_cli_spy.created)
    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as client:
        resp = await client.get("/dialogs/photos")
        assert resp.status_code == 200
        assert "Photo Loader" in resp.text
        assert 'option value="+7000" selected' in resp.text
        assert "Target +7000" in resp.text
        assert 'data-target-phone="+7000"' in resp.text
        assert "data-initial-target-id=" not in resp.text
        assert 'name="target_title" value="Target +7000"' not in resp.text
        assert 'name="target_type" value="channel"' not in resp.text
        assert "Цель не выбрана" in resp.text

    # With persistent sessions, dialogs page reuses the connection from connect_account
    assert len(telethon_cli_spy.created) - created_before == 0
    await db.close()


@pytest.mark.asyncio
async def test_photo_loader_page_without_selectable_targets_disables_forms(
    tmp_path,
    telethon_cli_spy,
    native_auth_spy,
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

    telethon_cli_spy.default_client = _make_photo_dialog_client(
        [{"channel_id": 77, "title": "Only Bot", "channel_type": "bot"}]
    )
    harness = RealPoolHarness.build(
        db=db,
        telethon_cli_spy=telethon_cli_spy,
        native_auth_spy=native_auth_spy,
        session_cache_dir=str(tmp_path / "sessions"),
    )
    await harness.connect_account("+7000", session_string="s", is_primary=True)
    app.state.auth = harness.auth
    app.state.pool = harness.pool
    app.state.collector = Collector(app.state.pool, db, config.scheduler)
    app.state.search_engine = MagicMock()
    app.state.ai_search = MagicMock()
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = "secret"

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as client:
        resp = await client.get("/dialogs/photos?phone=%2B7000")
        assert resp.status_code == 200
        assert "нет доступных целей отправки" in resp.text.lower()
        assert 'id="photo-target-picker"' not in resp.text
        assert resp.text.count("disabled") >= 4

    await db.close()


@pytest.mark.asyncio
async def test_photo_loader_page_without_accounts_renders_empty_state(
    tmp_path,
    telethon_cli_spy,
    native_auth_spy,
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

    harness = RealPoolHarness.build(
        db=db,
        telethon_cli_spy=telethon_cli_spy,
        native_auth_spy=native_auth_spy,
        session_cache_dir=str(tmp_path / "sessions"),
    )
    app.state.auth = harness.auth
    app.state.pool = harness.pool
    app.state.collector = Collector(app.state.pool, db, config.scheduler)
    app.state.search_engine = MagicMock()
    app.state.ai_search = MagicMock()
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = "secret"

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as client:
        resp = await client.get("/dialogs/photos")
        assert resp.status_code == 200
        assert "Нет подключённых аккаунтов." in resp.text
        assert "Добавьте аккаунт" in resp.text

    await db.close()


@pytest.mark.asyncio
async def test_photo_loader_refresh_warms_dialog_cache(
    tmp_path,
    telethon_cli_spy,
    native_auth_spy,
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

    telethon_cli_spy.default_client = _make_photo_dialog_client(
        [{"channel_id": -1001, "title": "Target Channel", "channel_type": "channel"}]
    )
    harness = RealPoolHarness.build(
        db=db,
        telethon_cli_spy=telethon_cli_spy,
        native_auth_spy=native_auth_spy,
        session_cache_dir=str(tmp_path / "sessions"),
    )
    await harness.connect_account("+7000", session_string="s", is_primary=True)
    app.state.auth = harness.auth
    app.state.pool = harness.pool
    app.state.collector = Collector(app.state.pool, db, config.scheduler)
    app.state.search_engine = MagicMock()
    app.state.ai_search = MagicMock()
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = "secret"

    created_before = len(telethon_cli_spy.created)
    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as client:
        resp = await client.post("/dialogs/photos/refresh", data={"phone": "+7000"})
        assert resp.status_code == 200
        assert "Photo Loader" in resp.text
        assert "Target Channel" in resp.text

    cached = await db.repos.dialog_cache.list_dialogs("+7000")
    assert cached == [
        {
            "channel_id": -1001,
            "title": "Target Channel",
            "username": None,
            "channel_type": "channel",
            "deactivate": False,
            "is_own": False,
        }
    ]
    # With persistent sessions, get_dialogs reuses the connection from connect_account
    assert len(telethon_cli_spy.created) - created_before == 0
    await db.close()


def test_photo_loader_cli_parser():
    parser = build_parser()
    args = parser.parse_args(
        [
            "photo-loader",
            "schedule-send",
            "--phone",
            "+7000",
            "--target",
            "-1001",
            "--files",
            "/tmp/a.jpg",
            "/tmp/b.jpg",
            "--mode",
            "album",
            "--at",
            "2026-03-11T18:30:00+00:00",
        ]
    )
    assert args.command == "photo-loader"
    assert args.photo_loader_action == "schedule-send"
    assert args.mode == "album"


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
        from src.telegram.auth import TelegramAuth

        return TelegramAuth(0, ""), fake_pool

    with (
        cli_init_patch(db, "src.cli.runtime.init_db"),
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
async def test_photo_schedule_logs_exception(tmp_path, caplog, telethon_cli_spy, native_auth_spy):
    app, db = await _build_photo_loader_app(tmp_path, telethon_cli_spy, native_auth_spy)
    app.state.photo_task_service = SimpleNamespace(
        schedule_send=AsyncMock(side_effect=RuntimeError("boom")),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as client:
        with caplog.at_level(logging.ERROR, logger="src.web.routes.photo_loader"):
            resp = await client.post(
                "/dialogs/photos/schedule",
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
async def test_photo_schedule_redirects_when_target_validation_raises(
    tmp_path,
    caplog,
    telethon_cli_spy,
    native_auth_spy,
):
    app, db = await _build_photo_loader_app(
        tmp_path,
        telethon_cli_spy,
        native_auth_spy,
        dialogs_error=RuntimeError("dialogs boom"),
    )
    app.state.photo_task_service = SimpleNamespace(
        schedule_send=AsyncMock(),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as client:
        with caplog.at_level(logging.ERROR, logger="src.web.routes.photo_loader"):
            resp = await client.post(
                "/dialogs/photos/schedule",
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
async def test_photo_schedule_requires_target_selection(
    tmp_path,
    telethon_cli_spy,
    native_auth_spy,
):
    app, db = await _build_photo_loader_app(tmp_path, telethon_cli_spy, native_auth_spy)
    app.state.photo_task_service = SimpleNamespace(
        schedule_send=AsyncMock(),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as client:
        resp = await client.post(
            "/dialogs/photos/schedule",
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
async def test_photo_schedule_rejects_unknown_target(tmp_path, telethon_cli_spy, native_auth_spy):
    app, db = await _build_photo_loader_app(tmp_path, telethon_cli_spy, native_auth_spy)
    app.state.photo_task_service = SimpleNamespace(
        schedule_send=AsyncMock(),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as client:
        resp = await client.post(
            "/dialogs/photos/schedule",
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
async def test_photo_send_logs_exception(tmp_path, caplog, telethon_cli_spy, native_auth_spy):
    app, db = await _build_photo_loader_app(tmp_path, telethon_cli_spy, native_auth_spy)
    app.state.photo_task_service = SimpleNamespace(
        send_now=AsyncMock(side_effect=RuntimeError("boom")),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as client:
        with caplog.at_level(logging.ERROR, logger="src.web.routes.photo_loader"):
            resp = await client.post(
                "/dialogs/photos/send",
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
async def test_photo_send_redirects_when_target_validation_raises(
    tmp_path,
    caplog,
    telethon_cli_spy,
    native_auth_spy,
):
    app, db = await _build_photo_loader_app(
        tmp_path,
        telethon_cli_spy,
        native_auth_spy,
        dialogs_error=RuntimeError("dialogs boom"),
    )
    app.state.photo_task_service = SimpleNamespace(
        send_now=AsyncMock(),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as client:
        with caplog.at_level(logging.ERROR, logger="src.web.routes.photo_loader"):
            resp = await client.post(
                "/dialogs/photos/send",
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
async def test_photo_send_requires_target_selection(tmp_path, telethon_cli_spy, native_auth_spy):
    app, db = await _build_photo_loader_app(tmp_path, telethon_cli_spy, native_auth_spy)
    app.state.photo_task_service = SimpleNamespace(
        send_now=AsyncMock(),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as client:
        resp = await client.post(
            "/dialogs/photos/send",
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
async def test_photo_batch_logs_exception(tmp_path, caplog, telethon_cli_spy, native_auth_spy):
    app, db = await _build_photo_loader_app(tmp_path, telethon_cli_spy, native_auth_spy)
    app.state.photo_task_service = SimpleNamespace(
        create_batch=AsyncMock(side_effect=RuntimeError("boom")),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as client:
        with caplog.at_level(logging.ERROR, logger="src.web.routes.photo_loader"):
            resp = await client.post(
                "/dialogs/photos/batch",
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
async def test_photo_batch_redirects_when_target_validation_raises(
    tmp_path,
    caplog,
    telethon_cli_spy,
    native_auth_spy,
):
    app, db = await _build_photo_loader_app(
        tmp_path,
        telethon_cli_spy,
        native_auth_spy,
        dialogs_error=RuntimeError("dialogs boom"),
    )
    app.state.photo_task_service = SimpleNamespace(
        create_batch=AsyncMock(),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as client:
        with caplog.at_level(logging.ERROR, logger="src.web.routes.photo_loader"):
            resp = await client.post(
                "/dialogs/photos/batch",
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
async def test_photo_batch_rejects_unknown_target(tmp_path, telethon_cli_spy, native_auth_spy):
    app, db = await _build_photo_loader_app(tmp_path, telethon_cli_spy, native_auth_spy)
    app.state.photo_task_service = SimpleNamespace(
        create_batch=AsyncMock(),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as client:
        resp = await client.post(
            "/dialogs/photos/batch",
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
async def test_photo_send_rejects_bot_target(tmp_path, telethon_cli_spy, native_auth_spy):
    app, db = await _build_photo_loader_app(
        tmp_path,
        telethon_cli_spy,
        native_auth_spy,
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
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as client:
        resp = await client.post(
            "/dialogs/photos/send",
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
async def test_photo_auto_logs_exception(tmp_path, caplog, telethon_cli_spy, native_auth_spy):
    app, db = await _build_photo_loader_app(tmp_path, telethon_cli_spy, native_auth_spy)
    app.state.photo_auto_upload_service = SimpleNamespace(
        create_job=AsyncMock(side_effect=RuntimeError("boom")),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as client:
        with caplog.at_level(logging.ERROR, logger="src.web.routes.photo_loader"):
            resp = await client.post(
                "/dialogs/photos/auto",
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
async def test_photo_auto_redirects_when_target_validation_raises(
    tmp_path,
    caplog,
    telethon_cli_spy,
    native_auth_spy,
):
    app, db = await _build_photo_loader_app(
        tmp_path,
        telethon_cli_spy,
        native_auth_spy,
        dialogs_error=RuntimeError("dialogs boom"),
    )
    app.state.photo_auto_upload_service = SimpleNamespace(
        create_job=AsyncMock(),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as client:
        with caplog.at_level(logging.ERROR, logger="src.web.routes.photo_loader"):
            resp = await client.post(
                "/dialogs/photos/auto",
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
async def test_photo_auto_requires_target_selection(tmp_path, telethon_cli_spy, native_auth_spy):
    app, db = await _build_photo_loader_app(tmp_path, telethon_cli_spy, native_auth_spy)
    app.state.photo_auto_upload_service = SimpleNamespace(
        create_job=AsyncMock(),
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as client:
        resp = await client.post(
            "/dialogs/photos/auto",
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
async def test_photo_run_due_logs_exception(tmp_path, caplog, telethon_cli_spy, native_auth_spy):
    app, db = await _build_photo_loader_app(tmp_path, telethon_cli_spy, native_auth_spy)
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
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as client:
        with caplog.at_level(logging.ERROR, logger="src.web.routes.photo_loader"):
            resp = await client.post(
                "/dialogs/photos/run-due",
                data={"phone": "+7000"},
                follow_redirects=False,
            )

    assert resp.status_code == 303
    assert "error=photo_run_due_failed" in resp.headers["location"]
    assert "Photo run_due failed" in caplog.text
    await db.close()
