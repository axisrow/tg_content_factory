"""Tests for photo_loader CLI commands."""
from __future__ import annotations

import argparse
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from src.config import AppConfig
from src.database import Database
from src.models import Account


def _ns(**kwargs) -> argparse.Namespace:
    defaults = {"config": "config.yaml"}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def _setup_photo_db(db: Database) -> None:
    """Setup database with account for photo tests."""
    asyncio.run(db.add_account(Account(phone="+100", session_string="sess")))


def test_resolve_target_numeric(tmp_path, capsys):
    """Test _resolve_target with numeric ID."""
    from src.cli.commands.photo_loader import _resolve_target

    pool = MagicMock()
    result = asyncio.run(_resolve_target("-1001234567890", pool))
    assert result.dialog_id == -1001234567890


def test_resolve_target_username(tmp_path, capsys):
    """Test _resolve_target with username."""
    from src.cli.commands.photo_loader import _resolve_target

    pool = MagicMock()
    pool.resolve_channel = AsyncMock(
        return_value={
            "channel_id": -1001234567890,
            "title": "Test Channel",
            "channel_type": "channel",
        }
    )
    result = asyncio.run(_resolve_target("@testchannel", pool))
    assert result.dialog_id == -1001234567890
    assert result.title == "Test Channel"


def test_resolve_target_not_found(tmp_path, capsys):
    """Test _resolve_target when target cannot be resolved."""
    from src.cli.commands.photo_loader import _resolve_target

    pool = MagicMock()
    pool.resolve_channel = AsyncMock(return_value=None)

    try:
        asyncio.run(_resolve_target("@nonexistent", pool))
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Could not resolve target" in str(e)


def test_parse_schedule_at_with_tz():
    """Test _parse_schedule_at with timezone."""
    from src.cli.commands.photo_loader import _parse_schedule_at

    dt = _parse_schedule_at("2024-01-15T10:30:00+03:00")
    assert dt.tzinfo is not None


def test_parse_schedule_at_without_tz():
    """Test _parse_schedule_at without timezone (local)."""
    from src.cli.commands.photo_loader import _parse_schedule_at

    dt = _parse_schedule_at("2024-01-15T10:30:00")
    assert dt.tzinfo is not None


def test_dialogs_action(tmp_path, capsys):
    """Test dialogs action prints dialog list."""
    db_path = str(tmp_path / "photo_dialogs.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)
    asyncio.run(db.close())

    async def fake_init_db(_config_path: str):
        config = AppConfig()
        database = Database(db_path)
        await database.initialize()
        return config, database

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    with (
        patch("src.cli.commands.photo_loader.runtime.init_db", side_effect=fake_init_db),
        patch("src.cli.commands.photo_loader.runtime.init_pool", side_effect=fake_init_pool),
    ):
        from src.cli.commands.photo_loader import run

        with patch("src.cli.commands.photo_loader.ChannelService") as mock_channel_service:
            mock_instance = MagicMock()
            mock_instance.get_my_dialogs = AsyncMock(
                return_value=[
                    {"channel_id": 100, "channel_type": "channel", "title": "Test Channel"},
                    {"channel_id": 200, "channel_type": "chat", "title": "Test Chat"},
                ]
            )
            mock_channel_service.return_value = mock_instance

            run(_ns(photo_loader_action="dialogs", phone="+100"))

    out = capsys.readouterr().out
    assert "100" in out
    assert "Test Channel" in out


def _create_fake_services(task_methods=None, auto_methods=None):
    """Create fake service classes with proper __init__."""

    class FakePhotoTaskService:
        def __init__(self, *args, **kwargs):
            pass

    if task_methods:
        for name, method in task_methods.items():
            setattr(FakePhotoTaskService, name, method)

    class FakePhotoAutoUploadService:
        def __init__(self, *args, **kwargs):
            pass

    if auto_methods:
        for name, method in auto_methods.items():
            setattr(FakePhotoAutoUploadService, name, method)

    return FakePhotoTaskService, FakePhotoAutoUploadService


def test_send_action(tmp_path, capsys):
    """Test send action sends photo immediately."""
    db_path = str(tmp_path / "photo_send.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)
    asyncio.run(db.close())

    async def fake_init_db(_config_path: str):
        config = AppConfig()
        database = Database(db_path)
        await database.initialize()
        return config, database

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.resolve_channel = AsyncMock(
            return_value={"channel_id": -100, "title": "Target", "channel_type": "channel"}
        )
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    fake_task, fake_auto = _create_fake_services(
        task_methods={"send_now": AsyncMock(return_value=MagicMock(id=1, status="sent"))}
    )

    with (
        patch("src.cli.commands.photo_loader.runtime.init_db", side_effect=fake_init_db),
        patch("src.cli.commands.photo_loader.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.photo_loader.PhotoTaskService", fake_task),
        patch("src.cli.commands.photo_loader.PhotoAutoUploadService", fake_auto),
    ):
        from src.cli.commands.photo_loader import run

        run(
            _ns(
                photo_loader_action="send",
                phone="+100",
                target="-100",
                files=["/tmp/test.jpg"],
                mode="album",
                caption=None,
            )
        )

    out = capsys.readouterr().out
    assert "Sent photo item #1" in out


def test_schedule_send_action(tmp_path, capsys):
    """Test schedule-send action schedules photo."""
    db_path = str(tmp_path / "photo_schedule.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)
    asyncio.run(db.close())

    async def fake_init_db(_config_path: str):
        config = AppConfig()
        database = Database(db_path)
        await database.initialize()
        return config, database

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.resolve_channel = AsyncMock(
            return_value={"channel_id": -100, "title": "Target", "channel_type": "channel"}
        )
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    fake_task, fake_auto = _create_fake_services(
        task_methods={"schedule_send": AsyncMock(return_value=MagicMock(id=2, status="scheduled"))}
    )

    with (
        patch("src.cli.commands.photo_loader.runtime.init_db", side_effect=fake_init_db),
        patch("src.cli.commands.photo_loader.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.photo_loader.PhotoTaskService", fake_task),
        patch("src.cli.commands.photo_loader.PhotoAutoUploadService", fake_auto),
    ):
        from src.cli.commands.photo_loader import run

        run(
            _ns(
                photo_loader_action="schedule-send",
                phone="+100",
                target="-100",
                files=["/tmp/test.jpg"],
                mode="album",
                at="2024-12-31T12:00:00",
                caption=None,
            )
        )

    out = capsys.readouterr().out
    assert "Scheduled photo item #2" in out


def test_batch_create_action(tmp_path, capsys):
    """Test batch-create action creates photo batch."""
    db_path = str(tmp_path / "photo_batch_create.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)
    asyncio.run(db.close())

    async def fake_init_db(_config_path: str):
        config = AppConfig()
        database = Database(db_path)
        await database.initialize()
        return config, database

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.resolve_channel = AsyncMock(
            return_value={"channel_id": -100, "title": "Target", "channel_type": "channel"}
        )
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    fake_task, fake_auto = _create_fake_services(
        task_methods={
            "load_manifest": lambda self, path: [{"file": "test.jpg"}],
            "create_batch": AsyncMock(return_value=3),
        }
    )

    with (
        patch("src.cli.commands.photo_loader.runtime.init_db", side_effect=fake_init_db),
        patch("src.cli.commands.photo_loader.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.photo_loader.PhotoTaskService", fake_task),
        patch("src.cli.commands.photo_loader.PhotoAutoUploadService", fake_auto),
    ):
        from src.cli.commands.photo_loader import run

        run(
            _ns(
                photo_loader_action="batch-create",
                phone="+100",
                target="-100",
                manifest="/tmp/manifest.json",
                caption=None,
            )
        )

    out = capsys.readouterr().out
    assert "Created photo batch #3" in out


def test_batch_list_action(tmp_path, capsys):
    """Test batch-list action lists batches."""
    db_path = str(tmp_path / "photo_batch_list.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)
    asyncio.run(db.close())

    async def fake_init_db(_config_path: str):
        config = AppConfig()
        database = Database(db_path)
        await database.initialize()
        return config, database

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    fake_task, fake_auto = _create_fake_services(
        task_methods={
            "list_batches": AsyncMock(
                return_value=[
                    MagicMock(id=1, phone="+100", target_dialog_id=-100, status="completed"),
                    MagicMock(id=2, phone="+100", target_dialog_id=-200, status="pending"),
                ]
            )
        }
    )

    with (
        patch("src.cli.commands.photo_loader.runtime.init_db", side_effect=fake_init_db),
        patch("src.cli.commands.photo_loader.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.photo_loader.PhotoTaskService", fake_task),
        patch("src.cli.commands.photo_loader.PhotoAutoUploadService", fake_auto),
    ):
        from src.cli.commands.photo_loader import run

        run(_ns(photo_loader_action="batch-list"))

    out = capsys.readouterr().out
    assert "#1" in out
    assert "#2" in out


def test_auto_create_action(tmp_path, capsys):
    """Test auto-create action creates auto upload job."""
    db_path = str(tmp_path / "photo_auto_create.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)
    asyncio.run(db.close())

    async def fake_init_db(_config_path: str):
        config = AppConfig()
        database = Database(db_path)
        await database.initialize()
        return config, database

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.resolve_channel = AsyncMock(
            return_value={"channel_id": -100, "title": "Target", "channel_type": "channel"}
        )
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    fake_task, fake_auto = _create_fake_services(auto_methods={"create_job": AsyncMock(return_value=4)})

    with (
        patch("src.cli.commands.photo_loader.runtime.init_db", side_effect=fake_init_db),
        patch("src.cli.commands.photo_loader.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.photo_loader.PhotoTaskService", fake_task),
        patch("src.cli.commands.photo_loader.PhotoAutoUploadService", fake_auto),
    ):
        from src.cli.commands.photo_loader import run

        run(
            _ns(
                photo_loader_action="auto-create",
                phone="+100",
                target="-100",
                folder="/tmp/photos",
                mode="separate",
                caption=None,
                interval=60,
            )
        )

    out = capsys.readouterr().out
    assert "Created auto job #4" in out


def test_auto_list_action(tmp_path, capsys):
    """Test auto-list action lists auto jobs."""
    db_path = str(tmp_path / "photo_auto_list.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)
    asyncio.run(db.close())

    async def fake_init_db(_config_path: str):
        config = AppConfig()
        database = Database(db_path)
        await database.initialize()
        return config, database

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    fake_task, fake_auto = _create_fake_services(
        auto_methods={
            "list_jobs": AsyncMock(
                return_value=[
                    MagicMock(
                        id=1,
                        target_dialog_id=-100,
                        folder_path="/tmp/photos",
                        interval_minutes=60,
                        is_active=True,
                    ),
                ]
            )
        }
    )

    with (
        patch("src.cli.commands.photo_loader.runtime.init_db", side_effect=fake_init_db),
        patch("src.cli.commands.photo_loader.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.photo_loader.PhotoTaskService", fake_task),
        patch("src.cli.commands.photo_loader.PhotoAutoUploadService", fake_auto),
    ):
        from src.cli.commands.photo_loader import run

        run(_ns(photo_loader_action="auto-list"))

    out = capsys.readouterr().out
    assert "#1" in out
    assert "/tmp/photos" in out


def test_auto_toggle_action(tmp_path, capsys):
    """Test auto-toggle action toggles job active state."""
    db_path = str(tmp_path / "photo_auto_toggle.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)
    asyncio.run(db.close())

    async def fake_init_db(_config_path: str):
        config = AppConfig()
        database = Database(db_path)
        await database.initialize()
        return config, database

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    fake_task, fake_auto = _create_fake_services(
        auto_methods={
            "get_job": AsyncMock(return_value=MagicMock(id=1, is_active=True)),
            "update_job": AsyncMock(return_value=None),
        }
    )

    with (
        patch("src.cli.commands.photo_loader.runtime.init_db", side_effect=fake_init_db),
        patch("src.cli.commands.photo_loader.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.photo_loader.PhotoTaskService", fake_task),
        patch("src.cli.commands.photo_loader.PhotoAutoUploadService", fake_auto),
    ):
        from src.cli.commands.photo_loader import run

        run(_ns(photo_loader_action="auto-toggle", id=1))

    out = capsys.readouterr().out
    assert "Toggled auto job #1" in out


def test_auto_toggle_not_found(tmp_path, capsys):
    """Test auto-toggle with non-existent job."""
    db_path = str(tmp_path / "photo_auto_toggle_nf.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)
    asyncio.run(db.close())

    async def fake_init_db(_config_path: str):
        config = AppConfig()
        database = Database(db_path)
        await database.initialize()
        return config, database

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    fake_task, fake_auto = _create_fake_services(auto_methods={"get_job": AsyncMock(return_value=None)})

    with (
        patch("src.cli.commands.photo_loader.runtime.init_db", side_effect=fake_init_db),
        patch("src.cli.commands.photo_loader.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.photo_loader.PhotoTaskService", fake_task),
        patch("src.cli.commands.photo_loader.PhotoAutoUploadService", fake_auto),
    ):
        from src.cli.commands.photo_loader import run

        run(_ns(photo_loader_action="auto-toggle", id=999))

    out = capsys.readouterr().out
    assert "not found" in out


def test_run_due_action(tmp_path, capsys):
    """Test run-due action processes due items and jobs."""
    db_path = str(tmp_path / "photo_run_due.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)
    asyncio.run(db.close())

    async def fake_init_db(_config_path: str):
        config = AppConfig()
        database = Database(db_path)
        await database.initialize()
        return config, database

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    fake_task, fake_auto = _create_fake_services(
        task_methods={"run_due": AsyncMock(return_value=5)},
        auto_methods={"run_due": AsyncMock(return_value=2)},
    )

    with (
        patch("src.cli.commands.photo_loader.runtime.init_db", side_effect=fake_init_db),
        patch("src.cli.commands.photo_loader.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.photo_loader.PhotoTaskService", fake_task),
        patch("src.cli.commands.photo_loader.PhotoAutoUploadService", fake_auto),
    ):
        from src.cli.commands.photo_loader import run

        run(_ns(photo_loader_action="run-due"))

    out = capsys.readouterr().out
    assert "items=5" in out
    assert "auto_jobs=2" in out


def test_auto_delete_action(tmp_path, capsys):
    """Test auto-delete action deletes job."""
    db_path = str(tmp_path / "photo_auto_delete.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)
    asyncio.run(db.close())

    async def fake_init_db(_config_path: str):
        config = AppConfig()
        database = Database(db_path)
        await database.initialize()
        return config, database

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    fake_task, fake_auto = _create_fake_services(auto_methods={"delete_job": AsyncMock(return_value=None)})

    with (
        patch("src.cli.commands.photo_loader.runtime.init_db", side_effect=fake_init_db),
        patch("src.cli.commands.photo_loader.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.photo_loader.PhotoTaskService", fake_task),
        patch("src.cli.commands.photo_loader.PhotoAutoUploadService", fake_auto),
    ):
        from src.cli.commands.photo_loader import run

        run(_ns(photo_loader_action="auto-delete", id=1))

    out = capsys.readouterr().out
    assert "Deleted auto job #1" in out


def test_batch_cancel_action(tmp_path, capsys):
    """Test batch-cancel action cancels item."""
    db_path = str(tmp_path / "photo_batch_cancel.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)
    asyncio.run(db.close())

    async def fake_init_db(_config_path: str):
        config = AppConfig()
        database = Database(db_path)
        await database.initialize()
        return config, database

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    fake_task, fake_auto = _create_fake_services(task_methods={"cancel_item": AsyncMock(return_value=True)})

    with (
        patch("src.cli.commands.photo_loader.runtime.init_db", side_effect=fake_init_db),
        patch("src.cli.commands.photo_loader.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.photo_loader.PhotoTaskService", fake_task),
        patch("src.cli.commands.photo_loader.PhotoAutoUploadService", fake_auto),
    ):
        from src.cli.commands.photo_loader import run

        run(_ns(photo_loader_action="batch-cancel", id=1))

    out = capsys.readouterr().out
    assert "Cancelled" in out


def test_auto_update_action(tmp_path, capsys):
    """Test auto-update action updates job."""
    db_path = str(tmp_path / "photo_auto_update.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)
    asyncio.run(db.close())

    async def fake_init_db(_config_path: str):
        config = AppConfig()
        database = Database(db_path)
        await database.initialize()
        return config, database

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    fake_task, fake_auto = _create_fake_services(auto_methods={"update_job": AsyncMock(return_value=None)})

    with (
        patch("src.cli.commands.photo_loader.runtime.init_db", side_effect=fake_init_db),
        patch("src.cli.commands.photo_loader.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.photo_loader.PhotoTaskService", fake_task),
        patch("src.cli.commands.photo_loader.PhotoAutoUploadService", fake_auto),
    ):
        from src.cli.commands.photo_loader import run

        run(
            _ns(
                photo_loader_action="auto-update",
                id=1,
                folder="/new/path",
                mode="album",
                caption="New caption",
                interval=30,
                active=True,
                paused=False,
            )
        )

    out = capsys.readouterr().out
    assert "Updated auto job #1" in out
