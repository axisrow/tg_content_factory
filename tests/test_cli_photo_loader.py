"""Tests for photo_loader CLI commands."""
from __future__ import annotations

import argparse
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.database import Database
from src.models import Account, PhotoSendMode

pytestmark = pytest.mark.aiosqlite_serial

_PHOTO_LOADER_INIT_DB_TARGET = "src.cli.commands.photo_loader.runtime.init_db"


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
        raise AssertionError("Should have raised ValueError")
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


def test_dialogs_action(tmp_path, cli_init_patch, capsys):
    """Test dialogs action prints dialog list."""
    db_path = str(tmp_path / "photo_dialogs.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    with (
        cli_init_patch(db, _PHOTO_LOADER_INIT_DB_TARGET),
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


def test_send_action(tmp_path, cli_init_patch, capsys):
    """Test send action sends photo immediately."""
    db_path = str(tmp_path / "photo_send.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)

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
        cli_init_patch(db, _PHOTO_LOADER_INIT_DB_TARGET),
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


def test_schedule_send_action(tmp_path, cli_init_patch, capsys):
    """Test schedule-send action schedules photo."""
    db_path = str(tmp_path / "photo_schedule.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)

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
        cli_init_patch(db, _PHOTO_LOADER_INIT_DB_TARGET),
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


def test_batch_create_action(tmp_path, cli_init_patch, capsys):
    """Test batch-create action creates photo batch."""
    db_path = str(tmp_path / "photo_batch_create.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)

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
        cli_init_patch(db, _PHOTO_LOADER_INIT_DB_TARGET),
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


def test_publish_action(tmp_path, cli_init_patch, capsys):
    """Test publish action moves held batch items into the due queue."""
    db_path = str(tmp_path / "photo_publish.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    publish_batch = AsyncMock(return_value=2)
    fake_task, fake_auto = _create_fake_services(
        task_methods={"publish_batch": publish_batch},
    )

    with (
        cli_init_patch(db, _PHOTO_LOADER_INIT_DB_TARGET),
        patch("src.cli.commands.photo_loader.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.photo_loader.PhotoTaskService", fake_task),
        patch("src.cli.commands.photo_loader.PhotoAutoUploadService", fake_auto),
    ):
        from src.cli.commands.photo_loader import run

        run(_ns(photo_loader_action="publish", id=3))

    out = capsys.readouterr().out
    assert "Published photo batch #3: items=2" in out
    publish_batch.assert_awaited_once_with(3)


def test_batch_list_action(tmp_path, cli_init_patch, capsys):
    """Test batch-list action lists batches."""
    db_path = str(tmp_path / "photo_batch_list.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)

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
        cli_init_patch(db, _PHOTO_LOADER_INIT_DB_TARGET),
        patch("src.cli.commands.photo_loader.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.photo_loader.PhotoTaskService", fake_task),
        patch("src.cli.commands.photo_loader.PhotoAutoUploadService", fake_auto),
    ):
        from src.cli.commands.photo_loader import run

        run(_ns(photo_loader_action="batch-list"))

    out = capsys.readouterr().out
    assert "#1" in out
    assert "#2" in out


def test_auto_create_action(tmp_path, cli_init_patch, capsys):
    """Test auto-create action creates auto upload job."""
    db_path = str(tmp_path / "photo_auto_create.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.resolve_channel = AsyncMock(
            return_value={"channel_id": -100, "title": "Target", "channel_type": "channel"}
        )
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    fake_task, fake_auto = _create_fake_services(auto_methods={"create_job": AsyncMock(return_value=4)})

    with (
        cli_init_patch(db, _PHOTO_LOADER_INIT_DB_TARGET),
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


def test_auto_list_action(tmp_path, cli_init_patch, capsys):
    """Test auto-list action lists auto jobs."""
    db_path = str(tmp_path / "photo_auto_list.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)

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
        cli_init_patch(db, _PHOTO_LOADER_INIT_DB_TARGET),
        patch("src.cli.commands.photo_loader.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.photo_loader.PhotoTaskService", fake_task),
        patch("src.cli.commands.photo_loader.PhotoAutoUploadService", fake_auto),
    ):
        from src.cli.commands.photo_loader import run

        run(_ns(photo_loader_action="auto-list"))

    out = capsys.readouterr().out
    assert "#1" in out
    assert "/tmp/photos" in out


def test_auto_toggle_action(tmp_path, cli_init_patch, capsys):
    """Test auto-toggle action toggles job active state."""
    db_path = str(tmp_path / "photo_auto_toggle.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)

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
        cli_init_patch(db, _PHOTO_LOADER_INIT_DB_TARGET),
        patch("src.cli.commands.photo_loader.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.photo_loader.PhotoTaskService", fake_task),
        patch("src.cli.commands.photo_loader.PhotoAutoUploadService", fake_auto),
    ):
        from src.cli.commands.photo_loader import run

        run(_ns(photo_loader_action="auto-toggle", id=1))

    out = capsys.readouterr().out
    assert "Toggled auto job #1" in out


def test_auto_toggle_not_found(tmp_path, cli_init_patch, capsys):
    """Test auto-toggle with non-existent job."""
    db_path = str(tmp_path / "photo_auto_toggle_nf.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    fake_task, fake_auto = _create_fake_services(auto_methods={"get_job": AsyncMock(return_value=None)})

    with (
        cli_init_patch(db, _PHOTO_LOADER_INIT_DB_TARGET),
        patch("src.cli.commands.photo_loader.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.photo_loader.PhotoTaskService", fake_task),
        patch("src.cli.commands.photo_loader.PhotoAutoUploadService", fake_auto),
    ):
        from src.cli.commands.photo_loader import run

        run(_ns(photo_loader_action="auto-toggle", id=999))

    out = capsys.readouterr().out
    assert "not found" in out


def test_run_due_action(tmp_path, cli_init_patch, capsys):
    """Test run-due action processes due items and jobs."""
    db_path = str(tmp_path / "photo_run_due.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    fake_task, fake_auto = _create_fake_services(
        task_methods={"run_due": AsyncMock(return_value=5)},
        auto_methods={"run_due": AsyncMock(return_value=2)},
    )

    with (
        cli_init_patch(db, _PHOTO_LOADER_INIT_DB_TARGET),
        patch("src.cli.commands.photo_loader.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.photo_loader.PhotoTaskService", fake_task),
        patch("src.cli.commands.photo_loader.PhotoAutoUploadService", fake_auto),
    ):
        from src.cli.commands.photo_loader import run

        run(_ns(photo_loader_action="run-due"))

    out = capsys.readouterr().out
    assert "items=5" in out
    assert "auto_jobs=2" in out


def test_run_due_action_prints_progress(tmp_path, cli_init_patch, capsys):
    """run-due prints plain [N/M] progress lines when services report progress."""
    db_path = str(tmp_path / "photo_run_due_progress.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    async def _run_items(*, item_id=None, on_progress=None):
        assert item_id is None
        assert on_progress is not None
        on_progress(1, 2)
        on_progress(2, 2)
        return 2

    async def _run_auto(*, on_progress=None, dry_run=False):
        assert dry_run is False
        assert on_progress is not None
        on_progress(1, 1)
        return 1

    fake_task, fake_auto = _create_fake_services(
        task_methods={"run_due": AsyncMock(side_effect=_run_items)},
        auto_methods={"run_due": AsyncMock(side_effect=_run_auto)},
    )

    with (
        cli_init_patch(db, _PHOTO_LOADER_INIT_DB_TARGET),
        patch("src.cli.commands.photo_loader.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.photo_loader.PhotoTaskService", fake_task),
        patch("src.cli.commands.photo_loader.PhotoAutoUploadService", fake_auto),
    ):
        from src.cli.commands.photo_loader import run

        run(_ns(photo_loader_action="run-due"))

    out = capsys.readouterr().out
    assert "[1/2] photo item processed" in out
    assert "[2/2] photo item processed" in out
    assert "Progress: 2/2 photo items processed." in out
    assert "[1/1] auto job processed" in out
    assert "Progress: 1/1 auto jobs processed." in out


def test_run_due_action_with_item_id_skips_auto_jobs(tmp_path, cli_init_patch, capsys):
    """Test run-due --item-id only processes the requested item."""
    db_path = str(tmp_path / "photo_run_due_item.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    run_due = AsyncMock(return_value=1)
    auto_run_due = AsyncMock(return_value=2)
    fake_task, fake_auto = _create_fake_services(
        task_methods={"run_due": run_due},
        auto_methods={"run_due": auto_run_due},
    )

    with (
        cli_init_patch(db, _PHOTO_LOADER_INIT_DB_TARGET),
        patch("src.cli.commands.photo_loader.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.photo_loader.PhotoTaskService", fake_task),
        patch("src.cli.commands.photo_loader.PhotoAutoUploadService", fake_auto),
    ):
        from src.cli.commands.photo_loader import run

        run(_ns(photo_loader_action="run-due", item_id=77))

    out = capsys.readouterr().out
    assert "items=1" in out
    assert "auto_jobs=0" in out
    run_due.assert_awaited_once()
    assert run_due.await_args.kwargs["item_id"] == 77
    assert callable(run_due.await_args.kwargs["on_progress"])
    auto_run_due.assert_not_awaited()


def test_run_due_dry_run_previews_without_running_items(tmp_path, cli_init_patch, capsys):
    """run-due --dry-run prints the auto-job plan, asks auto.run_due(dry_run=True),
    and never touches the photo-item path (which has no dry-run)."""
    from src.services.photo_auto_upload_service import PhotoAutoPreview

    db_path = str(tmp_path / "photo_run_due_dry.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    preview = PhotoAutoPreview(
        job_id=3,
        target_dialog_id=-100500,
        target_title="My Channel",
        target_type="channel",
        send_mode=PhotoSendMode.ALBUM,
        files=["/photos/a.jpg", "/photos/b.png"],
    )
    task_run_due = AsyncMock(return_value=9)
    auto_run_due = AsyncMock(return_value=[preview])
    fake_task, fake_auto = _create_fake_services(
        task_methods={"run_due": task_run_due},
        auto_methods={"run_due": auto_run_due},
    )

    with (
        cli_init_patch(db, _PHOTO_LOADER_INIT_DB_TARGET),
        patch("src.cli.commands.photo_loader.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.photo_loader.PhotoTaskService", fake_task),
        patch("src.cli.commands.photo_loader.PhotoAutoUploadService", fake_auto),
    ):
        from src.cli.commands.photo_loader import run

        run(_ns(photo_loader_action="run-due", dry_run=True))

    out = capsys.readouterr().out
    assert "dry-run" in out
    assert "job #3" in out
    assert "My Channel" in out
    assert "/photos/a.jpg" in out
    assert "/photos/b.png" in out
    assert "2 file(s)" in out
    # The auto path is asked for a preview, the item path is never run.
    auto_run_due.assert_awaited_once_with(dry_run=True)
    task_run_due.assert_not_awaited()


def test_auto_delete_action(tmp_path, cli_init_patch, capsys):
    """Test auto-delete action deletes job."""
    db_path = str(tmp_path / "photo_auto_delete.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    fake_task, fake_auto = _create_fake_services(auto_methods={"delete_job": AsyncMock(return_value=None)})

    with (
        cli_init_patch(db, _PHOTO_LOADER_INIT_DB_TARGET),
        patch("src.cli.commands.photo_loader.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.photo_loader.PhotoTaskService", fake_task),
        patch("src.cli.commands.photo_loader.PhotoAutoUploadService", fake_auto),
    ):
        from src.cli.commands.photo_loader import run

        run(_ns(photo_loader_action="auto-delete", id=1))

    out = capsys.readouterr().out
    assert "Deleted auto job #1" in out


def test_batch_cancel_action(tmp_path, cli_init_patch, capsys):
    """Test batch-cancel action cancels item."""
    db_path = str(tmp_path / "photo_batch_cancel.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    fake_task, fake_auto = _create_fake_services(task_methods={"cancel_item": AsyncMock(return_value=True)})

    with (
        cli_init_patch(db, _PHOTO_LOADER_INIT_DB_TARGET),
        patch("src.cli.commands.photo_loader.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.photo_loader.PhotoTaskService", fake_task),
        patch("src.cli.commands.photo_loader.PhotoAutoUploadService", fake_auto),
    ):
        from src.cli.commands.photo_loader import run

        run(_ns(photo_loader_action="batch-cancel", id=1))

    out = capsys.readouterr().out
    assert "Cancelled" in out


def test_auto_update_action(tmp_path, cli_init_patch, capsys):
    """Test auto-update action updates job."""
    db_path = str(tmp_path / "photo_auto_update.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _setup_photo_db(db)

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    fake_task, fake_auto = _create_fake_services(auto_methods={"update_job": AsyncMock(return_value=None)})

    with (
        cli_init_patch(db, _PHOTO_LOADER_INIT_DB_TARGET),
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
