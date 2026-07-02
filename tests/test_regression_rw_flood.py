"""Tests for CLI read/write checks and flood-wait pool availability paths."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------












# ===========================================================================
# 1. cli/commands/test.py — read/write check functions
# ===========================================================================




class TestReadChecks:
    async def test_check_get_stats_pass(self, db):
        from src.cli.commands.test import _check_get_stats
        result = await _check_get_stats(db)
        assert result.status.value == "PASS"

    async def test_check_get_stats_fail(self):
        from src.cli.commands.test import _check_get_stats
        mock_db = MagicMock()
        mock_db.get_stats = AsyncMock(side_effect=RuntimeError("boom"))
        result = await _check_get_stats(mock_db)
        assert result.status.value == "FAIL"

    async def test_check_account_list_pass(self, db):
        from src.cli.commands.test import _check_account_list
        result = await _check_account_list(db)
        assert result.status.value == "PASS"

    async def test_check_account_list_fail(self):
        from src.cli.commands.test import _check_account_list
        mock_db = MagicMock()
        mock_db.get_accounts = AsyncMock(side_effect=RuntimeError("boom"))
        result = await _check_account_list(mock_db)
        assert result.status.value == "FAIL"

    async def test_check_channel_list_pass(self, db):
        from src.cli.commands.test import _check_channel_list
        result = await _check_channel_list(db)
        assert result.status.value == "PASS"

    async def test_check_channel_list_fail(self):
        from src.cli.commands.test import _check_channel_list
        mock_db = MagicMock()
        mock_db.get_channels_with_counts = AsyncMock(side_effect=RuntimeError("boom"))
        result = await _check_channel_list(mock_db)
        assert result.status.value == "FAIL"

    async def test_check_notification_queries_none(self, db):
        from src.cli.commands.test import _check_notification_queries
        result = await _check_notification_queries(db)
        assert result.status.value in ("SKIP", "PASS")

    async def test_check_notification_queries_fail(self):
        from src.cli.commands.test import _check_notification_queries
        mock_db = MagicMock()
        mock_db.get_notification_queries = AsyncMock(side_effect=RuntimeError("boom"))
        result = await _check_notification_queries(mock_db)
        assert result.status.value == "FAIL"

    async def test_check_local_search(self, db):
        from src.cli.commands.test import _check_local_search
        result = await _check_local_search(db)
        assert result.status.value == "PASS"

    async def test_check_local_search_fail(self):
        from src.cli.commands.test import _check_local_search
        mock_db = MagicMock()
        mock_db.search_messages = AsyncMock(side_effect=RuntimeError("boom"))
        result = await _check_local_search(mock_db)
        assert result.status.value == "FAIL"

    async def test_check_collection_tasks(self, db):
        from src.cli.commands.test import _check_collection_tasks
        result = await _check_collection_tasks(db)
        assert result.status.value == "PASS"

    async def test_check_collection_tasks_fail(self):
        from src.cli.commands.test import _check_collection_tasks
        mock_db = MagicMock()
        mock_db.get_collection_tasks = AsyncMock(side_effect=RuntimeError("boom"))
        result = await _check_collection_tasks(mock_db)
        assert result.status.value == "FAIL"

    async def test_check_recent_searches_empty(self, db):
        from src.cli.commands.test import _check_recent_searches
        result = await _check_recent_searches(db)
        assert result.status.value in ("SKIP", "PASS")

    async def test_check_recent_searches_fail(self):
        from src.cli.commands.test import _check_recent_searches
        mock_db = MagicMock()
        mock_db.get_recent_searches = AsyncMock(side_effect=RuntimeError("boom"))
        result = await _check_recent_searches(mock_db)
        assert result.status.value == "FAIL"

    async def test_check_pipeline_list(self, db):
        from src.cli.commands.test import _check_pipeline_list
        result = await _check_pipeline_list(db)
        assert result.status.value == "PASS"

    async def test_check_pipeline_list_fail(self):
        from src.cli.commands.test import _check_pipeline_list
        mock_db = MagicMock()
        mock_db.repos.content_pipelines.get_all = AsyncMock(side_effect=RuntimeError("boom"))
        result = await _check_pipeline_list(mock_db)
        assert result.status.value == "FAIL"

    async def test_check_notification_bot(self, db):
        from src.cli.commands.test import _check_notification_bot
        result = await _check_notification_bot(db)
        assert result.status.value == "PASS"

    async def test_check_notification_bot_fail(self):
        from src.cli.commands.test import _check_notification_bot
        mock_db = MagicMock()
        mock_db.repos.notification_bots.count = AsyncMock(side_effect=RuntimeError("boom"))
        result = await _check_notification_bot(mock_db)
        assert result.status.value == "FAIL"

    async def test_check_photo_tasks(self, db):
        from src.cli.commands.test import _check_photo_tasks
        result = await _check_photo_tasks(db)
        assert result.status.value == "PASS"

    async def test_check_photo_tasks_fail(self):
        from src.cli.commands.test import _check_photo_tasks
        mock_db = MagicMock()
        mock_db.repos.photo_loader.list_batches = AsyncMock(side_effect=RuntimeError("boom"))
        result = await _check_photo_tasks(mock_db)
        assert result.status.value == "FAIL"




class TestDisableFloodAutoSleep:
    async def test_with_clients(self):
        from src.cli.commands.test import _disable_flood_auto_sleep
        raw_client = SimpleNamespace(flood_sleep_threshold=30)
        session = SimpleNamespace(raw_client=raw_client)
        pool = SimpleNamespace(clients={"+1111": SimpleNamespace()})
        with patch("src.cli.commands.test.adapt_transport_session", return_value=session):
            await _disable_flood_auto_sleep(pool)
        assert raw_client.flood_sleep_threshold == 0

    async def test_no_raw_client(self):
        from src.cli.commands.test import _disable_flood_auto_sleep
        session = SimpleNamespace(raw_client=None)
        pool = SimpleNamespace(clients={"+1111": SimpleNamespace()})
        with patch("src.cli.commands.test.adapt_transport_session", return_value=session):
            await _disable_flood_auto_sleep(pool)

    async def test_no_clients(self):
        from src.cli.commands.test import _disable_flood_auto_sleep
        pool = SimpleNamespace(clients=None)
        await _disable_flood_auto_sleep(pool)




class TestSkipRemainingTgChecks:
    def test_adds_skip_results(self):
        from src.cli.commands.test import _skip_remaining_tg_checks
        results = []
        _skip_remaining_tg_checks(results, "reason", ["a", "b"])
        assert len(results) == 2
        assert all(r.status.value == "SKIP" for r in results)




class TestGetLiveFloodAvailability:
    async def test_premium_flood(self):
        from src.cli.commands.test import _get_live_flood_availability
        from src.telegram.flood_wait import FloodWaitInfo
        info = FloodWaitInfo(
            operation="search_telegram", wait_seconds=10,
            next_available_at_utc=datetime.now(timezone.utc), detail="test",
        )
        avail = SimpleNamespace(state="ok")
        pool = SimpleNamespace(
            get_premium_stats_availability=AsyncMock(return_value=avail),
            get_stats_availability=AsyncMock(return_value=None),
        )
        result = await _get_live_flood_availability(pool, info)
        assert result is avail

    async def test_non_premium_flood(self):
        from src.cli.commands.test import _get_live_flood_availability
        from src.telegram.flood_wait import FloodWaitInfo
        info = FloodWaitInfo(
            operation="get_entity", wait_seconds=10,
            next_available_at_utc=datetime.now(timezone.utc), detail="test",
        )
        avail = SimpleNamespace(state="available")
        pool = SimpleNamespace(get_stats_availability=AsyncMock(return_value=avail))
        result = await _get_live_flood_availability(pool, info)
        assert result is avail

    async def test_no_getter(self):
        from src.cli.commands.test import _get_live_flood_availability
        from src.telegram.flood_wait import FloodWaitInfo
        info = FloodWaitInfo(
            operation="get_entity", wait_seconds=10,
            next_available_at_utc=datetime.now(timezone.utc), detail="test",
        )
        pool = SimpleNamespace()
        result = await _get_live_flood_availability(pool, info)
        assert result is None




class TestDecideLiveFloodActionExtended:
    async def test_all_flooded_short_wait(self):
        from src.cli.commands.test import _decide_live_test_flood_action
        from src.telegram.flood_wait import FloodWaitInfo
        info = FloodWaitInfo(
            operation="get_entity", wait_seconds=5,
            next_available_at_utc=datetime.now(timezone.utc), detail="test",
        )
        avail = SimpleNamespace(
            state="all_flooded", retry_after_sec=3,
            next_available_at_utc=datetime.now(timezone.utc),
        )
        pool = SimpleNamespace(get_stats_availability=AsyncMock(return_value=avail))
        decision = await _decide_live_test_flood_action(pool, info)
        assert decision.action == "wait_retry"

    async def test_all_flooded_long_wait(self):
        from src.cli.commands.test import _decide_live_test_flood_action
        from src.telegram.flood_wait import FloodWaitInfo
        info = FloodWaitInfo(
            operation="get_entity", wait_seconds=600,
            next_available_at_utc=datetime.now(timezone.utc), detail="test",
        )
        avail = SimpleNamespace(
            state="all_flooded", retry_after_sec=600, next_available_at_utc=None,
        )
        pool = SimpleNamespace(get_stats_availability=AsyncMock(return_value=avail))
        decision = await _decide_live_test_flood_action(pool, info)
        assert decision.action == "skip"




class TestHandleLiveFloodWait:
    async def test_rotate_action(self):
        from src.cli.commands.test import _handle_live_flood_wait
        from src.telegram.flood_wait import FloodWaitInfo
        info = FloodWaitInfo(
            operation="get_entity", wait_seconds=10,
            next_available_at_utc=datetime.now(timezone.utc), detail="flood", phone="+1111",
        )
        avail = SimpleNamespace(state="available")
        pool = SimpleNamespace(get_stats_availability=AsyncMock(return_value=avail))
        await _handle_live_flood_wait(pool, "check", info)

    async def test_wait_retry_action(self):
        from src.cli.commands.test import _handle_live_flood_wait
        from src.telegram.flood_wait import FloodWaitInfo
        info = FloodWaitInfo(
            operation="get_entity", wait_seconds=2,
            next_available_at_utc=datetime.now(timezone.utc), detail="flood", phone="+1111",
        )
        avail = SimpleNamespace(
            state="all_flooded", retry_after_sec=1, next_available_at_utc=None,
        )
        pool = SimpleNamespace(get_stats_availability=AsyncMock(return_value=avail))
        with patch("src.cli.commands.test.asyncio.sleep", new_callable=AsyncMock):
            await _handle_live_flood_wait(pool, "check", info)

    async def test_skip_action_raises(self):
        from src.cli.commands.test import TelegramLiveStepSkipError, _handle_live_flood_wait
        from src.telegram.flood_wait import FloodWaitInfo
        info = FloodWaitInfo(
            operation="get_entity", wait_seconds=600,
            next_available_at_utc=datetime.now(timezone.utc), detail="flood", phone="+1111",
        )
        avail = SimpleNamespace(
            state="all_flooded", retry_after_sec=600, next_available_at_utc=None,
        )
        pool = SimpleNamespace(get_stats_availability=AsyncMock(return_value=avail))
        with pytest.raises(TelegramLiveStepSkipError):
            await _handle_live_flood_wait(pool, "check", info)




class TestWaitForAvailableClientWindow:
    async def test_not_callable(self):
        from src.cli.commands.test import _wait_for_available_client_window
        pool = SimpleNamespace()
        result = await _wait_for_available_client_window(pool, "check")
        assert result == "No available client"

    async def test_not_all_flooded(self):
        from src.cli.commands.test import _wait_for_available_client_window
        avail = SimpleNamespace(state="available")
        pool = SimpleNamespace(get_stats_availability=AsyncMock(return_value=avail))
        result = await _wait_for_available_client_window(pool, "check")
        assert result is not None

    async def test_short_wait_returns_none(self):
        from src.cli.commands.test import _wait_for_available_client_window
        avail = SimpleNamespace(
            state="all_flooded", retry_after_sec=2,
            next_available_at_utc=datetime.now(timezone.utc),
        )
        pool = SimpleNamespace(get_stats_availability=AsyncMock(return_value=avail))
        with patch("src.cli.commands.test.asyncio.sleep", new_callable=AsyncMock):
            result = await _wait_for_available_client_window(pool, "check")
        assert result is None

    async def test_long_wait_returns_detail(self):
        from src.cli.commands.test import _wait_for_available_client_window
        avail = SimpleNamespace(
            state="all_flooded", retry_after_sec=600, next_available_at_utc=None,
        )
        pool = SimpleNamespace(get_stats_availability=AsyncMock(return_value=avail))
        result = await _wait_for_available_client_window(pool, "check")
        assert result is not None
        assert "flood" in result.lower()

    async def test_premium_mode(self):
        from src.cli.commands.test import _wait_for_available_client_window
        avail = SimpleNamespace(state="available")
        pool = SimpleNamespace(get_premium_stats_availability=AsyncMock(return_value=avail))
        result = await _wait_for_available_client_window(pool, "check", premium=True)
        assert result is not None


# ===========================================================================
# 2. cli/commands/dialogs.py — action branches
# ===========================================================================




class TestWriteChecks:
    async def test_run_write_checks_no_accounts(self, db, tmp_path):
        """_run_write_checks with empty DB — account_toggle should SKIP."""
        from src.cli.commands.test import _run_write_checks

        # Create a temp config pointing at our in-memory DB's path
        db_path = str(tmp_path / "test.db")
        # Save in-memory db state to a file
        import aiosqlite
        async with aiosqlite.connect(db_path) as conn:
            # Create minimal schema
            await conn.executescript("""
                CREATE TABLE IF NOT EXISTS accounts (
                    id INTEGER PRIMARY KEY,
                    phone TEXT, session_string TEXT, is_active INTEGER DEFAULT 1,
                    is_primary INTEGER DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS channels (
                    id INTEGER PRIMARY KEY,
                    channel_id INTEGER UNIQUE,
                    username TEXT, title TEXT, is_active INTEGER DEFAULT 1,
                    is_filtered INTEGER DEFAULT 0, channel_type TEXT,
                    subscriber_count INTEGER DEFAULT 0,
                    message_count INTEGER DEFAULT 0,
                    last_collected_id INTEGER DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY,
                    channel_id INTEGER, message_id INTEGER,
                    text TEXT, date TEXT,
                    UNIQUE(channel_id, message_id)
                );
            """)
            await conn.commit()

        config_path = str(tmp_path / "config.yaml")
        with open(config_path, "w") as f:
            f.write(f"database:\n  path: {db_path}\n")

        # Patch _init_db_copy to return our test DB
        from src.cli.commands.test import Status
        mock_db = MagicMock()
        mock_db.get_accounts = AsyncMock(return_value=[])
        mock_db.get_channels_with_counts = AsyncMock(return_value=[])
        mock_db.get_stats = AsyncMock(return_value={"channels": 0})
        mock_db.repos = MagicMock()
        mock_db.repos.search_queries = MagicMock()
        mock_db.repos.search_queries.add = AsyncMock(return_value=1)
        mock_db.repos.search_queries.get_all = AsyncMock(return_value=[
            SimpleNamespace(id=1, query="q", is_active=True),
        ])
        mock_db.repos.search_queries.set_active = AsyncMock()
        mock_db.repos.search_queries.delete = AsyncMock()
        mock_db.close = AsyncMock()
        mock_db.set_channel_active = AsyncMock()

        with patch("src.cli.commands.test._init_db_copy",
                    new_callable=AsyncMock,
                    return_value=(mock_db, db_path, {})):
            results = await _run_write_checks(config_path)

        # Should have passed write_db_copy, skipped account_toggle, passed SQ ops
        names = [r.name for r in results]
        assert "write_db_copy" in names
        assert "account_toggle" in names
        at = next(r for r in results if r.name == "account_toggle")
        assert at.status == Status.SKIP

    async def test_init_db_copy_error(self, tmp_path):
        """_init_db_copy should raise when the DB file cannot be copied."""
        from unittest.mock import AsyncMock, patch

        from src.cli.commands.test import _init_db_copy

        mock_config = MagicMock()
        mock_db = MagicMock()
        mock_db._db_path = str(tmp_path / "does_not_exist.db")
        mock_db._session_encryption_secret = None
        mock_db.close = AsyncMock()

        with patch("src.cli.runtime.init_db", new_callable=AsyncMock, return_value=(mock_config, mock_db)):
            with pytest.raises(sqlite3.Error):
                await _init_db_copy("any_config.yaml")


# ===========================================================================
# 25. messaging.py — phone/perm gate paths for each tool
# ===========================================================================

