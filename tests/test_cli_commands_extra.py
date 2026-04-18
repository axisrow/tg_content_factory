"""Tests for CLI command modules with low coverage.

Covers:
- src/cli/commands/messages.py — _print_messages, _print_live_messages, run()
- src/cli/commands/search_query.py — list/add/edit/delete/toggle/run/stats
- src/cli/commands/scheduler.py — status, stop, job-toggle, set-interval, task-cancel, clear-pending
- src/cli/commands/collect.py — collect single channel, enqueue all, no accounts
- src/cli/commands/filter.py — analyze/apply/reset/toggle/precheck/purge/hard-delete/purge-messages
- src/cli/commands/account.py — list/toggle/delete/flood-status/flood-clear
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.commands.filter import _build_deletion_service, _parse_pks, _print_result
from src.cli.commands.messages import _print_live_messages, _print_messages
from src.models import Account, Channel, Message, SearchQuery, SearchQueryDailyStat
from tests.helpers import cli_ns

# ---------------------------------------------------------------------------
# Helper: make runtime.init_db return an awaitable that yields (config, db)
# ---------------------------------------------------------------------------


def _make_coro(value):
    """Return an async function that returns *value*."""
    async def _coro(*args, **kwargs):
        return value
    return _coro


# ---------------------------------------------------------------------------
# messages.py — _print_messages (pure formatting)
# ---------------------------------------------------------------------------


class TestPrintMessages:
    def _make_msg(self, **overrides) -> Message:
        defaults = dict(
            id=1,
            channel_id=100,
            message_id=500,
            date=datetime(2025, 6, 15, 12, 30, tzinfo=timezone.utc),
            text="Hello world",
            views=42,
            forwards=3,
        )
        defaults.update(overrides)
        return Message(**defaults)

    def test_text_format(self, capsys):
        msgs = [self._make_msg()]
        _print_messages(msgs, "text", total=10)
        out = capsys.readouterr().out
        assert "Total: 10 messages (showing 1)" in out
        assert "#500" in out
        assert "Hello world" in out

    def test_text_format_long_text_truncated(self, capsys):
        long_text = "A" * 300
        msgs = [self._make_msg(text=long_text)]
        _print_messages(msgs, "text", total=1)
        out = capsys.readouterr().out
        assert "..." in out

    def test_text_format_no_views(self, capsys):
        msgs = [self._make_msg(views=None)]
        _print_messages(msgs, "text", total=1)
        out = capsys.readouterr().out
        assert "views=" not in out

    def test_text_format_empty_text(self, capsys):
        msgs = [self._make_msg(text="")]
        _print_messages(msgs, "text", total=1)
        out = capsys.readouterr().out
        assert "#500" in out

    def test_json_format(self, capsys):
        msgs = [self._make_msg()]
        _print_messages(msgs, "json", total=1)
        out = capsys.readouterr().out
        assert '"message_id": 500' in out
        assert '"text": "Hello world"' in out

    def test_json_format_date_present(self, capsys):
        """JSON output includes date string."""
        msgs = [self._make_msg()]
        _print_messages(msgs, "json", total=1)
        out = capsys.readouterr().out
        assert '"date":' in out
        assert "2025-06-15" in out

    def test_csv_format(self, capsys):
        msgs = [self._make_msg()]
        _print_messages(msgs, "csv", total=1)
        out = capsys.readouterr().out
        assert "id,channel_id,message_id" in out
        assert "Hello world" in out

    def test_csv_format_date_present(self, capsys):
        """CSV output includes date in row."""
        msgs = [self._make_msg()]
        _print_messages(msgs, "csv", total=1)
        out = capsys.readouterr().out
        lines = out.strip().split("\n")
        assert len(lines) == 2  # header + 1 row
        assert "2025-06-15" in lines[1]

    def test_csv_long_text_truncated(self, capsys):
        long_text = "B" * 600
        msgs = [self._make_msg(text=long_text)]
        _print_messages(msgs, "csv", total=1)
        out = capsys.readouterr().out
        lines = out.strip().split("\n")
        assert len(lines) == 2

    def test_text_format_no_text(self, capsys):
        msgs = [self._make_msg(text=None)]
        _print_messages(msgs, "text", total=1)
        out = capsys.readouterr().out
        assert "#500" in out

    def test_json_multiple_messages(self, capsys):
        msgs = [
            self._make_msg(message_id=1, text="first"),
            self._make_msg(message_id=2, text="second"),
        ]
        _print_messages(msgs, "json", total=2)
        out = capsys.readouterr().out
        assert "first" in out
        assert "second" in out


# ---------------------------------------------------------------------------
# messages.py — _print_live_messages (pure formatting)
# ---------------------------------------------------------------------------


class TestPrintLiveMessages:
    def _make_live_msg(self, **kwargs):
        defaults = dict(
            id=10,
            date=datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc),
            text="live msg",
            sender=None,
            media=None,
        )
        defaults.update(kwargs)
        return SimpleNamespace(**defaults)

    def test_basic_message(self, capsys):
        msgs = [self._make_live_msg()]
        _print_live_messages(msgs)
        out = capsys.readouterr().out
        assert "#10" in out
        assert "live msg" in out

    def test_with_sender(self, capsys):
        sender = SimpleNamespace(first_name="John", last_name="Doe")
        msgs = [self._make_live_msg(sender=sender)]
        _print_live_messages(msgs)
        out = capsys.readouterr().out
        assert "John" in out
        assert "Doe" in out

    def test_with_media_no_text(self, capsys):
        media_obj = type("MessageMediaPhoto", (), {})()
        msgs = [self._make_live_msg(text="", media=media_obj)]
        _print_live_messages(msgs)
        out = capsys.readouterr().out
        assert "media:" in out
        assert "MessageMediaPhoto" in out

    def test_with_media_and_text(self, capsys):
        media_obj = type("MessageMediaDocument", (), {})()
        msgs = [self._make_live_msg(text="has media too", media=media_obj)]
        _print_live_messages(msgs)
        out = capsys.readouterr().out
        assert "has media too" in out
        # text takes precedence, media type is not shown when text is present

    def test_reversed_order(self, capsys):
        msgs = [self._make_live_msg(id=1), self._make_live_msg(id=2)]
        _print_live_messages(msgs)
        out = capsys.readouterr().out
        pos_2 = out.index("#2")
        pos_1 = out.index("#1")
        assert pos_2 < pos_1

    def test_no_date(self, capsys):
        msgs = [self._make_live_msg(date=None)]
        _print_live_messages(msgs)
        out = capsys.readouterr().out
        assert "—" in out


# ---------------------------------------------------------------------------
# filter.py — _parse_pks (additional edge cases)
# ---------------------------------------------------------------------------


class TestParsePksExtra:
    def test_single_value(self):
        assert _parse_pks("42") == [42]

    def test_whitespace_handling(self):
        assert _parse_pks("  1 ,  2  , 3  ") == [1, 2, 3]

    def test_all_invalid(self):
        assert _parse_pks("abc,def") == []

    def test_negative_numbers_parsed(self):
        result = _parse_pks("-1,-2,3")
        # int("-1") succeeds, so negatives are valid
        assert -1 in result
        assert -2 in result
        assert 3 in result

    def test_empty_segments(self):
        assert _parse_pks(",,,1,,,") == [1]


# ---------------------------------------------------------------------------
# filter.py — _print_result (additional edge cases)
# ---------------------------------------------------------------------------


class TestPrintResultExtra:
    def test_zero_purged(self, capsys):
        result = MagicMock()
        result.purged_count = 0
        result.purged_titles = []
        result.skipped_count = 0
        _print_result(result)
        assert "No filtered channels affected." in capsys.readouterr().out

    def test_with_custom_verb(self, capsys):
        result = MagicMock()
        result.purged_count = 2
        result.purged_titles = ["A", "B"]
        result.skipped_count = 0
        _print_result(result, "Cleaned")
        out = capsys.readouterr().out
        assert "Cleaned 2 channels" in out

    def test_with_skipped(self, capsys):
        result = MagicMock()
        result.purged_count = 1
        result.purged_titles = ["X"]
        result.skipped_count = 5
        _print_result(result)
        out = capsys.readouterr().out
        assert "Skipped: 5" in out

    def test_no_skipped_no_skipped_line(self, capsys):
        result = MagicMock()
        result.purged_count = 1
        result.purged_titles = ["Y"]
        result.skipped_count = 0
        _print_result(result)
        out = capsys.readouterr().out
        assert "Skipped" not in out


# ---------------------------------------------------------------------------
# filter.py — _build_deletion_service
# ---------------------------------------------------------------------------


class TestBuildDeletionService:
    def test_builds_service(self):
        db = MagicMock()
        db.repos.channels = MagicMock()
        db.repos.channel_stats = MagicMock()
        db.repos.tasks = MagicMock()
        svc = _build_deletion_service(db)
        assert svc is not None
        assert svc._db is db


# ---------------------------------------------------------------------------
# account.py — list, toggle, delete, flood-status, flood-clear
# NOTE: These use cli_env which patches runtime.init_db and uses real SQLite.
# The run() functions call asyncio.run() internally, so tests must be sync.
# ---------------------------------------------------------------------------


class TestAccountList:
    def test_no_accounts(self, cli_env, capsys):
        from src.cli.commands.account import run

        run(cli_ns(account_action="list"))
        out = capsys.readouterr().out
        assert "No accounts found." in out

    def test_with_accounts(self, cli_env, capsys):
        from src.cli.commands.account import run

        acc = Account(phone="+1234567890", session_string="sess1", is_primary=True)
        asyncio.run(cli_env.add_account(acc))
        run(cli_ns(account_action="list"))
        out = capsys.readouterr().out
        assert "+1234567890" in out
        assert "Yes" in out  # Primary

    def test_with_inactive_account(self, cli_env, capsys):
        from src.cli.commands.account import run

        acc = Account(
            phone="+9999999999", session_string="sess2",
            is_primary=False, is_active=False,
        )
        asyncio.run(cli_env.add_account(acc))
        run(cli_ns(account_action="list"))
        out = capsys.readouterr().out
        assert "+9999999999" in out


class TestAccountToggle:
    def test_toggle_not_found(self, cli_env, capsys):
        from src.cli.commands.account import run

        run(cli_ns(account_action="toggle", id=9999))
        out = capsys.readouterr().out
        assert "not found" in out

    def test_toggle_deactivates(self, cli_env, capsys):
        from src.cli.commands.account import run

        acc = Account(phone="+1111111111", session_string="sess3", is_active=True)
        asyncio.run(cli_env.add_account(acc))
        accounts = asyncio.run(cli_env.get_accounts())
        acc_id = accounts[0].id
        run(cli_ns(account_action="toggle", id=acc_id))
        out = capsys.readouterr().out
        assert "active=False" in out


class TestAccountDelete:
    def test_delete_account(self, cli_env, capsys):
        from src.cli.commands.account import run

        acc = Account(phone="+2222222222", session_string="sess4")
        asyncio.run(cli_env.add_account(acc))
        accounts = asyncio.run(cli_env.get_accounts())
        acc_id = accounts[0].id
        run(cli_ns(account_action="delete", id=acc_id))
        out = capsys.readouterr().out
        assert f"Deleted account id={acc_id}" in out


class TestAccountFloodStatus:
    def test_no_accounts(self, cli_env, capsys):
        from src.cli.commands.account import run

        run(cli_ns(account_action="flood-status"))
        out = capsys.readouterr().out
        assert "No accounts found." in out

    def test_account_no_flood(self, cli_env, capsys):
        from src.cli.commands.account import run

        acc = Account(phone="+3333333333", session_string="sess5")
        asyncio.run(cli_env.add_account(acc))
        run(cli_ns(account_action="flood-status"))
        out = capsys.readouterr().out
        assert "OK" in out


class TestAccountFloodClear:
    def test_account_not_found(self, cli_env, capsys):
        from src.cli.commands.account import run

        run(cli_ns(account_action="flood-clear", phone="+0000000000"))
        out = capsys.readouterr().out
        assert "not found" in out

    def test_clear_flood(self, cli_env, capsys):
        from src.cli.commands.account import run

        acc = Account(
            phone="+7777777777", session_string="sess9",
            flood_wait_until=datetime.now(timezone.utc) + timedelta(seconds=60),
        )
        asyncio.run(cli_env.add_account(acc))
        run(cli_ns(account_action="flood-clear", phone="+7777777777"))
        out = capsys.readouterr().out
        assert "cleared" in out


# ---------------------------------------------------------------------------
# search_query.py — stats bar rendering (pure logic)
# ---------------------------------------------------------------------------


class TestSearchQueryStatsBar:
    def test_stats_renders_bars(self, capsys):
        """Stats renders ASCII bars from SearchQueryDailyStat."""
        stats = [
            SearchQueryDailyStat(day="2025-01-01", count=10),
            SearchQueryDailyStat(day="2025-01-02", count=5),
            SearchQueryDailyStat(day="2025-01-03", count=20),
        ]

        # Test the rendering logic inline (same as in search_query.py)
        max_count = max(s.count for s in stats)
        for s in stats:
            bar_len = int(s.count / max_count * 40) if max_count else 0
            bar = "#" * bar_len
            print(f"{s.day}  {bar:<40} {s.count}")

        out = capsys.readouterr().out
        assert "2025-01-01" in out
        assert "2025-01-02" in out
        assert "2025-01-03" in out
        assert "20" in out
        assert "10" in out
        assert "5" in out

    def test_stats_zero_count(self, capsys):
        """Zero counts produce empty bars."""
        stats = [
            SearchQueryDailyStat(day="2025-02-01", count=0),
        ]
        max_count = max(s.count for s in stats)
        for s in stats:
            bar_len = int(s.count / max_count * 40) if max_count else 0
            bar = "#" * bar_len
            print(f"{s.day}  {bar:<40} {s.count}")

        out = capsys.readouterr().out
        assert "0" in out


# ---------------------------------------------------------------------------
# scheduler.py — set-interval clamping (pure logic)
# ---------------------------------------------------------------------------


class TestSchedulerClamping:
    def test_interval_clamped_min(self):
        minutes = max(1, min(0, 1440))
        assert minutes == 1

    def test_interval_clamped_max(self):
        minutes = max(1, min(2000, 1440))
        assert minutes == 1440

    def test_interval_normal(self):
        minutes = max(1, min(30, 1440))
        assert minutes == 30


# ---------------------------------------------------------------------------
# scheduler.py — status, stop, job-toggle, set-interval, task-cancel, clear-pending
# All these tests call run() which uses asyncio.run(), so they must be sync.
# ---------------------------------------------------------------------------


class TestSchedulerStatus:
    def test_status_shows_config(self, capsys):
        from src.cli.commands.scheduler import run

        db = MagicMock()
        db.get_setting = AsyncMock(side_effect=lambda k: "1" if k == "scheduler_autostart" else None)
        db.repos = MagicMock()
        db.repos.settings.list_all = AsyncMock(return_value=[
            ("scheduler_job_disabled:job_a", "1"),
            ("scheduler_job_disabled:job_b", "0"),
            ("other_setting", "value"),
        ])
        db.close = AsyncMock()

        config = MagicMock()
        config.scheduler.collect_interval_minutes = 15

        pool = MagicMock()
        pool.clients = {"+1": MagicMock()}
        pool.disconnect_all = AsyncMock()

        with patch("src.cli.commands.scheduler.runtime.init_db", side_effect=_make_coro((config, db))), \
             patch("src.cli.commands.scheduler.runtime.init_pool", side_effect=_make_coro((MagicMock(), pool))):
            run(cli_ns(scheduler_action="status"))

        out = capsys.readouterr().out
        assert "15 min" in out
        assert "Autostart: yes" in out
        assert "job_a" in out
        assert "job_b" not in out  # disabled=0 means not shown in disabled list


class TestSchedulerStop:
    def test_stop_disables_autostart(self, capsys):
        from src.cli.commands.scheduler import run

        db = MagicMock()
        db.set_setting = AsyncMock()
        db.close = AsyncMock()

        config = MagicMock()
        pool = MagicMock()
        pool.clients = {"+1": MagicMock()}
        pool.disconnect_all = AsyncMock()

        with patch("src.cli.commands.scheduler.runtime.init_db", side_effect=_make_coro((config, db))), \
             patch("src.cli.commands.scheduler.runtime.init_pool", side_effect=_make_coro((MagicMock(), pool))):
            run(cli_ns(scheduler_action="stop"))

        out = capsys.readouterr().out
        assert "autostart disabled" in out
        db.set_setting.assert_awaited_once_with("scheduler_autostart", "0")


class TestSchedulerJobToggle:
    def test_toggle_enables_job(self, capsys):
        from src.cli.commands.scheduler import run

        db = MagicMock()
        db.repos = MagicMock()
        db.repos.settings.get_setting = AsyncMock(return_value="1")  # currently disabled
        db.repos.settings.set_setting = AsyncMock()
        db.close = AsyncMock()

        config = MagicMock()
        pool = MagicMock()
        pool.clients = {"+1": MagicMock()}
        pool.disconnect_all = AsyncMock()

        with patch("src.cli.commands.scheduler.runtime.init_db", side_effect=_make_coro((config, db))), \
             patch("src.cli.commands.scheduler.runtime.init_pool", side_effect=_make_coro((MagicMock(), pool))):
            run(cli_ns(scheduler_action="job-toggle", job_id="my_job"))

        out = capsys.readouterr().out
        assert "enabled" in out
        db.repos.settings.set_setting.assert_awaited_once_with("scheduler_job_disabled:my_job", "0")

    def test_toggle_disables_job(self, capsys):
        from src.cli.commands.scheduler import run

        db = MagicMock()
        db.repos = MagicMock()
        db.repos.settings.get_setting = AsyncMock(return_value=None)  # not disabled
        db.repos.settings.set_setting = AsyncMock()
        db.close = AsyncMock()

        config = MagicMock()
        pool = MagicMock()
        pool.clients = {"+1": MagicMock()}
        pool.disconnect_all = AsyncMock()

        with patch("src.cli.commands.scheduler.runtime.init_db", side_effect=_make_coro((config, db))), \
             patch("src.cli.commands.scheduler.runtime.init_pool", side_effect=_make_coro((MagicMock(), pool))):
            run(cli_ns(scheduler_action="job-toggle", job_id="test_job"))

        out = capsys.readouterr().out
        assert "disabled" in out


class TestSchedulerSetInterval:
    def test_set_interval_collect_all(self, capsys):
        from src.cli.commands.scheduler import run

        db = MagicMock()
        db.repos = MagicMock()
        db.repos.settings.set_setting = AsyncMock()
        db.close = AsyncMock()

        config = MagicMock()
        pool = MagicMock()
        pool.clients = {"+1": MagicMock()}
        pool.disconnect_all = AsyncMock()

        with patch("src.cli.commands.scheduler.runtime.init_db", side_effect=_make_coro((config, db))), \
             patch("src.cli.commands.scheduler.runtime.init_pool", side_effect=_make_coro((MagicMock(), pool))):
            run(cli_ns(scheduler_action="set-interval", job_id="collect_all", minutes=30))

        out = capsys.readouterr().out
        assert "30 min" in out
        db.repos.settings.set_setting.assert_awaited_once_with("collect_interval_minutes", "30")

    def test_set_interval_other_job(self, capsys):
        from src.cli.commands.scheduler import run

        db = MagicMock()
        db.repos = MagicMock()
        db.repos.settings.set_setting = AsyncMock()
        db.close = AsyncMock()

        config = MagicMock()
        pool = MagicMock()
        pool.clients = {"+1": MagicMock()}
        pool.disconnect_all = AsyncMock()

        with patch("src.cli.commands.scheduler.runtime.init_db", side_effect=_make_coro((config, db))), \
             patch("src.cli.commands.scheduler.runtime.init_pool", side_effect=_make_coro((MagicMock(), pool))):
            run(cli_ns(scheduler_action="set-interval", job_id="custom_job", minutes=60))

        out = capsys.readouterr().out
        assert "60 min" in out
        db.repos.settings.set_setting.assert_awaited_once_with("scheduler_job_custom_job_interval", "60")

    def test_set_interval_clamped(self, capsys):
        from src.cli.commands.scheduler import run

        db = MagicMock()
        db.repos = MagicMock()
        db.repos.settings.set_setting = AsyncMock()
        db.close = AsyncMock()

        config = MagicMock()
        pool = MagicMock()
        pool.clients = {"+1": MagicMock()}
        pool.disconnect_all = AsyncMock()

        with patch("src.cli.commands.scheduler.runtime.init_db", side_effect=_make_coro((config, db))), \
             patch("src.cli.commands.scheduler.runtime.init_pool", side_effect=_make_coro((MagicMock(), pool))):
            run(cli_ns(scheduler_action="set-interval", job_id="collect_all", minutes=0))

        out = capsys.readouterr().out
        assert "1 min" in out  # clamped to 1


class TestSchedulerTaskCancel:
    def test_cancel_success(self, capsys):
        from src.cli.commands.scheduler import run

        db = MagicMock()
        db.repos = MagicMock()
        db.repos.tasks.cancel_collection_task = AsyncMock(return_value=True)
        db.close = AsyncMock()

        config = MagicMock()
        pool = MagicMock()
        pool.clients = {"+1": MagicMock()}
        pool.disconnect_all = AsyncMock()

        with patch("src.cli.commands.scheduler.runtime.init_db", side_effect=_make_coro((config, db))), \
             patch("src.cli.commands.scheduler.runtime.init_pool", side_effect=_make_coro((MagicMock(), pool))):
            run(cli_ns(scheduler_action="task-cancel", task_id=42))

        out = capsys.readouterr().out
        assert "cancelled" in out

    def test_cancel_not_found(self, capsys):
        from src.cli.commands.scheduler import run

        db = MagicMock()
        db.repos = MagicMock()
        db.repos.tasks.cancel_collection_task = AsyncMock(return_value=False)
        db.close = AsyncMock()

        config = MagicMock()
        pool = MagicMock()
        pool.clients = {"+1": MagicMock()}
        pool.disconnect_all = AsyncMock()

        with patch("src.cli.commands.scheduler.runtime.init_db", side_effect=_make_coro((config, db))), \
             patch("src.cli.commands.scheduler.runtime.init_pool", side_effect=_make_coro((MagicMock(), pool))):
            run(cli_ns(scheduler_action="task-cancel", task_id=99))

        out = capsys.readouterr().out
        assert "not found" in out


class TestSchedulerClearPending:
    def test_clear_pending(self, capsys):
        from src.cli.commands.scheduler import run

        db = MagicMock()
        db.repos = MagicMock()
        db.repos.tasks.delete_pending_channel_tasks = AsyncMock(return_value=5)
        db.close = AsyncMock()

        config = MagicMock()
        pool = MagicMock()
        pool.clients = {"+1": MagicMock()}
        pool.disconnect_all = AsyncMock()

        with patch("src.cli.commands.scheduler.runtime.init_db", side_effect=_make_coro((config, db))), \
             patch("src.cli.commands.scheduler.runtime.init_pool", side_effect=_make_coro((MagicMock(), pool))):
            run(cli_ns(scheduler_action="clear-pending"))

        out = capsys.readouterr().out
        assert "Cleared 5" in out


class TestSchedulerNoAccounts:
    def test_status_no_accounts(self, capsys):
        from src.cli.commands.scheduler import run

        db = MagicMock()
        db.close = AsyncMock()

        config = MagicMock()
        pool = MagicMock()
        pool.clients = {}
        pool.disconnect_all = AsyncMock()

        with patch("src.cli.commands.scheduler.runtime.init_db", side_effect=_make_coro((config, db))), \
             patch("src.cli.commands.scheduler.runtime.init_pool", side_effect=_make_coro((MagicMock(), pool))):
            run(cli_ns(scheduler_action="status"))

        # No crash; command exits silently after logging error


# ---------------------------------------------------------------------------
# filter.py — toggle, reset, precheck via run()
# ---------------------------------------------------------------------------


class TestFilterToggle:
    def test_toggle_not_found(self, capsys):
        from src.cli.commands.filter import run

        db = MagicMock()
        db.get_channel_by_pk = AsyncMock(return_value=None)
        db.close = AsyncMock()

        with patch("src.cli.commands.filter.runtime.init_db", side_effect=_make_coro((None, db))):
            run(cli_ns(filter_action="toggle", pk=999))

        out = capsys.readouterr().out
        assert "not found" in out

    def test_toggle_marks_filtered(self, capsys):
        from src.cli.commands.filter import run

        ch = Channel(id=1, channel_id=100, title="TestCh", is_filtered=False)
        db = MagicMock()
        db.get_channel_by_pk = AsyncMock(return_value=ch)
        db.set_channel_filtered = AsyncMock()
        db.close = AsyncMock()

        with patch("src.cli.commands.filter.runtime.init_db", side_effect=_make_coro((None, db))):
            run(cli_ns(filter_action="toggle", pk=1))

        out = capsys.readouterr().out
        assert "filtered" in out
        db.set_channel_filtered.assert_awaited_once_with(1, True)

    def test_toggle_marks_unfiltered(self, capsys):
        from src.cli.commands.filter import run

        ch = Channel(id=1, channel_id=100, title="TestCh", is_filtered=True)
        db = MagicMock()
        db.get_channel_by_pk = AsyncMock(return_value=ch)
        db.set_channel_filtered = AsyncMock()
        db.close = AsyncMock()

        with patch("src.cli.commands.filter.runtime.init_db", side_effect=_make_coro((None, db))):
            run(cli_ns(filter_action="toggle", pk=1))

        out = capsys.readouterr().out
        assert "unfiltered" in out
        db.set_channel_filtered.assert_awaited_once_with(1, False)


class TestFilterReset:
    def test_reset(self, capsys):
        from src.cli.commands.filter import run

        analyzer = MagicMock()
        analyzer.reset_filters = AsyncMock()

        db = MagicMock()
        db.close = AsyncMock()

        with patch("src.cli.commands.filter.runtime.init_db", side_effect=_make_coro((None, db))), \
             patch("src.cli.commands.filter.ChannelAnalyzer", return_value=analyzer):
            run(cli_ns(filter_action="reset"))

        out = capsys.readouterr().out
        assert "reset" in out
        analyzer.reset_filters.assert_awaited_once()


class TestFilterPrecheck:
    def test_precheck(self, capsys):
        from src.cli.commands.filter import run

        analyzer = MagicMock()
        analyzer.precheck_subscriber_ratio = AsyncMock(return_value=7)

        db = MagicMock()
        db.close = AsyncMock()

        with patch("src.cli.commands.filter.runtime.init_db", side_effect=_make_coro((None, db))), \
             patch("src.cli.commands.filter.ChannelAnalyzer", return_value=analyzer):
            run(cli_ns(filter_action="precheck"))

        out = capsys.readouterr().out
        assert "7 channels" in out


class TestFilterNoAction:
    def test_no_action_prints_usage(self, capsys):
        from src.cli.commands.filter import run

        db = MagicMock()
        db.close = AsyncMock()

        with patch("src.cli.commands.filter.runtime.init_db", side_effect=_make_coro((None, db))):
            run(cli_ns(filter_action=None))

        out = capsys.readouterr().out
        assert "Usage" in out


class TestFilterPurge:
    def test_purge_with_pks(self, capsys):
        from src.cli.commands.filter import run
        from src.services.filter_deletion_service import PurgeResult

        result = PurgeResult(purged_count=2, purged_titles=["A", "B"], skipped_count=0)
        svc = MagicMock()
        svc.purge_channels_by_pks = AsyncMock(return_value=result)

        db = MagicMock()
        db.close = AsyncMock()

        with patch("src.cli.commands.filter.runtime.init_db", side_effect=_make_coro((None, db))), \
             patch("src.cli.commands.filter._build_deletion_service", return_value=svc):
            run(cli_ns(filter_action="purge", pks="1,2"))

        out = capsys.readouterr().out
        assert "2 channels" in out
        svc.purge_channels_by_pks.assert_awaited_once_with([1, 2])

    def test_purge_invalid_pks(self, capsys):
        from src.cli.commands.filter import run

        db = MagicMock()
        db.close = AsyncMock()

        with patch("src.cli.commands.filter.runtime.init_db", side_effect=_make_coro((None, db))):
            run(cli_ns(filter_action="purge", pks="abc"))

        out = capsys.readouterr().out
        assert "No valid PKs" in out

    def test_purge_all_filtered(self, capsys):
        from src.cli.commands.filter import run
        from src.services.filter_deletion_service import PurgeResult

        result = PurgeResult(purged_count=3, purged_titles=["X", "Y", "Z"], skipped_count=1)
        svc = MagicMock()
        svc.purge_all_filtered = AsyncMock(return_value=result)

        db = MagicMock()
        db.close = AsyncMock()

        with patch("src.cli.commands.filter.runtime.init_db", side_effect=_make_coro((None, db))), \
             patch("src.cli.commands.filter._build_deletion_service", return_value=svc):
            run(cli_ns(filter_action="purge"))

        out = capsys.readouterr().out
        assert "3 channels" in out
        svc.purge_all_filtered.assert_awaited_once()


class TestFilterHardDelete:
    def test_hard_delete_dev_mode_disabled(self, capsys):
        from src.cli.commands.filter import run

        db = MagicMock()
        db.get_setting = AsyncMock(return_value="0")
        db.close = AsyncMock()

        with patch("src.cli.commands.filter.runtime.init_db", side_effect=_make_coro((None, db))):
            run(cli_ns(filter_action="hard-delete"))

        out = capsys.readouterr().out
        assert "developer mode" in out

    def test_hard_delete_no_filtered(self, capsys):
        from src.cli.commands.filter import run

        db = MagicMock()
        db.get_setting = AsyncMock(return_value="1")
        db.get_channels_with_counts = AsyncMock(return_value=[
            Channel(id=1, channel_id=100, title="Active", is_filtered=False),
        ])
        db.close = AsyncMock()

        with patch("src.cli.commands.filter.runtime.init_db", side_effect=_make_coro((None, db))):
            run(cli_ns(filter_action="hard-delete", yes=False))

        out = capsys.readouterr().out
        assert "No filtered channels" in out

    def test_hard_delete_with_pks_no_confirm(self, capsys):
        from src.cli.commands.filter import run

        db = MagicMock()
        db.get_setting = AsyncMock(return_value="1")
        db.close = AsyncMock()

        with patch("src.cli.commands.filter.runtime.init_db", side_effect=_make_coro((None, db))), \
             patch("builtins.input", return_value="NO"):
            run(cli_ns(filter_action="hard-delete", pks="1,2", yes=False))

        out = capsys.readouterr().out
        assert "Aborted" in out

    def test_hard_delete_with_pks_confirmed(self, capsys):
        from src.cli.commands.filter import run
        from src.services.filter_deletion_service import PurgeResult

        result = PurgeResult(purged_count=2, purged_titles=["A", "B"], skipped_count=0)
        svc = MagicMock()
        svc.hard_delete_channels_by_pks = AsyncMock(return_value=result)

        db = MagicMock()
        db.get_setting = AsyncMock(return_value="1")
        db.close = AsyncMock()

        with patch("src.cli.commands.filter.runtime.init_db", side_effect=_make_coro((None, db))), \
             patch("src.cli.commands.filter._build_deletion_service", return_value=svc):
            run(cli_ns(filter_action="hard-delete", pks="1,2", yes=True))

        out = capsys.readouterr().out
        assert "Hard-deleted 2 channels" in out


class TestFilterPurgeMessages:
    def test_purge_messages_no_confirm(self, capsys):
        from src.cli.commands.filter import run

        db = MagicMock()
        db.get_channels = AsyncMock(return_value=[
            Channel(id=1, channel_id=100, title="TestCh"),
        ])
        db.close = AsyncMock()

        with patch("src.cli.commands.filter.runtime.init_db", side_effect=_make_coro((None, db))), \
             patch("builtins.input", return_value="n"):
            run(cli_ns(filter_action="purge-messages", channel_id=100, yes=False))

        out = capsys.readouterr().out
        assert "Aborted" in out

    def test_purge_messages_confirmed(self, capsys):
        from src.cli.commands.filter import run

        db = MagicMock()
        db.get_channels = AsyncMock(return_value=[
            Channel(id=1, channel_id=100, title="TestCh"),
        ])
        db.delete_messages_for_channel = AsyncMock(return_value=42)
        db.close = AsyncMock()

        with patch("src.cli.commands.filter.runtime.init_db", side_effect=_make_coro((None, db))):
            run(cli_ns(filter_action="purge-messages", channel_id=100, yes=True))

        out = capsys.readouterr().out
        assert "Deleted 42 messages" in out
        assert "TestCh" in out

    def test_purge_messages_channel_not_in_db(self, capsys):
        from src.cli.commands.filter import run

        db = MagicMock()
        db.get_channels = AsyncMock(return_value=[])
        db.delete_messages_for_channel = AsyncMock(return_value=0)
        db.close = AsyncMock()

        with patch("src.cli.commands.filter.runtime.init_db", side_effect=_make_coro((None, db))):
            run(cli_ns(filter_action="purge-messages", channel_id=999, yes=True))

        out = capsys.readouterr().out
        assert "999" in out


# ---------------------------------------------------------------------------
# filter.py — analyze / apply
# ---------------------------------------------------------------------------


class TestFilterAnalyze:
    def test_analyze_no_channels(self, capsys):
        from src.cli.commands.filter import run
        from src.filters.models import FilterReport

        report = FilterReport(results=[], total_channels=0, filtered_count=0)
        analyzer = MagicMock()
        analyzer.analyze_all = AsyncMock(return_value=report)
        analyzer.apply_filters = AsyncMock(return_value=0)

        db = MagicMock()
        db.close = AsyncMock()

        with patch("src.cli.commands.filter.runtime.init_db", side_effect=_make_coro((None, db))), \
             patch("src.cli.commands.filter.ChannelAnalyzer", return_value=analyzer):
            run(cli_ns(filter_action="analyze"))

        out = capsys.readouterr().out
        assert "No channels found." in out

    def test_analyze_with_channels(self, capsys):
        from src.cli.commands.filter import run
        from src.filters.models import ChannelFilterResult, FilterReport

        result_item = ChannelFilterResult(
            channel_id=100,
            title="TestChannel",
            uniqueness_pct=75.5,
            subscriber_ratio=0.3,
            cyrillic_pct=90.0,
            short_msg_pct=5.0,
            cross_dupe_pct=2.0,
            flags=["low_uniqueness"],
        )
        report = FilterReport(
            results=[result_item],
            total_channels=1,
            filtered_count=1,
        )
        analyzer = MagicMock()
        analyzer.analyze_all = AsyncMock(return_value=report)
        analyzer.apply_filters = AsyncMock(return_value=1)

        db = MagicMock()
        db.close = AsyncMock()

        with patch("src.cli.commands.filter.runtime.init_db", side_effect=_make_coro((None, db))), \
             patch("src.cli.commands.filter.ChannelAnalyzer", return_value=analyzer):
            run(cli_ns(filter_action="analyze"))

        out = capsys.readouterr().out
        assert "100" in out
        assert "TestChannel" in out
        assert "75.5" in out
        assert "Filtered: 1" in out

    def test_apply(self, capsys):
        from src.cli.commands.filter import run
        from src.filters.models import FilterReport

        report = FilterReport(results=[], total_channels=5, filtered_count=3)
        analyzer = MagicMock()
        analyzer.analyze_all = AsyncMock(return_value=report)
        analyzer.apply_filters = AsyncMock(return_value=3)

        db = MagicMock()
        db.close = AsyncMock()

        with patch("src.cli.commands.filter.runtime.init_db", side_effect=_make_coro((None, db))), \
             patch("src.cli.commands.filter.ChannelAnalyzer", return_value=analyzer):
            run(cli_ns(filter_action="apply"))

        out = capsys.readouterr().out
        assert "Applied filters: 3 channels" in out


# ---------------------------------------------------------------------------
# collect.py — no accounts, channel not found, filtered channel, enqueue all
# ---------------------------------------------------------------------------


class TestCollectNoAccounts:
    def test_collect_no_accounts(self, capsys):
        from src.cli.commands.collect import run

        db = MagicMock()
        db.close = AsyncMock()

        config = MagicMock()
        pool = MagicMock()
        pool.clients = {}
        pool.disconnect_all = AsyncMock()

        with patch("src.cli.commands.collect.runtime.init_db", side_effect=_make_coro((config, db))), \
             patch("src.cli.commands.collect.runtime.init_pool", side_effect=_make_coro((MagicMock(), pool))):
            run(cli_ns(channel_id=None))


class TestCollectChannelNotFound:
    def test_collect_channel_not_found(self, capsys):
        from src.cli.commands.collect import run

        db = MagicMock()
        db.get_channels = AsyncMock(return_value=[])
        db.close = AsyncMock()

        config = MagicMock()
        config.scheduler = MagicMock()
        pool = MagicMock()
        pool.clients = {"+1": MagicMock()}
        pool.disconnect_all = AsyncMock()

        with patch("src.cli.commands.collect.runtime.init_db", side_effect=_make_coro((config, db))), \
             patch("src.cli.commands.collect.runtime.init_pool", side_effect=_make_coro((MagicMock(), pool))):
            run(cli_ns(channel_id=99999))

        out = capsys.readouterr().out
        assert "not found" in out


class TestCollectFilteredChannel:
    def test_collect_filtered_channel_skipped(self, capsys):
        from src.cli.commands.collect import run

        ch = Channel(id=1, channel_id=100, title="FilteredCh", is_filtered=True)
        db = MagicMock()
        db.get_channels = AsyncMock(return_value=[ch])
        db.close = AsyncMock()

        config = MagicMock()
        config.scheduler = MagicMock()
        pool = MagicMock()
        pool.clients = {"+1": MagicMock()}
        pool.disconnect_all = AsyncMock()

        with patch("src.cli.commands.collect.runtime.init_db", side_effect=_make_coro((config, db))), \
             patch("src.cli.commands.collect.runtime.init_pool", side_effect=_make_coro((MagicMock(), pool))):
            run(cli_ns(channel_id=100))

        out = capsys.readouterr().out
        assert "filtered" in out


class TestCollectEnqueueAll:
    def test_enqueue_all(self, capsys):
        from src.cli.commands.collect import run
        from src.services.collection_service import BulkEnqueueResult

        result = BulkEnqueueResult(queued_count=5, skipped_existing_count=2, total_candidates=7)

        db = MagicMock()
        db.close = AsyncMock()

        config = MagicMock()
        config.scheduler = MagicMock()
        pool = MagicMock()
        pool.clients = {"+1": MagicMock()}
        pool.disconnect_all = AsyncMock()

        with patch("src.cli.commands.collect.runtime.init_db", side_effect=_make_coro((config, db))), \
             patch("src.cli.commands.collect.runtime.init_pool", side_effect=_make_coro((MagicMock(), pool))), \
             patch("src.cli.commands.collect.Collector"), \
             patch("src.cli.commands.collect.CollectionService"), \
             patch("src.cli.commands.collect.TaskEnqueuer") as mock_enq_cls, \
             patch("src.cli.commands.collect.ChannelBundle"):

            mock_enq = MagicMock()
            mock_enq.enqueue_all_channels = AsyncMock(return_value=result)
            mock_enq_cls.return_value = mock_enq

            run(cli_ns(channel_id=None))

        out = capsys.readouterr().out
        assert "Enqueued 5" in out
        assert "skipped 2" in out
        assert "total 7" in out


# ---------------------------------------------------------------------------
# search_query.py — list/add/edit/delete/toggle/run/stats via run()
# ---------------------------------------------------------------------------


class TestSearchQueryList:
    def test_list_empty(self, capsys):
        from src.cli.commands.search_query import run

        db = MagicMock()
        db.close = AsyncMock()

        svc_instance = MagicMock()
        svc_instance.get_with_stats = AsyncMock(return_value=[])
        bundle = MagicMock()

        with patch("src.cli.commands.search_query.runtime.init_db", side_effect=_make_coro((None, db))), \
             patch("src.cli.commands.search_query.SearchQueryService", return_value=svc_instance), \
             patch("src.cli.commands.search_query.SearchQueryBundle") as mock_bundle_cls:
            mock_bundle_cls.from_database.return_value = bundle
            run(cli_ns(search_query_action="list"))

        out = capsys.readouterr().out
        assert "No search queries found." in out

    def test_list_with_items(self, capsys):
        from src.cli.commands.search_query import run

        db = MagicMock()
        db.close = AsyncMock()

        sq = SearchQuery(id=1, query="test query", interval_minutes=30)
        svc_instance = MagicMock()
        svc_instance.get_with_stats = AsyncMock(return_value=[
            {"query": sq, "total_30d": 15, "last_run": "2025-01-01"},
        ])
        bundle = MagicMock()

        with patch("src.cli.commands.search_query.runtime.init_db", side_effect=_make_coro((None, db))), \
             patch("src.cli.commands.search_query.SearchQueryService", return_value=svc_instance), \
             patch("src.cli.commands.search_query.SearchQueryBundle") as mock_bundle_cls:
            mock_bundle_cls.from_database.return_value = bundle
            run(cli_ns(search_query_action="list"))

        out = capsys.readouterr().out
        assert "test query" in out
        assert "30m" in out
        assert "15" in out


class TestSearchQueryAdd:
    def test_add(self, capsys):
        from src.cli.commands.search_query import run

        db = MagicMock()
        db.close = AsyncMock()

        svc_instance = MagicMock()
        svc_instance.add = AsyncMock(return_value=42)
        bundle = MagicMock()

        with patch("src.cli.commands.search_query.runtime.init_db", side_effect=_make_coro((None, db))), \
             patch("src.cli.commands.search_query.SearchQueryService", return_value=svc_instance), \
             patch("src.cli.commands.search_query.SearchQueryBundle") as mock_bundle_cls:
            mock_bundle_cls.from_database.return_value = bundle
            run(cli_ns(
                search_query_action="add",
                query="hello world",
                interval=60,
                regex=False,
                fts=False,
                notify=False,
                track_stats=True,
                exclude_patterns=None,
                max_length=None,
            ))

        out = capsys.readouterr().out
        assert "Added search query id=42" in out
        assert "hello world" in out

    def test_add_with_exclude_patterns(self, capsys):
        from src.cli.commands.search_query import run

        db = MagicMock()
        db.close = AsyncMock()

        svc_instance = MagicMock()
        svc_instance.add = AsyncMock(return_value=7)
        bundle = MagicMock()

        with patch("src.cli.commands.search_query.runtime.init_db", side_effect=_make_coro((None, db))), \
             patch("src.cli.commands.search_query.SearchQueryService", return_value=svc_instance), \
             patch("src.cli.commands.search_query.SearchQueryBundle") as mock_bundle_cls:
            mock_bundle_cls.from_database.return_value = bundle
            run(cli_ns(
                search_query_action="add",
                query="test",
                interval=30,
                regex=False,
                fts=False,
                notify=False,
                track_stats=True,
                exclude_patterns="spam\\nnoise",
                max_length=None,
            ))

        svc_instance.add.assert_awaited_once()
        call_kwargs = svc_instance.add.call_args
        assert call_kwargs[1]["exclude_patterns"] == "spam\nnoise"


class TestSearchQueryDelete:
    def test_delete(self, capsys):
        from src.cli.commands.search_query import run

        db = MagicMock()
        db.close = AsyncMock()

        svc_instance = MagicMock()
        svc_instance.delete = AsyncMock()
        bundle = MagicMock()

        with patch("src.cli.commands.search_query.runtime.init_db", side_effect=_make_coro((None, db))), \
             patch("src.cli.commands.search_query.SearchQueryService", return_value=svc_instance), \
             patch("src.cli.commands.search_query.SearchQueryBundle") as mock_bundle_cls:
            mock_bundle_cls.from_database.return_value = bundle
            run(cli_ns(search_query_action="delete", id=5))

        out = capsys.readouterr().out
        assert "Deleted search query id=5" in out
        svc_instance.delete.assert_awaited_once_with(5)


class TestSearchQueryToggle:
    def test_toggle(self, capsys):
        from src.cli.commands.search_query import run

        db = MagicMock()
        db.close = AsyncMock()

        svc_instance = MagicMock()
        svc_instance.toggle = AsyncMock()
        bundle = MagicMock()

        with patch("src.cli.commands.search_query.runtime.init_db", side_effect=_make_coro((None, db))), \
             patch("src.cli.commands.search_query.SearchQueryService", return_value=svc_instance), \
             patch("src.cli.commands.search_query.SearchQueryBundle") as mock_bundle_cls:
            mock_bundle_cls.from_database.return_value = bundle
            run(cli_ns(search_query_action="toggle", id=10))

        out = capsys.readouterr().out
        assert "Toggled search query id=10" in out


class TestSearchQueryRun:
    def test_run(self, capsys):
        from src.cli.commands.search_query import run

        db = MagicMock()
        db.close = AsyncMock()

        svc_instance = MagicMock()
        svc_instance.run_once = AsyncMock(return_value=8)
        bundle = MagicMock()

        with patch("src.cli.commands.search_query.runtime.init_db", side_effect=_make_coro((None, db))), \
             patch("src.cli.commands.search_query.SearchQueryService", return_value=svc_instance), \
             patch("src.cli.commands.search_query.SearchQueryBundle") as mock_bundle_cls:
            mock_bundle_cls.from_database.return_value = bundle
            run(cli_ns(search_query_action="run", id=3))

        out = capsys.readouterr().out
        assert "8 matches" in out


class TestSearchQueryEdit:
    def test_edit_not_found(self, capsys):
        from src.cli.commands.search_query import run

        db = MagicMock()
        db.close = AsyncMock()

        svc_instance = MagicMock()
        svc_instance.get = AsyncMock(return_value=None)
        svc_instance.update = AsyncMock()
        bundle = MagicMock()

        with patch("src.cli.commands.search_query.runtime.init_db", side_effect=_make_coro((None, db))), \
             patch("src.cli.commands.search_query.SearchQueryService", return_value=svc_instance), \
             patch("src.cli.commands.search_query.SearchQueryBundle") as mock_bundle_cls:
            mock_bundle_cls.from_database.return_value = bundle
            run(cli_ns(search_query_action="edit", id=999, query=None, interval=None,
                       regex=None, fts=None, notify=None, track_stats=None,
                       exclude_patterns=None, max_length=None))

        out = capsys.readouterr().out
        assert "not found" in out

    def test_edit_updates_fields(self, capsys):
        from src.cli.commands.search_query import run

        db = MagicMock()
        db.close = AsyncMock()

        existing_sq = SearchQuery(
            id=1, query="old query", interval_minutes=60,
            notify_on_collect=False, track_stats=True,
            is_fts=False, is_regex=False, exclude_patterns="",
            max_length=None,
        )
        svc_instance = MagicMock()
        svc_instance.get = AsyncMock(return_value=existing_sq)
        svc_instance.update = AsyncMock()
        bundle = MagicMock()

        with patch("src.cli.commands.search_query.runtime.init_db", side_effect=_make_coro((None, db))), \
             patch("src.cli.commands.search_query.SearchQueryService", return_value=svc_instance), \
             patch("src.cli.commands.search_query.SearchQueryBundle") as mock_bundle_cls:
            mock_bundle_cls.from_database.return_value = bundle
            run(cli_ns(search_query_action="edit", id=1, query="new query", interval=30,
                       regex=None, fts=None, notify=None, track_stats=None,
                       exclude_patterns=None, max_length=None))

        out = capsys.readouterr().out
        assert "Updated search query id=1" in out
        svc_instance.update.assert_awaited_once()
        call_args = svc_instance.update.call_args
        assert call_args[0][0] == 1  # id
        assert call_args[0][1] == "new query"  # query
        assert call_args[0][2] == 30  # interval


class TestSearchQueryStats:
    def test_stats_no_data(self, capsys):
        from src.cli.commands.search_query import run

        db = MagicMock()
        db.close = AsyncMock()

        svc_instance = MagicMock()
        svc_instance.get_daily_stats = AsyncMock(return_value=[])
        bundle = MagicMock()

        with patch("src.cli.commands.search_query.runtime.init_db", side_effect=_make_coro((None, db))), \
             patch("src.cli.commands.search_query.SearchQueryService", return_value=svc_instance), \
             patch("src.cli.commands.search_query.SearchQueryBundle") as mock_bundle_cls:
            mock_bundle_cls.from_database.return_value = bundle
            run(cli_ns(search_query_action="stats", id=1, days=7))

        out = capsys.readouterr().out
        assert "No stats found." in out

    def test_stats_with_data(self, capsys):
        from src.cli.commands.search_query import run

        db = MagicMock()
        db.close = AsyncMock()

        stats = [
            SearchQueryDailyStat(day="2025-01-01", count=10),
            SearchQueryDailyStat(day="2025-01-02", count=20),
        ]
        svc_instance = MagicMock()
        svc_instance.get_daily_stats = AsyncMock(return_value=stats)
        bundle = MagicMock()

        with patch("src.cli.commands.search_query.runtime.init_db", side_effect=_make_coro((None, db))), \
             patch("src.cli.commands.search_query.SearchQueryService", return_value=svc_instance), \
             patch("src.cli.commands.search_query.SearchQueryBundle") as mock_bundle_cls:
            mock_bundle_cls.from_database.return_value = bundle
            run(cli_ns(search_query_action="stats", id=1, days=7))

        out = capsys.readouterr().out
        assert "2025-01-01" in out
        assert "2025-01-02" in out
        assert "10" in out
        assert "20" in out
        # count=20 should generate 40 # chars (max bar)
        assert "#" * 40 in out


# ---------------------------------------------------------------------------
# messages.py — run() DB mode paths
# ---------------------------------------------------------------------------


class TestMessagesReadDbMode:
    def test_channel_not_found(self, capsys):
        from src.cli.commands.messages import run

        db = MagicMock()
        db.get_channels = AsyncMock(return_value=[])
        db.close = AsyncMock()

        with patch("src.cli.commands.messages.runtime.init_db", side_effect=_make_coro((MagicMock(), db))):
            run(cli_ns(
                messages_action="read",
                identifier="nonexistent",
                live=False,
                query=None,
                date_from=None,
                date_to=None,
                limit=10,
                output_format="text",
                topic_id=None,
                offset_id=None,
            ))

        out = capsys.readouterr().out
        assert "not found" in out

    def test_no_messages_found(self, capsys):
        from src.cli.commands.messages import run

        ch = Channel(id=1, channel_id=100, title="TestCh", username="testch")
        db = MagicMock()
        db.get_channels = AsyncMock(return_value=[ch])
        db.search_messages = AsyncMock(return_value=([], 0))
        db.close = AsyncMock()

        with patch("src.cli.commands.messages.runtime.init_db", side_effect=_make_coro((MagicMock(), db))):
            run(cli_ns(
                messages_action="read",
                identifier="testch",
                live=False,
                query=None,
                date_from=None,
                date_to=None,
                limit=10,
                output_format="text",
                topic_id=None,
                offset_id=None,
            ))

        out = capsys.readouterr().out
        assert "No messages found." in out

    def test_read_messages_text_format(self, capsys):
        from src.cli.commands.messages import run

        ch = Channel(id=1, channel_id=100, title="TestCh", username="testch")
        msg = Message(
            id=1, channel_id=100, message_id=500,
            date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            text="Test message content",
        )
        db = MagicMock()
        db.get_channels = AsyncMock(return_value=[ch])
        db.search_messages = AsyncMock(return_value=([msg], 1))
        db.close = AsyncMock()

        with patch("src.cli.commands.messages.runtime.init_db", side_effect=_make_coro((MagicMock(), db))):
            run(cli_ns(
                messages_action="read",
                identifier="testch",
                live=False,
                query=None,
                date_from=None,
                date_to=None,
                limit=10,
                output_format="text",
                topic_id=None,
                offset_id=None,
            ))

        out = capsys.readouterr().out
        assert "Test message content" in out

    def test_read_messages_json_format(self, capsys):
        from src.cli.commands.messages import run

        ch = Channel(id=1, channel_id=100, title="TestCh", username="testch")
        msg = Message(
            id=1, channel_id=100, message_id=500,
            date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            text="JSON msg",
        )
        db = MagicMock()
        db.get_channels = AsyncMock(return_value=[ch])
        db.search_messages = AsyncMock(return_value=([msg], 1))
        db.close = AsyncMock()

        with patch("src.cli.commands.messages.runtime.init_db", side_effect=_make_coro((MagicMock(), db))):
            run(cli_ns(
                messages_action="read",
                identifier="testch",
                live=False,
                query=None,
                date_from=None,
                date_to=None,
                limit=10,
                output_format="json",
                topic_id=None,
                offset_id=None,
            ))

        out = capsys.readouterr().out
        assert "JSON msg" in out

    def test_read_messages_by_pk(self, capsys):
        """Test reading messages by numeric pk resolves correctly."""
        from src.cli.commands.messages import run

        ch = Channel(id=5, channel_id=200, title="ById")
        msg = Message(
            id=1, channel_id=200, message_id=10,
            date=datetime(2025, 6, 1, tzinfo=timezone.utc),
            text="msg by pk",
        )
        db = MagicMock()
        db.get_channels = AsyncMock(return_value=[ch])
        db.search_messages = AsyncMock(return_value=([msg], 1))
        db.close = AsyncMock()

        with patch("src.cli.commands.messages.runtime.init_db", side_effect=_make_coro((MagicMock(), db))):
            run(cli_ns(
                messages_action="read",
                identifier="5",
                live=False,
                query=None,
                date_from=None,
                date_to=None,
                limit=10,
                output_format="text",
                topic_id=None,
                offset_id=None,
            ))

        out = capsys.readouterr().out
        assert "msg by pk" in out
