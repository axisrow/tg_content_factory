"""CLI smoke tests — every command exercised via run(args) with mocked runtime."""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from src.config import AppConfig
from src.database import Database
from src.models import Account, Channel, Message

NOW = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def cli_db(tmp_path):
    """Sync fixture: real SQLite for CLI tests."""
    db_path = str(tmp_path / "cli_test.db")
    database = Database(db_path)
    asyncio.run(database.initialize())
    yield database
    asyncio.run(database.close())


@pytest.fixture
def cli_env(cli_db):
    """Patch runtime.init_db to return real db without loading config.yaml."""
    config = AppConfig()

    async def fake_init_db(config_path: str):
        return config, cli_db

    with patch("src.cli.runtime.init_db", side_effect=fake_init_db):
        yield cli_db


@pytest.fixture
def cli_env_with_pool(cli_env):
    """Additionally patch runtime.init_pool to return a pool with no clients."""
    fake_pool = AsyncMock()
    fake_pool.clients = {}
    fake_pool.disconnect_all = AsyncMock()

    async def fake_init_pool(config, db):
        from src.telegram.auth import TelegramAuth
        return TelegramAuth(0, ""), fake_pool

    with patch("src.cli.runtime.init_pool", side_effect=fake_init_pool):
        yield cli_env


def _ns(**kwargs) -> argparse.Namespace:
    """Build Namespace with defaults."""
    defaults = {"config": "config.yaml"}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def _add_account(db: Database, phone: str = "+70001112233") -> int:
    return asyncio.run(
        db.add_account(Account(phone=phone, session_string="sess"))
    )


def _add_channel(db: Database, channel_id: int = 100, title: str = "TestCh") -> int:
    return asyncio.run(
        db.add_channel(Channel(channel_id=channel_id, title=title))
    )


def _add_message(db: Database, channel_id: int = 100, message_id: int = 1, text: str = "hello"):
    asyncio.run(
        db.insert_message(
            Message(channel_id=channel_id, message_id=message_id, text=text, date=NOW)
        )
    )


# ---------------------------------------------------------------------------
# account
# ---------------------------------------------------------------------------


class TestCLIAccount:
    def test_list_empty(self, cli_env, capsys):
        from src.cli.commands.account import run
        run(_ns(account_action="list"))
        assert "No accounts found" in capsys.readouterr().out

    def test_list_with_data(self, cli_env, capsys):
        _add_account(cli_env)
        from src.cli.commands.account import run
        run(_ns(account_action="list"))
        assert "+70001112233" in capsys.readouterr().out

    def test_toggle(self, cli_env, capsys):
        pk = _add_account(cli_env)
        from src.cli.commands.account import run
        run(_ns(account_action="toggle", id=pk))
        assert "active=False" in capsys.readouterr().out

    def test_toggle_not_found(self, cli_env, capsys):
        from src.cli.commands.account import run
        run(_ns(account_action="toggle", id=999))
        assert "not found" in capsys.readouterr().out

    def test_delete(self, cli_env, capsys):
        pk = _add_account(cli_env)
        from src.cli.commands.account import run
        run(_ns(account_action="delete", id=pk))
        assert "Deleted" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# channel (DB-only)
# ---------------------------------------------------------------------------


class TestCLIChannelDB:
    def test_list_empty(self, cli_env, capsys):
        from src.cli.commands.channel import run
        run(_ns(channel_action="list"))
        assert "No channels found" in capsys.readouterr().out

    def test_list_with_data(self, cli_env, capsys):
        _add_channel(cli_env, title="MyChan")
        from src.cli.commands.channel import run
        run(_ns(channel_action="list"))
        assert "MyChan" in capsys.readouterr().out

    def test_delete(self, cli_env, capsys):
        pk = _add_channel(cli_env, channel_id=200, title="DelCh")
        from src.cli.commands.channel import run
        run(_ns(channel_action="delete", identifier=str(pk)))
        assert "Deleted" in capsys.readouterr().out

    def test_toggle(self, cli_env, capsys):
        pk = _add_channel(cli_env, channel_id=201, title="TogCh")
        from src.cli.commands.channel import run
        run(_ns(channel_action="toggle", identifier=str(pk)))
        assert "active=" in capsys.readouterr().out

    def test_toggle_not_found(self, cli_env, capsys):
        from src.cli.commands.channel import run
        run(_ns(channel_action="toggle", identifier="99999"))
        assert "not found" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# channel (pool-requiring)
# ---------------------------------------------------------------------------


class TestCLIChannelPool:
    def test_add_no_clients(self, cli_env_with_pool, capsys, caplog):
        from src.cli.commands.channel import run
        run(_ns(channel_action="add", identifier="@testchan"))
        assert "No connected accounts" in caplog.text

    def test_collect_not_found(self, cli_env_with_pool, capsys, caplog):
        from src.cli.commands.channel import run
        run(_ns(channel_action="collect", identifier="99999"))
        # Pool has no clients → logs error
        assert "No connected accounts" in caplog.text

    def test_stats_no_args(self, cli_env_with_pool, capsys, caplog):
        from src.cli.commands.channel import run
        run(_ns(channel_action="stats", identifier=None, all=False))
        # Pool has no clients → logs error
        assert "No connected accounts" in caplog.text

    def test_import_no_identifiers(self, cli_env_with_pool, capsys):
        from src.cli.commands.channel import run
        run(_ns(channel_action="import", source=""))
        assert "No identifiers found" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# filter
# ---------------------------------------------------------------------------


class TestCLIFilter:
    def test_analyze_empty(self, cli_env, capsys):
        from src.cli.commands.filter import run
        run(_ns(filter_action="analyze"))
        assert "No channels found" in capsys.readouterr().out

    def test_apply_empty(self, cli_env, capsys):
        from src.cli.commands.filter import run
        run(_ns(filter_action="apply"))
        assert "Applied filters: 0" in capsys.readouterr().out

    def test_reset(self, cli_env, capsys):
        from src.cli.commands.filter import run
        run(_ns(filter_action="reset"))
        assert "All channel filters have been reset" in capsys.readouterr().out

    def test_precheck(self, cli_env, capsys):
        from src.cli.commands.filter import run
        run(_ns(filter_action="precheck"))
        assert "Pre-filter applied" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------


class TestCLISearch:
    def test_local_empty(self, cli_env, capsys):
        from src.cli.commands.search import run
        ns = _ns(
            query="nonexistent", limit=20, mode="local",
            channel_id=None, min_length=None, max_length=None, fts=False,
        )
        run(ns)
        assert "Found 0 results" in capsys.readouterr().out

    def test_local_with_data(self, cli_env, capsys):
        _add_channel(cli_env, channel_id=300, title="SearchCh")
        _add_message(cli_env, channel_id=300, message_id=1, text="important message")
        from src.cli.commands.search import run
        ns = _ns(
            query="important", limit=20, mode="local",
            channel_id=None, min_length=None, max_length=None, fts=False,
        )
        run(ns)
        out = capsys.readouterr().out
        assert "Found" in out
        assert "important" in out


# ---------------------------------------------------------------------------
# search-query
# ---------------------------------------------------------------------------


def _add_search_query(
    db: Database, query: str = "test query", interval: int = 60,
    is_fts: bool = False, notify: bool = False, track_stats: bool = True,
) -> int:
    from src.database.bundles import SearchQueryBundle
    from src.services.search_query_service import SearchQueryService

    async def _add():
        svc = SearchQueryService(SearchQueryBundle.from_database(db))
        return await svc.add(
            query, interval, is_fts=is_fts,
            notify_on_collect=notify, track_stats=track_stats,
        )

    return asyncio.run(_add())


def _sq_ns(**kwargs) -> argparse.Namespace:
    defaults = dict(
        search_query_action=None, query=None, interval=60, id=None,
        regex=False, fts=False, notify=False, track_stats=True,
        exclude_patterns="", max_length=None, days=30,
    )
    defaults.update(kwargs)
    return _ns(**defaults)


class TestCLISearchQuery:
    def test_list_empty(self, cli_env, capsys):
        from src.cli.commands.search_query import run
        run(_sq_ns(search_query_action="list"))
        assert "No search queries found" in capsys.readouterr().out

    def test_list_with_data(self, cli_env, capsys):
        _add_search_query(cli_env, query="hello world")
        from src.cli.commands.search_query import run
        run(_sq_ns(search_query_action="list"))
        assert "hello world" in capsys.readouterr().out

    def test_add(self, cli_env, capsys):
        from src.cli.commands.search_query import run
        run(_sq_ns(search_query_action="add", query="new query"))
        assert "Added search query" in capsys.readouterr().out

    def test_add_with_fts_and_notify(self, cli_env, capsys):
        from src.cli.commands.search_query import run
        run(_sq_ns(search_query_action="add", query="foo OR bar", fts=True, notify=True))
        assert "Added search query" in capsys.readouterr().out

    def test_add_no_track_stats(self, cli_env, capsys):
        from src.cli.commands.search_query import run
        run(_sq_ns(search_query_action="add", query="test", track_stats=False))
        assert "Added search query" in capsys.readouterr().out

    def test_edit(self, cli_env, capsys):
        sq_id = _add_search_query(cli_env, query="original")
        from src.cli.commands.search_query import run
        run(_sq_ns(
            search_query_action="edit", id=sq_id, query="updated",
            regex=None, fts=None, notify=None, track_stats=None,
            exclude_patterns=None, max_length=None, interval=None,
        ))
        assert "Updated search query" in capsys.readouterr().out

    def test_toggle(self, cli_env, capsys):
        sq_id = _add_search_query(cli_env, query="toggle_me")
        from src.cli.commands.search_query import run
        run(_sq_ns(search_query_action="toggle", id=sq_id))
        assert "Toggled search query" in capsys.readouterr().out

    def test_delete(self, cli_env, capsys):
        sq_id = _add_search_query(cli_env, query="to_delete")
        from src.cli.commands.search_query import run
        run(_sq_ns(search_query_action="delete", id=sq_id))
        assert "Deleted search query" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# collect
# ---------------------------------------------------------------------------


class TestCLICollect:
    def test_no_clients(self, cli_env_with_pool, capsys, caplog):
        from src.cli.commands.collect import run
        run(_ns(channel_id=None))
        assert "No connected accounts" in caplog.text


# ---------------------------------------------------------------------------
# scheduler
# ---------------------------------------------------------------------------


class TestCLIScheduler:
    def test_no_clients(self, cli_env_with_pool, capsys, caplog):
        from src.cli.commands.scheduler import run
        run(_ns(scheduler_action="trigger"))
        assert "No connected accounts" in caplog.text


# ---------------------------------------------------------------------------
# test command
# ---------------------------------------------------------------------------


class TestCLITest:
    def test_read(self, cli_env, capsys):
        from src.cli.commands.test import run
        run(_ns(command="test", test_action="read"))
        out = capsys.readouterr().out
        assert "PASS" in out
        assert "db_init" in out

    def test_write(self, cli_env, capsys):
        from src.cli.commands.test import run
        run(_ns(command="test", test_action="write"))
        out = capsys.readouterr().out
        assert "Write Tests" in out

    def test_all(self, cli_env_with_pool, capsys):
        from src.cli.commands.test import run
        run(_ns(command="test", test_action="all"))
        out = capsys.readouterr().out
        assert "Read Tests" in out
        assert "Write Tests" in out
        assert "Telegram Live Tests" in out

    def test_parser_namespace(self):
        from src.cli.parser import build_parser
        parser = build_parser()
        args = parser.parse_args(["test", "read"])
        assert args.command == "test"
        assert args.test_action == "read"

        args = parser.parse_args(["test", "all"])
        assert args.test_action == "all"

        args = parser.parse_args(["test", "telegram"])
        assert args.test_action == "telegram"
