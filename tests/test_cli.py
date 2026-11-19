"""CLI smoke tests — every command exercised via run(args) with mocked runtime."""

from __future__ import annotations

import argparse
import asyncio
import subprocess
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import typer

from tests.helpers import cli_add_channel as _add_channel
from tests.helpers import cli_ns as _ns
from tg_agent.database import Database
from tg_agent.models import Account, Message

NOW = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
pytestmark = pytest.mark.aiosqlite_serial


@pytest.fixture
def cli_env_with_pool(cli_env):
    """Additionally patch runtime.init_pool to return a pool with no clients."""
    fake_pool = AsyncMock()
    fake_pool.clients = {}
    fake_pool.disconnect_all = AsyncMock()

    async def fake_init_pool(config, db, **_kwargs):
        from tg_shared.auth import TelegramAuth

        return TelegramAuth(0, ""), fake_pool

    with patch("tg_agent.cli.runtime.init_pool", side_effect=fake_init_pool):
        yield cli_env


def _add_account(db: Database, phone: str = "+70001112233") -> int:
    return asyncio.run(db.add_account(Account(phone=phone, session_string="sess")))



def _add_message(db: Database, channel_id: int = 100, message_id: int = 1, text: str = "hello"):
    asyncio.run(
        db.insert_message(
            Message(channel_id=channel_id, message_id=message_id, text=text, date=NOW)
        )
    )


def _query_db(db, sql: str, params: tuple = ()) -> list[dict]:
    """Read DB state after cli run() has closed the aiosqlite connection."""
    import sqlite3

    conn = sqlite3.connect(db._db_path)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# account
# ---------------------------------------------------------------------------


class TestCLIAccount:
    def test_toggle(self, cli_env, capsys):
        pk = _add_account(cli_env)
        from tg_agent.cli.commands.account import run

        run(_ns(account_action="toggle", id=pk))
        assert "active=False" in capsys.readouterr().out

    def test_toggle_not_found(self, cli_env, capsys):
        from tg_agent.cli.commands.account import run

        run(_ns(account_action="toggle", id=999))
        assert "not found" in capsys.readouterr().out

    def test_delete(self, cli_env, capsys):
        pk = _add_account(cli_env)
        from tg_agent.cli.commands.account import run

        run(_ns(account_action="delete", id=pk))
        assert "Deleted" in capsys.readouterr().out

    def test_delete_not_found(self, cli_env, capsys):
        from tg_agent.cli.commands.account import run

        run(_ns(account_action="delete", id=999))
        assert "not found" in capsys.readouterr().out

    def test_delete_notification_account_with_notify_to(self, cli_env, capsys):
        pk = _add_account(cli_env, phone="+70001112233")
        _add_account(cli_env, phone="+70001112234")
        _add_account(cli_env, phone="+70001112235")
        asyncio.run(cli_env.set_setting("notification_account_phone", "+70001112233"))
        from tg_agent.cli.commands.account import run

        run(_ns(account_action="delete", id=pk, notify_to="+70001112235"))
        out = capsys.readouterr().out
        assert "Deleted" in out
        assert "переназначены на +70001112235" in out
        rows = _query_db(
            cli_env, "SELECT value FROM settings WHERE key=?", ("notification_account_phone",)
        )
        assert rows[0]["value"] == "+70001112235"

    def test_delete_notification_account_auto_single_remaining(self, cli_env, capsys):
        pk = _add_account(cli_env, phone="+70001112233")
        _add_account(cli_env, phone="+70001112234")
        asyncio.run(cli_env.set_setting("notification_account_phone", "+70001112233"))
        from tg_agent.cli.commands.account import run

        run(_ns(account_action="delete", id=pk))
        out = capsys.readouterr().out
        assert "Deleted" in out
        assert "переназначены на +70001112234" in out
        rows = _query_db(
            cli_env, "SELECT value FROM settings WHERE key=?", ("notification_account_phone",)
        )
        assert rows[0]["value"] == "+70001112234"

    def test_delete_notification_account_clears_without_choice(self, cli_env, capsys):
        """Non-TTY, several remaining, no --notify-to: setting falls back to Primary."""
        pk = _add_account(cli_env, phone="+70001112233")
        _add_account(cli_env, phone="+70001112234")
        _add_account(cli_env, phone="+70001112235")
        asyncio.run(cli_env.set_setting("notification_account_phone", "+70001112233"))
        from tg_agent.cli.commands.account import run

        with patch("sys.stdin") as stdin_mock:
            stdin_mock.isatty.return_value = False
            run(_ns(account_action="delete", id=pk))
        out = capsys.readouterr().out
        assert "Deleted" in out
        assert "сброшен" in out
        rows = _query_db(
            cli_env, "SELECT value FROM settings WHERE key=?", ("notification_account_phone",)
        )
        assert rows[0]["value"] == ""

    def test_delete_notification_account_invalid_notify_to_aborts(self, cli_env, capsys):
        pk = _add_account(cli_env, phone="+70001112233")
        _add_account(cli_env, phone="+70001112234")
        asyncio.run(cli_env.set_setting("notification_account_phone", "+70001112233"))
        from tg_agent.cli.commands.account import run

        with pytest.raises(typer.Exit):
            run(_ns(account_action="delete", id=pk, notify_to="+9999"))
        captured = capsys.readouterr()
        assert "ERROR" in captured.err
        assert "не удалён" in captured.err
        assert captured.out == ""
        rows = _query_db(cli_env, "SELECT COUNT(*) AS cnt FROM accounts WHERE id=?", (pk,))
        assert rows[0]["cnt"] == 1
        rows = _query_db(
            cli_env, "SELECT value FROM settings WHERE key=?", ("notification_account_phone",)
        )
        assert rows[0]["value"] == "+70001112233"

    def test_delete_notification_account_interactive_prompt(self, cli_env, capsys):
        pk = _add_account(cli_env, phone="+70001112233")
        _add_account(cli_env, phone="+70001112234")
        _add_account(cli_env, phone="+70001112235")
        asyncio.run(cli_env.set_setting("notification_account_phone", "+70001112233"))
        from tg_agent.cli.commands.account import run

        with (
            patch("sys.stdin") as stdin_mock,
            patch("builtins.input", return_value="2"),
        ):
            stdin_mock.isatty.return_value = True
            run(_ns(account_action="delete", id=pk))
        out = capsys.readouterr().out
        assert "Deleted" in out
        assert "переназначены на +70001112235" in out
        rows = _query_db(
            cli_env, "SELECT value FROM settings WHERE key=?", ("notification_account_phone",)
        )
        assert rows[0]["value"] == "+70001112235"

    def test_delete_notification_account_prompt_eof_falls_back(self, cli_env, capsys):
        """Ctrl-D in the interactive prompt applies the auto-rule instead of crashing."""
        pk = _add_account(cli_env, phone="+70001112233")
        _add_account(cli_env, phone="+70001112234")
        _add_account(cli_env, phone="+70001112235")
        asyncio.run(cli_env.set_setting("notification_account_phone", "+70001112233"))
        from tg_agent.cli.commands.account import run

        with (
            patch("sys.stdin") as stdin_mock,
            patch("builtins.input", side_effect=EOFError),
        ):
            stdin_mock.isatty.return_value = True
            run(_ns(account_action="delete", id=pk))
        out = capsys.readouterr().out
        assert "Deleted" in out
        assert "сброшен" in out
        rows = _query_db(
            cli_env, "SELECT value FROM settings WHERE key=?", ("notification_account_phone",)
        )
        assert rows[0]["value"] == ""

    def test_info_shows_live_account_profile(self, cli_env_with_pool, capsys):
        """account info: shows connected account profile diagnostics."""
        from tg_agent.models import TelegramUserInfo

        db = cli_env_with_pool
        phone = "+10009998877"
        _add_account(db, phone=phone)

        user_info = TelegramUserInfo(
            phone=phone,
            first_name="Alice",
            last_name="Smith",
            username="alice",
            is_primary=True,
        )

        fake_pool = AsyncMock(
            clients={phone: MagicMock()},
            get_users_info=AsyncMock(return_value=[user_info]),
            disconnect_all=AsyncMock(),
        )
        init_pool_mock = AsyncMock(return_value=(MagicMock(), fake_pool))
        with patch("tg_agent.cli.runtime.init_pool", new=init_pool_mock):
            from tg_agent.cli.commands.account import run

            run(_ns(account_action="info", phone=None))

        out = capsys.readouterr().out
        assert phone in out
        assert "Alice Smith" in out
        assert "@alice" in out
        assert "Live Telegram accounts (1)" in out

    def test_info_filter_by_phone(self, cli_env_with_pool, capsys):
        """account info --phone: only shows matching account."""
        from tg_agent.models import TelegramUserInfo

        db = cli_env_with_pool
        phone1 = "+10009998877"
        phone2 = "+10001112233"
        _add_account(db, phone=phone1)
        _add_account(db, phone=phone2)

        users = [
            TelegramUserInfo(phone=phone1, first_name="Alice"),
            TelegramUserInfo(phone=phone2, first_name="Bob"),
        ]

        fake_pool = AsyncMock(
            clients={phone1: MagicMock(), phone2: MagicMock()},
            get_users_info=AsyncMock(return_value=users),
            disconnect_all=AsyncMock(),
        )
        init_pool_mock = AsyncMock(return_value=(MagicMock(), fake_pool))
        with patch("tg_agent.cli.runtime.init_pool", new=init_pool_mock):
            from tg_agent.cli.commands.account import run

            run(_ns(account_action="info", phone=phone1))

        out = capsys.readouterr().out
        assert phone1 in out
        assert phone2 not in out
        assert init_pool_mock.await_args.kwargs == {"phones": (phone1,)}

    def test_info_no_connected_accounts(self, cli_env_with_pool, capsys):
        """account info: prints shared no-live-profile diagnostics."""
        fake_pool = AsyncMock(
            clients={},
            get_users_info=AsyncMock(return_value=[]),
            disconnect_all=AsyncMock(),
        )
        init_pool_mock = AsyncMock(return_value=(MagicMock(), fake_pool))
        with patch("tg_agent.cli.runtime.init_pool", new=init_pool_mock):
            from tg_agent.cli.commands.account import run

            run(_ns(account_action="info", phone=None))

        assert "Live Telegram accounts not found" in capsys.readouterr().out

    def test_info_connected_runtime_with_empty_profiles(self, cli_env_with_pool, capsys):
        """account info does not treat connected phones as disconnected when profile fetch is empty."""
        phone = "+10009998877"
        _add_account(cli_env_with_pool, phone=phone)
        fake_pool = AsyncMock(
            clients={phone: MagicMock()},
            get_users_info=AsyncMock(return_value=[]),
            disconnect_all=AsyncMock(),
        )
        init_pool_mock = AsyncMock(return_value=(MagicMock(), fake_pool))
        with patch("tg_agent.cli.runtime.init_pool", new=init_pool_mock):
            from tg_agent.cli.commands.account import run

            run(_ns(account_action="info", phone=None))

        out = capsys.readouterr().out
        assert "Runtime connected phones" in out
        assert phone in out
        assert "do not treat this as disconnected" in out

    def test_flood_status_no_flood(self, cli_env, capsys):
        _add_account(cli_env, phone="+10001112233")
        from tg_agent.cli.commands.account import run

        run(_ns(account_action="flood-status"))
        out = capsys.readouterr().out
        assert "+10001112233" in out
        assert "OK" in out

    def test_flood_status_with_flood(self, cli_env, capsys):
        import re
        from datetime import datetime, timedelta, timezone

        phone = "+10001112244"
        _add_account(cli_env, phone=phone)
        until = datetime.now(timezone.utc) + timedelta(seconds=300)
        asyncio.run(cli_env.update_account_flood(phone, until))
        from tg_agent.cli.commands.account import run

        run(_ns(account_action="flood-status"))
        out = capsys.readouterr().out
        assert phone in out
        assert re.search(r"\d+s", out)

    def test_flood_status_no_accounts(self, cli_env, capsys):
        from tg_agent.cli.commands.account import run

        run(_ns(account_action="flood-status"))
        assert "No accounts found." in capsys.readouterr().out

    def test_flood_clear(self, cli_env, capsys):
        from datetime import datetime, timedelta, timezone

        phone = "+10001112255"
        _add_account(cli_env, phone=phone)
        until = datetime.now(timezone.utc) + timedelta(seconds=120)
        asyncio.run(cli_env.update_account_flood(phone, until))
        from tg_agent.cli.commands.account import run

        run(_ns(account_action="flood-clear", phone=phone))
        assert "cleared" in capsys.readouterr().out

    def test_flood_clear_not_found(self, cli_env, capsys):
        from tg_agent.cli.commands.account import run

        run(_ns(account_action="flood-clear", phone="+19990000000"))
        assert "not found" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# channel (DB-only)
# ---------------------------------------------------------------------------


class TestCLIChannelDB:
    def test_delete(self, cli_env, capsys):
        pk = _add_channel(cli_env, channel_id=200, title="DelCh")
        from tg_agent.cli.commands.channel import run

        run(_ns(channel_action="delete", identifier=str(pk)))
        assert "Deleted" in capsys.readouterr().out

    def test_toggle(self, cli_env, capsys):
        pk = _add_channel(cli_env, channel_id=201, title="TogCh")
        from tg_agent.cli.commands.channel import run

        run(_ns(channel_action="toggle", identifier=str(pk)))
        assert "active=" in capsys.readouterr().out

    def test_toggle_not_found(self, cli_env, capsys):
        from tg_agent.cli.commands.channel import run

        run(_ns(channel_action="toggle", identifier="99999"))
        assert "not found" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# channel (pool-requiring)
# ---------------------------------------------------------------------------


class TestCLIChannelPool:
    def test_add_no_clients(self, cli_env_with_pool, capsys, caplog):
        from tg_agent.cli.commands.channel import run

        run(_ns(channel_action="add", identifier="@testchan"))
        assert "No connected accounts" in caplog.text

    def test_collect_not_found(self, cli_env_with_pool, capsys, caplog):
        from tg_agent.cli.commands.channel import run

        run(_ns(channel_action="collect", identifier="99999"))
        # Pool has no clients → logs error
        assert "No connected accounts" in caplog.text

    def test_stats_no_args(self, cli_env_with_pool, capsys, caplog):
        from tg_agent.cli.commands.channel import run

        run(_ns(channel_action="stats", identifier=None, all=False))
        # stats_impl no longer opens a pool (#1569) — it just asks for a target.
        assert "Specify a channel identifier or use --all" in capsys.readouterr().out

    def test_import_no_identifiers(self, cli_env_with_pool, capsys):
        from tg_agent.cli.commands.channel import run

        run(_ns(channel_action="import", source=""))
        assert "No identifiers found" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# filter
# ---------------------------------------------------------------------------


class TestCLIFilter:
    def test_analyze_empty(self, cli_env, capsys):
        from tg_agent.cli.commands.filter import run

        run(_ns(filter_action="analyze"))
        assert "No channels found" in capsys.readouterr().out

    def test_apply_empty(self, cli_env, capsys):
        from tg_agent.cli.commands.filter import run

        run(_ns(filter_action="apply"))
        assert "Applied filters: 0" in capsys.readouterr().out

    def test_reset(self, cli_env, capsys):
        from tg_agent.cli.commands.filter import run

        run(_ns(filter_action="reset"))
        assert "All channel filters have been reset" in capsys.readouterr().out

    def test_precheck(self, cli_env, capsys):
        from tg_agent.cli.commands.filter import run

        run(_ns(filter_action="precheck"))
        assert "Pre-filter applied" in capsys.readouterr().out

    def test_purge_no_filtered(self, cli_env, capsys):
        from tg_agent.cli.commands.filter import run

        run(_ns(filter_action="purge", pks=None, yes=True))
        assert "No filtered channels affected." in capsys.readouterr().out

    def test_purge_filtered_channel(self, cli_env, capsys):
        ch_pk = _add_channel(cli_env, channel_id=999, title="FilteredCh")
        asyncio.run(cli_env.set_channel_filtered(ch_pk, True))
        _add_message(cli_env, channel_id=999, message_id=1, text="to be deleted")

        from tg_agent.cli.commands.filter import run

        run(_ns(filter_action="purge", pks=None, yes=True))
        out = capsys.readouterr().out
        assert "1 channels" in out
        assert "FilteredCh" in out

        rows = _query_db(cli_env, "SELECT COUNT(*) AS cnt FROM messages WHERE channel_id=?", (999,))
        assert rows[0]["cnt"] == 0  # messages deleted

        rows = _query_db(cli_env, "SELECT COUNT(*) AS cnt FROM channels WHERE channel_id=?", (999,))
        assert rows[0]["cnt"] == 1  # channel row retained

    def test_purge_with_pks(self, cli_env, capsys):
        ch1_pk = _add_channel(cli_env, channel_id=990, title="PurgeCh1")
        ch2_pk = _add_channel(cli_env, channel_id=991, title="PurgeCh2")
        asyncio.run(cli_env.set_channel_filtered(ch1_pk, True))
        asyncio.run(cli_env.set_channel_filtered(ch2_pk, True))
        _add_message(cli_env, channel_id=990, message_id=1)
        _add_message(cli_env, channel_id=991, message_id=1)

        from tg_agent.cli.commands.filter import run

        run(_ns(filter_action="purge", pks=str(ch1_pk), yes=True))
        out = capsys.readouterr().out
        assert "PurgeCh1" in out
        assert "PurgeCh2" not in out

        rows = _query_db(cli_env, "SELECT COUNT(*) AS cnt FROM messages WHERE channel_id=?", (990,))
        assert rows[0]["cnt"] == 0  # ch1 messages deleted

        rows = _query_db(cli_env, "SELECT COUNT(*) AS cnt FROM messages WHERE channel_id=?", (991,))
        assert rows[0]["cnt"] == 1  # ch2 messages untouched

    def test_hard_delete_requires_dev_mode(self, cli_env, capsys):
        from tg_agent.cli.commands.filter import run

        run(_ns(filter_action="hard-delete", pks=None, yes=True))
        assert "dev" in capsys.readouterr().out.lower()

    def test_hard_delete_removes_channel(self, cli_env, capsys):
        asyncio.run(cli_env.set_setting("agent_dev_mode_enabled", "1"))
        ch_pk = _add_channel(cli_env, channel_id=888, title="ToDelete")
        asyncio.run(cli_env.set_channel_filtered(ch_pk, True))

        from tg_agent.cli.commands.filter import run

        run(_ns(filter_action="hard-delete", pks=None, yes=True))
        out = capsys.readouterr().out
        # _print_result prints channel title when deletion succeeds
        assert "ToDelete" in out
        assert "1 channels" in out

        rows = _query_db(cli_env, "SELECT COUNT(*) AS cnt FROM channels WHERE channel_id=?", (888,))
        assert rows[0]["cnt"] == 0  # channel deleted

    def test_hard_delete_no_filtered(self, cli_env, capsys):
        asyncio.run(cli_env.set_setting("agent_dev_mode_enabled", "1"))
        _add_channel(cli_env, channel_id=887, title="NotFiltered")

        from tg_agent.cli.commands.filter import run

        run(_ns(filter_action="hard-delete", pks=None, yes=True))
        assert "No filtered channels to delete." in capsys.readouterr().out


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------


class TestCLISearch:
    def test_local_empty(self, cli_env, capsys):
        from tg_agent.cli.commands.search import run

        ns = _ns(
            query="nonexistent",
            limit=20,
            mode="local",
            channel_id=None,
            min_length=None,
            max_length=None,
            fts=False,
        )
        run(ns)
        assert "Found 0 results" in capsys.readouterr().out

    def test_local_with_data(self, cli_env, capsys):
        _add_channel(cli_env, channel_id=300, title="SearchCh")
        _add_message(cli_env, channel_id=300, message_id=1, text="important message")
        from tg_agent.cli.commands.search import run

        ns = _ns(
            query="important",
            limit=20,
            mode="local",
            channel_id=None,
            min_length=None,
            max_length=None,
            fts=False,
        )
        run(ns)
        out = capsys.readouterr().out
        assert "Found" in out
        assert "important" in out

    def test_semantic_with_data(self, cli_env, monkeypatch, capsys):
        _add_channel(cli_env, channel_id=301, title="SemanticCh")
        _add_message(cli_env, channel_id=301, message_id=1, text="Bitcoin outlook")
        rows = asyncio.run(cli_env.execute_fetchall("SELECT id FROM messages ORDER BY id"))
        emb = [(int(rows[0]["id"]), [1.0, 0.0])]
        asyncio.run(cli_env.repos.messages.upsert_message_embeddings(emb))
        asyncio.run(cli_env.repos.messages.upsert_message_embedding_json(emb))

        from tg_agent.cli.commands.search import run
        from tg_agent.services.embedding_service import EmbeddingService

        monkeypatch.setattr(
            EmbeddingService,
            "index_pending_messages",
            AsyncMock(return_value=0),
        )
        monkeypatch.setattr(
            EmbeddingService,
            "embed_query",
            AsyncMock(return_value=[1.0, 0.0]),
        )

        ns = _ns(
            query="bitcoin",
            limit=20,
            mode="semantic",
            channel_id=None,
            min_length=None,
            max_length=None,
            fts=False,
        )
        run(ns)
        out = capsys.readouterr().out
        assert "Found" in out
        assert "Bitcoin outlook" in out

    def test_index_now(self, cli_env, monkeypatch, capsys):
        from tg_agent.cli.commands.search import run
        from tg_agent.services.embedding_service import EmbeddingService

        monkeypatch.setattr(
            EmbeddingService,
            "index_pending_messages",
            AsyncMock(return_value=3),
        )

        ns = _ns(query="", limit=20, mode="local", index_now=True, reset_index=False)
        run(ns)

        assert "Indexed 3 messages for semantic search." in capsys.readouterr().out


# ---------------------------------------------------------------------------
# search-query
# ---------------------------------------------------------------------------


def _add_search_query(
    db: Database,
    query: str = "test query",
    interval: int = 60,
    is_fts: bool = False,
    notify: bool = False,
    track_stats: bool = True,
) -> int:
    from tg_agent.database.bundles import SearchQueryBundle
    from tg_agent.services.search_query_service import SearchQueryService

    async def _add():
        svc = SearchQueryService(SearchQueryBundle.from_database(db))
        return await svc.add(
            query,
            interval,
            is_fts=is_fts,
            notify_on_collect=notify,
            track_stats=track_stats,
        )

    return asyncio.run(_add())


def _sq_ns(**kwargs) -> argparse.Namespace:
    defaults = dict(
        search_query_action=None,
        query=None,
        interval=60,
        id=None,
        regex=False,
        fts=False,
        notify=False,
        track_stats=True,
        exclude_patterns="",
        max_length=None,
        days=30,
    )
    defaults.update(kwargs)
    return _ns(**defaults)


class TestCLISearchQuery:
    def test_add(self, cli_env, capsys):
        from tg_agent.cli.commands.search_query import run

        run(_sq_ns(search_query_action="add", query="new query"))
        assert "Added search query" in capsys.readouterr().out

    def test_add_with_fts_and_notify(self, cli_env, capsys):
        from tg_agent.cli.commands.search_query import run

        run(_sq_ns(search_query_action="add", query="foo OR bar", fts=True, notify=True))
        assert "Added search query" in capsys.readouterr().out

    def test_add_no_track_stats(self, cli_env, capsys):
        from tg_agent.cli.commands.search_query import run

        run(_sq_ns(search_query_action="add", query="test", track_stats=False))
        assert "Added search query" in capsys.readouterr().out

    def test_edit(self, cli_env, capsys):
        sq_id = _add_search_query(cli_env, query="original")
        from tg_agent.cli.commands.search_query import run

        run(
            _sq_ns(
                search_query_action="edit",
                id=sq_id,
                query="updated",
                regex=None,
                fts=None,
                notify=None,
                track_stats=None,
                exclude_patterns=None,
                max_length=None,
                interval=None,
            )
        )
        assert "Updated search query" in capsys.readouterr().out

    def test_toggle(self, cli_env, capsys):
        sq_id = _add_search_query(cli_env, query="toggle_me")
        from tg_agent.cli.commands.search_query import run

        run(_sq_ns(search_query_action="toggle", id=sq_id))
        assert "Toggled search query" in capsys.readouterr().out

    def test_delete(self, cli_env, capsys):
        sq_id = _add_search_query(cli_env, query="to_delete")
        from tg_agent.cli.commands.search_query import run

        run(_sq_ns(search_query_action="delete", id=sq_id))
        assert "Deleted search query" in capsys.readouterr().out

    def test_stats_no_data(self, cli_env, capsys):
        sq_id = _add_search_query(cli_env, query="bitcoin")
        from tg_agent.cli.commands.search_query import run

        run(_sq_ns(search_query_action="stats", id=sq_id, days=30))
        assert "No stats found." in capsys.readouterr().out

    def test_stats_with_data(self, cli_env, capsys):
        from tg_agent.database.bundles import SearchQueryBundle

        sq_id = _add_search_query(cli_env, query="bitcoin")

        async def _record():
            bundle = SearchQueryBundle.from_database(cli_env)
            await bundle.record_stat(sq_id, 42)

        asyncio.run(_record())

        from tg_agent.cli.commands.search_query import run

        run(_sq_ns(search_query_action="stats", id=sq_id, days=30))
        assert "42" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# collect
# ---------------------------------------------------------------------------


class TestCLICollect:
    def test_no_clients(self, cli_env_with_pool, capsys, caplog):
        from tg_agent.cli.commands.collect import run

        run(_ns(channel_id=None))
        assert "No connected accounts" in caplog.text

    def test_sample_no_clients(self, cli_env_with_pool, capsys, caplog):
        from tg_agent.cli.commands.collect import run

        run(_ns(collect_action="sample", channel_id=-100123, limit=5))
        assert "No connected accounts" in caplog.text

    def test_sample_returns_previews(self, cli_env_with_pool, capsys):
        from datetime import datetime, timezone
        from unittest.mock import AsyncMock, patch

        from tg_agent.cli.commands.collect import run

        fake_previews = [
            {
                "message_id": 42,
                "date": datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc),
                "text_preview": "Hello world",
                "media_type": None,
            },
            {
                "message_id": 41,
                "date": datetime(2024, 6, 1, 11, 0, tzinfo=timezone.utc),
                "text_preview": None,
                "media_type": "photo",
            },
        ]

        fake_pool = AsyncMock()
        fake_pool.clients = {"dummy": object()}
        fake_pool.disconnect_all = AsyncMock()

        async def fake_init_pool(config, db, **_kwargs):
            from tg_shared.auth import TelegramAuth

            return TelegramAuth(0, ""), fake_pool

        with (
            patch("tg_agent.cli.runtime.init_pool", side_effect=fake_init_pool),
            patch(
                "tg_agent.telegram.collector.Collector.sample_channel",
                new=AsyncMock(return_value=fake_previews),
            ),
        ):
            run(_ns(collect_action="sample", channel_id=-100123, limit=2))

        out = capsys.readouterr().out
        assert "Sampling" in out
        assert "#42" in out
        assert "Hello world" in out
        assert "#41" in out
        assert "photo" in out

    def test_sample_no_messages(self, cli_env_with_pool, capsys):
        from unittest.mock import AsyncMock, patch

        from tg_agent.cli.commands.collect import run

        fake_pool = AsyncMock()
        fake_pool.clients = {"dummy": object()}
        fake_pool.disconnect_all = AsyncMock()

        async def fake_init_pool(config, db, **_kwargs):
            from tg_shared.auth import TelegramAuth

            return TelegramAuth(0, ""), fake_pool

        with (
            patch("tg_agent.cli.runtime.init_pool", side_effect=fake_init_pool),
            patch(
                "tg_agent.telegram.collector.Collector.sample_channel",
                new=AsyncMock(return_value=[]),
            ),
        ):
            run(_ns(collect_action="sample", channel_id=-100123, limit=10))

        out = capsys.readouterr().out
        assert "No messages found" in out


# ---------------------------------------------------------------------------
# scheduler
# ---------------------------------------------------------------------------


class TestCLIScheduler:
    def test_trigger_hands_off_without_clients(self, cli_env_with_pool, capsys):
        """trigger enqueues the worker command; no connected accounts is fine (#1568)."""
        from tg_agent.cli.commands.scheduler import run

        run(_ns(scheduler_action="trigger"))

        out = capsys.readouterr().out
        assert "scheduler.trigger_collection" in out
        assert "Command #" in out
        assert "No connected accounts" not in out


# ---------------------------------------------------------------------------
# server control
# ---------------------------------------------------------------------------


class TestCLIServerControl:
    def test_stop_command(self, capsys):
        from tg_agent.cli.commands.server_control import run_stop
        from tg_agent.cli.process_control import StopOutcome, StopResult

        with patch(
            "tg_agent.cli.commands.server_control.stop_server",
            return_value=StopOutcome(
                StopResult.STOPPED,
                "Server stopped (PID 123).",
            ),
        ):
            run_stop(_ns(command="stop"))

        out = capsys.readouterr().out
        assert "Server stopped" in out

    def test_stop_command_not_running_is_not_error(self, capsys):
        from tg_agent.cli.commands.server_control import run_stop
        from tg_agent.cli.process_control import StopOutcome, StopResult

        with patch(
            "tg_agent.cli.commands.server_control.stop_server",
            return_value=StopOutcome(
                StopResult.NOT_RUNNING,
                "Server is not running (no PID file: data/tg_search.pid).",
            ),
        ):
            run_stop(_ns(command="stop"))

        out = capsys.readouterr().out
        assert "not running" in out

    def test_stop_command_exits_for_unmanaged_process(self):
        from tg_agent.cli.commands.server_control import run_stop
        from tg_agent.cli.process_control import StopOutcome, StopResult

        with patch(
            "tg_agent.cli.commands.server_control.stop_server",
            return_value=StopOutcome(
                StopResult.UNMANAGED,
                "PID 321 is not a managed tg_agent.main serve process.",
            ),
        ):
            with pytest.raises(SystemExit, match="1"):
                run_stop(_ns(command="stop"))

    def test_stop_command_exits_for_timeout(self):
        from tg_agent.cli.commands.server_control import run_stop
        from tg_agent.cli.process_control import StopOutcome, StopResult

        with patch(
            "tg_agent.cli.commands.server_control.stop_server",
            return_value=StopOutcome(
                StopResult.TIMEOUT,
                "Timed out waiting for server PID 321 to stop.",
            ),
        ):
            with pytest.raises(SystemExit, match="1"):
                run_stop(_ns(command="stop"))

    def test_restart_command_starts_worker_after_stop(self, capsys):
        from tg_agent.cli.commands.server_control import run_restart
        from tg_agent.cli.process_control import StopOutcome, StopResult

        with patch(
            "tg_agent.cli.commands.server_control.stop_server",
            return_value=StopOutcome(
                StopResult.STOPPED,
                "Server stopped (PID 123).",
            ),
        ):
            with patch("tg_agent.cli.commands.worker.serve_worker") as mock_worker:
                run_restart(_ns(command="restart"))

        out = capsys.readouterr().out
        assert "Server stopped" in out
        # restart becomes the worker daemon — the automation-first default has
        # no web panel (serve is the explicit legacy entry point).
        mock_worker.assert_called_once_with("config.yaml")

    def test_restart_command_starts_worker_when_not_running(self, capsys):
        from tg_agent.cli.commands.server_control import run_restart
        from tg_agent.cli.process_control import StopOutcome, StopResult

        with patch(
            "tg_agent.cli.commands.server_control.stop_server",
            return_value=StopOutcome(
                StopResult.NOT_RUNNING,
                "Server is not running (no PID file: data/tg_search.pid).",
            ),
        ):
            with patch("tg_agent.cli.commands.worker.serve_worker") as mock_worker:
                run_restart(_ns(command="restart"))

        out = capsys.readouterr().out
        assert "not running" in out
        mock_worker.assert_called_once_with("config.yaml")

    def test_restart_command_exits_for_timeout(self):
        from tg_agent.cli.commands.server_control import run_restart
        from tg_agent.cli.process_control import StopOutcome, StopResult

        with patch(
            "tg_agent.cli.commands.server_control.stop_server",
            return_value=StopOutcome(
                StopResult.TIMEOUT,
                "Timed out waiting for server PID 321 to stop.",
            ),
        ):
            with pytest.raises(SystemExit, match="1"):
                run_restart(_ns(command="restart", web_pass=None))

    def test_stop_command_exits_for_process_control_error(self):
        from tg_agent.cli.commands.server_control import run_stop
        from tg_agent.cli.process_control import ProcessControlError

        with patch(
            "tg_agent.cli.commands.server_control.stop_server",
            side_effect=ProcessControlError("broken pid file"),
        ):
            with pytest.raises(SystemExit, match="1"):
                run_stop(_ns(command="stop"))

    def test_restart_command_exits_for_process_control_error(self):
        from tg_agent.cli.commands.server_control import run_restart
        from tg_agent.cli.process_control import ProcessControlError

        with patch(
            "tg_agent.cli.commands.server_control.stop_server",
            side_effect=ProcessControlError("broken pid file"),
        ):
            with pytest.raises(SystemExit, match="1"):
                run_restart(_ns(command="restart", web_pass=None))

    def test_parser_stop_and_restart(self):
        from unittest.mock import patch

        from typer.testing import CliRunner

        from tg_agent.cli.typer_app import app

        runner = CliRunner()

        with patch("tg_agent.cli.typer_commands.server_control_cmd.stop_web") as mock_stop:
            result = runner.invoke(app, ["stop"])
        assert result.exit_code == 0, result.output
        mock_stop.assert_called_once()

        with patch("tg_agent.cli.typer_commands.server_control_cmd.restart_web") as mock_restart:
            result = runner.invoke(app, ["restart"])
        assert result.exit_code == 0, result.output
        mock_restart.assert_called_once()


# ---------------------------------------------------------------------------
# test command
# ---------------------------------------------------------------------------


class TestCLITest:
    def test_read(self, cli_env, capsys):
        from tg_agent.cli.commands.test import run

        run(_ns(command="test", test_action="read"))
        out = capsys.readouterr().out
        assert "PASS" in out
        assert "db_init" in out

    def test_write(self, cli_env, capsys):
        from tg_agent.cli.commands.test import run

        run(_ns(command="test", test_action="write"))
        out = capsys.readouterr().out
        assert "Write Tests" in out

    def test_all(self, cli_env_with_pool, capsys):
        from tg_agent.cli.commands.test import run

        run(_ns(command="test", test_action="all"))
        out = capsys.readouterr().out
        assert "Read Tests" in out
        assert "Write Tests" in out
        assert "Telegram Live Tests" in out

    def test_parser_namespace(self):
        from unittest.mock import patch

        from typer.testing import CliRunner

        from tg_agent.cli.typer_app import app

        runner = CliRunner()

        for action in ("read", "all", "telegram", "benchmark"):
            with patch("tg_agent.cli.typer_commands.test_cmd.run_impl") as mock_run:
                result = runner.invoke(app, ["test", action])
            assert result.exit_code == 0, result.output
            mock_run.assert_called_once()
            assert mock_run.call_args.args[1] == action

    def test_benchmark(self, capsys, monkeypatch):
        from tg_agent.cli.commands import test as test_cmd

        monkeypatch.setenv("RUN_CLI_REAL_TG_LIVE", "1")
        monkeypatch.setenv("CLI_REAL_TG_PHONE", "+70000000000")
        monkeypatch.setenv("ANTHROPIC_API_KEY", "live-key")
        monkeypatch.setenv("AGENT_FALLBACK_MODEL", "openai:gpt-4.1-mini")
        monkeypatch.setenv("ENV", "DEV")

        completed = subprocess.CompletedProcess(args=["pytest"], returncode=0)
        with (
            patch(
                "tg_agent.cli.commands.test.subprocess.run",
                side_effect=[completed, completed, completed],
            ) as run_mock,
            patch(
                "tg_agent.cli.commands.test.time.perf_counter",
                side_effect=[100.0, 112.0, 200.0, 206.0, 300.0, 304.0],
            ),
        ):
            test_cmd.run(_ns(command="test", test_action="benchmark"))

        out = capsys.readouterr().out
        assert "serial_full_suite" in out
        assert "parallel_safe_suite" in out
        assert "aiosqlite_serial_suite" in out
        assert "two_pass_total: 10.00s" in out
        assert "speedup_vs_serial: 1.20x" in out
        assert run_mock.call_count == 3
        assert run_mock.call_args_list[0].args[0] == test_cmd.SERIAL_PYTEST_COMMAND
        assert run_mock.call_args_list[1].args[0] == test_cmd.PARALLEL_SAFE_PYTEST_COMMAND
        assert run_mock.call_args_list[2].args[0] == test_cmd.AIOSQLITE_SERIAL_PYTEST_COMMAND
        assert run_mock.call_args_list[0].kwargs["cwd"] == test_cmd.REPO_ROOT
        env = run_mock.call_args_list[0].kwargs["env"]
        assert "RUN_CLI_REAL_TG_LIVE" not in env
        assert "CLI_REAL_TG_PHONE" not in env
        assert "ANTHROPIC_API_KEY" not in env
        assert "AGENT_FALLBACK_MODEL" not in env
        assert "ENV" not in env

    def test_benchmark_fails_on_failed_subprocess(self, capsys):
        from tg_agent.cli.commands.test import run

        failed = subprocess.CompletedProcess(args=["pytest"], returncode=2)
        with (
            patch("tg_agent.cli.commands.test.subprocess.run", return_value=failed),
            patch(
                "tg_agent.cli.commands.test.time.perf_counter",
                side_effect=[10.0, 11.0],
            ),
            pytest.raises(SystemExit, match="2"),
        ):
            run(_ns(command="test", test_action="benchmark"))

        out = capsys.readouterr().out
        assert "Benchmark step failed: serial_full_suite exited with code 2" in out

    def test_sqlite_backup_copy_includes_committed_wal_data(self, tmp_path):
        import sqlite3

        from tg_agent.cli.commands.test import _backup_sqlite_db

        source_path = tmp_path / "source.db"
        target_path = tmp_path / "target.db"
        conn = sqlite3.connect(source_path)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            conn.execute("INSERT INTO items (name) VALUES ('live')")
            conn.commit()

            _backup_sqlite_db(str(source_path), str(target_path))

            copied = sqlite3.connect(target_path)
            try:
                rows = copied.execute("SELECT name FROM items").fetchall()
            finally:
                copied.close()
        finally:
            conn.close()

        assert rows == [("live",)]


# ---------------------------------------------------------------------------
# dialogs topics
# ---------------------------------------------------------------------------


class TestCLIDialogsTopics:
    def test_topics_from_pool(self, cli_env_with_pool, capsys):
        """Topics fetched from Telegram pool are displayed in a table."""
        from tg_agent.cli.commands.dialogs import run

        fp = AsyncMock()
        fp.clients = {}
        fp.disconnect_all = AsyncMock()
        fp.get_forum_topics = AsyncMock(
            return_value=[
                {
                    "id": 1,
                    "title": "General",
                    "icon_emoji_id": None,
                    "date": "2025-01-01T00:00:00+00:00",
                },
                {
                    "id": 2,
                    "title": "Dev",
                    "icon_emoji_id": 12345,
                    "date": "2025-02-01T00:00:00+00:00",
                },
            ]
        )

        async def fake_init_pool(config, db, **_kwargs):
            from tg_shared.auth import TelegramAuth

            return TelegramAuth(0, ""), fp

        with patch("tg_agent.cli.runtime.init_pool", side_effect=fake_init_pool):
            run(_ns(dialogs_action="topics", channel_id=500, phone=None))

        out = capsys.readouterr().out
        assert "General" in out
        assert "Dev" in out
        assert "12345" in out
        assert "2025-01-01" in out

    def test_topics_fallback_to_db(self, cli_env_with_pool, capsys):
        """When pool returns no topics, falls back to DB cache."""
        from tg_agent.cli.commands.dialogs import run

        _add_channel(cli_env_with_pool, channel_id=501, title="ForumChan")
        asyncio.run(
            cli_env_with_pool.upsert_forum_topics(
                501,
                [
                    {"id": 10, "title": "Cached Topic"},
                ],
            )
        )

        fp = AsyncMock()
        fp.clients = {}
        fp.disconnect_all = AsyncMock()
        fp.get_forum_topics = AsyncMock(return_value=[])  # pool returns nothing

        async def fake_init_pool(config, db, **_kwargs):
            from tg_shared.auth import TelegramAuth

            return TelegramAuth(0, ""), fp

        with patch("tg_agent.cli.runtime.init_pool", side_effect=fake_init_pool):
            run(_ns(dialogs_action="topics", channel_id=501, phone=None))

        out = capsys.readouterr().out
        assert "Cached Topic" in out

    def test_topics_not_forum(self, cli_env_with_pool, capsys):
        """Channel with no topics prints a descriptive message."""
        from tg_agent.cli.commands.dialogs import run

        fp = AsyncMock()
        fp.clients = {}
        fp.disconnect_all = AsyncMock()
        fp.get_forum_topics = AsyncMock(return_value=[])

        async def fake_init_pool(config, db, **_kwargs):
            from tg_shared.auth import TelegramAuth

            return TelegramAuth(0, ""), fp

        with patch("tg_agent.cli.runtime.init_pool", side_effect=fake_init_pool):
            run(_ns(dialogs_action="topics", channel_id=999, phone=None))

        out = capsys.readouterr().out
        assert "No forum topics" in out

    def test_parser_topics_subcommand(self):
        """Typer surface correctly handles the dialogs topics subcommand."""
        from unittest.mock import MagicMock, patch

        from typer.testing import CliRunner

        from tg_agent.cli.typer_app import app

        runner = CliRunner()

        mock_dispatch = MagicMock()
        with (
            patch("tg_agent.cli.typer_commands.dialogs_cmd._dispatch", mock_dispatch),
            patch("tg_agent.cli.typer_commands.run_async", side_effect=lambda coro: coro.close()),
        ):
            result = runner.invoke(app, ["dialogs", "topics", "--channel-id", "123"])
        assert result.exit_code == 0, result.output
        ns = mock_dispatch.call_args.args[0]
        assert ns.dialogs_action == "topics"
        assert ns.channel_id == 123
        assert ns.phone is None

        mock_dispatch = MagicMock()
        with (
            patch("tg_agent.cli.typer_commands.dialogs_cmd._dispatch", mock_dispatch),
            patch("tg_agent.cli.typer_commands.run_async", side_effect=lambda coro: coro.close()),
        ):
            result = runner.invoke(
                app, ["dialogs", "topics", "--channel-id", "456", "--phone", "+10001112233"]
            )
        assert result.exit_code == 0, result.output
        ns = mock_dispatch.call_args.args[0]
        assert ns.channel_id == 456
        assert ns.phone == "+10001112233"


class TestCLIParityParser:
    def test_new_parity_subcommands_parse(self):
        """The #303 parity sub-commands parse on the Typer surface with the right flags."""
        from unittest.mock import MagicMock, patch

        from typer.testing import CliRunner

        from tg_agent.cli.typer_app import app

        runner = CliRunner()

        # account add (no --code) → send-code path
        with (
            patch("tg_agent.cli.typer_commands.account_cmd.send_code_impl", new_callable=MagicMock) as mock_send,
            patch("tg_agent.cli.typer_commands.run_async", side_effect=lambda coro: coro.close()),
        ):
            result = runner.invoke(app, ["account", "add", "--phone", "+1000"])
        assert result.exit_code == 0, result.output
        assert mock_send.call_args.kwargs["phone"] == "+1000"

        # account add (with --code) → verify-code path
        with (
            patch("tg_agent.cli.typer_commands.account_cmd.verify_code_impl", new_callable=MagicMock) as mock_verify,
            patch("tg_agent.cli.typer_commands.run_async", side_effect=lambda coro: coro.close()),
        ):
            result = runner.invoke(app, ["account", "add", "--phone", "+1000", "--code", "12345"])
        assert result.exit_code == 0, result.output
        assert mock_verify.call_args.kwargs["code"] == "12345"

        # search-query get <id>
        with (
            patch("tg_agent.cli.typer_commands.search_query_cmd.get_impl", new_callable=MagicMock) as mock_get,
            patch("tg_agent.cli.typer_commands.run_async", side_effect=lambda coro: coro.close()),
        ):
            result = runner.invoke(app, ["search-query", "get", "7"])
        assert result.exit_code == 0, result.output
        assert mock_get.call_args.kwargs["query_id"] == 7

        # pipeline moderation-list / moderation-view dispatch via Namespace
        mock_dispatch = MagicMock()
        with (
            patch("tg_agent.cli.typer_commands.pipeline_cmd._dispatch", mock_dispatch),
            patch("tg_agent.cli.typer_commands.run_async", side_effect=lambda coro: coro.close()),
        ):
            result = runner.invoke(
                app, ["pipeline", "moderation-list", "--pipeline-id", "3", "--limit", "5"]
            )
        assert result.exit_code == 0, result.output
        ns = mock_dispatch.call_args.args[0]
        assert ns.pipeline_action == "moderation-list"
        assert ns.pipeline_id == 3
        assert ns.limit == 5

        mock_dispatch = MagicMock()
        with (
            patch("tg_agent.cli.typer_commands.pipeline_cmd._dispatch", mock_dispatch),
            patch("tg_agent.cli.typer_commands.run_async", side_effect=lambda coro: coro.close()),
        ):
            result = runner.invoke(app, ["pipeline", "moderation-view", "9"])
        assert result.exit_code == 0, result.output
        ns = mock_dispatch.call_args.args[0]
        assert ns.pipeline_action == "moderation-view"
        assert ns.run_id == 9

        # photo-loader items --batch-id
        with (
            patch("tg_agent.cli.typer_commands.photo_loader_cmd.items_impl", new_callable=MagicMock) as mock_items,
            patch("tg_agent.cli.typer_commands.run_async", side_effect=lambda coro: coro.close()),
        ):
            result = runner.invoke(app, ["photo-loader", "items", "--batch-id", "4"])
        assert result.exit_code == 0, result.output
        assert mock_items.call_args.kwargs["batch_id"] == 4

        # dialogs resolve @example --phone dispatches via Namespace
        mock_dispatch = MagicMock()
        with (
            patch("tg_agent.cli.typer_commands.dialogs_cmd._dispatch", mock_dispatch),
            patch("tg_agent.cli.typer_commands.run_async", side_effect=lambda coro: coro.close()),
        ):
            result = runner.invoke(app, ["dialogs", "resolve", "@example", "--phone", "+1000"])
        assert result.exit_code == 0, result.output
        ns = mock_dispatch.call_args.args[0]
        assert ns.dialogs_action == "resolve"
        assert ns.identifier == "@example"
        assert ns.phone == "+1000"

        # image generated --limit
        with (
            patch("tg_agent.cli.typer_commands.image_cmd.generated_impl", new_callable=MagicMock) as mock_generated,
            patch("tg_agent.cli.typer_commands.run_async", side_effect=lambda coro: coro.close()),
        ):
            result = runner.invoke(app, ["image", "generated", "--limit", "3"])
        assert result.exit_code == 0, result.output
        assert mock_generated.call_args.kwargs["limit"] == 3
