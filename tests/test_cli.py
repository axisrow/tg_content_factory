"""CLI smoke tests — every command exercised via run(args) with mocked runtime."""

from __future__ import annotations

import argparse
import asyncio
import subprocess
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import AppConfig
from src.database import Database
from src.models import Account, Message
from tests.helpers import cli_add_channel as _add_channel
from tests.helpers import cli_ns as _ns

NOW = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
pytestmark = pytest.mark.aiosqlite_serial


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

    def test_info_shows_live_account_profile(self, cli_env_with_pool, capsys):
        """account info: shows connected account profile diagnostics."""
        from src.models import TelegramUserInfo

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
        with patch("src.cli.runtime.init_pool", new=init_pool_mock):
            from src.cli.commands.account import run

            run(_ns(account_action="info", phone=None))

        out = capsys.readouterr().out
        assert phone in out
        assert "Alice Smith" in out
        assert "@alice" in out
        assert "Live Telegram accounts (1)" in out

    def test_info_filter_by_phone(self, cli_env_with_pool, capsys):
        """account info --phone: only shows matching account."""
        from src.models import TelegramUserInfo

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
        with patch("src.cli.runtime.init_pool", new=init_pool_mock):
            from src.cli.commands.account import run

            run(_ns(account_action="info", phone=phone1))

        out = capsys.readouterr().out
        assert phone1 in out
        assert phone2 not in out

    def test_info_no_connected_accounts(self, cli_env_with_pool, capsys):
        """account info: prints shared no-live-profile diagnostics."""
        fake_pool = AsyncMock(
            clients={},
            get_users_info=AsyncMock(return_value=[]),
            disconnect_all=AsyncMock(),
        )
        init_pool_mock = AsyncMock(return_value=(MagicMock(), fake_pool))
        with patch("src.cli.runtime.init_pool", new=init_pool_mock):
            from src.cli.commands.account import run

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
        with patch("src.cli.runtime.init_pool", new=init_pool_mock):
            from src.cli.commands.account import run

            run(_ns(account_action="info", phone=None))

        out = capsys.readouterr().out
        assert "Runtime connected phones" in out
        assert phone in out
        assert "do not treat this as disconnected" in out

    def test_flood_status_no_flood(self, cli_env, capsys):
        _add_account(cli_env, phone="+10001112233")
        from src.cli.commands.account import run

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
        from src.cli.commands.account import run

        run(_ns(account_action="flood-status"))
        out = capsys.readouterr().out
        assert phone in out
        assert re.search(r"\d+s", out)

    def test_flood_status_no_accounts(self, cli_env, capsys):
        from src.cli.commands.account import run

        run(_ns(account_action="flood-status"))
        assert "No accounts found." in capsys.readouterr().out

    def test_flood_clear(self, cli_env, capsys):
        from datetime import datetime, timedelta, timezone

        phone = "+10001112255"
        _add_account(cli_env, phone=phone)
        until = datetime.now(timezone.utc) + timedelta(seconds=120)
        asyncio.run(cli_env.update_account_flood(phone, until))
        from src.cli.commands.account import run

        run(_ns(account_action="flood-clear", phone=phone))
        assert "cleared" in capsys.readouterr().out

    def test_flood_clear_not_found(self, cli_env, capsys):
        from src.cli.commands.account import run

        run(_ns(account_action="flood-clear", phone="+19990000000"))
        assert "not found" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# channel (DB-only)
# ---------------------------------------------------------------------------


class TestCLIChannelDB:
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

    def test_purge_no_filtered(self, cli_env, capsys):
        from src.cli.commands.filter import run

        run(_ns(filter_action="purge", pks=None))
        assert "No filtered channels affected." in capsys.readouterr().out

    def test_purge_filtered_channel(self, cli_env, capsys):
        ch_pk = _add_channel(cli_env, channel_id=999, title="FilteredCh")
        asyncio.run(cli_env.set_channel_filtered(ch_pk, True))
        _add_message(cli_env, channel_id=999, message_id=1, text="to be deleted")

        from src.cli.commands.filter import run

        run(_ns(filter_action="purge", pks=None))
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

        from src.cli.commands.filter import run

        run(_ns(filter_action="purge", pks=str(ch1_pk)))
        out = capsys.readouterr().out
        assert "PurgeCh1" in out
        assert "PurgeCh2" not in out

        rows = _query_db(cli_env, "SELECT COUNT(*) AS cnt FROM messages WHERE channel_id=?", (990,))
        assert rows[0]["cnt"] == 0  # ch1 messages deleted

        rows = _query_db(cli_env, "SELECT COUNT(*) AS cnt FROM messages WHERE channel_id=?", (991,))
        assert rows[0]["cnt"] == 1  # ch2 messages untouched

    def test_hard_delete_requires_dev_mode(self, cli_env, capsys):
        from src.cli.commands.filter import run

        run(_ns(filter_action="hard-delete", pks=None, yes=True))
        assert "dev" in capsys.readouterr().out.lower()

    def test_hard_delete_removes_channel(self, cli_env, capsys):
        asyncio.run(cli_env.set_setting("agent_dev_mode_enabled", "1"))
        ch_pk = _add_channel(cli_env, channel_id=888, title="ToDelete")
        asyncio.run(cli_env.set_channel_filtered(ch_pk, True))

        from src.cli.commands.filter import run

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

        from src.cli.commands.filter import run

        run(_ns(filter_action="hard-delete", pks=None, yes=True))
        assert "No filtered channels to delete." in capsys.readouterr().out


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------


class TestCLISearch:
    def test_local_empty(self, cli_env, capsys):
        from src.cli.commands.search import run

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
        from src.cli.commands.search import run

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

        from src.cli.commands.search import run
        from src.services.embedding_service import EmbeddingService

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
        from src.cli.commands.search import run
        from src.services.embedding_service import EmbeddingService

        monkeypatch.setattr(
            EmbeddingService,
            "index_pending_messages",
            AsyncMock(return_value=3),
        )

        ns = _ns(query="", limit=20, mode="local", index_now=True, reset_index=False)
        run(ns)

        assert "Indexed 3 messages for semantic search." in capsys.readouterr().out


class TestCLIAgentDbProviders:
    def test_chat_refreshes_db_backed_provider_cache_before_initialize(
        self,
        cli_env,
        cli_init_patch,
        capsys,
    ):
        from src.agent.provider_registry import ProviderRuntimeConfig
        from src.cli.commands.agent import run
        from src.services.agent_provider_service import AgentProviderService

        config = AppConfig()
        config.security.session_encryption_key = "provider-secret"
        service = AgentProviderService(cli_env, config)
        asyncio.run(
            service.save_provider_configs(
                [
                    ProviderRuntimeConfig(
                        provider="openai",
                        enabled=True,
                        priority=0,
                        selected_model="gpt-4.1-mini",
                        secret_fields={"api_key": "db-key"},
                    )
                ]
            )
        )

        thread_id = asyncio.run(cli_env.create_agent_thread("cli chat"))

        def fake_init_chat_model(*, model, model_provider, **kwargs):
            assert model_provider == "openai"
            assert model == "gpt-4.1-mini"
            assert kwargs["api_key"] == "db-key"
            return MagicMock(model_provider=model_provider)

        fake_agent = MagicMock(run=MagicMock(return_value="ok-from-db-provider"))

        with (
            cli_init_patch(cli_env, "src.cli.runtime.init_db", config=config),
            patch("langchain.chat_models.init_chat_model", side_effect=fake_init_chat_model),
            patch("deepagents.create_deep_agent", return_value=fake_agent),
        ):
            run(_ns(agent_action="chat", thread_id=thread_id, prompt="hello", model=None))

        assert "ok-from-db-provider" in capsys.readouterr().out


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
    from src.database.bundles import SearchQueryBundle
    from src.services.search_query_service import SearchQueryService

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
        from src.cli.commands.search_query import run

        run(_sq_ns(search_query_action="toggle", id=sq_id))
        assert "Toggled search query" in capsys.readouterr().out

    def test_delete(self, cli_env, capsys):
        sq_id = _add_search_query(cli_env, query="to_delete")
        from src.cli.commands.search_query import run

        run(_sq_ns(search_query_action="delete", id=sq_id))
        assert "Deleted search query" in capsys.readouterr().out

    def test_stats_no_data(self, cli_env, capsys):
        sq_id = _add_search_query(cli_env, query="bitcoin")
        from src.cli.commands.search_query import run

        run(_sq_ns(search_query_action="stats", id=sq_id, days=30))
        assert "No stats found." in capsys.readouterr().out

    def test_stats_with_data(self, cli_env, capsys):
        from src.database.bundles import SearchQueryBundle

        sq_id = _add_search_query(cli_env, query="bitcoin")

        async def _record():
            bundle = SearchQueryBundle.from_database(cli_env)
            await bundle.record_stat(sq_id, 42)

        asyncio.run(_record())

        from src.cli.commands.search_query import run

        run(_sq_ns(search_query_action="stats", id=sq_id, days=30))
        assert "42" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# collect
# ---------------------------------------------------------------------------


class TestCLICollect:
    def test_no_clients(self, cli_env_with_pool, capsys, caplog):
        from src.cli.commands.collect import run

        run(_ns(channel_id=None))
        assert "No connected accounts" in caplog.text

    def test_sample_no_clients(self, cli_env_with_pool, capsys, caplog):
        from src.cli.commands.collect import run

        run(_ns(collect_action="sample", channel_id=-100123, limit=5))
        assert "No connected accounts" in caplog.text

    def test_sample_returns_previews(self, cli_env_with_pool, capsys):
        from datetime import datetime, timezone
        from unittest.mock import AsyncMock, patch

        from src.cli.commands.collect import run

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

        async def fake_init_pool(config, db):
            from src.telegram.auth import TelegramAuth

            return TelegramAuth(0, ""), fake_pool

        with (
            patch("src.cli.runtime.init_pool", side_effect=fake_init_pool),
            patch(
                "src.telegram.collector.Collector.sample_channel",
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

        from src.cli.commands.collect import run

        fake_pool = AsyncMock()
        fake_pool.clients = {"dummy": object()}
        fake_pool.disconnect_all = AsyncMock()

        async def fake_init_pool(config, db):
            from src.telegram.auth import TelegramAuth

            return TelegramAuth(0, ""), fake_pool

        with (
            patch("src.cli.runtime.init_pool", side_effect=fake_init_pool),
            patch(
                "src.telegram.collector.Collector.sample_channel",
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
    def test_no_clients(self, cli_env_with_pool, capsys, caplog):
        from src.cli.commands.scheduler import run

        run(_ns(scheduler_action="trigger"))
        assert "No connected accounts" in caplog.text


# ---------------------------------------------------------------------------
# server control
# ---------------------------------------------------------------------------


class TestCLIServerControl:
    def test_stop_command(self, capsys):
        from src.cli.commands.server_control import run_stop
        from src.cli.process_control import StopOutcome, StopResult

        with patch(
            "src.cli.commands.server_control.stop_server",
            return_value=StopOutcome(
                StopResult.STOPPED,
                "Server stopped (PID 123).",
            ),
        ):
            run_stop(_ns(command="stop"))

        out = capsys.readouterr().out
        assert "Server stopped" in out

    def test_stop_command_not_running_is_not_error(self, capsys):
        from src.cli.commands.server_control import run_stop
        from src.cli.process_control import StopOutcome, StopResult

        with patch(
            "src.cli.commands.server_control.stop_server",
            return_value=StopOutcome(
                StopResult.NOT_RUNNING,
                "Server is not running (no PID file: data/tg_search.pid).",
            ),
        ):
            run_stop(_ns(command="stop"))

        out = capsys.readouterr().out
        assert "not running" in out

    def test_stop_command_exits_for_unmanaged_process(self):
        from src.cli.commands.server_control import run_stop
        from src.cli.process_control import StopOutcome, StopResult

        with patch(
            "src.cli.commands.server_control.stop_server",
            return_value=StopOutcome(
                StopResult.UNMANAGED,
                "PID 321 is not a managed src.main serve process.",
            ),
        ):
            with pytest.raises(SystemExit, match="1"):
                run_stop(_ns(command="stop"))

    def test_stop_command_exits_for_timeout(self):
        from src.cli.commands.server_control import run_stop
        from src.cli.process_control import StopOutcome, StopResult

        with patch(
            "src.cli.commands.server_control.stop_server",
            return_value=StopOutcome(
                StopResult.TIMEOUT,
                "Timed out waiting for server PID 321 to stop.",
            ),
        ):
            with pytest.raises(SystemExit, match="1"):
                run_stop(_ns(command="stop"))

    def test_restart_command_starts_serve_after_stop(self, capsys):
        from src.cli.commands.server_control import run_restart
        from src.cli.process_control import StopOutcome, StopResult

        with patch(
            "src.cli.commands.server_control.stop_server",
            return_value=StopOutcome(
                StopResult.STOPPED,
                "Server stopped (PID 123).",
            ),
        ):
            with patch("src.cli.commands.server_control.serve.run") as mock_serve_run:
                args = _ns(command="restart", web_pass="secret")
                run_restart(args)

        out = capsys.readouterr().out
        assert "Server stopped" in out
        mock_serve_run.assert_called_once_with(args)

    def test_restart_command_starts_serve_when_not_running(self, capsys):
        from src.cli.commands.server_control import run_restart
        from src.cli.process_control import StopOutcome, StopResult

        with patch(
            "src.cli.commands.server_control.stop_server",
            return_value=StopOutcome(
                StopResult.NOT_RUNNING,
                "Server is not running (no PID file: data/tg_search.pid).",
            ),
        ):
            with patch("src.cli.commands.server_control.serve.run") as mock_serve_run:
                args = _ns(command="restart", web_pass=None)
                run_restart(args)

        out = capsys.readouterr().out
        assert "not running" in out
        mock_serve_run.assert_called_once_with(args)

    def test_restart_command_exits_for_timeout(self):
        from src.cli.commands.server_control import run_restart
        from src.cli.process_control import StopOutcome, StopResult

        with patch(
            "src.cli.commands.server_control.stop_server",
            return_value=StopOutcome(
                StopResult.TIMEOUT,
                "Timed out waiting for server PID 321 to stop.",
            ),
        ):
            with pytest.raises(SystemExit, match="1"):
                run_restart(_ns(command="restart", web_pass=None))

    def test_stop_command_exits_for_process_control_error(self):
        from src.cli.commands.server_control import run_stop
        from src.cli.process_control import ProcessControlError

        with patch(
            "src.cli.commands.server_control.stop_server",
            side_effect=ProcessControlError("broken pid file"),
        ):
            with pytest.raises(SystemExit, match="1"):
                run_stop(_ns(command="stop"))

    def test_restart_command_exits_for_process_control_error(self):
        from src.cli.commands.server_control import run_restart
        from src.cli.process_control import ProcessControlError

        with patch(
            "src.cli.commands.server_control.stop_server",
            side_effect=ProcessControlError("broken pid file"),
        ):
            with pytest.raises(SystemExit, match="1"):
                run_restart(_ns(command="restart", web_pass=None))

    def test_parser_stop_and_restart(self):
        from src.cli.parser import build_parser

        parser = build_parser()

        args = parser.parse_args(["stop"])
        assert args.command == "stop"

        args = parser.parse_args(["restart", "--web-pass", "secret"])
        assert args.command == "restart"
        assert args.web_pass == "secret"


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

        args = parser.parse_args(["test", "benchmark"])
        assert args.test_action == "benchmark"

    def test_benchmark(self, capsys):
        from src.cli.commands import test as test_cmd

        completed = subprocess.CompletedProcess(args=["pytest"], returncode=0)
        with (
            patch(
                "src.cli.commands.test.subprocess.run",
                side_effect=[completed, completed, completed],
            ) as run_mock,
            patch(
                "src.cli.commands.test.time.perf_counter",
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

    def test_benchmark_fails_on_failed_subprocess(self, capsys):
        from src.cli.commands.test import run

        failed = subprocess.CompletedProcess(args=["pytest"], returncode=2)
        with (
            patch("src.cli.commands.test.subprocess.run", return_value=failed),
            patch(
                "src.cli.commands.test.time.perf_counter",
                side_effect=[10.0, 11.0],
            ),
            pytest.raises(SystemExit, match="2"),
        ):
            run(_ns(command="test", test_action="benchmark"))

        out = capsys.readouterr().out
        assert "Benchmark step failed: serial_full_suite exited with code 2" in out


# ---------------------------------------------------------------------------
# agent
# ---------------------------------------------------------------------------


def _create_thread(db: Database, title: str = "CLI Thread") -> int:
    return asyncio.run(db.create_agent_thread(title))


def _save_agent_msg(db: Database, thread_id: int, role: str, content: str):
    asyncio.run(db.save_agent_message(thread_id, role, content))


class TestCLIAgent:
    def test_threads_empty(self, cli_env, capsys):
        from src.cli.commands.agent import run

        run(_ns(agent_action="threads"))
        assert "Нет тредов" in capsys.readouterr().out

    def test_threads_with_data(self, cli_env, capsys):
        _create_thread(cli_env, "MyThread")
        from src.cli.commands.agent import run

        run(_ns(agent_action="threads"))
        assert "MyThread" in capsys.readouterr().out

    def test_thread_create(self, cli_env, capsys):
        from src.cli.commands.agent import run

        run(_ns(agent_action="thread-create", title="New Thread"))
        assert "Создан тред" in capsys.readouterr().out

    def test_thread_create_default_title(self, cli_env, capsys):
        from src.cli.commands.agent import run

        run(_ns(agent_action="thread-create", title=None))
        out = capsys.readouterr().out
        assert "Создан тред" in out
        assert "Новый тред" in out

    def test_thread_delete(self, cli_env, capsys):
        tid = _create_thread(cli_env)
        from src.cli.commands.agent import run

        run(_ns(agent_action="thread-delete", thread_id=tid))
        assert "удалён" in capsys.readouterr().out

    def test_thread_rename(self, cli_env, capsys):
        tid = _create_thread(cli_env, "Old Title")
        from src.cli.commands.agent import run

        run(_ns(agent_action="thread-rename", thread_id=tid, title="New Title"))
        out = capsys.readouterr().out
        assert "переименован" in out
        assert "New Title" in out

    def test_thread_rename_truncates(self, cli_env, capsys):
        tid = _create_thread(cli_env)
        from src.cli.commands.agent import run

        long_title = "A" * 200
        run(_ns(agent_action="thread-rename", thread_id=tid, title=long_title))
        out = capsys.readouterr().out
        assert "переименован" in out
        # Title truncated to 100
        assert "A" * 100 in out
        assert "A" * 101 not in out

    def test_messages_empty(self, cli_env, capsys):
        tid = _create_thread(cli_env)
        from src.cli.commands.agent import run

        run(_ns(agent_action="messages", thread_id=tid, limit=None))
        assert "Нет сообщений" in capsys.readouterr().out

    def test_messages_with_data(self, cli_env, capsys):
        tid = _create_thread(cli_env)
        _save_agent_msg(cli_env, tid, "user", "Hello agent")
        _save_agent_msg(cli_env, tid, "assistant", "Hello human")
        from src.cli.commands.agent import run

        run(_ns(agent_action="messages", thread_id=tid, limit=None))
        out = capsys.readouterr().out
        assert "Hello agent" in out
        assert "Hello human" in out
        assert "[user]" in out
        assert "[assistant]" in out

    def test_messages_with_limit(self, cli_env, capsys):
        tid = _create_thread(cli_env)
        _save_agent_msg(cli_env, tid, "user", "First message")
        _save_agent_msg(cli_env, tid, "assistant", "Second message")
        _save_agent_msg(cli_env, tid, "user", "Third message")
        from src.cli.commands.agent import run

        run(_ns(agent_action="messages", thread_id=tid, limit=1))
        out = capsys.readouterr().out
        assert "Third message" in out
        assert "First message" not in out

    def test_context_thread_not_found(self, cli_env, capsys):
        from src.cli.commands.agent import run

        run(
            _ns(
                agent_action="context",
                thread_id=99999,
                channel_id=100,
                limit=100000,
                topic_id=None,
            )
        )
        assert "не найден" in capsys.readouterr().out

    def test_context_injects(self, cli_env, capsys):
        tid = _create_thread(cli_env)
        _add_channel(cli_env, channel_id=400, title="CtxChan")
        _add_message(cli_env, channel_id=400, message_id=1, text="context msg one")
        _add_message(cli_env, channel_id=400, message_id=2, text="context msg two")
        from src.cli.commands.agent import run

        run(
            _ns(
                agent_action="context",
                thread_id=tid,
                channel_id=400,
                limit=100000,
                topic_id=None,
            )
        )
        out = capsys.readouterr().out
        assert "КОНТЕКСТ: CtxChan" in out
        assert "2 сообщений" in out

    def test_context_with_topic_id(self, cli_env, capsys):
        tid = _create_thread(cli_env)
        _add_channel(cli_env, channel_id=401, title="TopicChan")
        # Add forum topic so name is resolved
        asyncio.run(cli_env.upsert_forum_topics(401, [{"id": 42, "title": "Обсуждение"}]))
        from src.cli.commands.agent import run

        run(
            _ns(
                agent_action="context",
                thread_id=tid,
                channel_id=401,
                limit=100,
                topic_id=42,
            )
        )
        out = capsys.readouterr().out
        assert 'тема "Обсуждение"' in out

    def test_context_with_unknown_topic_id(self, cli_env, capsys):
        tid = _create_thread(cli_env)
        _add_channel(cli_env, channel_id=402, title="TopicChan2")
        from src.cli.commands.agent import run

        run(
            _ns(
                agent_action="context",
                thread_id=tid,
                channel_id=402,
                limit=100,
                topic_id=99,
            )
        )
        out = capsys.readouterr().out
        assert "тема #99" in out

    def test_chat_with_model(self, cli_env, capsys, monkeypatch):
        from unittest.mock import MagicMock
        from unittest.mock import patch as _patch

        from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

        from src.cli.commands.agent import run

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-claude-key")

        text_block = TextBlock(text="model reply")
        assistant_msg = MagicMock(spec=AssistantMessage)
        assistant_msg.content = [text_block]
        result_msg = MagicMock(spec=ResultMessage)

        captured_opts = {}

        async def mock_query(prompt, options):
            captured_opts["model"] = getattr(options, "model", None)
            yield assistant_msg
            yield result_msg

        with _patch("src.agent.manager.query", mock_query):
            run(
                _ns(
                    agent_action="chat",
                    prompt="hello",
                    thread_id=None,
                    model="claude-haiku-4-5-20251001",
                )
            )

        out = capsys.readouterr().out
        assert "model reply" in out
        assert captured_opts.get("model") == "claude-haiku-4-5-20251001"

    def test_test_escaping_uses_runtime_config_for_db_providers(
        self,
        cli_db,
        cli_init_patch,
        capsys,
    ):
        from src.agent.provider_registry import ProviderRuntimeConfig
        from src.cli.commands.agent import run
        from src.services.agent_provider_service import AgentProviderService

        config = AppConfig()
        config.security.session_encryption_key = "provider-secret"
        service = AgentProviderService(cli_db, config)
        asyncio.run(
            service.save_provider_configs(
                [
                    ProviderRuntimeConfig(
                        provider="openai",
                        enabled=True,
                        priority=0,
                        selected_model="gpt-4.1-mini",
                        secret_fields={"api_key": "openai-key"},
                    )
                ]
            )
        )

        with (
            cli_init_patch(cli_db, "src.cli.runtime.init_db", config=config),
            patch(
                "src.agent.manager.DeepagentsBackend._build_agent",
                return_value=MagicMock(run=MagicMock(return_value="ok")),
            ),
        ):
            run(_ns(agent_action="test-escaping"))

        out = capsys.readouterr().out
        assert "пропуск" not in out
        assert "Итого: 10 passed, 0 failed" in out

    def test_parser_agent_subcommands(self):
        from src.cli.parser import build_parser

        parser = build_parser()

        args = parser.parse_args(["agent", "thread-rename", "5", "New Name"])
        assert args.agent_action == "thread-rename"
        assert args.thread_id == 5
        assert args.title == "New Name"

        args = parser.parse_args(["agent", "messages", "3", "--limit", "10"])
        assert args.agent_action == "messages"
        assert args.thread_id == 3
        assert args.limit == 10

        args = parser.parse_args(
            ["agent", "context", "7", "--channel-id", "100", "--topic-id", "42"]
        )
        assert args.agent_action == "context"
        assert args.thread_id == 7
        assert args.channel_id == 100
        assert args.topic_id == 42

        args = parser.parse_args(["agent", "chat", "--prompt", "hi", "--model", "claude-haiku-4-5-20251001"])
        assert args.agent_action == "chat"
        assert args.prompt == "hi"
        assert args.model == "claude-haiku-4-5-20251001"

        args = parser.parse_args(["agent", "chat", "-p", "hi"])
        assert args.agent_action == "chat"
        assert args.prompt == "hi"

        args = parser.parse_args(["agent", "chat"])
        assert args.agent_action == "chat"
        assert args.prompt is None


# ---------------------------------------------------------------------------
# dialogs topics
# ---------------------------------------------------------------------------


class TestCLIDialogsTopics:
    def test_topics_from_pool(self, cli_env_with_pool, capsys):
        """Topics fetched from Telegram pool are displayed in a table."""
        from src.cli.commands.dialogs import run

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

        async def fake_init_pool(config, db):
            from src.telegram.auth import TelegramAuth

            return TelegramAuth(0, ""), fp

        with patch("src.cli.runtime.init_pool", side_effect=fake_init_pool):
            run(_ns(dialogs_action="topics", channel_id=500, phone=None))

        out = capsys.readouterr().out
        assert "General" in out
        assert "Dev" in out
        assert "12345" in out
        assert "2025-01-01" in out

    def test_topics_fallback_to_db(self, cli_env_with_pool, capsys):
        """When pool returns no topics, falls back to DB cache."""
        from src.cli.commands.dialogs import run

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

        async def fake_init_pool(config, db):
            from src.telegram.auth import TelegramAuth

            return TelegramAuth(0, ""), fp

        with patch("src.cli.runtime.init_pool", side_effect=fake_init_pool):
            run(_ns(dialogs_action="topics", channel_id=501, phone=None))

        out = capsys.readouterr().out
        assert "Cached Topic" in out

    def test_topics_not_forum(self, cli_env_with_pool, capsys):
        """Channel with no topics prints a descriptive message."""
        from src.cli.commands.dialogs import run

        fp = AsyncMock()
        fp.clients = {}
        fp.disconnect_all = AsyncMock()
        fp.get_forum_topics = AsyncMock(return_value=[])

        async def fake_init_pool(config, db):
            from src.telegram.auth import TelegramAuth

            return TelegramAuth(0, ""), fp

        with patch("src.cli.runtime.init_pool", side_effect=fake_init_pool):
            run(_ns(dialogs_action="topics", channel_id=999, phone=None))

        out = capsys.readouterr().out
        assert "No forum topics" in out

    def test_parser_topics_subcommand(self):
        """Parser correctly handles the legacy alias for dialogs topics."""
        from src.cli.parser import build_parser

        parser = build_parser()

        args = parser.parse_args(["my-telegram", "topics", "--channel-id", "123"])
        assert args.dialogs_action == "topics"
        assert args.channel_id == 123
        assert args.phone is None

        args = parser.parse_args(
            ["my-telegram", "topics", "--channel-id", "456", "--phone", "+10001112233"]
        )
        assert args.channel_id == 456
        assert args.phone == "+10001112233"


class TestCLIParityParser:
    def test_new_parity_subcommands_parse(self):
        from src.cli.parser import build_parser

        parser = build_parser()

        args = parser.parse_args(["account", "add", "--phone", "+1000"])
        assert args.account_action == "add"
        assert args.phone == "+1000"
        assert args.code is None

        args = parser.parse_args(["account", "add", "--phone", "+1000", "--code", "12345"])
        assert args.account_action == "add"
        assert args.code == "12345"

        args = parser.parse_args(["search-query", "get", "7"])
        assert args.search_query_action == "get"
        assert args.id == 7

        args = parser.parse_args(["pipeline", "moderation-list", "--pipeline-id", "3", "--limit", "5"])
        assert args.pipeline_action == "moderation-list"
        assert args.pipeline_id == 3
        assert args.limit == 5

        args = parser.parse_args(["pipeline", "moderation-view", "9"])
        assert args.pipeline_action == "moderation-view"
        assert args.run_id == 9

        args = parser.parse_args(["photo-loader", "items", "--batch-id", "4"])
        assert args.photo_loader_action == "items"
        assert args.batch_id == 4

        args = parser.parse_args(["dialogs", "resolve", "@example", "--phone", "+1000"])
        assert args.dialogs_action == "resolve"
        assert args.identifier == "@example"
        assert args.phone == "+1000"

        args = parser.parse_args(["image", "generated", "--limit", "3"])
        assert args.image_action == "generated"
        assert args.limit == 3
