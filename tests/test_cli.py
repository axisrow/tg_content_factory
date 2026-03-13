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


class TestCLIAgentDbProviders:
    def test_chat_refreshes_db_backed_provider_cache_before_initialize(self, cli_env, capsys):
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

        async def fake_init_db(_config_path: str):
            return config, cli_env

        def fake_init_chat_model(*, model, model_provider, **kwargs):
            assert model_provider == "openai"
            assert model == "gpt-4.1-mini"
            assert kwargs["api_key"] == "db-key"
            return MagicMock(model_provider=model_provider)

        fake_agent = MagicMock(run=MagicMock(return_value="ok-from-db-provider"))

        with patch("src.cli.runtime.init_db", side_effect=fake_init_db), patch(
            "langchain.chat_models.init_chat_model", side_effect=fake_init_chat_model
        ), patch("deepagents.create_deep_agent", return_value=fake_agent):
            run(_ns(agent_action="chat", thread_id=thread_id, message="hello", model=None))

        assert "ok-from-db-provider" in capsys.readouterr().out


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
        assert "mixed_parallel_total: 10.00s" in out
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
        run(_ns(
            agent_action="context", thread_id=99999,
            channel_id=100, limit=100000, topic_id=None,
        ))
        assert "не найден" in capsys.readouterr().out

    def test_context_injects(self, cli_env, capsys):
        tid = _create_thread(cli_env)
        _add_channel(cli_env, channel_id=400, title="CtxChan")
        _add_message(cli_env, channel_id=400, message_id=1, text="context msg one")
        _add_message(cli_env, channel_id=400, message_id=2, text="context msg two")
        from src.cli.commands.agent import run
        run(_ns(
            agent_action="context", thread_id=tid,
            channel_id=400, limit=100000, topic_id=None,
        ))
        out = capsys.readouterr().out
        assert "КОНТЕКСТ: CtxChan" in out
        assert "2 сообщений" in out

    def test_context_with_topic_id(self, cli_env, capsys):
        tid = _create_thread(cli_env)
        _add_channel(cli_env, channel_id=401, title="TopicChan")
        # Add forum topic so name is resolved
        asyncio.run(
            cli_env.upsert_forum_topics(401, [{"id": 42, "title": "Обсуждение"}])
        )
        from src.cli.commands.agent import run
        run(_ns(
            agent_action="context", thread_id=tid,
            channel_id=401, limit=100, topic_id=42,
        ))
        out = capsys.readouterr().out
        assert 'тема "Обсуждение"' in out

    def test_context_with_unknown_topic_id(self, cli_env, capsys):
        tid = _create_thread(cli_env)
        _add_channel(cli_env, channel_id=402, title="TopicChan2")
        from src.cli.commands.agent import run
        run(_ns(
            agent_action="context", thread_id=tid,
            channel_id=402, limit=100, topic_id=99,
        ))
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
            run(_ns(
                agent_action="chat", message="hello",
                thread_id=None, model="claude-haiku-4-5",
            ))

        out = capsys.readouterr().out
        assert "model reply" in out
        assert captured_opts.get("model") == "claude-haiku-4-5"

    def test_test_escaping_uses_runtime_config_for_db_providers(self, cli_db, capsys):
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

        async def fake_init_db(config_path: str):
            return config, cli_db

        with patch("src.cli.runtime.init_db", side_effect=fake_init_db), patch(
            "src.agent.manager.DeepagentsBackend._build_agent",
            return_value=MagicMock(run=MagicMock(return_value="ok")),
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

        args = parser.parse_args(["agent", "chat", "hi", "--model", "claude-haiku-4-5"])
        assert args.agent_action == "chat"
        assert args.model == "claude-haiku-4-5"
