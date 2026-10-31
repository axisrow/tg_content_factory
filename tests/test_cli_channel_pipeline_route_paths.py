"""Tests for channel CLI, pipeline CLI, settings routes, channel routes, and photo loader paths."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.helpers import cli_add_channel as _add_channel
from tests.helpers import cli_ns as _ns
from tg_agent.config import AppConfig
from tg_agent.database import Database
from tg_agent.models import Account, Channel

# ---------------------------------------------------------------------------
# Helpers shared across CLI tests
# ---------------------------------------------------------------------------

NOW_STR = "2025-01-01T12:00:00+00:00"


def _fake_init_pool_no_clients():
    """Returns a fake_init_pool that yields a pool with no clients."""

    async def _inner(config, db):
        from tg_shared.auth import TelegramAuth

        pool = AsyncMock()
        pool.clients = {}
        pool.disconnect_all = AsyncMock()
        return TelegramAuth(0, ""), pool

    return _inner


# ===========================================================================
# 1. cli/commands/channel.py — edge-case branches
# ===========================================================================


class TestCLIChannelList:
    """channel list - various branches."""

    @pytest.mark.aiosqlite_serial
    def test_list_empty(self, cli_env, capsys):
        """list with no channels prints 'No channels found.'"""
        from tg_agent.cli.commands.channel import run

        run(_ns(channel_action="list"))
        assert "No channels found." in capsys.readouterr().out

    @pytest.mark.aiosqlite_serial
    def test_list_with_channel(self, cli_env, capsys):
        """list with a channel shows table row."""
        _add_channel(cli_env, channel_id=300, title="ListCh")
        from tg_agent.cli.commands.channel import run

        run(_ns(channel_action="list"))
        out = capsys.readouterr().out
        assert "ListCh" in out

    @pytest.mark.aiosqlite_serial
    def test_list_with_filtered_channel(self, cli_env, capsys):
        """list with a filtered channel shows filter flags."""
        pk = _add_channel(cli_env, channel_id=301, title="FilteredCh")
        asyncio.run(cli_env.set_channel_filtered(pk, True))
        from tg_agent.cli.commands.channel import run

        run(_ns(channel_action="list"))
        out = capsys.readouterr().out
        assert "FilteredCh" in out


class TestCLIChannelDeleteNotFound:
    @pytest.mark.aiosqlite_serial
    def test_delete_not_found(self, cli_env, capsys):
        from tg_agent.cli.commands.channel import run

        run(_ns(channel_action="delete", identifier="99999"))
        assert "not found" in capsys.readouterr().out


class TestCLIChannelStats:
    @pytest.mark.aiosqlite_serial
    def test_stats_no_clients(self, cli_env, capsys):
        """stats no longer opens a pool at all (#1569) — no-args asks for a target."""
        init_pool = AsyncMock()
        with patch("tg_agent.cli.runtime.init_pool", init_pool):
            from tg_agent.cli.commands.channel import run

            run(_ns(channel_action="stats", identifier=None, all=False))
        init_pool.assert_not_awaited()
        assert "Specify" in capsys.readouterr().out

    @pytest.mark.aiosqlite_serial
    def test_stats_no_identifier_and_no_all(self, cli_env, capsys):
        """stats without --all and without identifier prints usage hint."""
        fake_pool = AsyncMock()
        fake_pool.clients = {"+1": MagicMock()}
        fake_pool.disconnect_all = AsyncMock()

        async def _init_pool(config, db):
            from tg_shared.auth import TelegramAuth

            return TelegramAuth(0, ""), fake_pool

        with patch("tg_agent.cli.runtime.init_pool", side_effect=_init_pool):
            from tg_agent.cli.commands.channel import run

            run(_ns(channel_action="stats", identifier=None, all=False))
        assert "Specify" in capsys.readouterr().out

    @pytest.mark.aiosqlite_serial
    def test_stats_all(self, cli_env, capsys):
        """stats --all queues a STATS_ALL task for the worker (#1569)."""
        _add_channel(cli_env, channel_id=305, title="AllStatsCh")
        init_pool = AsyncMock()

        with patch("tg_agent.cli.runtime.init_pool", init_pool):
            from tg_agent.cli.commands.channel import run

            run(_ns(channel_action="stats", identifier=None, all=True))
        init_pool.assert_not_awaited()
        assert "Queued stats task" in capsys.readouterr().out

        # run() closed the DB in its finally; reopen to inspect the row.
        if cli_env._connection.db is None:
            asyncio.run(cli_env.initialize())
        task = asyncio.run(cli_env.get_active_stats_task())
        assert task is not None
        assert task.payload.channel_ids == [305]

    @pytest.mark.aiosqlite_serial
    def test_stats_single_channel_not_found(self, cli_env, capsys):
        """stats for identifier that doesn't exist prints not found."""
        fake_pool = AsyncMock()
        fake_pool.clients = {"+1": MagicMock()}
        fake_pool.disconnect_all = AsyncMock()
        fake_collector = AsyncMock()

        async def _init_pool(config, db):
            from tg_shared.auth import TelegramAuth

            return TelegramAuth(0, ""), fake_pool

        with patch("tg_agent.cli.runtime.init_pool", side_effect=_init_pool), patch(
            "tg_agent.cli.commands.channel.Collector", return_value=fake_collector
        ):
            from tg_agent.cli.commands.channel import run

            run(_ns(channel_action="stats", identifier="99999", all=False))
        assert "not found" in capsys.readouterr().out

    @pytest.mark.aiosqlite_serial
    def test_stats_single_channel_found(self, cli_env, capsys):
        """stats for an existing channel queues channels.collect_stats (#1569)."""
        pk = _add_channel(cli_env, channel_id=310, title="StatsCh")
        init_pool = AsyncMock()

        with patch("tg_agent.cli.runtime.init_pool", init_pool):
            from tg_agent.cli.commands.channel import run

            run(_ns(channel_action="stats", identifier=str(pk), all=False))
        init_pool.assert_not_awaited()
        out = capsys.readouterr().out
        assert "Queued stats collection" in out
        assert "StatsCh" in out

        # run() closed the DB in its finally; reopen to inspect the row.
        if cli_env._connection.db is None:
            asyncio.run(cli_env.initialize())
        commands = asyncio.run(cli_env.repos.telegram_commands.list_commands(limit=5))
        assert [c.command_type for c in commands] == ["channels.collect_stats"]
        assert commands[0].payload == {"channel_pk": pk}

    @pytest.mark.aiosqlite_serial
    def test_stats_single_channel_no_stats(self, cli_env, capsys):
        """Enqueue path does not depend on existing stats — same queued reply."""
        pk = _add_channel(cli_env, channel_id=311, title="StatsCh2")
        init_pool = AsyncMock()

        with patch("tg_agent.cli.runtime.init_pool", init_pool):
            from tg_agent.cli.commands.channel import run

            run(_ns(channel_action="stats", identifier=str(pk), all=False))
        init_pool.assert_not_awaited()
        assert "Queued stats collection" in capsys.readouterr().out


class TestCLIChannelRefreshTypes:
    @pytest.mark.aiosqlite_serial
    def test_refresh_types_no_clients(self, cli_env, caplog):
        with patch("tg_agent.cli.runtime.init_pool", side_effect=_fake_init_pool_no_clients()):
            from tg_agent.cli.commands.channel import run

            run(_ns(channel_action="refresh-types"))
        assert "No connected accounts" in caplog.text

    @pytest.mark.aiosqlite_serial
    def test_refresh_types_no_channels(self, cli_env, capsys):
        """refresh-types with no active channels still prints summary."""
        fake_pool = AsyncMock()
        fake_pool.clients = {"+1": MagicMock()}
        fake_pool.disconnect_all = AsyncMock()
        fake_pool.get_available_client = AsyncMock(return_value=None)
        fake_pool.release_client = AsyncMock()

        async def _init_pool(config, db):
            from tg_shared.auth import TelegramAuth

            return TelegramAuth(0, ""), fake_pool

        with patch("tg_agent.cli.runtime.init_pool", side_effect=_init_pool):
            from tg_agent.cli.commands.channel import run

            run(_ns(channel_action="refresh-types"))
        out = capsys.readouterr().out
        assert "Active channels to check:" in out


class TestCLIChannelRefreshMeta:
    @pytest.mark.aiosqlite_serial
    def test_refresh_meta_no_clients(self, cli_env, caplog):
        with patch("tg_agent.cli.runtime.init_pool", side_effect=_fake_init_pool_no_clients()):
            from tg_agent.cli.commands.channel import run

            run(_ns(channel_action="refresh-meta", all=False, identifier=None))
        assert "No connected accounts" in caplog.text

    @pytest.mark.aiosqlite_serial
    def test_refresh_meta_no_identifier_no_all(self, cli_env, capsys):
        """refresh-meta without --all or identifier prints usage hint."""
        fake_pool = AsyncMock()
        fake_pool.clients = {"+1": MagicMock()}
        fake_pool.disconnect_all = AsyncMock()

        async def _init_pool(config, db):
            from tg_shared.auth import TelegramAuth

            return TelegramAuth(0, ""), fake_pool

        with patch("tg_agent.cli.runtime.init_pool", side_effect=_init_pool):
            from tg_agent.cli.commands.channel import run

            run(_ns(channel_action="refresh-meta", all=False, identifier=None))
        assert "Please provide" in capsys.readouterr().out

    @pytest.mark.aiosqlite_serial
    def test_refresh_meta_identifier_not_found(self, cli_env, capsys):
        fake_pool = AsyncMock()
        fake_pool.clients = {"+1": MagicMock()}
        fake_pool.disconnect_all = AsyncMock()

        async def _init_pool(config, db):
            from tg_shared.auth import TelegramAuth

            return TelegramAuth(0, ""), fake_pool

        with patch("tg_agent.cli.runtime.init_pool", side_effect=_init_pool):
            from tg_agent.cli.commands.channel import run

            run(_ns(channel_action="refresh-meta", all=False, identifier="99999"))
        assert "not found" in capsys.readouterr().out

    @pytest.mark.aiosqlite_serial
    def test_refresh_meta_single_success(self, cli_env, capsys):
        pk = _add_channel(cli_env, channel_id=320, title="MetaCh")
        fake_pool = AsyncMock()
        fake_pool.clients = {"+1": MagicMock()}
        fake_pool.disconnect_all = AsyncMock()
        fake_pool.fetch_channel_meta = AsyncMock(
            return_value={"about": "test about", "linked_chat_id": None, "has_comments": False}
        )

        async def _init_pool(config, db):
            from tg_shared.auth import TelegramAuth

            return TelegramAuth(0, ""), fake_pool

        # update_channel_full_meta is not on Database facade; patch it
        with patch("tg_agent.cli.runtime.init_pool", side_effect=_init_pool), patch(
            "tg_agent.database.facade.Database.update_channel_full_meta",
            new=AsyncMock(),
            create=True,
        ):
            from tg_agent.cli.commands.channel import run

            run(_ns(channel_action="refresh-meta", all=False, identifier=str(pk)))
        assert "OK: Updated MetaCh" in capsys.readouterr().out

    @pytest.mark.aiosqlite_serial
    def test_refresh_meta_all_success(self, cli_env, capsys):
        _add_channel(cli_env, channel_id=321, title="MetaCh2")
        fake_pool = AsyncMock()
        fake_pool.clients = {"+1": MagicMock()}
        fake_pool.disconnect_all = AsyncMock()
        fake_pool.fetch_channel_meta = AsyncMock(
            return_value={"about": None, "linked_chat_id": None, "has_comments": False}
        )

        async def _init_pool(config, db):
            from tg_shared.auth import TelegramAuth

            return TelegramAuth(0, ""), fake_pool

        # update_channel_full_meta is not on Database facade; patch it
        with patch("tg_agent.cli.runtime.init_pool", side_effect=_init_pool), patch(
            "tg_agent.database.facade.Database.update_channel_full_meta",
            new=AsyncMock(),
            create=True,
        ):
            from tg_agent.cli.commands.channel import run

            run(_ns(channel_action="refresh-meta", all=True, identifier=None))
        out = capsys.readouterr().out
        assert "Refreshed:" in out


# ===========================================================================
# 2. cli/commands/pipeline.py — edge-case branches
# ===========================================================================


def _make_pipeline_db(tmp_path, db_name="pipeline.db"):
    from tg_agent.models import Account

    db_path = str(tmp_path / db_name)
    db = Database(db_path, session_encryption_secret="test-session-encryption-key")
    asyncio.run(db.initialize())
    asyncio.run(db.add_account(Account(phone="+100", session_string="sess")))
    asyncio.run(db.add_channel(Channel(channel_id=2001, title="Source")))
    asyncio.run(
        db.repos.dialog_cache.replace_dialogs(
            "+100",
            [{"channel_id": 77, "title": "Target", "username": "tgt", "channel_type": "channel"}],
        )
    )
    asyncio.run(db.close())
    return db_path


def _pipeline_fake_init_db(db_path):
    async def _inner(_config_path: str):
        config = AppConfig()
        database = Database(db_path, session_encryption_secret="test-session-encryption-key")
        await database.initialize()
        return config, database

    return _inner


class TestCLIPipelineRuns:
    def test_runs_not_found(self, tmp_path, capsys):
        db_path = str(tmp_path / "runs_nf.db")
        db = Database(db_path, session_encryption_secret="test-session-encryption-key")
        asyncio.run(db.initialize())
        asyncio.run(db.close())

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(pipeline_action="runs", id=999, limit=10, status=None))
        assert "not found" in capsys.readouterr().out

    def test_runs_empty(self, tmp_path, capsys):
        db_path = _make_pipeline_db(tmp_path, "runs_empty.db")

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            # First add a pipeline
            run(
                _ns(
                    pipeline_action="add",
                    name="RunsTest",
                    prompt_template="tmpl {source_messages}",
                    source=[2001],
                    target=["+100|77"],
                    llm_model=None,
                    image_model=None,
                    publish_mode="moderated",
                    generation_backend="chain",
                    interval=60,
                    inactive=False,
                )
            )

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            # list to get id
            db = Database(db_path, session_encryption_secret="test-session-encryption-key")
            asyncio.run(db.initialize())
            try:
                pipelines = asyncio.run(db.repos.content_pipelines.get_all())
                pid = pipelines[0].id
            finally:
                asyncio.run(db.close())

            run(_ns(pipeline_action="runs", id=pid, limit=10, status=None))
        assert "No generation runs found." in capsys.readouterr().out

    def test_runs_with_status_filter(self, tmp_path, capsys):
        db_path = _make_pipeline_db(tmp_path, "runs_status.db")

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(
                _ns(
                    pipeline_action="add",
                    name="StatusTest",
                    prompt_template="tmpl {source_messages}",
                    source=[2001],
                    target=["+100|77"],
                    llm_model=None,
                    image_model=None,
                    publish_mode="moderated",
                    generation_backend="chain",
                    interval=60,
                    inactive=False,
                )
            )

        db = Database(db_path, session_encryption_secret="test-session-encryption-key")
        asyncio.run(db.initialize())
        try:
            pipelines = asyncio.run(db.repos.content_pipelines.get_all())
            pid = pipelines[0].id
            # create a run
            run_id = asyncio.run(db.repos.generation_runs.create_run(pid, "tmpl"))
            asyncio.run(db.repos.generation_runs.set_status(run_id, "completed"))
        finally:
            asyncio.run(db.close())

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(pipeline_action="runs", id=pid, limit=10, status="failed"))
        # status filter should show no runs (we only have completed)
        assert "No generation runs found." in capsys.readouterr().out


class TestCLIPipelineEdit:
    def test_edit_not_found(self, tmp_path, capsys):
        db_path = str(tmp_path / "edit_nf.db")
        db = Database(db_path, session_encryption_secret="test-session-encryption-key")
        asyncio.run(db.initialize())
        asyncio.run(db.close())

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(
                _ns(
                    pipeline_action="edit",
                    id=999,
                    name=None,
                    prompt_template=None,
                    source=[],
                    target=[],
                    llm_model=None,
                    image_model=None,
                    publish_mode=None,
                    generation_backend=None,
                    interval=None,
                    active=None,
                )
            )
        assert "not found" in capsys.readouterr().out

    def test_edit_success(self, tmp_path, capsys):
        db_path = _make_pipeline_db(tmp_path, "edit_ok.db")

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(
                _ns(
                    pipeline_action="add",
                    name="EditTest",
                    prompt_template="tmpl {source_messages}",
                    source=[2001],
                    target=["+100|77"],
                    llm_model=None,
                    image_model=None,
                    publish_mode="moderated",
                    generation_backend="chain",
                    interval=60,
                    inactive=False,
                )
            )

        db = Database(db_path, session_encryption_secret="test-session-encryption-key")
        asyncio.run(db.initialize())
        try:
            pipelines = asyncio.run(db.repos.content_pipelines.get_all())
            pid = pipelines[0].id
        finally:
            asyncio.run(db.close())

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(
                _ns(
                    pipeline_action="edit",
                    id=pid,
                    name="EditedName",
                    prompt_template=None,
                    source=[],
                    target=[],
                    llm_model=None,
                    image_model=None,
                    publish_mode=None,
                    generation_backend=None,
                    interval=None,
                    active=None,
                )
            )
        assert "Updated pipeline" in capsys.readouterr().out


class TestCLIPipelineRunAction:
    def test_run_not_found(self, tmp_path, capsys):
        db_path = str(tmp_path / "run_nf.db")
        db = Database(db_path, session_encryption_secret="test-session-encryption-key")
        asyncio.run(db.initialize())
        asyncio.run(db.close())

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(
                _ns(
                    pipeline_action="run",
                    id=999,
                    limit=10,
                    max_tokens=None,
                    temperature=None,
                    preview=False,
                    publish=False,
                )
            )
        assert "not found" in capsys.readouterr().out


class TestCLIPipelineRunShow:
    def test_run_show_not_found(self, tmp_path, capsys):
        db_path = str(tmp_path / "run_show.db")
        db = Database(db_path, session_encryption_secret="test-session-encryption-key")
        asyncio.run(db.initialize())
        asyncio.run(db.close())

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(pipeline_action="run-show", run_id=9999))
        assert "not found" in capsys.readouterr().out


class TestCLIPipelineApproveReject:
    def test_approve_not_found(self, tmp_path, capsys):
        db_path = str(tmp_path / "approve_nf.db")
        db = Database(db_path, session_encryption_secret="test-session-encryption-key")
        asyncio.run(db.initialize())
        asyncio.run(db.close())

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(pipeline_action="approve", run_id=9999))
        assert "not found" in capsys.readouterr().out

    def test_reject_not_found(self, tmp_path, capsys):
        db_path = str(tmp_path / "reject_nf.db")
        db = Database(db_path, session_encryption_secret="test-session-encryption-key")
        asyncio.run(db.initialize())
        asyncio.run(db.close())

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(pipeline_action="reject", run_id=9999))
        assert "not found" in capsys.readouterr().out


class TestCLIPipelineQueue:
    def test_queue_not_found(self, tmp_path, capsys):
        db_path = str(tmp_path / "queue_nf.db")
        db = Database(db_path, session_encryption_secret="test-session-encryption-key")
        asyncio.run(db.initialize())
        asyncio.run(db.close())

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(pipeline_action="queue", id=999, limit=10))
        assert "not found" in capsys.readouterr().out


class TestCLIPipelineBulkApproveReject:
    def test_bulk_approve_empty(self, tmp_path, capsys):
        db_path = str(tmp_path / "bulk_approve.db")
        db = Database(db_path, session_encryption_secret="test-session-encryption-key")
        asyncio.run(db.initialize())
        asyncio.run(db.close())

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(pipeline_action="bulk-approve", run_ids=[]))
        assert "Bulk approved: 0/0" in capsys.readouterr().out

    def test_bulk_reject_empty(self, tmp_path, capsys):
        db_path = str(tmp_path / "bulk_reject.db")
        db = Database(db_path, session_encryption_secret="test-session-encryption-key")
        asyncio.run(db.initialize())
        asyncio.run(db.close())

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(pipeline_action="bulk-reject", run_ids=[]))
        assert "Bulk rejected: 0/0" in capsys.readouterr().out


class TestCLIPipelineDelete:
    def test_delete(self, tmp_path, capsys):
        db_path = _make_pipeline_db(tmp_path, "delete_pipe.db")

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(
                _ns(
                    pipeline_action="add",
                    name="DeleteMe",
                    prompt_template="tmpl {source_messages}",
                    source=[2001],
                    target=["+100|77"],
                    llm_model=None,
                    image_model=None,
                    publish_mode="moderated",
                    generation_backend="chain",
                    interval=60,
                    inactive=False,
                )
            )

        db = Database(db_path, session_encryption_secret="test-session-encryption-key")
        asyncio.run(db.initialize())
        try:
            pipelines = asyncio.run(db.repos.content_pipelines.get_all())
            pid = pipelines[0].id
        finally:
            asyncio.run(db.close())

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(pipeline_action="delete", id=pid))
        assert "Deleted pipeline" in capsys.readouterr().out


class TestCLIPipelineToggle:
    def test_toggle_not_found(self, tmp_path, capsys):
        db_path = str(tmp_path / "toggle_nf.db")
        db = Database(db_path, session_encryption_secret="test-session-encryption-key")
        asyncio.run(db.initialize())
        asyncio.run(db.close())

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(pipeline_action="toggle", id=999))
        assert "not found" in capsys.readouterr().out


class TestCLIPipelineHelpers:
    def test_parse_target_refs_valid(self):
        from tg_agent.cli.commands.pipeline import _parse_target_refs

        refs = _parse_target_refs(["+1|123", "+2|456"])
        assert len(refs) == 2
        assert refs[0].phone == "+1"
        assert refs[0].dialog_id == 123

    def test_parse_target_refs_no_separator(self):
        from tg_agent.cli.commands.pipeline import _parse_target_refs
        from tg_agent.services.pipeline_service import PipelineValidationError

        with pytest.raises(PipelineValidationError):
            _parse_target_refs(["no_separator"])

    def test_parse_target_refs_bad_id(self):
        from tg_agent.cli.commands.pipeline import _parse_target_refs
        from tg_agent.services.pipeline_service import PipelineValidationError

        with pytest.raises(PipelineValidationError):
            _parse_target_refs(["+1|notanumber"])

    def test_preview_text_empty(self):
        from tg_agent.cli.commands.pipeline import _preview_text

        assert _preview_text(None) == "—"
        assert _preview_text("") == "—"

    def test_preview_text_short(self):
        from tg_agent.cli.commands.pipeline import _preview_text

        assert _preview_text("hello") == "hello"

    def test_preview_text_long(self):
        from tg_agent.cli.commands.pipeline import _preview_text

        long_str = "a " * 50
        result = _preview_text(long_str)
        assert result.endswith("...")
        assert len(result) <= 60


# ===========================================================================
# 3. cli/commands/test.py — read / write / all actions
# ===========================================================================


class TestCLITestRead:
    @pytest.mark.aiosqlite_serial
    def test_read_action_runs(self, cli_env, capsys):
        """test read action completes and prints summary."""
        from tg_agent.cli.commands.test import run

        run(_ns(test_action="read"))
        out = capsys.readouterr().out
        assert "Read Tests" in out
        assert "passed" in out

    @pytest.mark.aiosqlite_serial
    def test_read_action_with_channel_and_account(self, cli_env, capsys):
        """test read with data in DB."""
        asyncio.run(cli_env.add_account(Account(phone="+1", session_string="s")))
        _add_channel(cli_env, channel_id=500, title="TestReadCh")
        from tg_agent.cli.commands.test import run

        run(_ns(test_action="read"))
        out = capsys.readouterr().out
        assert "passed" in out


class TestCLITestWrite:
    @pytest.mark.aiosqlite_serial
    def test_write_action_runs(self, cli_env, capsys):
        """test write action completes and prints summary."""
        from tg_agent.cli.commands.test import run

        run(_ns(test_action="write"))
        out = capsys.readouterr().out
        assert "Write Tests" in out
        assert "passed" in out


class TestCLITestAll:
    @pytest.mark.aiosqlite_serial
    def test_all_action_runs(self, cli_env, capsys):
        """test all runs both read and write sections."""
        from tg_agent.cli.commands.test import run

        run(_ns(test_action="all"))
        out = capsys.readouterr().out
        assert "Read Tests" in out
        assert "Write Tests" in out
        assert "passed" in out


class TestCLITestHelpers:
    def test_print_result_pass(self, capsys):
        from tg_agent.cli.commands.test import CheckResult, Status, _print_result

        _print_result(CheckResult("foo", Status.PASS, "detail"))
        out = capsys.readouterr().out
        assert "PASS" in out
        assert "foo" in out

    def test_print_result_fail(self, capsys):
        from tg_agent.cli.commands.test import CheckResult, Status, _print_result

        _print_result(CheckResult("bar", Status.FAIL, "oops"))
        out = capsys.readouterr().out
        assert "FAIL" in out

    def test_print_result_skip(self, capsys):
        from tg_agent.cli.commands.test import CheckResult, Status, _print_result

        _print_result(CheckResult("baz", Status.SKIP, "skipped"))
        out = capsys.readouterr().out
        assert "SKIP" in out

    def test_format_exception_with_detail(self):
        from tg_agent.cli.commands.test import _format_exception

        exc = ValueError("oops")
        assert _format_exception(exc) == "oops"

    def test_format_exception_empty_detail(self):
        from tg_agent.cli.commands.test import _format_exception

        class EmptyStrError(Exception):
            def __str__(self):
                return ""

        exc = EmptyStrError()
        assert _format_exception(exc) == "EmptyStrError"

    def test_format_all_flooded_detail_no_retry(self):
        from tg_agent.cli.commands.test import _format_all_flooded_detail

        result = _format_all_flooded_detail(
            "base", retry_after_sec=None, next_available_at_utc=None
        )
        assert "all clients are flood-waited" in result
        assert "retry" not in result

    def test_format_all_flooded_detail_with_retry(self):
        from tg_agent.cli.commands.test import _format_all_flooded_detail

        result = _format_all_flooded_detail(
            "base", retry_after_sec=30, next_available_at_utc=None
        )
        assert "retry after about 30s" in result

    def test_is_premium_flood(self):
        from datetime import datetime, timezone

        from telethon_floodgate import FloodWaitInfo

        from tg_agent.cli.commands.test import _is_premium_flood

        _now = datetime.now(timezone.utc)
        info_premium = FloodWaitInfo(
            operation="check_search_quota",
            phone="+1",
            wait_seconds=60,
            next_available_at_utc=_now,
            detail="flood wait",
        )
        info_regular = FloodWaitInfo(
            operation="get_dialogs",
            phone="+1",
            wait_seconds=60,
            next_available_at_utc=_now,
            detail="flood wait",
        )
        assert _is_premium_flood(info_premium) is True
        assert _is_premium_flood(info_regular) is False


class TestCLITestBenchmark:
    def test_benchmark_calls_subprocess(self):
        """test benchmark action invokes subprocess (mocked)."""
        completed_ok = MagicMock()
        completed_ok.returncode = 0

        with patch("subprocess.run", return_value=completed_ok) as mock_run:
            from tg_agent.cli.commands.test import run

            run(_ns(test_action="benchmark"))

        # subprocess.run should be called 3 times (serial, parallel_safe, aiosqlite_serial)
        assert mock_run.call_count == 3
