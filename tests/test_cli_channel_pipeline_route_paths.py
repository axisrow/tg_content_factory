"""Tests for channel CLI, pipeline CLI, settings routes, channel routes, and photo loader paths."""

from __future__ import annotations

import asyncio
import base64
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import AppConfig
from src.database import Database
from src.models import Account, Channel
from tests.helpers import cli_add_channel as _add_channel
from tests.helpers import cli_ns as _ns

# ---------------------------------------------------------------------------
# Helpers shared across CLI tests
# ---------------------------------------------------------------------------

NOW_STR = "2025-01-01T12:00:00+00:00"


def _fake_init_db(db: Database):
    """Return a fake_init_db coroutine factory that yields (config, db)."""
    config = AppConfig()

    async def _inner(_config_path: str):
        return config, db

    return _inner


def _fake_init_pool_no_clients():
    """Returns a fake_init_pool that yields a pool with no clients."""

    async def _inner(config, db):
        from src.telegram.auth import TelegramAuth

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
        from src.cli.commands.channel import run

        run(_ns(channel_action="list"))
        assert "No channels found." in capsys.readouterr().out

    @pytest.mark.aiosqlite_serial
    def test_list_with_channel(self, cli_env, capsys):
        """list with a channel shows table row."""
        _add_channel(cli_env, channel_id=300, title="ListCh")
        from src.cli.commands.channel import run

        run(_ns(channel_action="list"))
        out = capsys.readouterr().out
        assert "ListCh" in out

    @pytest.mark.aiosqlite_serial
    def test_list_with_filtered_channel(self, cli_env, capsys):
        """list with a filtered channel shows filter flags."""
        pk = _add_channel(cli_env, channel_id=301, title="FilteredCh")
        asyncio.run(cli_env.set_channel_filtered(pk, True))
        from src.cli.commands.channel import run

        run(_ns(channel_action="list"))
        out = capsys.readouterr().out
        assert "FilteredCh" in out


class TestCLIChannelDeleteNotFound:
    @pytest.mark.aiosqlite_serial
    def test_delete_not_found(self, cli_env, capsys):
        from src.cli.commands.channel import run

        run(_ns(channel_action="delete", identifier="99999"))
        assert "not found" in capsys.readouterr().out


class TestCLIChannelStats:
    @pytest.mark.aiosqlite_serial
    def test_stats_no_clients(self, cli_env, capsys, caplog):
        """stats action with no connected accounts logs error."""
        with patch("src.cli.runtime.init_pool", side_effect=_fake_init_pool_no_clients()):
            from src.cli.commands.channel import run

            run(_ns(channel_action="stats", identifier=None, all=False))
        assert "No connected accounts" in caplog.text

    @pytest.mark.aiosqlite_serial
    def test_stats_no_identifier_and_no_all(self, cli_env, capsys):
        """stats without --all and without identifier prints usage hint."""
        fake_pool = AsyncMock()
        fake_pool.clients = {"+1": MagicMock()}
        fake_pool.disconnect_all = AsyncMock()

        async def _init_pool(config, db):
            from src.telegram.auth import TelegramAuth

            return TelegramAuth(0, ""), fake_pool

        with patch("src.cli.runtime.init_pool", side_effect=_init_pool):
            from src.cli.commands.channel import run

            run(_ns(channel_action="stats", identifier=None, all=False))
        assert "Specify" in capsys.readouterr().out

    @pytest.mark.aiosqlite_serial
    def test_stats_all(self, cli_env, capsys):
        """stats --all calls collect_all_stats."""
        fake_pool = AsyncMock()
        fake_pool.clients = {"+1": MagicMock()}
        fake_pool.disconnect_all = AsyncMock()
        fake_collector = AsyncMock()
        fake_collector.collect_all_stats = AsyncMock(return_value=5)

        async def _init_pool(config, db):
            from src.telegram.auth import TelegramAuth

            return TelegramAuth(0, ""), fake_pool

        with patch("src.cli.runtime.init_pool", side_effect=_init_pool), patch(
            "src.cli.commands.channel.Collector", return_value=fake_collector
        ):
            from src.cli.commands.channel import run

            run(_ns(channel_action="stats", identifier=None, all=True))
        assert "Stats collected: 5" in capsys.readouterr().out

    @pytest.mark.aiosqlite_serial
    def test_stats_single_channel_not_found(self, cli_env, capsys):
        """stats for identifier that doesn't exist prints not found."""
        fake_pool = AsyncMock()
        fake_pool.clients = {"+1": MagicMock()}
        fake_pool.disconnect_all = AsyncMock()
        fake_collector = AsyncMock()

        async def _init_pool(config, db):
            from src.telegram.auth import TelegramAuth

            return TelegramAuth(0, ""), fake_pool

        with patch("src.cli.runtime.init_pool", side_effect=_init_pool), patch(
            "src.cli.commands.channel.Collector", return_value=fake_collector
        ):
            from src.cli.commands.channel import run

            run(_ns(channel_action="stats", identifier="99999", all=False))
        assert "not found" in capsys.readouterr().out

    @pytest.mark.aiosqlite_serial
    def test_stats_single_channel_found(self, cli_env, capsys):
        """stats for existing channel calls collect_channel_stats and prints result."""
        pk = _add_channel(cli_env, channel_id=310, title="StatsCh")
        fake_pool = AsyncMock()
        fake_pool.clients = {"+1": MagicMock()}
        fake_pool.disconnect_all = AsyncMock()
        from src.models import ChannelStats

        fake_stats = ChannelStats(
            channel_id=310,
            subscriber_count=1000,
            avg_views=50.0,
            avg_reactions=5.0,
            avg_forwards=2.0,
        )
        fake_collector = AsyncMock()
        fake_collector.collect_channel_stats = AsyncMock(return_value=fake_stats)

        async def _init_pool(config, db):
            from src.telegram.auth import TelegramAuth

            return TelegramAuth(0, ""), fake_pool

        with patch("src.cli.runtime.init_pool", side_effect=_init_pool), patch(
            "src.cli.commands.channel.Collector", return_value=fake_collector
        ):
            from src.cli.commands.channel import run

            run(_ns(channel_action="stats", identifier=str(pk), all=False))
        out = capsys.readouterr().out
        assert "Subscribers:" in out

    @pytest.mark.aiosqlite_serial
    def test_stats_single_channel_no_stats(self, cli_env, capsys):
        """stats returns None → prints 'No client available'."""
        pk = _add_channel(cli_env, channel_id=311, title="StatsCh2")
        fake_pool = AsyncMock()
        fake_pool.clients = {"+1": MagicMock()}
        fake_pool.disconnect_all = AsyncMock()
        fake_collector = AsyncMock()
        fake_collector.collect_channel_stats = AsyncMock(return_value=None)

        async def _init_pool(config, db):
            from src.telegram.auth import TelegramAuth

            return TelegramAuth(0, ""), fake_pool

        with patch("src.cli.runtime.init_pool", side_effect=_init_pool), patch(
            "src.cli.commands.channel.Collector", return_value=fake_collector
        ):
            from src.cli.commands.channel import run

            run(_ns(channel_action="stats", identifier=str(pk), all=False))
        assert "No client available" in capsys.readouterr().out


class TestCLIChannelRefreshTypes:
    @pytest.mark.aiosqlite_serial
    def test_refresh_types_no_clients(self, cli_env, caplog):
        with patch("src.cli.runtime.init_pool", side_effect=_fake_init_pool_no_clients()):
            from src.cli.commands.channel import run

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
            from src.telegram.auth import TelegramAuth

            return TelegramAuth(0, ""), fake_pool

        with patch("src.cli.runtime.init_pool", side_effect=_init_pool):
            from src.cli.commands.channel import run

            run(_ns(channel_action="refresh-types"))
        out = capsys.readouterr().out
        assert "Active channels to check:" in out


class TestCLIChannelRefreshMeta:
    @pytest.mark.aiosqlite_serial
    def test_refresh_meta_no_clients(self, cli_env, caplog):
        with patch("src.cli.runtime.init_pool", side_effect=_fake_init_pool_no_clients()):
            from src.cli.commands.channel import run

            run(_ns(channel_action="refresh-meta", all=False, identifier=None))
        assert "No connected accounts" in caplog.text

    @pytest.mark.aiosqlite_serial
    def test_refresh_meta_no_identifier_no_all(self, cli_env, capsys):
        """refresh-meta without --all or identifier prints usage hint."""
        fake_pool = AsyncMock()
        fake_pool.clients = {"+1": MagicMock()}
        fake_pool.disconnect_all = AsyncMock()

        async def _init_pool(config, db):
            from src.telegram.auth import TelegramAuth

            return TelegramAuth(0, ""), fake_pool

        with patch("src.cli.runtime.init_pool", side_effect=_init_pool):
            from src.cli.commands.channel import run

            run(_ns(channel_action="refresh-meta", all=False, identifier=None))
        assert "Please provide" in capsys.readouterr().out

    @pytest.mark.aiosqlite_serial
    def test_refresh_meta_identifier_not_found(self, cli_env, capsys):
        fake_pool = AsyncMock()
        fake_pool.clients = {"+1": MagicMock()}
        fake_pool.disconnect_all = AsyncMock()

        async def _init_pool(config, db):
            from src.telegram.auth import TelegramAuth

            return TelegramAuth(0, ""), fake_pool

        with patch("src.cli.runtime.init_pool", side_effect=_init_pool):
            from src.cli.commands.channel import run

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
            from src.telegram.auth import TelegramAuth

            return TelegramAuth(0, ""), fake_pool

        # update_channel_full_meta is not on Database facade; patch it
        with patch("src.cli.runtime.init_pool", side_effect=_init_pool), patch(
            "src.database.facade.Database.update_channel_full_meta",
            new=AsyncMock(),
            create=True,
        ):
            from src.cli.commands.channel import run

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
            from src.telegram.auth import TelegramAuth

            return TelegramAuth(0, ""), fake_pool

        # update_channel_full_meta is not on Database facade; patch it
        with patch("src.cli.runtime.init_pool", side_effect=_init_pool), patch(
            "src.database.facade.Database.update_channel_full_meta",
            new=AsyncMock(),
            create=True,
        ):
            from src.cli.commands.channel import run

            run(_ns(channel_action="refresh-meta", all=True, identifier=None))
        out = capsys.readouterr().out
        assert "Refreshed:" in out


# ===========================================================================
# 2. cli/commands/pipeline.py — edge-case branches
# ===========================================================================


def _make_pipeline_db(tmp_path, db_name="pipeline.db"):
    from src.models import Account

    db_path = str(tmp_path / db_name)
    db = Database(db_path)
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
        database = Database(db_path)
        await database.initialize()
        return config, database

    return _inner


class TestCLIPipelineRuns:
    def test_runs_not_found(self, tmp_path, capsys):
        db_path = str(tmp_path / "runs_nf.db")
        db = Database(db_path)
        asyncio.run(db.initialize())
        asyncio.run(db.close())

        with patch("src.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from src.cli.commands.pipeline import run

            run(_ns(pipeline_action="runs", id=999, limit=10, status=None))
        assert "not found" in capsys.readouterr().out

    def test_runs_empty(self, tmp_path, capsys):
        db_path = _make_pipeline_db(tmp_path, "runs_empty.db")

        with patch("src.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from src.cli.commands.pipeline import run

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

        with patch("src.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from src.cli.commands.pipeline import run

            # list to get id
            db = Database(db_path)
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

        with patch("src.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from src.cli.commands.pipeline import run

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

        db = Database(db_path)
        asyncio.run(db.initialize())
        try:
            pipelines = asyncio.run(db.repos.content_pipelines.get_all())
            pid = pipelines[0].id
            # create a run
            run_id = asyncio.run(db.repos.generation_runs.create_run(pid, "tmpl"))
            asyncio.run(db.repos.generation_runs.set_status(run_id, "completed"))
        finally:
            asyncio.run(db.close())

        with patch("src.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from src.cli.commands.pipeline import run

            run(_ns(pipeline_action="runs", id=pid, limit=10, status="failed"))
        # status filter should show no runs (we only have completed)
        assert "No generation runs found." in capsys.readouterr().out


class TestCLIPipelineEdit:
    def test_edit_not_found(self, tmp_path, capsys):
        db_path = str(tmp_path / "edit_nf.db")
        db = Database(db_path)
        asyncio.run(db.initialize())
        asyncio.run(db.close())

        with patch("src.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from src.cli.commands.pipeline import run

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

        with patch("src.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from src.cli.commands.pipeline import run

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

        db = Database(db_path)
        asyncio.run(db.initialize())
        try:
            pipelines = asyncio.run(db.repos.content_pipelines.get_all())
            pid = pipelines[0].id
        finally:
            asyncio.run(db.close())

        with patch("src.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from src.cli.commands.pipeline import run

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
        db = Database(db_path)
        asyncio.run(db.initialize())
        asyncio.run(db.close())

        with patch("src.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from src.cli.commands.pipeline import run

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
        db = Database(db_path)
        asyncio.run(db.initialize())
        asyncio.run(db.close())

        with patch("src.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from src.cli.commands.pipeline import run

            run(_ns(pipeline_action="run-show", run_id=9999))
        assert "not found" in capsys.readouterr().out


class TestCLIPipelineApproveReject:
    def test_approve_not_found(self, tmp_path, capsys):
        db_path = str(tmp_path / "approve_nf.db")
        db = Database(db_path)
        asyncio.run(db.initialize())
        asyncio.run(db.close())

        with patch("src.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from src.cli.commands.pipeline import run

            run(_ns(pipeline_action="approve", run_id=9999))
        assert "not found" in capsys.readouterr().out

    def test_reject_not_found(self, tmp_path, capsys):
        db_path = str(tmp_path / "reject_nf.db")
        db = Database(db_path)
        asyncio.run(db.initialize())
        asyncio.run(db.close())

        with patch("src.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from src.cli.commands.pipeline import run

            run(_ns(pipeline_action="reject", run_id=9999))
        assert "not found" in capsys.readouterr().out


class TestCLIPipelineQueue:
    def test_queue_not_found(self, tmp_path, capsys):
        db_path = str(tmp_path / "queue_nf.db")
        db = Database(db_path)
        asyncio.run(db.initialize())
        asyncio.run(db.close())

        with patch("src.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from src.cli.commands.pipeline import run

            run(_ns(pipeline_action="queue", id=999, limit=10))
        assert "not found" in capsys.readouterr().out


class TestCLIPipelineBulkApproveReject:
    def test_bulk_approve_empty(self, tmp_path, capsys):
        db_path = str(tmp_path / "bulk_approve.db")
        db = Database(db_path)
        asyncio.run(db.initialize())
        asyncio.run(db.close())

        with patch("src.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from src.cli.commands.pipeline import run

            run(_ns(pipeline_action="bulk-approve", run_ids=[]))
        assert "Bulk approved: 0/0" in capsys.readouterr().out

    def test_bulk_reject_empty(self, tmp_path, capsys):
        db_path = str(tmp_path / "bulk_reject.db")
        db = Database(db_path)
        asyncio.run(db.initialize())
        asyncio.run(db.close())

        with patch("src.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from src.cli.commands.pipeline import run

            run(_ns(pipeline_action="bulk-reject", run_ids=[]))
        assert "Bulk rejected: 0/0" in capsys.readouterr().out


class TestCLIPipelineDelete:
    def test_delete(self, tmp_path, capsys):
        db_path = _make_pipeline_db(tmp_path, "delete_pipe.db")

        with patch("src.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from src.cli.commands.pipeline import run

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

        db = Database(db_path)
        asyncio.run(db.initialize())
        try:
            pipelines = asyncio.run(db.repos.content_pipelines.get_all())
            pid = pipelines[0].id
        finally:
            asyncio.run(db.close())

        with patch("src.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from src.cli.commands.pipeline import run

            run(_ns(pipeline_action="delete", id=pid))
        assert "Deleted pipeline" in capsys.readouterr().out


class TestCLIPipelineToggle:
    def test_toggle_not_found(self, tmp_path, capsys):
        db_path = str(tmp_path / "toggle_nf.db")
        db = Database(db_path)
        asyncio.run(db.initialize())
        asyncio.run(db.close())

        with patch("src.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from src.cli.commands.pipeline import run

            run(_ns(pipeline_action="toggle", id=999))
        assert "not found" in capsys.readouterr().out


class TestCLIPipelineHelpers:
    def test_parse_target_refs_valid(self):
        from src.cli.commands.pipeline import _parse_target_refs

        refs = _parse_target_refs(["+1|123", "+2|456"])
        assert len(refs) == 2
        assert refs[0].phone == "+1"
        assert refs[0].dialog_id == 123

    def test_parse_target_refs_no_separator(self):
        from src.cli.commands.pipeline import _parse_target_refs
        from src.services.pipeline_service import PipelineValidationError

        with pytest.raises(PipelineValidationError):
            _parse_target_refs(["no_separator"])

    def test_parse_target_refs_bad_id(self):
        from src.cli.commands.pipeline import _parse_target_refs
        from src.services.pipeline_service import PipelineValidationError

        with pytest.raises(PipelineValidationError):
            _parse_target_refs(["+1|notanumber"])

    def test_preview_text_empty(self):
        from src.cli.commands.pipeline import _preview_text

        assert _preview_text(None) == "—"
        assert _preview_text("") == "—"

    def test_preview_text_short(self):
        from src.cli.commands.pipeline import _preview_text

        assert _preview_text("hello") == "hello"

    def test_preview_text_long(self):
        from src.cli.commands.pipeline import _preview_text

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
        from src.cli.commands.test import run

        run(_ns(test_action="read"))
        out = capsys.readouterr().out
        assert "Read Tests" in out
        assert "passed" in out

    @pytest.mark.aiosqlite_serial
    def test_read_action_with_channel_and_account(self, cli_env, capsys):
        """test read with data in DB."""
        asyncio.run(cli_env.add_account(Account(phone="+1", session_string="s")))
        _add_channel(cli_env, channel_id=500, title="TestReadCh")
        from src.cli.commands.test import run

        run(_ns(test_action="read"))
        out = capsys.readouterr().out
        assert "passed" in out


class TestCLITestWrite:
    @pytest.mark.aiosqlite_serial
    def test_write_action_runs(self, cli_env, capsys):
        """test write action completes and prints summary."""
        from src.cli.commands.test import run

        run(_ns(test_action="write"))
        out = capsys.readouterr().out
        assert "Write Tests" in out
        assert "passed" in out


class TestCLITestAll:
    @pytest.mark.aiosqlite_serial
    def test_all_action_runs(self, cli_env, capsys):
        """test all runs both read and write sections."""
        from src.cli.commands.test import run

        run(_ns(test_action="all"))
        out = capsys.readouterr().out
        assert "Read Tests" in out
        assert "Write Tests" in out
        assert "passed" in out


class TestCLITestHelpers:
    def test_print_result_pass(self, capsys):
        from src.cli.commands.test import CheckResult, Status, _print_result

        _print_result(CheckResult("foo", Status.PASS, "detail"))
        out = capsys.readouterr().out
        assert "PASS" in out
        assert "foo" in out

    def test_print_result_fail(self, capsys):
        from src.cli.commands.test import CheckResult, Status, _print_result

        _print_result(CheckResult("bar", Status.FAIL, "oops"))
        out = capsys.readouterr().out
        assert "FAIL" in out

    def test_print_result_skip(self, capsys):
        from src.cli.commands.test import CheckResult, Status, _print_result

        _print_result(CheckResult("baz", Status.SKIP, "skipped"))
        out = capsys.readouterr().out
        assert "SKIP" in out

    def test_format_exception_with_detail(self):
        from src.cli.commands.test import _format_exception

        exc = ValueError("oops")
        assert _format_exception(exc) == "oops"

    def test_format_exception_empty_detail(self):
        from src.cli.commands.test import _format_exception

        class EmptyStrError(Exception):
            def __str__(self):
                return ""

        exc = EmptyStrError()
        assert _format_exception(exc) == "EmptyStrError"

    def test_format_all_flooded_detail_no_retry(self):
        from src.cli.commands.test import _format_all_flooded_detail

        result = _format_all_flooded_detail(
            "base", retry_after_sec=None, next_available_at_utc=None
        )
        assert "all clients are flood-waited" in result
        assert "retry" not in result

    def test_format_all_flooded_detail_with_retry(self):
        from src.cli.commands.test import _format_all_flooded_detail

        result = _format_all_flooded_detail(
            "base", retry_after_sec=30, next_available_at_utc=None
        )
        assert "retry after about 30s" in result

    def test_is_premium_flood(self):
        from datetime import datetime, timezone

        from src.cli.commands.test import _is_premium_flood
        from src.telegram.flood_wait import FloodWaitInfo

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
            from src.cli.commands.test import run

            run(_ns(test_action="benchmark"))

        # subprocess.run should be called 3 times (serial, parallel_safe, aiosqlite_serial)
        assert mock_run.call_count == 3


# ===========================================================================
# 4 & 5. Web route tests — channels and settings
# ===========================================================================

# We replicate the route_client fixture inline since this file is in tests/
# (not tests/routes/) and we want to keep everything in one place.


@pytest.fixture
async def _base_app_b3(tmp_path):
    """Standalone base_app equivalent for batch3 tests."""
    from src.collection_queue import CollectionQueue
    from src.models import Account
    from src.scheduler.service import SchedulerManager
    from src.search.ai_search import AISearchEngine
    from src.search.engine import SearchEngine
    from src.telegram.auth import TelegramAuth
    from src.telegram.collector import Collector
    from src.web.app import create_app

    config = AppConfig()
    config.database.path = str(tmp_path / "batch3.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"

    app = create_app(config)
    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db
    app.state.config = config

    pool_mock = MagicMock()
    pool_mock.clients = {"+1234567890": MagicMock()}
    pool_mock.resolve_channel = AsyncMock(return_value=None)
    pool_mock.fetch_channel_meta = AsyncMock(return_value=None)
    pool_mock.get_forum_topics = AsyncMock(return_value=[])
    pool_mock.get_dialogs_for_phone = AsyncMock(return_value=[])
    app.state.pool = pool_mock

    app.state.auth = TelegramAuth(12345, "test_hash")
    app.state.notifier = None

    collector = Collector(pool_mock, db, config.scheduler)
    app.state.collector = collector

    collection_queue = CollectionQueue(collector, db)
    app.state.collection_queue = collection_queue

    app.state.search_engine = SearchEngine(db)
    app.state.ai_search = AISearchEngine(config.llm, db)
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = "test_secret_key"
    app.state.shutting_down = False

    await db.add_account(Account(phone="+1234567890", session_string="test_session"))
    await db.add_channel(Channel(channel_id=100, title="Test Channel"))

    yield app, db, pool_mock

    await collection_queue.shutdown()
    await db.close()


@pytest.fixture
async def _route_client_b3(_base_app_b3):
    from httpx import ASGITransport, AsyncClient

    app, db, pool_mock = _base_app_b3
    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={
            "Authorization": f"Basic {auth_header}",
            "Origin": "http://test",
        },
    ) as c:
        c._transport_app = app
        c._db = db
        c._pool = pool_mock
        yield c


# --- Channel routes ---


@pytest.mark.anyio
async def test_channels_refresh_types_redirect(_route_client_b3):
    """POST /channels/refresh-types enqueues a channels.refresh_types command."""
    client = _route_client_b3
    pool = client._pool
    db = client._db
    resp = await client.post("/channels/refresh-types", follow_redirects=False)
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]
    # web layer must not touch the pool directly anymore
    pool.resolve_channel.assert_not_called()
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert len(commands) == 1
    assert commands[0].command_type == "channels.refresh_types"


@pytest.mark.anyio
async def test_channels_refresh_types_enqueues_single_command(_route_client_b3):
    """A second invocation enqueues another command; web path never calls pool."""
    client = _route_client_b3
    pool = client._pool
    db = client._db
    resp = await client.post("/channels/refresh-types", follow_redirects=False)
    assert resp.status_code == 303
    pool.resolve_channel.assert_not_called()
    commands = await db.repos.telegram_commands.list_commands(limit=5)
    assert any(c.command_type == "channels.refresh_types" for c in commands)


# removed: replaced by queued-command model — web no longer runs resolve inline
# removed: replaced by queued-command model — web no longer runs resolve inline


@pytest.mark.anyio
async def test_channels_refresh_meta_success(_route_client_b3):
    """POST /channels/refresh-meta enqueues a channels.refresh_meta command."""
    client = _route_client_b3
    pool = client._pool
    db = client._db
    resp = await client.post("/channels/refresh-meta", follow_redirects=False)
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]
    pool.fetch_channel_meta.assert_not_called()
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert len(commands) == 1
    assert commands[0].command_type == "channels.refresh_meta"


# removed: replaced by queued-command model — web no longer fetches meta inline
# removed: replaced by queued-command model — web no longer fetches meta inline


# --- Settings routes ---


@pytest.fixture
def _settings_patch():
    """Patch AgentProviderService methods to avoid needing config."""
    with patch(
        "src.web.settings.handlers.AgentProviderService.load_provider_configs",
        AsyncMock(return_value=[]),
    ), patch(
        "src.web.settings.handlers.AgentProviderService.load_model_cache",
        AsyncMock(return_value={}),
    ), patch(
        "src.web.settings.handlers.ImageProviderService.load_provider_configs",
        AsyncMock(return_value=[]),
    ):
        yield


@pytest.mark.anyio
async def test_settings_page_renders_b3(_route_client_b3, _settings_patch):
    client = _route_client_b3
    resp = await client.get("/settings/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_settings_save_scheduler_valid_b3(_route_client_b3):
    client = _route_client_b3
    resp = await client.post(
        "/settings/save-scheduler",
        data={"collect_interval_minutes": "45"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=scheduler_saved" in resp.headers["location"]


@pytest.mark.anyio
async def test_settings_save_scheduler_invalid_b3(_route_client_b3):
    client = _route_client_b3
    resp = await client.post(
        "/settings/save-scheduler",
        data={"collect_interval_minutes": "notanumber"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=invalid_value" in resp.headers["location"]


@pytest.mark.anyio
async def test_settings_save_credentials_valid_b3(_route_client_b3):
    client = _route_client_b3
    resp = await client.post(
        "/settings/save-credentials",
        data={"api_id": "12345", "api_hash": "hashvalue123"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=credentials_saved" in resp.headers["location"]


@pytest.mark.anyio
async def test_settings_save_credentials_invalid_api_id_b3(_route_client_b3):
    client = _route_client_b3
    resp = await client.post(
        "/settings/save-credentials",
        data={"api_id": "notanumber", "api_hash": "hash"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=invalid_api_id" in resp.headers["location"]


@pytest.mark.anyio
async def test_settings_save_filters_valid_b3(_route_client_b3):
    client = _route_client_b3
    resp = await client.post(
        "/settings/save-filters",
        data={"min_subscribers_filter": "0", "auto_delete_filtered": "0"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=filters_saved" in resp.headers["location"]


@pytest.mark.anyio
async def test_settings_save_filters_invalid_b3(_route_client_b3):
    client = _route_client_b3
    resp = await client.post(
        "/settings/save-filters",
        data={"min_subscribers_filter": "abc"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=invalid_value" in resp.headers["location"]


@pytest.mark.anyio
async def test_settings_save_agent_backend_override_b3(_route_client_b3):
    client = _route_client_b3
    resp = await client.post(
        "/settings/save-agent",
        data={"agent_form_scope": "backend_override", "agent_backend_override": "auto"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=agent_saved" in resp.headers["location"]


@pytest.mark.anyio
async def test_settings_save_agent_tool_permissions_b3(_route_client_b3):
    """save-agent with tool_permissions scope saves permissions."""
    client = _route_client_b3
    resp = await client.post(
        "/settings/save-agent",
        data={"agent_form_scope": "tool_permissions", "phone": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "tool_permissions_saved" in resp.headers["location"]


@pytest.mark.anyio
async def test_settings_save_notification_account_empty_b3(_route_client_b3):
    """Saving empty notification phone clears it."""
    client = _route_client_b3
    with patch(
        "src.web.routes.settings.deps.get_notification_target_service"
    ) as mock_svc, patch("src.web.routes.settings.deps.get_notifier") as mock_notifier:
        mock_svc.return_value.set_configured_phone = AsyncMock()
        mock_notifier.return_value = None
        resp = await client.post(
            "/settings/save-notification-account",
            data={"notification_account_phone": ""},
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert "msg=notification_account_saved" in resp.headers["location"]


@pytest.mark.anyio
async def test_settings_save_semantic_search_valid_b3(_route_client_b3):
    client = _route_client_b3
    resp = await client.post(
        "/settings/save-semantic-search",
        data={
            "semantic_embeddings_provider": "openai",
            "semantic_embeddings_model": "text-embedding-ada-002",
            "semantic_embeddings_batch_size": "50",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=semantic_saved" in resp.headers["location"]


@pytest.mark.anyio
async def test_settings_save_semantic_search_invalid_batch_b3(_route_client_b3):
    client = _route_client_b3
    resp = await client.post(
        "/settings/save-semantic-search",
        data={
            "semantic_embeddings_provider": "openai",
            "semantic_embeddings_model": "text-embedding-ada-002",
            "semantic_embeddings_batch_size": "notanumber",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=semantic_invalid_value" in resp.headers["location"]


@pytest.mark.anyio
async def test_settings_save_semantic_search_empty_provider_b3(_route_client_b3):
    client = _route_client_b3
    resp = await client.post(
        "/settings/save-semantic-search",
        data={
            "semantic_embeddings_provider": "",
            "semantic_embeddings_model": "model",
            "semantic_embeddings_batch_size": "50",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=semantic_invalid_value" in resp.headers["location"]


# ===========================================================================
# 6. web/photo_loader/handlers.py — build_feedback helper
# ===========================================================================


class TestPhotoBuildFeedback:
    """Unit tests for the build_feedback helper in photo_loader handlers."""

    def _make_item(self, **kwargs):
        from types import SimpleNamespace

        return SimpleNamespace(**kwargs)

    def test_photo_sent_no_items(self):
        from src.web.photo_loader.handlers import build_feedback as _build_feedback

        result = _build_feedback("photo_sent", None, batches=[], items=[], auto_jobs=[])
        assert result is not None
        assert result["variant"] == "success"
        assert "Фото успешно отправлены" in result["body"]

    def test_photo_sent_with_target(self):
        from src.web.photo_loader.handlers import build_feedback as _build_feedback

        item = self._make_item(target_title="My Channel", target_dialog_id=123)
        result = _build_feedback("photo_sent", None, batches=[], items=[item], auto_jobs=[])
        assert "My Channel" in result["body"]

    def test_photo_scheduled_no_items(self):
        from src.web.photo_loader.handlers import build_feedback as _build_feedback

        result = _build_feedback("photo_scheduled", None, batches=[], items=[], auto_jobs=[])
        assert result["variant"] == "success"
        assert "Отложенная отправка создана" in result["body"]

    def test_photo_batch_created_with_batch(self):
        from src.web.photo_loader.handlers import build_feedback as _build_feedback

        batch = self._make_item(target_title="Batch Target", target_dialog_id=456)
        result = _build_feedback(
            "photo_batch_created", None, batches=[batch], items=[], auto_jobs=[]
        )
        assert "Batch Target" in result["body"]

    def test_photo_auto_created_with_job(self):
        from src.web.photo_loader.handlers import build_feedback as _build_feedback

        job = self._make_item(target_title="Auto Target", target_dialog_id=789)
        result = _build_feedback(
            "photo_auto_created", None, batches=[], items=[], auto_jobs=[job]
        )
        assert "Auto Target" in result["body"]

    def test_error_photo_send_failed(self):
        from src.web.photo_loader.handlers import build_feedback as _build_feedback

        result = _build_feedback(None, "photo_send_failed", batches=[], items=[], auto_jobs=[])
        assert result["variant"] == "error"

    def test_error_photo_target_required(self):
        from src.web.photo_loader.handlers import build_feedback as _build_feedback

        result = _build_feedback(
            None, "photo_target_required", batches=[], items=[], auto_jobs=[]
        )
        assert result["variant"] == "error"

    def test_error_photo_target_invalid(self):
        from src.web.photo_loader.handlers import build_feedback as _build_feedback

        result = _build_feedback(
            None, "photo_target_invalid", batches=[], items=[], auto_jobs=[]
        )
        assert result["variant"] == "error"

    def test_error_photo_schedule_failed(self):
        from src.web.photo_loader.handlers import build_feedback as _build_feedback

        result = _build_feedback(
            None, "photo_schedule_failed", batches=[], items=[], auto_jobs=[]
        )
        assert result["variant"] == "error"

    def test_error_photo_batch_failed(self):
        from src.web.photo_loader.handlers import build_feedback as _build_feedback

        result = _build_feedback(None, "photo_batch_failed", batches=[], items=[], auto_jobs=[])
        assert result["variant"] == "error"

    def test_error_photo_auto_failed(self):
        from src.web.photo_loader.handlers import build_feedback as _build_feedback

        result = _build_feedback(None, "photo_auto_failed", batches=[], items=[], auto_jobs=[])
        assert result["variant"] == "error"

    def test_no_msg_no_error_returns_none(self):
        from src.web.photo_loader.handlers import build_feedback as _build_feedback

        result = _build_feedback(None, None, batches=[], items=[], auto_jobs=[])
        assert result is None

    def test_target_label_with_title(self):
        from src.web.photo_loader.handlers import _target_label

        assert _target_label("My Channel", 123) == "My Channel"

    def test_target_label_with_id_only(self):
        from src.web.photo_loader.handlers import _target_label

        assert _target_label(None, 123) == "123"

    def test_target_label_none(self):
        from src.web.photo_loader.handlers import _target_label

        assert _target_label(None, None) is None

    def test_parse_schedule_at_naive(self):
        """_parse_schedule_at with naive datetime localises and converts to UTC."""
        from src.web.photo_loader.forms import parse_schedule_at as _parse_schedule_at

        result = _parse_schedule_at("2025-06-15T14:30:00")
        from datetime import timezone

        assert result.tzinfo == timezone.utc

    def test_parse_schedule_at_aware(self):
        from src.web.photo_loader.forms import parse_schedule_at as _parse_schedule_at

        result = _parse_schedule_at("2025-06-15T14:30:00+00:00")
        from datetime import timezone

        assert result.tzinfo == timezone.utc

    def test_parse_target_without_form_values(self):
        """_parse_target looks up title/type from dialogs when not in form."""
        from src.web.photo_loader.forms import parse_target as _parse_target

        dialogs = [{"channel_id": 99, "title": "Found Title", "channel_type": "channel"}]
        form = {"target_dialog_id": "99", "target_title": "", "target_type": ""}
        target = _parse_target(form, dialogs)
        assert target.title == "Found Title"
        assert target.target_type == "channel"
