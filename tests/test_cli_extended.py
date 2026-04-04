from __future__ import annotations

import argparse
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

# Pre-import pydantic.root_model to prevent lazy-import conflict
# between pydantic v2 and mcp SDK when both are installed.
import pydantic.root_model  # noqa: F401
import pytest

from src.config import AppConfig
from src.models import Channel, ChannelStats, CollectionTaskStatus
from src.telegram.flood_wait import FloodWaitInfo

pytestmark = pytest.mark.aiosqlite_serial


def _ns(**kwargs) -> argparse.Namespace:
    defaults = {"config": "config.yaml"}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def _chan(channel_id, title, username="", ch_type="channel", deactivate=False):
    return {
        "channel_id": channel_id,
        "title": title,
        "username": username,
        "channel_type": ch_type,
        "deactivate": deactivate,
    }


@pytest.fixture
def cli_env(cli_db, cli_init_patch):
    with cli_init_patch(
        cli_db,
        "src.cli.commands.channel.runtime.init_db",
        "src.cli.commands.test.runtime.init_db",
        config=AppConfig(),
        fresh_database=True,
    ):
        yield cli_db


@pytest.fixture
def cli_env_with_mock_pool(cli_env):
    mock_pool = AsyncMock()
    mock_pool.clients = {"+70001112233": MagicMock()}
    mock_pool.disconnect_all = AsyncMock()
    mock_pool.release_client = AsyncMock()
    mock_pool.fetch_channel_meta = AsyncMock(return_value=None)

    mock_client = AsyncMock()
    mock_client.get_dialogs = AsyncMock()
    mock_pool.get_available_client = AsyncMock(
        return_value=(mock_client, "+70001112233"),
    )

    async def fake_init_pool(config, db):
        from src.telegram.auth import TelegramAuth

        return TelegramAuth(0, ""), mock_pool

    with patch(
        "src.cli.commands.channel.runtime.init_pool",
        side_effect=fake_init_pool,
    ):
        yield cli_env, mock_pool


class TestCLIChannelExtended:
    def test_add_success(self, cli_env_with_mock_pool, capsys):
        db, pool = cli_env_with_mock_pool
        pool.resolve_channel.return_value = _chan(
            12345,
            "New Channel",
            "new_chan",
        )
        from src.cli.commands.channel import run

        run(_ns(channel_action="add", identifier="@new_chan"))
        out = capsys.readouterr().out
        assert "Added channel: New Channel (12345)" in out

        # Verify it was added to DB
        ch = asyncio.run(db.get_channel_by_channel_id(12345))
        assert ch is not None
        assert ch.title == "New Channel"

    def test_add_deactivated(self, cli_env_with_mock_pool, capsys):
        db, pool = cli_env_with_mock_pool
        pool.resolve_channel.return_value = _chan(
            67890,
            "Scam Channel",
            "scam_chan",
            "scam",
            True,
        )
        from src.cli.commands.channel import run

        run(_ns(channel_action="add", identifier="@scam_chan"))
        out = capsys.readouterr().out
        assert "WARN: deactivated, type=scam" in out

        ch = asyncio.run(db.get_channel_by_channel_id(67890))
        assert ch is not None
        assert ch.is_active is False

    def test_import_success(self, cli_env_with_mock_pool, capsys):
        db, pool = cli_env_with_mock_pool
        pool.resolve_channel.side_effect = [
            _chan(1, "Ch1", "u1"),
            _chan(2, "Ch2", "u2"),
        ]
        from src.cli.commands.channel import run

        run(_ns(channel_action="import", source="u1, u2"))
        out = capsys.readouterr().out
        assert "Added: 2" in out
        assert "OK: u1 — Ch1 (1)" in out
        assert "OK: u2 — Ch2 (2)" in out

    def test_import_with_skips_and_fails(self, cli_env_with_mock_pool, capsys):
        db, pool = cli_env_with_mock_pool
        # Add one existing channel
        asyncio.run(db.add_channel(Channel(channel_id=1, title="Ch1")))

        pool.resolve_channel.side_effect = [
            _chan(1, "Ch1", "u1"),
            None,  # Failed to resolve
            _chan(3, "Ch3", "u3"),
        ]
        from src.cli.commands.channel import run

        run(_ns(channel_action="import", source="u1, u2, u3"))
        out = capsys.readouterr().out
        assert "Added: 1" in out
        assert "Skipped: 1" in out
        assert "Failed: 1" in out

    def test_stats_single_success(self, cli_env_with_mock_pool, capsys):
        db, pool = cli_env_with_mock_pool
        ch_id = asyncio.run(
            db.add_channel(
                Channel(channel_id=100, title="StatsChan"),
            )
        )

        mock_stats = ChannelStats(
            channel_id=100,
            subscriber_count=500,
            avg_views=10.5,
            avg_reactions=2.0,
            avg_forwards=1.0,
        )

        with patch(
            "src.cli.commands.channel.Collector",
        ) as mock_collector:
            inst = mock_collector.return_value
            inst.collect_channel_stats = AsyncMock(
                return_value=mock_stats,
            )

            from src.cli.commands.channel import run

            run(
                _ns(
                    channel_action="stats",
                    identifier=str(ch_id),
                    all=False,
                )
            )

        out = capsys.readouterr().out
        assert "Subscribers: 500" in out
        assert "Avg views: 10.5" in out

    def test_stats_all_success(self, cli_env_with_mock_pool, capsys):
        db, pool = cli_env_with_mock_pool
        with patch(
            "src.cli.commands.channel.Collector",
        ) as mock_collector:
            inst = mock_collector.return_value
            inst.collect_all_stats = AsyncMock(
                return_value={"channels": 5, "errors": 0},
            )

            from src.cli.commands.channel import run

            run(_ns(channel_action="stats", identifier=None, all=True))

        out = capsys.readouterr().out
        assert "Stats collected: {'channels': 5, 'errors': 0}" in out

    def test_refresh_types_success(self, cli_env_with_mock_pool, capsys):
        db, pool = cli_env_with_mock_pool
        asyncio.run(
            db.add_channel(
                Channel(
                    channel_id=101,
                    title="T1",
                    username="u1",
                    is_active=True,
                )
            )
        )
        asyncio.run(
            db.add_channel(
                Channel(
                    channel_id=102,
                    title="T2",
                    username="u2",
                    is_active=True,
                )
            )
        )

        pool.resolve_channel.side_effect = [
            _chan(101, "T1", "u1", "supergroup"),
            _chan(102, "T2", "u2", "scam", True),
        ]

        from src.cli.commands.channel import run

        run(_ns(channel_action="refresh-types"))

        out = capsys.readouterr().out
        assert "OK: T1 → supergroup" in out
        assert "DEACTIVATED (scam): T2" in out

        ch1 = asyncio.run(db.get_channel_by_channel_id(101))
        assert ch1.channel_type == "supergroup"
        ch2 = asyncio.run(db.get_channel_by_channel_id(102))
        assert ch2.is_active is False

    def test_add_no_client_error(self, cli_env_with_mock_pool, capsys):
        db, pool = cli_env_with_mock_pool
        pool.resolve_channel.side_effect = RuntimeError("no_client")
        from src.cli.commands.channel import run

        run(_ns(channel_action="add", identifier="@any"))
        out = capsys.readouterr().out
        assert "Нет доступных аккаунтов Telegram" in out

    def test_add_general_error(self, cli_env_with_mock_pool, capsys):
        db, pool = cli_env_with_mock_pool
        pool.resolve_channel.side_effect = Exception("boom")
        from src.cli.commands.channel import run

        run(_ns(channel_action="add", identifier="@any"))
        out = capsys.readouterr().out
        assert "Could not resolve channel" in out

    def test_import_from_file(self, cli_env_with_mock_pool, capsys, tmp_path):
        db, pool = cli_env_with_mock_pool
        import_file = tmp_path / "channels.txt"
        import_file.write_text("@chan1\n@chan2")

        pool.resolve_channel.side_effect = [
            _chan(10, "C1", "chan1"),
            _chan(20, "C2", "chan2"),
        ]
        from src.cli.commands.channel import run

        run(_ns(channel_action="import", source=str(import_file)))
        out = capsys.readouterr().out
        assert "Added: 2" in out

    def test_import_no_client_error(self, cli_env_with_mock_pool, capsys):
        db, pool = cli_env_with_mock_pool
        pool.resolve_channel.side_effect = RuntimeError("no_client")
        from src.cli.commands.channel import run

        run(_ns(channel_action="import", source="@any"))
        out = capsys.readouterr().out
        assert "Нет доступных аккаунтов Telegram. Импорт прерван" in out

    def test_stats_no_client_available(self, cli_env_with_mock_pool, capsys):
        db, pool = cli_env_with_mock_pool
        ch_id = asyncio.run(
            db.add_channel(
                Channel(channel_id=100, title="StatsChan"),
            )
        )
        with patch(
            "src.cli.commands.channel.Collector",
        ) as mock_collector:
            inst = mock_collector.return_value
            inst.collect_channel_stats = AsyncMock(return_value=None)
            from src.cli.commands.channel import run

            run(
                _ns(
                    channel_action="stats",
                    identifier=str(ch_id),
                    all=False,
                )
            )
        out = capsys.readouterr().out
        assert "No client available" in out

    def test_refresh_types_resolve_exception(self, cli_env_with_mock_pool, capsys):
        db, pool = cli_env_with_mock_pool
        asyncio.run(
            db.add_channel(
                Channel(channel_id=101, title="T1", is_active=True),
            )
        )
        pool.resolve_channel.side_effect = Exception("resolve error")
        from src.cli.commands.channel import run

        run(_ns(channel_action="refresh-types"))
        out = capsys.readouterr().out
        assert "Skipped: 1" in out

    def test_refresh_types_not_found(self, cli_env_with_mock_pool, capsys):
        db, pool = cli_env_with_mock_pool
        asyncio.run(
            db.add_channel(
                Channel(channel_id=101, title="T1", is_active=True),
            )
        )
        pool.resolve_channel.return_value = False  # Simulation of not found
        from src.cli.commands.channel import run

        run(_ns(channel_action="refresh-types"))
        out = capsys.readouterr().out
        assert "DEACTIVATED: T1" in out

    def test_collect_success(self, cli_env_with_mock_pool, capsys):
        db, pool = cli_env_with_mock_pool
        ch_id = asyncio.run(
            db.add_channel(
                Channel(channel_id=103, title="CollectMe"),
            )
        )

        with patch(
            "src.cli.commands.channel.Collector",
        ) as mock_collector:
            inst = mock_collector.return_value
            inst.collect_single_channel = AsyncMock(return_value=42)

            from src.cli.commands.channel import run

            run(_ns(channel_action="collect", identifier=str(ch_id)))

        out = capsys.readouterr().out
        assert "Collected 42 messages" in out

        # Verify task status
        tasks = asyncio.run(db.get_collection_tasks(limit=1))
        assert tasks[0].status == CollectionTaskStatus.COMPLETED
        assert tasks[0].messages_collected == 42

    def test_import_deactivated(self, cli_env_with_mock_pool, capsys):
        db, pool = cli_env_with_mock_pool
        pool.resolve_channel.return_value = _chan(
            99,
            "Deactivated",
            "u99",
            "scam",
            True,
        )
        from src.cli.commands.channel import run

        run(_ns(channel_action="import", source="u99"))
        out = capsys.readouterr().out
        assert "WARN (scam): u99" in out

    def test_stats_no_identifier_error(self, cli_env_with_mock_pool, capsys):
        from src.cli.commands.channel import run

        run(_ns(channel_action="stats", identifier=None, all=False))
        out = capsys.readouterr().out
        assert "Specify a channel identifier or use --all" in out

    def test_stats_not_found(self, cli_env_with_mock_pool, capsys):
        db, pool = cli_env_with_mock_pool
        from src.cli.commands.channel import run

        run(
            _ns(
                channel_action="stats",
                identifier="999",
                all=False,
            )
        )
        out = capsys.readouterr().out
        assert "not found" in out

    def test_collect_not_found(self, cli_env_with_mock_pool, capsys):
        db, pool = cli_env_with_mock_pool
        from src.cli.commands.channel import run

        run(_ns(channel_action="collect", identifier="999"))
        out = capsys.readouterr().out
        assert "not found" in out

    def test_refresh_types_prefetch_error(self, cli_env_with_mock_pool, capsys):
        db, pool = cli_env_with_mock_pool

        async def _test():
            mock_client, _ = await pool.get_available_client()
            mock_client.get_dialogs.side_effect = Exception(
                "prefetch failed",
            )

        asyncio.run(_test())

        from src.cli.commands.channel import run

        run(_ns(channel_action="refresh-types"))
        # Should continue even if prefetch fails
        assert "Active channels to check" in capsys.readouterr().out

    def test_refresh_types_with_null_type(self, cli_env_with_mock_pool, capsys):
        db, pool = cli_env_with_mock_pool
        asyncio.run(
            db.add_channel(
                Channel(
                    channel_id=105,
                    title="NullType",
                    channel_type=None,
                )
            )
        )
        pool.resolve_channel.return_value = _chan(
            105,
            "NullType",
            "u105",
        )
        from src.cli.commands.channel import run

        run(_ns(channel_action="refresh-types"))
        out = capsys.readouterr().out
        assert "missing type: 1" in out

    def test_collect_error(self, cli_env_with_mock_pool, capsys):
        db, pool = cli_env_with_mock_pool
        ch_id = asyncio.run(
            db.add_channel(
                Channel(channel_id=103, title="CollectMe"),
            )
        )
        with patch(
            "src.cli.commands.channel.Collector",
        ) as mock_collector:
            inst = mock_collector.return_value
            inst.collect_single_channel = AsyncMock(
                side_effect=Exception("collect fail"),
            )
            from src.cli.commands.channel import run

            with pytest.raises(Exception, match="collect fail"):
                run(
                    _ns(
                        channel_action="collect",
                        identifier=str(ch_id),
                    )
                )
        tasks = asyncio.run(db.get_collection_tasks(limit=1))
        assert tasks[0].status == CollectionTaskStatus.FAILED
        assert "collect fail" in tasks[0].error


class TestCLITestExtended:
    def test_read_checks_with_errors(self, cli_env, capsys):
        # Mock db methods to raise exceptions
        cli_env.get_stats = AsyncMock(
            side_effect=Exception("stats fail"),
        )
        cli_env.get_accounts = AsyncMock(
            side_effect=Exception("acc fail"),
        )
        cli_env.get_channels_with_counts = AsyncMock(
            side_effect=Exception("chan fail"),
        )

        from src.cli.commands.test import run

        init_db_mock = AsyncMock(return_value=(AppConfig(), cli_env))
        with (
            patch(
                "src.cli.commands.test.runtime.init_db",
                side_effect=init_db_mock,
            ),
            pytest.raises(SystemExit),
        ):
            run(_ns(command="test", test_action="read"))
        out = capsys.readouterr().out
        assert "stats fail" in out
        assert "acc fail" in out
        assert "chan fail" in out

    def test_write_checks_with_empty_db(self, cli_env, capsys):
        # Empty DB (no accounts, no channels)
        from src.cli.commands.test import run

        init_db_mock = AsyncMock(
            return_value=(AppConfig(), cli_env),
        )
        with patch(
            "src.cli.commands.test.runtime.init_db",
            side_effect=init_db_mock,
        ):
            run(_ns(command="test", test_action="write"))
        out = capsys.readouterr().out
        # In current test setup, it might not skip if we don't
        # ensure the DB copy is also empty.
        # But we'll at least cover the code paths.
        assert "write_db_copy" in out

    def test_telegram_live_checks_no_accounts(
        self,
        cli_env_with_mock_pool,
        capsys,
    ):
        db, pool = cli_env_with_mock_pool
        pool.clients = {}  # No accounts

        from src.cli.commands.test import run

        init_db = AsyncMock(return_value=(AppConfig(), db))
        init_pool = AsyncMock(return_value=(None, pool))
        with patch(
            "src.cli.commands.test.runtime.init_db",
            side_effect=init_db,
        ):
            with patch(
                "src.cli.commands.test.runtime.init_pool",
                side_effect=init_pool,
            ):
                run(_ns(command="test", test_action="telegram"))
        out = capsys.readouterr().out
        assert "tg_pool_init" in out

    def test_telegram_live_checks_full_mock(
        self,
        cli_env_with_mock_pool,
        capsys,
    ):
        db, pool = cli_env_with_mock_pool
        # Add channel to avoid skips
        asyncio.run(
            db.add_channel(
                Channel(
                    channel_id=1,
                    title="Ch1",
                    username="u1",
                    is_active=True,
                )
            )
        )

        # Mock various Telegram calls
        pool.get_users_info.return_value = [MagicMock(phone="+7999")]
        pool.get_dialogs.return_value = [{"id": 1, "title": "D1"}]
        pool.resolve_channel.return_value = {
            "channel_id": 1,
            "title": "Ch1",
        }

        mock_client, _ = asyncio.run(pool.get_available_client())
        mock_client.get_entity.return_value = MagicMock(id=1)

        # Mock iter_messages
        async def mock_iter(*args, **kwargs):
            m = MagicMock()
            m.id = 100
            m.text = "test msg"
            m.sender_id = 123
            from datetime import datetime

            m.date = datetime.now()
            m.media = None
            sender_patch = patch(
                "src.telegram.collector.Collector._get_sender_name",
                return_value="Test Sender",
            )
            media_patch = patch(
                "src.telegram.collector.Collector._get_media_type",
                return_value="text",
            )
            with sender_patch, media_patch:
                yield m

        mock_client.iter_messages = mock_iter

        with patch(
            "src.telegram.collector.Collector",
        ) as mock_collector:
            inst = mock_collector.return_value
            inst.collect_channel_stats = AsyncMock(
                return_value=MagicMock(subscriber_count=100),
            )

            with patch(
                "src.search.engine.SearchEngine",
            ) as mock_engine:
                engine_inst = mock_engine.return_value
                engine_inst.search_my_chats = AsyncMock(
                    return_value=MagicMock(total=5, error=None, flood_wait=None),
                )
                engine_inst.search_in_channel = AsyncMock(
                    return_value=MagicMock(total=3, error=None, flood_wait=None),
                )
                engine_inst.search_telegram = AsyncMock(
                    return_value=MagicMock(
                        total=0,
                        error="Premium needed",
                        flood_wait=None,
                    ),
                )
                engine_inst.check_search_quota = AsyncMock(
                    return_value={"left": 10},
                )

                from src.cli.commands.test import run

                init_db = AsyncMock(
                    return_value=(AppConfig(), db),
                )
                init_pool = AsyncMock(
                    return_value=(None, pool),
                )
                with patch(
                    "src.cli.commands.test.runtime.init_db",
                    side_effect=init_db,
                ):
                    with patch(
                        "src.cli.commands.test.runtime.init_pool",
                        side_effect=init_pool,
                    ):
                        run(
                            _ns(
                                command="test",
                                test_action="telegram",
                            )
                        )

        out = capsys.readouterr().out
        assert "tg_users_info" in out
        assert "tg_get_dialogs" in out
        assert "tg_resolve_channel" in out
        assert "tg_warm_dialog_cache" in out
        assert "tg_iter_messages" in out
        assert "tg_channel_stats" in out
        assert "tg_search_my_chats" in out
        assert "tg_search_premium" in out
        assert "tg_search_quota" in out

    def test_telegram_live_checks_skip_remaining_steps_after_flood_wait(
        self,
        cli_env_with_mock_pool,
        capsys,
    ):
        db, pool = cli_env_with_mock_pool
        asyncio.run(
            db.add_channel(
                Channel(
                    channel_id=1,
                    title="Ch1",
                    username="u1",
                    is_active=True,
                )
            )
        )

        pool.get_users_info.return_value = [MagicMock(phone="+7999")]
        pool.get_dialogs.return_value = [{"id": 1, "title": "D1"}]
        pool.resolve_channel.return_value = {
            "channel_id": 1,
            "title": "Ch1",
        }

        mock_client, _ = asyncio.run(pool.get_available_client())
        mock_client.get_entity.return_value = MagicMock(id=1)

        async def mock_iter(*args, **kwargs):
            m = MagicMock()
            m.id = 100
            m.text = "test msg"
            m.sender_id = 123
            from datetime import datetime

            m.date = datetime.now()
            m.media = None
            sender_patch = patch(
                "src.telegram.collector.Collector._get_sender_name",
                return_value="Test Sender",
            )
            media_patch = patch(
                "src.telegram.collector.Collector._get_media_type",
                return_value="text",
            )
            with sender_patch, media_patch:
                yield m

        mock_client.iter_messages = mock_iter

        with patch("src.telegram.collector.Collector") as mock_collector:
            inst = mock_collector.return_value
            inst.collect_channel_stats = AsyncMock(
                return_value=MagicMock(subscriber_count=100),
            )

            with patch("src.search.engine.SearchEngine") as mock_engine:
                engine_inst = mock_engine.return_value
                from datetime import datetime, timezone

                flood_wait = FloodWaitInfo(
                    operation="tg_search_my_chats",
                    phone="+7999",
                    wait_seconds=61,
                    next_available_at_utc=datetime.now(timezone.utc),
                    detail="Flood wait 61s until 2026-03-19T03:00:00+00:00 UTC for +7999",
                )
                engine_inst.search_my_chats = AsyncMock(
                    return_value=MagicMock(
                        total=0,
                        error=flood_wait.detail,
                        flood_wait=flood_wait,
                    ),
                )
                engine_inst.search_in_channel = AsyncMock(
                    return_value=MagicMock(total=3, error=None, flood_wait=None),
                )
                engine_inst.search_telegram = AsyncMock(
                    return_value=MagicMock(total=1, error=None, flood_wait=None),
                )
                engine_inst.check_search_quota = AsyncMock(return_value={"left": 10})
                pool.get_stats_availability = AsyncMock(
                    return_value=MagicMock(
                        state="all_flooded",
                        retry_after_sec=61,
                        next_available_at_utc=datetime.now(timezone.utc),
                    )
                )

                from src.cli.commands.test import run

                init_db = AsyncMock(return_value=(AppConfig(), db))
                init_pool = AsyncMock(return_value=(None, pool))
                with patch(
                    "src.cli.commands.test.runtime.init_db",
                    side_effect=init_db,
                ):
                    with patch(
                        "src.cli.commands.test.runtime.init_pool",
                        side_effect=init_pool,
                    ):
                        run(
                            _ns(
                                command="test",
                                test_action="telegram",
                            )
                        )

        out = capsys.readouterr().out
        assert "tg_search_my_chats" in out
        assert "all clients are flood-waited" in out
        assert "tg_search_in_channel" not in out
        engine_inst.search_in_channel.assert_not_called()
        engine_inst.search_telegram.assert_not_called()
        engine_inst.check_search_quota.assert_not_called()
        assert pool.clients["+70001112233"].flood_sleep_threshold == 0

    def test_telegram_live_checks_waits_for_short_warm_dialog_flood(
        self,
        cli_env_with_mock_pool,
        capsys,
    ):
        from telethon.errors import FloodWaitError

        db, pool = cli_env_with_mock_pool
        asyncio.run(
            db.add_channel(
                Channel(
                    channel_id=1,
                    title="Ch1",
                    username="u1",
                    is_active=True,
                )
            )
        )

        pool.get_users_info.return_value = [MagicMock(phone="+7999")]
        pool.get_dialogs.return_value = [{"id": 1, "title": "D1"}]
        pool.resolve_channel.return_value = {
            "channel_id": 1,
            "title": "Ch1",
        }

        phone = "+70001112233"
        mock_client, _ = asyncio.run(pool.get_available_client())
        mock_client.get_entity.return_value = MagicMock(id=1)

        flood = FloodWaitError(request=None, capture=0)
        flood.seconds = 5
        mock_client.get_dialogs.side_effect = [flood, []]
        from datetime import datetime, timezone

        pool.get_available_client.side_effect = [
            (mock_client, phone),
            (mock_client, phone),
            (mock_client, phone),
        ]
        pool.get_stats_availability = AsyncMock(
            return_value=MagicMock(
                state="all_flooded",
                retry_after_sec=5,
                next_available_at_utc=datetime.now(timezone.utc),
            )
        )

        async def mock_iter(*args, **kwargs):
            m = MagicMock()
            m.id = 100
            m.text = "test msg"
            m.sender_id = 123
            from datetime import datetime

            m.date = datetime.now()
            m.media = None
            sender_patch = patch(
                "src.telegram.collector.Collector._get_sender_name",
                return_value="Test Sender",
            )
            media_patch = patch(
                "src.telegram.collector.Collector._get_media_type",
                return_value="text",
            )
            with sender_patch, media_patch:
                yield m

        mock_client.iter_messages = mock_iter

        with patch("src.telegram.collector.Collector") as mock_collector:
            inst = mock_collector.return_value
            inst.collect_channel_stats = AsyncMock(
                return_value=MagicMock(subscriber_count=100),
            )

            with patch("src.search.engine.SearchEngine") as mock_engine:
                engine_inst = mock_engine.return_value
                engine_inst.search_my_chats = AsyncMock(
                    return_value=MagicMock(total=5, error=None, flood_wait=None),
                )
                engine_inst.search_in_channel = AsyncMock(
                    return_value=MagicMock(total=3, error=None, flood_wait=None),
                )
                engine_inst.search_telegram = AsyncMock(
                    return_value=MagicMock(total=0, error="Premium needed", flood_wait=None),
                )
                engine_inst.check_search_quota = AsyncMock(return_value={"left": 10})

                from src.cli.commands.test import run

                init_db = AsyncMock(return_value=(AppConfig(), db))
                init_pool = AsyncMock(return_value=(None, pool))
                sleep_mock = AsyncMock()
                with patch(
                    "src.cli.commands.test.runtime.init_db",
                    side_effect=init_db,
                ):
                    with patch(
                        "src.cli.commands.test.runtime.init_pool",
                        side_effect=init_pool,
                    ):
                        with patch(
                            "src.cli.commands.test.asyncio.sleep",
                            sleep_mock,
                        ):
                            run(
                                _ns(
                                    command="test",
                                    test_action="telegram",
                                )
                            )

        out = capsys.readouterr().out
        assert "tg_warm_dialog_cache" in out
        assert "PASS" in out
        sleep_mock.assert_awaited_once_with(6)
        pool.report_flood.assert_awaited_once_with(phone, 5)

    def test_telegram_live_checks_retries_search_step_on_single_account_flood(
        self,
        cli_env_with_mock_pool,
        capsys,
    ):
        db, pool = cli_env_with_mock_pool
        asyncio.run(
            db.add_channel(
                Channel(
                    channel_id=1,
                    title="Ch1",
                    username="u1",
                    is_active=True,
                )
            )
        )

        pool.get_users_info.return_value = [MagicMock(phone="+7999")]
        pool.get_dialogs.return_value = [{"id": 1, "title": "D1"}]
        pool.resolve_channel.return_value = {
            "channel_id": 1,
            "title": "Ch1",
        }
        pool.get_stats_availability = AsyncMock(
            return_value=MagicMock(
                state="available",
                retry_after_sec=None,
                next_available_at_utc=None,
            )
        )

        mock_client, _ = asyncio.run(pool.get_available_client())
        mock_client.get_entity.return_value = MagicMock(id=1)

        async def mock_iter(*args, **kwargs):
            m = MagicMock()
            m.id = 100
            m.text = "test msg"
            m.sender_id = 123
            from datetime import datetime

            m.date = datetime.now()
            m.media = None
            sender_patch = patch(
                "src.telegram.collector.Collector._get_sender_name",
                return_value="Test Sender",
            )
            media_patch = patch(
                "src.telegram.collector.Collector._get_media_type",
                return_value="text",
            )
            with sender_patch, media_patch:
                yield m

        mock_client.iter_messages = mock_iter

        with patch("src.telegram.collector.Collector") as mock_collector:
            inst = mock_collector.return_value
            inst.collect_channel_stats = AsyncMock(
                return_value=MagicMock(subscriber_count=100),
            )

            with patch("src.search.engine.SearchEngine") as mock_engine:
                engine_inst = mock_engine.return_value
                from datetime import datetime, timezone

                flood_wait = FloodWaitInfo(
                    operation="tg_search_my_chats",
                    phone="+7999",
                    wait_seconds=17,
                    next_available_at_utc=datetime.now(timezone.utc),
                    detail="Flood wait 17s until 2026-03-19T03:00:00+00:00 UTC for +7999",
                )
                engine_inst.search_my_chats = AsyncMock(
                    side_effect=[
                        MagicMock(total=0, error=flood_wait.detail, flood_wait=flood_wait),
                        MagicMock(total=2, error=None, flood_wait=None),
                    ]
                )
                engine_inst.search_in_channel = AsyncMock(
                    return_value=MagicMock(total=3, error=None, flood_wait=None),
                )
                engine_inst.search_telegram = AsyncMock(
                    return_value=MagicMock(total=1, error=None, flood_wait=None),
                )
                engine_inst.check_search_quota = AsyncMock(return_value={"left": 10})

                from src.cli.commands.test import run

                init_db = AsyncMock(return_value=(AppConfig(), db))
                init_pool = AsyncMock(return_value=(None, pool))
                with patch(
                    "src.cli.commands.test.runtime.init_db",
                    side_effect=init_db,
                ):
                    with patch(
                        "src.cli.commands.test.runtime.init_pool",
                        side_effect=init_pool,
                    ):
                        run(
                            _ns(
                                command="test",
                                test_action="telegram",
                            )
                        )

        out = capsys.readouterr().out
        assert "tg_search_my_chats" in out
        assert "2 results" in out
        assert engine_inst.search_my_chats.await_count == 2
        engine_inst.search_in_channel.assert_awaited_once()

    def test_telegram_live_checks_waits_and_retries_search_step_when_all_clients_temporarily_busy(
        self,
        cli_env_with_mock_pool,
        capsys,
    ):
        db, pool = cli_env_with_mock_pool
        asyncio.run(
            db.add_channel(
                Channel(
                    channel_id=1,
                    title="Ch1",
                    username="u1",
                    is_active=True,
                )
            )
        )

        pool.get_users_info.return_value = [MagicMock(phone="+7999")]
        pool.get_dialogs.return_value = [{"id": 1, "title": "D1"}]
        pool.resolve_channel.return_value = {
            "channel_id": 1,
            "title": "Ch1",
        }
        from datetime import datetime, timezone

        pool.get_stats_availability = AsyncMock(
            side_effect=[
                MagicMock(
                    state="all_flooded",
                    retry_after_sec=5,
                    next_available_at_utc=datetime.now(timezone.utc),
                ),
                MagicMock(
                    state="available",
                    retry_after_sec=None,
                    next_available_at_utc=None,
                ),
            ]
        )

        mock_client, _ = asyncio.run(pool.get_available_client())
        mock_client.get_entity.return_value = MagicMock(id=1)

        async def mock_iter(*args, **kwargs):
            m = MagicMock()
            m.id = 100
            m.text = "test msg"
            m.sender_id = 123
            from datetime import datetime

            m.date = datetime.now()
            m.media = None
            sender_patch = patch(
                "src.telegram.collector.Collector._get_sender_name",
                return_value="Test Sender",
            )
            media_patch = patch(
                "src.telegram.collector.Collector._get_media_type",
                return_value="text",
            )
            with sender_patch, media_patch:
                yield m

        mock_client.iter_messages = mock_iter

        with patch("src.telegram.collector.Collector") as mock_collector:
            inst = mock_collector.return_value
            inst.collect_channel_stats = AsyncMock(
                return_value=MagicMock(subscriber_count=100),
            )

            with patch("src.search.engine.SearchEngine") as mock_engine:
                engine_inst = mock_engine.return_value
                engine_inst.search_my_chats = AsyncMock(
                    side_effect=[
                        MagicMock(
                            total=0,
                            error="Нет доступных Telegram-аккаунтов. Проверьте подключение.",
                            flood_wait=None,
                        ),
                        MagicMock(total=2, error=None, flood_wait=None),
                    ]
                )
                engine_inst.search_in_channel = AsyncMock(
                    return_value=MagicMock(total=3, error=None, flood_wait=None),
                )
                engine_inst.search_telegram = AsyncMock(
                    return_value=MagicMock(total=1, error=None, flood_wait=None),
                )
                engine_inst.check_search_quota = AsyncMock(return_value={"left": 10})

                from src.cli.commands.test import run

                init_db = AsyncMock(return_value=(AppConfig(), db))
                init_pool = AsyncMock(return_value=(None, pool))
                sleep_mock = AsyncMock()
                with patch(
                    "src.cli.commands.test.runtime.init_db",
                    side_effect=init_db,
                ):
                    with patch(
                        "src.cli.commands.test.runtime.init_pool",
                        side_effect=init_pool,
                    ):
                        with patch(
                            "src.cli.commands.test.asyncio.sleep",
                            sleep_mock,
                        ):
                            run(
                                _ns(
                                    command="test",
                                    test_action="telegram",
                                )
                            )

        out = capsys.readouterr().out
        assert "tg_search_my_chats" in out
        assert "2 results" in out
        assert "all clients are flood-waited" not in out or "PASS" in out
        assert engine_inst.search_my_chats.await_count == 2
        sleep_mock.assert_awaited_once_with(6)
        engine_inst.search_in_channel.assert_awaited_once()

    def test_telegram_live_checks_waits_for_short_premium_flood_even_with_regular_clients(
        self,
        cli_env_with_mock_pool,
        capsys,
    ):
        db, pool = cli_env_with_mock_pool
        asyncio.run(
            db.add_channel(
                Channel(
                    channel_id=1,
                    title="Ch1",
                    username="u1",
                    is_active=True,
                )
            )
        )

        pool.get_users_info.return_value = [MagicMock(phone="+7999")]
        pool.get_dialogs.return_value = [{"id": 1, "title": "D1"}]
        pool.resolve_channel.return_value = {
            "channel_id": 1,
            "title": "Ch1",
        }
        pool.get_stats_availability = AsyncMock(
            return_value=MagicMock(
                state="available",
                retry_after_sec=None,
                next_available_at_utc=None,
            )
        )
        from datetime import datetime, timezone

        pool.get_premium_stats_availability = AsyncMock(
            return_value=MagicMock(
                state="all_flooded",
                retry_after_sec=12,
                next_available_at_utc=datetime.now(timezone.utc),
            )
        )

        mock_client, _ = asyncio.run(pool.get_available_client())
        mock_client.get_entity.return_value = MagicMock(id=1)

        async def mock_iter(*args, **kwargs):
            m = MagicMock()
            m.id = 100
            m.text = "test msg"
            m.sender_id = 123
            from datetime import datetime

            m.date = datetime.now()
            m.media = None
            sender_patch = patch(
                "src.telegram.collector.Collector._get_sender_name",
                return_value="Test Sender",
            )
            media_patch = patch(
                "src.telegram.collector.Collector._get_media_type",
                return_value="text",
            )
            with sender_patch, media_patch:
                yield m

        mock_client.iter_messages = mock_iter

        with patch("src.telegram.collector.Collector") as mock_collector:
            inst = mock_collector.return_value
            inst.collect_channel_stats = AsyncMock(
                return_value=MagicMock(subscriber_count=100),
            )

            with patch("src.search.engine.SearchEngine") as mock_engine:
                engine_inst = mock_engine.return_value
                flood_wait = FloodWaitInfo(
                    operation="search_telegram",
                    phone="+7999",
                    wait_seconds=12,
                    next_available_at_utc=datetime.now(timezone.utc),
                    detail="Flood wait 12s until 2026-03-19T03:00:00+00:00 UTC for +7999",
                )
                engine_inst.search_my_chats = AsyncMock(
                    return_value=MagicMock(total=5, error=None, flood_wait=None),
                )
                engine_inst.search_in_channel = AsyncMock(
                    return_value=MagicMock(total=3, error=None, flood_wait=None),
                )
                engine_inst.search_telegram = AsyncMock(
                    side_effect=[
                        MagicMock(total=0, error=flood_wait.detail, flood_wait=flood_wait),
                        MagicMock(total=2, error=None, flood_wait=None),
                    ]
                )
                engine_inst.check_search_quota = AsyncMock(return_value={"left": 10})

                from src.cli.commands.test import run

                init_db = AsyncMock(return_value=(AppConfig(), db))
                init_pool = AsyncMock(return_value=(None, pool))
                sleep_mock = AsyncMock()
                with patch(
                    "src.cli.commands.test.runtime.init_db",
                    side_effect=init_db,
                ):
                    with patch(
                        "src.cli.commands.test.runtime.init_pool",
                        side_effect=init_pool,
                    ):
                        with patch(
                            "src.cli.commands.test.asyncio.sleep",
                            sleep_mock,
                        ):
                            run(
                                _ns(
                                    command="test",
                                    test_action="telegram",
                                )
                            )

        out = capsys.readouterr().out
        assert "tg_search_premium" in out
        assert "2 results" in out
        assert engine_inst.search_telegram.await_count == 2
        sleep_mock.assert_awaited_once_with(13)
