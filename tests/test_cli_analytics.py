"""CLI analytics command tests — top/content-types/hourly subcommands."""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from src.config import AppConfig
from src.database import Database
from src.models import Channel, Message

NOW = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def cli_db(tmp_path):
    """Sync fixture: real SQLite for CLI tests."""
    db_path = str(tmp_path / "cli_analytics_test.db")
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


def _ns(**kwargs) -> argparse.Namespace:
    defaults = {"config": "config.yaml"}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def _add_channel(db: Database, channel_id: int = 100, title: str = "TestCh") -> int:
    return asyncio.run(db.add_channel(Channel(channel_id=channel_id, title=title)))


def _insert_message(db: Database, msg: Message) -> None:
    asyncio.run(db.insert_message(msg))


class TestCLIAnalytics:
    def test_top_empty(self, cli_env, capsys):
        from src.cli.commands.analytics import run

        run(_ns(analytics_action="top", limit=20, date_from=None, date_to=None))
        assert "No messages with reactions found." in capsys.readouterr().out

    def test_top_with_reactions(self, cli_env, capsys):
        _add_channel(cli_env, channel_id=700, title="ReactCh")
        _insert_message(
            cli_env,
            Message(
                channel_id=700,
                message_id=1,
                text="hot post",
                date=NOW,
                reactions_json='[{"emoji": "👍", "count": 10}]',
            ),
        )

        from src.cli.commands.analytics import run

        run(_ns(analytics_action="top", limit=20, date_from=None, date_to=None))
        out = capsys.readouterr().out
        assert "10" in out
        assert "hot post" in out

    def test_top_limit(self, cli_env, capsys):
        _add_channel(cli_env, channel_id=701, title="LimitCh")
        for i in range(5):
            _insert_message(
                cli_env,
                Message(
                    channel_id=701,
                    message_id=i + 1,
                    text=f"msg{i + 1:04d}",
                    date=NOW,
                    reactions_json=f'[{{"emoji": "👍", "count": {i + 1}}}]',
                ),
            )

        from src.cli.commands.analytics import run

        run(_ns(analytics_action="top", limit=2, date_from=None, date_to=None))
        out = capsys.readouterr().out
        assert "msg0005" in out   # top post (count=5)
        assert "msg0001" not in out  # lowest post not in top 2

    def test_content_types_empty(self, cli_env, capsys):
        from src.cli.commands.analytics import run

        run(_ns(analytics_action="content-types", date_from=None, date_to=None))
        assert "No data." in capsys.readouterr().out

    def test_content_types_with_data(self, cli_env, capsys):
        _add_channel(cli_env, channel_id=702, title="MediaCh")
        _insert_message(
            cli_env,
            Message(channel_id=702, message_id=1, text="text post", date=NOW),
        )
        _insert_message(
            cli_env,
            Message(channel_id=702, message_id=2, media_type="photo", date=NOW),
        )

        from src.cli.commands.analytics import run

        run(_ns(analytics_action="content-types", date_from=None, date_to=None))
        out = capsys.readouterr().out
        assert "photo" in out
        assert "text" in out

    def test_hourly_empty(self, cli_env, capsys):
        from src.cli.commands.analytics import run

        run(_ns(analytics_action="hourly", date_from=None, date_to=None))
        assert "No data." in capsys.readouterr().out

    def test_hourly_with_data(self, cli_env, capsys):
        _add_channel(cli_env, channel_id=703, title="HourlyCh")
        _insert_message(
            cli_env,
            Message(
                channel_id=703,
                message_id=1,
                text="morning post",
                date=datetime(2025, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            ),
        )

        from src.cli.commands.analytics import run

        run(_ns(analytics_action="hourly", date_from=None, date_to=None))
        out = capsys.readouterr().out
        assert "09:00" in out
