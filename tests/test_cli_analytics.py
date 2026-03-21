"""CLI analytics command tests — top/content-types/hourly subcommands."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from src.database import Database
from src.models import Message
from tests.helpers import cli_add_channel as _add_channel
from tests.helpers import cli_ns as _ns

NOW = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


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
