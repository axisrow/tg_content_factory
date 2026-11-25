"""CLI analytics command tests — top/content-types/hourly/authority subcommands."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

from tests.helpers import cli_add_channel as _add_channel
from tests.helpers import cli_ns as _ns
from tg_agent.database import Database
from tg_agent.models import Message

NOW = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def _insert_message(db: Database, msg: Message) -> None:
    asyncio.run(db.insert_message(msg))


class TestCLIAnalytics:
    def test_top_empty(self, cli_env, capsys):
        from tg_agent.cli.commands.analytics import run

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

        from tg_agent.cli.commands.analytics import run

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

        from tg_agent.cli.commands.analytics import run

        run(_ns(analytics_action="top", limit=2, date_from=None, date_to=None))
        out = capsys.readouterr().out
        assert "msg0005" in out   # top post (count=5)
        assert "msg0001" not in out  # lowest post not in top 2

    def test_content_types_empty(self, cli_env, capsys):
        from tg_agent.cli.commands.analytics import run

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

        from tg_agent.cli.commands.analytics import run

        run(_ns(analytics_action="content-types", date_from=None, date_to=None))
        out = capsys.readouterr().out
        assert "photo" in out
        assert "text" in out

    def test_hourly_empty(self, cli_env, capsys):
        from tg_agent.cli.commands.analytics import run

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

        from tg_agent.cli.commands.analytics import run

        run(_ns(analytics_action="hourly", date_from=None, date_to=None))
        out = capsys.readouterr().out
        assert "09:00" in out

    def test_authority_report_e2e(self, cli_env, capsys):
        """E2E по :memory:-БД: скуп/эхо в dry-run отчёте (#1748).

        Один run() на cli_env: каждый *_impl закрывает свою БД в finally,
        разделять одну cli_db между прогонами нельзя.
        """
        _add_channel(cli_env, channel_id=710, title="ScoopCh")
        _add_channel(cli_env, channel_id=711, title="EchoCh")
        now = datetime.now(timezone.utc)
        story = (
            "Anthropic выпустила Claude 5.2 с режимом длинного контекста: "
            "модель держит весь репозиторий в памяти."
        )
        _insert_message(
            cli_env,
            Message(channel_id=710, message_id=1, text=story, date=now - timedelta(hours=3)),
        )
        _insert_message(
            cli_env,
            Message(
                channel_id=711,
                message_id=1,
                text=f"⚡ Коротко: {story} Подробности у первоисточника.",
                date=now - timedelta(hours=1),
            ),
        )

        from tg_agent.cli.commands.analytics import run

        run(_ns(analytics_action="authority", days=30, apply=False, json=True, limit=100))
        payload = json.loads(capsys.readouterr().out)
        assert payload["posts"] == 2 and payload["channels"] == 2 and payload["applied"] == 0
        rows = {r["channel_id"]: r for r in payload["rows"]}
        assert rows[710]["scoop_rate"] == 1.0 and rows[710]["score"] == 1.0
        assert rows[711]["scoop_rate"] == 0.0
        assert rows[710]["tier"] is None  # volume < MIN_POSTS_FOR_TIER — dry-run без тиров

    def test_authority_apply_e2e(self, cli_env, capsys):
        """E2E: --apply на пригодных каналах пишет ровно два тега тиров (#1748).

        10 попарно непохожих новостей, эхо-канал печатает каждую позже —
        скуп-канал собирает все 11 кластеров, эхо — ни одного.
        """
        _add_channel(cli_env, channel_id=720, title="FastCh")
        _add_channel(cli_env, channel_id=721, title="LateCh")
        now = datetime.now(timezone.utc)
        story = (
            "Anthropic выпустила Claude 5.2 с режимом длинного контекста: "
            "модель держит весь репозиторий в памяти."
        )
        fillers = [
            "Postgres 18 ускорил чтение таблиц в полтора раза на обычных дисках.",
            "Rust 1.90 стабилизировал async-генераторы и ускорил компиляцию проектов.",
            "Linux 6.14 включил планировщик EEVDF по умолчанию на десктопах.",
            "Python 3.14 ускорил интерпретатор на пятнадцать процентов в бенчмарках.",
            "Meta открыла веса Llama 6 с контекстом в два миллиона токенов.",
            "Apple добавила в Xcode локального агента для правок по горячей клавише.",
            "Microsoft перевела Azure на жидкостное охлаждение новых стоек.",
            "Kubernetes 1.35 упростил kubectl apply и почистил дубли конфигов.",
            "Telegram открыл API тем каналов без ботов с лимитом запросов.",
            "Google выпустила Gemini 4 для локального запуска весом в двенадцать гигабайт.",
        ]
        _insert_message(
            cli_env,
            Message(channel_id=720, message_id=1, text=story, date=now - timedelta(hours=6)),
        )
        _insert_message(
            cli_env,
            Message(
                channel_id=721,
                message_id=1,
                text=f"⚡ Коротко: {story} Подробности у первоисточника.",
                date=now - timedelta(hours=5),
            ),
        )
        for i, filler in enumerate(fillers):
            _insert_message(
                cli_env,
                Message(
                    channel_id=720, message_id=10 + i, text=filler,
                    date=now - timedelta(hours=2 * (i + 1)),
                ),
            )
            _insert_message(
                cli_env,
                Message(
                    channel_id=721, message_id=10 + i, text=filler,
                    date=now - timedelta(hours=2 * (i + 1) - 1),
                ),
            )

        from tg_agent.cli.commands.analytics import run

        run(_ns(analytics_action="authority", days=30, apply=True, json=False, limit=100))
        out = capsys.readouterr().out
        # 720 напечатал все 11 кластеров первым → тиры по терцилям: A против C
        assert "tiers A/B/C=1/0/1" in out
        assert "tags_written=2 (applied)" in out
        assert "  A" in out and "720" in out and "721" in out
