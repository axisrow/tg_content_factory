from __future__ import annotations

import argparse
import asyncio
from unittest.mock import patch

from src.config import AppConfig
from src.database import Database
from src.models import Account, Channel


def _ns(**kwargs) -> argparse.Namespace:
    defaults = {"config": "config.yaml"}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def _add_pipeline_prereqs(db: Database) -> None:
    asyncio.run(db.add_account(Account(phone="+100", session_string="sess")))
    asyncio.run(db.add_channel(Channel(channel_id=1001, title="Source A")))
    asyncio.run(
        db.repos.dialog_cache.replace_dialogs(
            "+100",
            [
                {
                    "channel_id": 77,
                    "title": "Target A",
                    "username": "targeta",
                    "channel_type": "channel",
                }
            ],
        )
    )


def test_pipeline_add_and_list(tmp_path, capsys):
    db_path = str(tmp_path / "cli_pipeline.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _add_pipeline_prereqs(db)
    asyncio.run(db.close())

    async def fake_init_db(_config_path: str):
        config = AppConfig()
        database = Database(db_path)
        await database.initialize()
        return config, database

    with patch("src.cli.runtime.init_db", side_effect=fake_init_db):
        from src.cli.commands.pipeline import run

        run(
            _ns(
                pipeline_action="add",
                name="Digest",
                prompt_template="Summarize {source_messages}",
                source=[1001],
                target=["+100|77"],
                llm_model=None,
                image_model=None,
                publish_mode="moderated",
                generation_backend="chain",
                interval=60,
                inactive=False,
            )
        )
        run(_ns(pipeline_action="list"))

    out = capsys.readouterr().out
    assert "Added pipeline id=" in out
    assert "Digest" in out


def test_pipeline_show_not_found(tmp_path, capsys):
    db_path = str(tmp_path / "cli_pipeline_not_found.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    asyncio.run(db.close())

    async def fake_init_db(_config_path: str):
        config = AppConfig()
        database = Database(db_path)
        await database.initialize()
        return config, database

    with patch("src.cli.runtime.init_db", side_effect=fake_init_db):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="show", id=999))

    out = capsys.readouterr().out
    assert "not found" in out
