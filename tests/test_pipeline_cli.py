from __future__ import annotations

import argparse
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.publish_service import PublishResult

from src.config import AppConfig
from src.database import Database
from src.models import Account, Channel, ContentPipeline, PipelineTarget


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


def test_pipeline_queue_approve_reject_and_publish(tmp_path, capsys):
    db_path = str(tmp_path / "cli_pipeline_actions.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _add_pipeline_prereqs(db)
    pipeline_id = asyncio.run(
        db.repos.content_pipelines.add(
            pipeline=ContentPipeline(
                name="Digest",
                prompt_template="Summarize {source_messages}",
                publish_mode="moderated",
            ),
            source_channel_ids=[1001],
            targets=[
                PipelineTarget(
                    pipeline_id=0,
                    phone="+100",
                    dialog_id=77,
                    title="Target A",
                    dialog_type="channel",
                )
            ],
        )
    )
    run_id = asyncio.run(db.repos.generation_runs.create_run(pipeline_id, "prompt"))
    asyncio.run(db.repos.generation_runs.save_result(run_id, "Generated draft for CLI"))
    asyncio.run(db.close())

    async def fake_init_db(_config_path: str):
        config = AppConfig()
        database = Database(db_path)
        await database.initialize()
        return config, database

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.clients = {"+100": object()}
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    class FakePublishService:
        def __init__(self, db, pool):
            self.db = db
            self.pool = pool

        async def publish_run(self, run, pipeline):
            return [PublishResult(success=True, message_id=123)]

    with (
        patch("src.cli.commands.pipeline.runtime.init_db", side_effect=fake_init_db),
        patch("src.cli.commands.pipeline.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.pipeline.PublishService", FakePublishService),
    ):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="queue", id=pipeline_id, limit=20))
        run(_ns(pipeline_action="approve", run_id=run_id))
        run(_ns(pipeline_action="publish", run_id=run_id))
        run(_ns(pipeline_action="reject", run_id=run_id))

    out = capsys.readouterr().out
    assert f"{run_id}" in out
    assert f"Approved run id={run_id}" in out
    assert f"Published run id={run_id} to 1 target(s)" in out
    assert f"Rejected run id={run_id}" in out

    verify_db = Database(db_path)
    asyncio.run(verify_db.initialize())
    run_after = asyncio.run(verify_db.repos.generation_runs.get(run_id))
    asyncio.run(verify_db.close())
    assert run_after is not None
    assert run_after.moderation_status == "rejected"


def test_pipeline_publish_not_found(tmp_path, capsys):
    db_path = str(tmp_path / "cli_pipeline_publish_not_found.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    asyncio.run(db.close())

    async def fake_init_db(_config_path: str):
        config = AppConfig()
        database = Database(db_path)
        await database.initialize()
        return config, database

    with patch("src.cli.commands.pipeline.runtime.init_db", side_effect=fake_init_db):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="publish", run_id=999))

    out = capsys.readouterr().out
    assert "Run id=999 not found" in out
