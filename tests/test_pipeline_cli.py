from __future__ import annotations

import argparse
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from src.database import Database
from src.models import (
    Account,
    Channel,
    ContentPipeline,
    GenerationRun,
    PipelineGenerationBackend,
    PipelinePublishMode,
    PipelineTarget,
)
from src.services.publish_service import PublishResult

pytestmark = pytest.mark.aiosqlite_serial

_PIPELINE_INIT_DB_TARGETS = ("src.cli.runtime.init_db", "src.cli.commands.pipeline.runtime.init_db")


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


def test_pipeline_add_and_list(tmp_path, cli_init_patch, capsys):
    db_path = str(tmp_path / "cli_pipeline.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _add_pipeline_prereqs(db)

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
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


def test_pipeline_show_not_found(tmp_path, cli_init_patch, capsys):
    db_path = str(tmp_path / "cli_pipeline_not_found.db")
    db = Database(db_path)
    asyncio.run(db.initialize())

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="show", id=999))

    out = capsys.readouterr().out
    assert "not found" in out


def test_pipeline_queue_approve_reject_and_publish(tmp_path, cli_init_patch, capsys):
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
        cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS),
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


def test_pipeline_publish_not_found(tmp_path, cli_init_patch, capsys):
    db_path = str(tmp_path / "cli_pipeline_publish_not_found.db")
    db = Database(db_path)
    asyncio.run(db.initialize())

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="publish", run_id=999))

    out = capsys.readouterr().out
    assert "Run id=999 not found" in out


def test_pipeline_generate_prints_draft_preview(tmp_path, cli_init_patch, capsys):
    db_path = str(tmp_path / "cli_pipeline_generate.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _add_pipeline_prereqs(db)
    pipeline_id = asyncio.run(
        db.repos.content_pipelines.add(
            pipeline=ContentPipeline(
                name="Digest",
                prompt_template="Summarize {source_messages}",
                publish_mode=PipelinePublishMode.MODERATED,
                generation_backend=PipelineGenerationBackend.CHAIN,
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

    class FakeContentGenerationService:
        def __init__(self, db, engine, agent_manager=None, **kwargs):
            self.db = db
            self.engine = engine
            self.agent_manager = agent_manager
            self.quality_service = kwargs.get("quality_service")

        async def generate(self, pipeline, model=None, max_tokens=512, temperature=0.7):
            return GenerationRun(
                id=123,
                pipeline_id=pipeline.id,
                status="completed",
                generated_text="Generated draft for CLI",
                moderation_status="pending",
            )

    with (
        cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS),
        patch("src.cli.commands.pipeline.ContentGenerationService", FakeContentGenerationService),
    ):
        from src.cli.commands.pipeline import run

        run(
            _ns(
                pipeline_action="generate",
                id=pipeline_id,
                model=None,
                max_tokens=512,
                temperature=0.7,
                preview=False,
            )
        )

    out = capsys.readouterr().out
    assert "Created generation run id=123" in out
    assert "--- DRAFT PREVIEW ---" in out
    assert "Generated draft for CLI" in out


def test_pipeline_generate_wires_agent_manager_for_deep_agents(tmp_path, cli_init_patch, capsys):
    db_path = str(tmp_path / "cli_pipeline_generate_deep_agents.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _add_pipeline_prereqs(db)
    pipeline_id = asyncio.run(
        db.repos.content_pipelines.add(
            pipeline=ContentPipeline(
                name="Digest",
                prompt_template="Summarize {source_messages}",
                publish_mode=PipelinePublishMode.MODERATED,
                generation_backend=PipelineGenerationBackend.DEEP_AGENTS,
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

    captured = {}

    class FakeAgentManager:
        def __init__(self, db, config):
            captured["db"] = db
            captured["config"] = config

    class FakeContentGenerationService:
        def __init__(self, db, engine, agent_manager=None, **kwargs):
            captured["agent_manager"] = agent_manager
            captured["quality_service"] = kwargs.get("quality_service")

        async def generate(self, pipeline, model=None, max_tokens=512, temperature=0.7):
            return GenerationRun(
                id=124,
                pipeline_id=pipeline.id,
                status="completed",
                generated_text="Generated draft with agent",
                moderation_status="pending",
            )

    with (
        cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS),
        patch("src.agent.manager.AgentManager", FakeAgentManager),
        patch("src.cli.commands.pipeline.ContentGenerationService", FakeContentGenerationService),
    ):
        from src.cli.commands.pipeline import run

        run(
            _ns(
                pipeline_action="generate",
                id=pipeline_id,
                model=None,
                max_tokens=512,
                temperature=0.7,
                preview=False,
            )
        )

    assert captured["agent_manager"] is not None
    assert captured["quality_service"] is not None
    out = capsys.readouterr().out
    assert "Created generation run id=124" in out


def test_pipeline_runs_and_run_show(tmp_path, cli_init_patch, capsys):
    db_path = str(tmp_path / "cli_pipeline_runs.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _add_pipeline_prereqs(db)
    pipeline_id = asyncio.run(
        db.repos.content_pipelines.add(
            pipeline=ContentPipeline(
                name="Digest",
                prompt_template="Summarize {source_messages}",
                publish_mode=PipelinePublishMode.MODERATED,
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

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="runs", id=pipeline_id, limit=20, status=None))
        run(_ns(pipeline_action="run-show", run_id=run_id))

    out = capsys.readouterr().out
    assert f"{run_id}" in out
    assert "completed" in out
    assert "--- GENERATED TEXT ---" in out
    assert "Generated draft for CLI" in out


def test_pipeline_show_found(tmp_path, cli_init_patch, capsys):
    """Test show action with existing pipeline."""
    db_path = str(tmp_path / "cli_pipeline_show.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _add_pipeline_prereqs(db)
    pipeline_id = asyncio.run(
        db.repos.content_pipelines.add(
            pipeline=ContentPipeline(
                name="Show Pipeline",
                prompt_template="Test {context}",
                publish_mode=PipelinePublishMode.MODERATED,
                generation_backend=PipelineGenerationBackend.CHAIN,
                generate_interval_minutes=30,
                is_active=True,
                llm_model="gpt-4",
            ),
            source_channel_ids=[1001],
            targets=[
                PipelineTarget(
                    pipeline_id=0,
                    phone="+100",
                    dialog_id=77,
                    title="Target Channel",
                    dialog_type="channel",
                )
            ],
        )
    )

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="show", id=pipeline_id))

    out = capsys.readouterr().out
    assert f"id={pipeline_id}" in out
    assert "name=Show Pipeline" in out
    assert "backend=chain" in out
    assert "publish_mode=moderated" in out


def test_pipeline_edit_found(tmp_path, cli_init_patch, capsys):
    """Test edit action with existing pipeline."""
    db_path = str(tmp_path / "cli_pipeline_edit.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _add_pipeline_prereqs(db)
    pipeline_id = asyncio.run(
        db.repos.content_pipelines.add(
            pipeline=ContentPipeline(
                name="Original Name",
                prompt_template="Original prompt",
                publish_mode=PipelinePublishMode.MODERATED,
            ),
            source_channel_ids=[1001],
            targets=[
                PipelineTarget(
                    pipeline_id=0,
                    phone="+100",
                    dialog_id=77,
                    title="Target",
                    dialog_type="channel",
                )
            ],
        )
    )

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(
            _ns(
                pipeline_action="edit",
                id=pipeline_id,
                name="Edited Name",
                prompt_template="Edited prompt",
                source=None,
                target=None,
                llm_model=None,
                image_model=None,
                publish_mode=None,
                generation_backend=None,
                interval=None,
                active=None,
            )
        )

    out = capsys.readouterr().out
    assert f"Updated pipeline id={pipeline_id}" in out


def test_pipeline_edit_not_found(tmp_path, cli_init_patch, capsys):
    """Test edit action with non-existent pipeline."""
    db_path = str(tmp_path / "cli_pipeline_edit_nf.db")
    db = Database(db_path)
    asyncio.run(db.initialize())

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(
            _ns(
                pipeline_action="edit",
                id=999,
                name="Name",
                prompt_template="prompt",
                source=None,
                target=None,
                llm_model=None,
                image_model=None,
                publish_mode=None,
                generation_backend=None,
                interval=None,
                active=None,
            )
        )

    out = capsys.readouterr().out
    assert "not found" in out


def test_pipeline_toggle_found(tmp_path, cli_init_patch, capsys):
    """Test toggle action with existing pipeline."""
    db_path = str(tmp_path / "cli_pipeline_toggle.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _add_pipeline_prereqs(db)
    pipeline_id = asyncio.run(
        db.repos.content_pipelines.add(
            pipeline=ContentPipeline(
                name="Toggle Pipeline",
                prompt_template="Test",
                publish_mode=PipelinePublishMode.MODERATED,
                is_active=True,
            ),
            source_channel_ids=[1001],
            targets=[
                PipelineTarget(
                    pipeline_id=0,
                    phone="+100",
                    dialog_id=77,
                    title="Target",
                    dialog_type="channel",
                )
            ],
        )
    )

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="toggle", id=pipeline_id))

    out = capsys.readouterr().out
    assert f"Toggled pipeline id={pipeline_id}" in out


def test_pipeline_toggle_not_found(tmp_path, cli_init_patch, capsys):
    """Test toggle action with non-existent pipeline."""
    db_path = str(tmp_path / "cli_pipeline_toggle_nf.db")
    db = Database(db_path)
    asyncio.run(db.initialize())

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="toggle", id=999))

    out = capsys.readouterr().out
    assert "not found" in out


def test_pipeline_delete(tmp_path, cli_init_patch, capsys):
    """Test delete action."""
    db_path = str(tmp_path / "cli_pipeline_delete.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _add_pipeline_prereqs(db)
    pipeline_id = asyncio.run(
        db.repos.content_pipelines.add(
            pipeline=ContentPipeline(
                name="Delete Pipeline",
                prompt_template="Test",
                publish_mode=PipelinePublishMode.MODERATED,
            ),
            source_channel_ids=[1001],
            targets=[
                PipelineTarget(
                    pipeline_id=0,
                    phone="+100",
                    dialog_id=77,
                    title="Target",
                    dialog_type="channel",
                )
            ],
        )
    )

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="delete", id=pipeline_id))

    out = capsys.readouterr().out
    assert f"Deleted pipeline id={pipeline_id}" in out


def test_pipeline_run_with_preview(tmp_path, cli_init_patch, capsys):
    """Test run action with preview flag."""
    db_path = str(tmp_path / "cli_pipeline_run_preview.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _add_pipeline_prereqs(db)
    pipeline_id = asyncio.run(
        db.repos.content_pipelines.add(
            pipeline=ContentPipeline(
                name="Run Pipeline",
                prompt_template="Summarize {context}",
                publish_mode=PipelinePublishMode.MODERATED,
                llm_model="gpt-4",
            ),
            source_channel_ids=[1001],
            targets=[
                PipelineTarget(
                    pipeline_id=0,
                    phone="+100",
                    dialog_id=77,
                    title="Target",
                    dialog_type="channel",
                )
            ],
        )
    )

    class FakeGenerationService:
        def __init__(self, engine, provider_callable=None):
            pass

        async def generate(self, **kwargs):
            return {"generated_text": "Preview content here", "citations": []}

    with (
        cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS),
        patch("src.cli.commands.pipeline.GenerationService", FakeGenerationService),
    ):
        from src.cli.commands.pipeline import run

        run(
            _ns(
                pipeline_action="run",
                id=pipeline_id,
                limit=10,
                max_tokens=256,
                temperature=0.7,
                preview=True,
                publish=False,
            )
        )

    out = capsys.readouterr().out
    assert "Generation completed" in out
    assert "--- DRAFT PREVIEW ---" in out
    assert "Preview content here" in out


def test_pipeline_run_not_found(tmp_path, cli_init_patch, capsys):
    """Test run action with non-existent pipeline."""
    db_path = str(tmp_path / "cli_pipeline_run_nf.db")
    db = Database(db_path)
    asyncio.run(db.initialize())

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="run", id=999, limit=10, max_tokens=256, temperature=0.7, preview=False, publish=False))

    out = capsys.readouterr().out
    assert "not found" in out


def test_pipeline_runs_with_status_filter(tmp_path, cli_init_patch, capsys):
    """Test runs action with status filter."""
    db_path = str(tmp_path / "cli_pipeline_runs_filter.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _add_pipeline_prereqs(db)
    pipeline_id = asyncio.run(
        db.repos.content_pipelines.add(
            pipeline=ContentPipeline(
                name="Runs Filter Pipeline",
                prompt_template="Test",
                publish_mode=PipelinePublishMode.MODERATED,
            ),
            source_channel_ids=[1001],
            targets=[
                PipelineTarget(
                    pipeline_id=0,
                    phone="+100",
                    dialog_id=77,
                    title="Target",
                    dialog_type="channel",
                )
            ],
        )
    )
    run_id = asyncio.run(db.repos.generation_runs.create_run(pipeline_id, "prompt"))
    asyncio.run(db.repos.generation_runs.save_result(run_id, "Test output"))

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="runs", id=pipeline_id, limit=20, status="completed"))

    out = capsys.readouterr().out
    assert f"{run_id}" in out
    assert "completed" in out


def test_pipeline_runs_not_found(tmp_path, cli_init_patch, capsys):
    """Test runs action with non-existent pipeline."""
    db_path = str(tmp_path / "cli_pipeline_runs_nf.db")
    db = Database(db_path)
    asyncio.run(db.initialize())

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="runs", id=999, limit=20, status=None))

    out = capsys.readouterr().out
    assert "not found" in out


def test_pipeline_queue_empty(tmp_path, cli_init_patch, capsys):
    """Test queue action when no pending runs."""
    db_path = str(tmp_path / "cli_pipeline_queue_empty.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _add_pipeline_prereqs(db)
    pipeline_id = asyncio.run(
        db.repos.content_pipelines.add(
            pipeline=ContentPipeline(
                name="Queue Empty Pipeline",
                prompt_template="Test",
                publish_mode=PipelinePublishMode.MODERATED,
            ),
            source_channel_ids=[1001],
            targets=[
                PipelineTarget(
                    pipeline_id=0,
                    phone="+100",
                    dialog_id=77,
                    title="Target",
                    dialog_type="channel",
                )
            ],
        )
    )

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="queue", id=pipeline_id, limit=20))

    out = capsys.readouterr().out
    assert "No pending moderation runs" in out


def test_pipeline_queue_not_found(tmp_path, cli_init_patch, capsys):
    """Test queue action with non-existent pipeline."""
    db_path = str(tmp_path / "cli_pipeline_queue_nf.db")
    db = Database(db_path)
    asyncio.run(db.initialize())

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="queue", id=999, limit=20))

    out = capsys.readouterr().out
    assert "not found" in out


def test_pipeline_publish_no_clients(tmp_path, cli_init_patch, capsys):
    """Test publish action when no Telegram clients available."""
    db_path = str(tmp_path / "cli_pipeline_publish_no_clients.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _add_pipeline_prereqs(db)
    pipeline_id = asyncio.run(
        db.repos.content_pipelines.add(
            pipeline=ContentPipeline(
                name="Publish Pipeline",
                prompt_template="Test",
                publish_mode=PipelinePublishMode.MODERATED,
            ),
            source_channel_ids=[1001],
            targets=[
                PipelineTarget(
                    pipeline_id=0,
                    phone="+100",
                    dialog_id=77,
                    title="Target",
                    dialog_type="channel",
                )
            ],
        )
    )
    run_id = asyncio.run(db.repos.generation_runs.create_run(pipeline_id, "prompt"))
    asyncio.run(db.repos.generation_runs.save_result(run_id, "Generated text"))

    async def fake_init_pool(_config, _db):
        pool = MagicMock()
        pool.clients = {}  # No clients available
        pool.disconnect_all = AsyncMock()
        return MagicMock(), pool

    with (
        cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS),
        patch("src.cli.commands.pipeline.runtime.init_pool", side_effect=fake_init_pool),
    ):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="publish", run_id=run_id))

    out = capsys.readouterr().out
    assert "Нет доступных аккаунтов" in out


def test_pipeline_run_show_not_found(tmp_path, cli_init_patch, capsys):
    """Test run-show action with non-existent run."""
    db_path = str(tmp_path / "cli_pipeline_runshow_nf.db")
    db = Database(db_path)
    asyncio.run(db.initialize())

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="run-show", run_id=999))

    out = capsys.readouterr().out
    assert "not found" in out


def test_pipeline_approve_not_found(tmp_path, cli_init_patch, capsys):
    """Test approve action with non-existent run."""
    db_path = str(tmp_path / "cli_pipeline_approve_nf.db")
    db = Database(db_path)
    asyncio.run(db.initialize())

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="approve", run_id=999))

    out = capsys.readouterr().out
    assert "not found" in out


def test_pipeline_reject_not_found(tmp_path, cli_init_patch, capsys):
    """Test reject action with non-existent run."""
    db_path = str(tmp_path / "cli_pipeline_reject_nf.db")
    db = Database(db_path)
    asyncio.run(db.initialize())

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="reject", run_id=999))

    out = capsys.readouterr().out
    assert "not found" in out


def test_pipeline_generate_not_found(tmp_path, cli_init_patch, capsys):
    """Test generate action with non-existent pipeline."""
    db_path = str(tmp_path / "cli_pipeline_generate_nf.db")
    db = Database(db_path)
    asyncio.run(db.initialize())

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="generate", id=999, model=None, max_tokens=256, temperature=0.7))

    out = capsys.readouterr().out
    assert "not found" in out


def test_pipeline_generate_exception(tmp_path, cli_init_patch, capsys):
    """Test generate action handles exception."""
    db_path = str(tmp_path / "cli_pipeline_generate_exc.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _add_pipeline_prereqs(db)
    pipeline_id = asyncio.run(
        db.repos.content_pipelines.add(
            pipeline=ContentPipeline(
                name="Generate Exc Pipeline",
                prompt_template="Test",
                publish_mode=PipelinePublishMode.MODERATED,
            ),
            source_channel_ids=[1001],
            targets=[
                PipelineTarget(
                    pipeline_id=0,
                    phone="+100",
                    dialog_id=77,
                    title="Target",
                    dialog_type="channel",
                )
            ],
        )
    )

    class FakeContentGenerationService:
        def __init__(self, *args, **kwargs):
            pass

        async def generate(self, **kwargs):
            raise RuntimeError("Generation failed")

    with (
        cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS),
        patch("src.cli.commands.pipeline.ContentGenerationService", FakeContentGenerationService),
    ):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="generate", id=pipeline_id, model=None, max_tokens=256, temperature=0.7))

    out = capsys.readouterr().out
    assert "Generation failed" in out


def test_pipeline_list_empty(tmp_path, cli_init_patch, capsys):
    """Test list action when no pipelines exist."""
    db_path = str(tmp_path / "cli_pipeline_list_empty.db")
    db = Database(db_path)
    asyncio.run(db.initialize())

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="list"))

    out = capsys.readouterr().out
    assert "No pipelines found" in out
