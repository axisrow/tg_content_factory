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
        patch("src.services.provider_service.AgentProviderService.has_providers", return_value=True),
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
        patch("src.services.provider_service.AgentProviderService.has_providers", return_value=True),
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
        patch("src.services.provider_service.AgentProviderService.has_providers", return_value=True),
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
        patch("src.services.provider_service.AgentProviderService.has_providers", return_value=True),
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


# ---------------------------------------------------------------------------
# DAG mode tests (issue #426)
# ---------------------------------------------------------------------------


def test_pipeline_add_dag_with_node(tmp_path, cli_init_patch, capsys):
    """DAG mode: pipeline add with --node creates a DAG pipeline."""
    db_path = str(tmp_path / "cli_pipeline_dag_add.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _add_pipeline_prereqs(db)

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(
            _ns(
                pipeline_action="add",
                name="React Pipeline",
                prompt_template=None,
                source=[1001],
                target=["+100|77"],
                llm_model=None,
                image_model=None,
                publish_mode="moderated",
                generation_backend="chain",
                interval=60,
                inactive=False,
                node_specs=["react:emoji=heart"],
                edge=None,
                node_configs=None,
            )
        )

    out = capsys.readouterr().out
    assert "Added pipeline id=" in out
    assert "React Pipeline" in out

    # Verify graph was stored
    verify_db = Database(db_path)
    asyncio.run(verify_db.initialize())
    pipelines = asyncio.run(verify_db.repos.content_pipelines.get_all())
    asyncio.run(verify_db.close())
    assert len(pipelines) == 1
    p = pipelines[0]
    assert p.pipeline_json is not None
    types = [n.type.value for n in p.pipeline_json.nodes]
    assert "source" in types
    assert "fetch_messages" in types
    assert "react" in types
    assert "publish" in types


def test_pipeline_add_dag_invalid_spec(tmp_path, cli_init_patch, capsys):
    """DAG mode: invalid node spec prints error."""
    db_path = str(tmp_path / "cli_pipeline_dag_invalid.db")
    db = Database(db_path)
    asyncio.run(db.initialize())

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(
            _ns(
                pipeline_action="add",
                name="Bad",
                prompt_template=None,
                source=[1001],
                target=None,
                llm_model=None,
                image_model=None,
                publish_mode="moderated",
                generation_backend="chain",
                interval=60,
                inactive=False,
                node_specs=["nonexistent_type:x=1"],
                edge=None,
                node_configs=None,
            )
        )

    out = capsys.readouterr().out
    assert "Invalid node spec" in out


def test_pipeline_add_legacy_requires_prompt_template(tmp_path, cli_init_patch, capsys):
    """Legacy mode without --prompt-template prints error."""
    db_path = str(tmp_path / "cli_pipeline_legacy_no_prompt.db")
    db = Database(db_path)
    asyncio.run(db.initialize())

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(
            _ns(
                pipeline_action="add",
                name="NoPrompt",
                prompt_template=None,
                source=[1001],
                target=["+100|77"],
                llm_model=None,
                image_model=None,
                publish_mode="moderated",
                generation_backend="chain",
                interval=60,
                inactive=False,
                node_specs=None,
                edge=None,
                node_configs=None,
            )
        )

    out = capsys.readouterr().out
    assert "--prompt-template is required" in out


def test_pipeline_add_dag_with_edges(tmp_path, cli_init_patch, capsys):
    """DAG mode: --edge adds explicit edges on top of linear chain."""
    db_path = str(tmp_path / "cli_pipeline_dag_edges.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _add_pipeline_prereqs(db)

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(
            _ns(
                pipeline_action="add",
                name="EdgeTest",
                prompt_template=None,
                source=[1001],
                target=["+100|77"],
                llm_model=None,
                image_model=None,
                publish_mode="moderated",
                generation_backend="chain",
                interval=60,
                inactive=False,
                node_specs=["delay:id=a", "delay:id=b", "delay:id=c"],
                edge=["a->c"],
                node_configs=None,
            )
        )

    out = capsys.readouterr().out
    assert "Added pipeline id=" in out

    verify_db = Database(db_path)
    asyncio.run(verify_db.initialize())
    pipelines = asyncio.run(verify_db.repos.content_pipelines.get_all())
    asyncio.run(verify_db.close())
    p = pipelines[0]
    edge_set = {(e.from_node, e.to_node) for e in p.pipeline_json.edges}
    assert ("a", "c") in edge_set


def test_pipeline_graph_cmd(tmp_path, cli_init_patch, capsys):
    """pipeline graph shows ASCII visualization."""
    db_path = str(tmp_path / "cli_pipeline_graph_cmd.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _add_pipeline_prereqs(db)

    from src.models import PipelineEdge, PipelineGraph, PipelineNode, PipelineNodeType

    pid = asyncio.run(
        db.repos.content_pipelines.add(
            pipeline=ContentPipeline(
                name="GraphTest",
                prompt_template=".",
                publish_mode="moderated",
            ),
            source_channel_ids=[1001],
            targets=[],
        )
    )
    graph = PipelineGraph(
        nodes=[
            PipelineNode(id="s", type=PipelineNodeType.SOURCE, name="src"),
            PipelineNode(id="f", type=PipelineNodeType.FETCH_MESSAGES, name="fetch"),
        ],
        edges=[PipelineEdge(from_node="s", to_node="f")],
    )
    asyncio.run(db.repos.content_pipelines.set_pipeline_json(pid, graph))

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="graph", id=pid))

    out = capsys.readouterr().out
    assert "source" in out
    assert "fetch_messages" in out


def test_pipeline_graph_legacy_pipeline(tmp_path, cli_init_patch, capsys):
    """pipeline graph on legacy pipeline shows 'has no graph' message."""
    db_path = str(tmp_path / "cli_pipeline_graph_legacy.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _add_pipeline_prereqs(db)
    pipeline_id = asyncio.run(
        db.repos.content_pipelines.add(
            pipeline=ContentPipeline(
                name="Legacy",
                prompt_template="Test",
                publish_mode="moderated",
            ),
            source_channel_ids=[1001],
            targets=[
                PipelineTarget(
                    pipeline_id=0, phone="+100", dialog_id=77, title="T", dialog_type="channel"
                )
            ],
        )
    )

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="graph", id=pipeline_id))

    out = capsys.readouterr().out
    assert "has no graph" in out


def test_pipeline_node_add(tmp_path, cli_init_patch, capsys):
    """pipeline node add adds a node to existing graph."""
    db_path = str(tmp_path / "cli_pipeline_node_add.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _add_pipeline_prereqs(db)

    from src.models import PipelineGraph, PipelineNode, PipelineNodeType

    pid = asyncio.run(
        db.repos.content_pipelines.add(
            pipeline=ContentPipeline(name="NodeAdd", prompt_template=".", publish_mode="moderated"),
            source_channel_ids=[1001],
            targets=[],
        )
    )
    graph = PipelineGraph(
        nodes=[PipelineNode(id="src", type=PipelineNodeType.SOURCE, name="src")],
        edges=[],
    )
    asyncio.run(db.repos.content_pipelines.set_pipeline_json(pid, graph))

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="node", node_action="add", pipeline_id=pid, node_spec="delay:min_seconds=1"))

    out = capsys.readouterr().out
    assert "Added node" in out

    verify_db = Database(db_path)
    asyncio.run(verify_db.initialize())
    p = asyncio.run(verify_db.repos.content_pipelines.get_by_id(pid))
    asyncio.run(verify_db.close())
    assert len(p.pipeline_json.nodes) == 2


def test_pipeline_node_remove(tmp_path, cli_init_patch, capsys):
    """pipeline node remove removes a node and its edges."""
    db_path = str(tmp_path / "cli_pipeline_node_rm.db")
    db = Database(db_path)
    asyncio.run(db.initialize())

    from src.models import PipelineEdge, PipelineGraph, PipelineNode, PipelineNodeType

    pid = asyncio.run(
        db.repos.content_pipelines.add(
            pipeline=ContentPipeline(name="NodeRm", prompt_template=".", publish_mode="moderated"),
            source_channel_ids=[],
            targets=[],
        )
    )
    graph = PipelineGraph(
        nodes=[
            PipelineNode(id="a", type=PipelineNodeType.DELAY, name="d1"),
            PipelineNode(id="b", type=PipelineNodeType.DELAY, name="d2"),
        ],
        edges=[PipelineEdge(from_node="a", to_node="b")],
    )
    asyncio.run(db.repos.content_pipelines.set_pipeline_json(pid, graph))

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="node", node_action="remove", pipeline_id=pid, node_id="b"))

    out = capsys.readouterr().out
    assert "Removed node" in out


def test_pipeline_node_replace(tmp_path, cli_init_patch, capsys):
    """pipeline node replace swaps a node's type and config."""
    db_path = str(tmp_path / "cli_pipeline_node_replace.db")
    db = Database(db_path)
    asyncio.run(db.initialize())

    from src.models import PipelineGraph, PipelineNode, PipelineNodeType

    pid = asyncio.run(
        db.repos.content_pipelines.add(
            pipeline=ContentPipeline(name="NodeReplace", prompt_template=".", publish_mode="moderated"),
            source_channel_ids=[],
            targets=[],
        )
    )
    graph = PipelineGraph(
        nodes=[
            PipelineNode(id="gen", type=PipelineNodeType.LLM_GENERATE, name="gen", config={"model": "claude"}),
            PipelineNode(id="pub", type=PipelineNodeType.PUBLISH, name="pub"),
        ],
        edges=[],
    )
    asyncio.run(db.repos.content_pipelines.set_pipeline_json(pid, graph))

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(
            _ns(
                pipeline_action="node",
                node_action="replace",
                pipeline_id=pid,
                node_id="gen",
                node_spec="agent_loop:max_steps=5",
            )
        )

    out = capsys.readouterr().out
    assert "Replaced node" in out

    verify_db = Database(db_path)
    asyncio.run(verify_db.initialize())
    p = asyncio.run(verify_db.repos.content_pipelines.get_by_id(pid))
    asyncio.run(verify_db.close())
    replaced = next(n for n in p.pipeline_json.nodes if n.id == "gen")
    assert replaced.type == PipelineNodeType.AGENT_LOOP
    assert replaced.config["max_steps"] == 5


def test_pipeline_edge_add_remove(tmp_path, cli_init_patch, capsys):
    """pipeline edge add and remove work correctly."""
    db_path = str(tmp_path / "cli_pipeline_edge.db")
    db = Database(db_path)
    asyncio.run(db.initialize())

    from src.models import PipelineGraph, PipelineNode, PipelineNodeType

    pid = asyncio.run(
        db.repos.content_pipelines.add(
            pipeline=ContentPipeline(name="EdgeTest", prompt_template=".", publish_mode="moderated"),
            source_channel_ids=[],
            targets=[],
        )
    )
    graph = PipelineGraph(
        nodes=[
            PipelineNode(id="a", type=PipelineNodeType.DELAY, name="d1"),
            PipelineNode(id="b", type=PipelineNodeType.DELAY, name="d2"),
        ],
        edges=[],
    )
    asyncio.run(db.repos.content_pipelines.set_pipeline_json(pid, graph))

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="edge", edge_action="add", pipeline_id=pid, from_node="a", to_node="b"))

    out = capsys.readouterr().out
    assert "Added edge a -> b" in out

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(_ns(pipeline_action="edge", edge_action="remove", pipeline_id=pid, from_node="a", to_node="b"))

    out = capsys.readouterr().out
    assert "Removed edge a -> b" in out


# ---------------------------------------------------------------------------
# Integration tests: fresh DB connection per CLI call (fresh_database=True)
# ---------------------------------------------------------------------------


def _make_db(tmp_path, name):
    db = Database(str(tmp_path / name))
    asyncio.run(db.initialize())
    return db


def test_pipeline_add_node_dsl_integration(tmp_path, cli_init_patch, capsys):
    """DAG pipeline via --node persists valid pipeline_json to SQLite (fresh connection per call)."""
    db = _make_db(tmp_path, "integ_dag.db")
    _add_pipeline_prereqs(db)

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS, fresh_database=True):
        from src.cli.commands.pipeline import run

        run(_ns(
            pipeline_action="add", name="Integration DAG",
            prompt_template=None, source=[1001], target=["+100|77"],
            llm_model=None, image_model=None, publish_mode="moderated",
            generation_backend="chain", interval=120, inactive=False,
            node_specs=["react:emoji=fire"], edge=None, node_configs=None,
        ))
        add_out = capsys.readouterr().out
        assert "Added pipeline id=" in add_out
        pipeline_id = int(add_out.split("id=")[1].split(":")[0].strip())

        run(_ns(pipeline_action="show", id=pipeline_id))
        assert "Integration DAG" in capsys.readouterr().out

        run(_ns(pipeline_action="graph", id=pipeline_id))
        graph_out = capsys.readouterr().out
        assert "source" in graph_out
        assert "react" in graph_out

    asyncio.run(db.close())

    # Verify via a third fresh connection
    final_db = _make_db(tmp_path, "integ_dag.db")
    pipeline = asyncio.run(final_db.repos.content_pipelines.get_by_id(pipeline_id))
    asyncio.run(final_db.close())

    assert pipeline is not None and pipeline.pipeline_json is not None
    node_types = {n.type.value for n in pipeline.pipeline_json.nodes}
    assert {"source", "fetch_messages", "react", "publish"} <= node_types

    edge_froms = {e.from_node for e in pipeline.pipeline_json.edges}
    for node in pipeline.pipeline_json.nodes:
        if node.type.value != "publish":
            assert node.id in edge_froms, f"node {node.id} ({node.type}) has no outgoing edge"


def test_pipeline_add_legacy_integration(tmp_path, cli_init_patch, capsys):
    """Legacy prompt-template pipeline persists all fields to SQLite (fresh connection per call)."""
    db = _make_db(tmp_path, "integ_legacy.db")
    _add_pipeline_prereqs(db)

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS, fresh_database=True):
        from src.cli.commands.pipeline import run

        run(_ns(
            pipeline_action="add", name="Legacy Integration",
            prompt_template="Summarize: {source_messages}",
            source=[1001], target=["+100|77"],
            llm_model=None, image_model=None, publish_mode="auto",
            generation_backend="chain", interval=30, inactive=False,
            node_specs=None, edge=None, node_configs=None,
        ))
        add_out = capsys.readouterr().out
        assert "Added pipeline id=" in add_out
        pipeline_id = int(add_out.split("id=")[1].split(":")[0].strip())

        run(_ns(pipeline_action="list"))
        assert "Legacy Integration" in capsys.readouterr().out

    asyncio.run(db.close())

    final_db = _make_db(tmp_path, "integ_legacy.db")
    pipeline = asyncio.run(final_db.repos.content_pipelines.get_by_id(pipeline_id))
    sources = asyncio.run(final_db.repos.content_pipelines.list_sources(pipeline_id))
    targets = asyncio.run(final_db.repos.content_pipelines.list_targets(pipeline_id))
    asyncio.run(final_db.close())

    assert pipeline.prompt_template == "Summarize: {source_messages}"
    assert pipeline.publish_mode.value == "auto"
    assert pipeline.generate_interval_minutes == 30
    assert pipeline.is_active is True
    assert len(sources) == 1 and sources[0].channel_id == 1001
    assert len(targets) == 1 and targets[0].dialog_id == 77


# ---------------------------------------------------------------------------
# pipeline add – missing required fields validation (legacy mode)
# ---------------------------------------------------------------------------


def test_pipeline_add_legacy_requires_source(tmp_path, cli_init_patch, capsys):
    """Legacy mode without --source prints error."""
    db_path = str(tmp_path / "cli_pipeline_add_no_source.db")
    db = Database(db_path)
    asyncio.run(db.initialize())

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(
            _ns(
                pipeline_action="add",
                name="NoSource",
                prompt_template="Summarize {source_messages}",
                source=None,
                target=["+100|77"],
                llm_model=None,
                image_model=None,
                publish_mode="moderated",
                generation_backend="chain",
                interval=60,
                inactive=False,
                node_specs=None,
                edge=None,
                node_configs=None,
            )
        )

    asyncio.run(db.close())
    out = capsys.readouterr().out
    assert "--source is required" in out


def test_pipeline_add_legacy_requires_target(tmp_path, cli_init_patch, capsys):
    """Legacy mode without --target prints error."""
    db_path = str(tmp_path / "cli_pipeline_add_no_target.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _add_pipeline_prereqs(db)

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(
            _ns(
                pipeline_action="add",
                name="NoTarget",
                prompt_template="Summarize {source_messages}",
                source=[1001],
                target=None,
                llm_model=None,
                image_model=None,
                publish_mode="moderated",
                generation_backend="chain",
                interval=60,
                inactive=False,
                node_specs=None,
                edge=None,
                node_configs=None,
            )
        )

    asyncio.run(db.close())
    out = capsys.readouterr().out
    assert "--target is required" in out


# ---------------------------------------------------------------------------
# pipeline add – --inactive flag
# ---------------------------------------------------------------------------


def test_pipeline_add_inactive_flag(tmp_path, cli_init_patch, capsys):
    """--inactive creates a pipeline with is_active=False."""
    db_path = str(tmp_path / "cli_pipeline_add_inactive.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _add_pipeline_prereqs(db)

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(
            _ns(
                pipeline_action="add",
                name="InactivePipeline",
                prompt_template="Summarize {source_messages}",
                source=[1001],
                target=["+100|77"],
                llm_model=None,
                image_model=None,
                publish_mode="moderated",
                generation_backend="chain",
                interval=60,
                inactive=True,
                node_specs=None,
                edge=None,
                node_configs=None,
            )
        )

    out = capsys.readouterr().out
    assert "Added pipeline id=" in out
    assert "InactivePipeline" in out

    asyncio.run(db.close())
    verify_db = Database(db_path)
    asyncio.run(verify_db.initialize())
    pipelines = asyncio.run(verify_db.repos.content_pipelines.get_all())
    asyncio.run(verify_db.close())
    assert len(pipelines) == 1
    assert pipelines[0].is_active is False


# ---------------------------------------------------------------------------
# pipeline add – --run-after flag
# ---------------------------------------------------------------------------


def test_pipeline_add_run_after(tmp_path, cli_init_patch, capsys):
    """--run-after enqueues a pipeline run immediately after creation."""
    db_path = str(tmp_path / "cli_pipeline_add_run_after.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _add_pipeline_prereqs(db)

    enqueued = {}

    async def fake_enqueue(pipeline_id, since_hours=24):
        enqueued["pipeline_id"] = pipeline_id
        enqueued["since_hours"] = since_hours

    with (
        cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS),
        patch("src.services.task_enqueuer.TaskEnqueuer") as MockEnqueuer,
    ):
        MockEnqueuer.return_value.enqueue_pipeline_run = fake_enqueue

        from src.cli.commands.pipeline import run

        run(
            _ns(
                pipeline_action="add",
                name="RunAfterPipeline",
                prompt_template="Summarize {source_messages}",
                source=[1001],
                target=["+100|77"],
                llm_model=None,
                image_model=None,
                publish_mode="moderated",
                generation_backend="chain",
                interval=60,
                inactive=False,
                node_specs=None,
                edge=None,
                node_configs=None,
                run_after=True,
                since_value=12,
                since_unit="h",
            )
        )

    asyncio.run(db.close())
    out = capsys.readouterr().out
    assert "Added pipeline id=" in out
    assert "Enqueued pipeline run" in out
    assert enqueued.get("pipeline_id") is not None
    assert enqueued["since_hours"] == 12


# ---------------------------------------------------------------------------
# pipeline add – --json-file import
# ---------------------------------------------------------------------------


def test_pipeline_add_json_file(tmp_path, cli_init_patch, capsys):
    """--json-file imports a DAG pipeline from a JSON file."""
    import json

    db_path = str(tmp_path / "cli_pipeline_add_json.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    _add_pipeline_prereqs(db)

    graph_data = {
        "nodes": [
            {"id": "s", "type": "source", "name": "src", "config": {}},
            {"id": "f", "type": "fetch_messages", "name": "fetch", "config": {}},
            {"id": "p", "type": "publish", "name": "pub", "config": {}},
        ],
        "edges": [
            {"from_node": "s", "to_node": "f"},
            {"from_node": "f", "to_node": "p"},
        ],
    }
    json_path = str(tmp_path / "graph.json")
    with open(json_path, "w") as f:
        json.dump(graph_data, f)

    with cli_init_patch(db, *_PIPELINE_INIT_DB_TARGETS):
        from src.cli.commands.pipeline import run

        run(
            _ns(
                pipeline_action="add",
                name="JsonFilePipeline",
                prompt_template=None,
                source=[1001],
                target=["+100|77"],
                llm_model=None,
                image_model=None,
                publish_mode="moderated",
                generation_backend="chain",
                interval=60,
                inactive=False,
                json_file=json_path,
                node_specs=None,
                edge=None,
                node_configs=None,
            )
        )

    out = capsys.readouterr().out
    assert "Added pipeline id=" in out
    assert "JsonFilePipeline" in out

    verify_db = Database(db_path)
    asyncio.run(verify_db.initialize())
    pipelines = asyncio.run(verify_db.repos.content_pipelines.get_all())
    asyncio.run(verify_db.close())
    assert len(pipelines) == 1
    assert pipelines[0].pipeline_json is not None
