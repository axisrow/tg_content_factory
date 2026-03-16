"""Tests for PipelinesRepository."""
from __future__ import annotations

import pytest

from src.database.repositories.pipelines import PipelinesRepository
from src.models import Pipeline, PipelinePublishMode, PipelineTarget


@pytest.fixture
async def repo(db):
    """Create repository instance."""
    return PipelinesRepository(db.db)


def _make_pipeline(**kwargs) -> Pipeline:
    defaults = dict(
        name="Test Pipeline",
        phone="+1234567890",
        source_channel_ids=[-100111, -100222],
        targets=[PipelineTarget(dialog_id=-100333)],
        prompt_template="Summarize: {text}",
        llm_model="gpt-4o",
        publish_mode=PipelinePublishMode.DRAFT,
    )
    defaults.update(kwargs)
    return Pipeline(**defaults)


async def test_add_and_get(repo):
    p = _make_pipeline()
    pid = await repo.add(p)
    assert pid > 0

    fetched = await repo.get_by_id(pid)
    assert fetched is not None
    assert fetched.name == "Test Pipeline"
    assert fetched.phone == "+1234567890"
    assert fetched.source_channel_ids == [-100111, -100222]
    assert len(fetched.targets) == 1
    assert fetched.targets[0].dialog_id == -100333
    assert fetched.prompt_template == "Summarize: {text}"
    assert fetched.llm_model == "gpt-4o"
    assert fetched.publish_mode == PipelinePublishMode.DRAFT
    assert fetched.is_active is True
    assert fetched.created_at is not None


async def test_get_by_id_not_found(repo):
    assert await repo.get_by_id(999) is None


async def test_get_all_empty(repo):
    assert await repo.get_all() == []


async def test_get_all(repo):
    await repo.add(_make_pipeline(name="A"))
    await repo.add(_make_pipeline(name="B"))

    all_items = await repo.get_all()
    assert len(all_items) == 2
    assert all_items[0].name == "A"
    assert all_items[1].name == "B"


async def test_get_all_active_only(repo):
    await repo.add(_make_pipeline(name="Active"))
    pid2 = await repo.add(_make_pipeline(name="Inactive"))
    await repo.set_active(pid2, False)

    active = await repo.get_all(active_only=True)
    assert len(active) == 1
    assert active[0].name == "Active"


async def test_update(repo):
    pid = await repo.add(_make_pipeline())

    updated = Pipeline(
        name="Updated",
        phone="+9876543210",
        source_channel_ids=[-100444],
        targets=[PipelineTarget(dialog_id=-100555)],
        prompt_template="New prompt",
        llm_model="claude-3",
        publish_mode=PipelinePublishMode.AUTO,
    )
    await repo.update(pid, updated)

    fetched = await repo.get_by_id(pid)
    assert fetched.name == "Updated"
    assert fetched.phone == "+9876543210"
    assert fetched.source_channel_ids == [-100444]
    assert fetched.targets[0].dialog_id == -100555
    assert fetched.llm_model == "claude-3"
    assert fetched.publish_mode == PipelinePublishMode.AUTO


async def test_set_active(repo):
    pid = await repo.add(_make_pipeline())
    assert (await repo.get_by_id(pid)).is_active is True

    await repo.set_active(pid, False)
    assert (await repo.get_by_id(pid)).is_active is False

    await repo.set_active(pid, True)
    assert (await repo.get_by_id(pid)).is_active is True


async def test_delete(repo):
    pid = await repo.add(_make_pipeline())
    assert await repo.get_by_id(pid) is not None

    await repo.delete(pid)
    assert await repo.get_by_id(pid) is None


async def test_add_minimal(repo):
    """Pipeline with no optional fields."""
    p = Pipeline(name="Minimal", phone="+111")
    pid = await repo.add(p)

    fetched = await repo.get_by_id(pid)
    assert fetched.source_channel_ids == []
    assert fetched.targets == []
    assert fetched.prompt_template is None
    assert fetched.llm_model is None
