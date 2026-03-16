"""Tests for PipelineService."""
from __future__ import annotations

import pytest

from src.database.bundles import PipelineBundle
from src.models import PipelinePublishMode, PipelineTarget
from src.services.pipeline_service import PipelineService


@pytest.fixture
async def svc(db):
    bundle = PipelineBundle.from_database(db)
    return PipelineService(bundle)


async def test_add_and_list(svc):
    pid = await svc.add("My pipe", "+111", source_channel_ids=[-100])
    assert pid > 0

    items = await svc.list()
    assert len(items) == 1
    assert items[0].name == "My pipe"


async def test_get(svc):
    pid = await svc.add("P", "+111")
    p = await svc.get(pid)
    assert p is not None
    assert p.name == "P"

    assert await svc.get(999) is None


async def test_toggle(svc):
    pid = await svc.add("P", "+111")
    assert (await svc.get(pid)).is_active is True

    await svc.toggle(pid)
    assert (await svc.get(pid)).is_active is False

    await svc.toggle(pid)
    assert (await svc.get(pid)).is_active is True


async def test_toggle_nonexistent(svc):
    await svc.toggle(999)  # should not raise


async def test_update(svc):
    pid = await svc.add("P", "+111")

    result = await svc.update(
        pid,
        "Updated",
        "+222",
        source_channel_ids=[-200],
        targets=[PipelineTarget(dialog_id=-300)],
        prompt_template="prompt",
        llm_model="gpt-4o",
        publish_mode=PipelinePublishMode.AUTO,
    )
    assert result is True

    p = await svc.get(pid)
    assert p.name == "Updated"
    assert p.phone == "+222"
    assert p.publish_mode == PipelinePublishMode.AUTO


async def test_update_nonexistent(svc):
    result = await svc.update(999, "X", "+111")
    assert result is False


async def test_delete(svc):
    pid = await svc.add("P", "+111")
    await svc.delete(pid)
    assert await svc.get(pid) is None


async def test_list_active_only(svc):
    await svc.add("A", "+111")
    pid2 = await svc.add("B", "+222")
    await svc.toggle(pid2)

    active = await svc.list(active_only=True)
    assert len(active) == 1
    assert active[0].name == "A"
