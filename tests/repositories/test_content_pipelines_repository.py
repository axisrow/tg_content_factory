from __future__ import annotations

import pytest

from src.database.repositories.content_pipelines import ContentPipelinesRepository
from src.models import Channel, ContentPipeline, PipelineTarget
def make_pipeline(**kwargs) -> ContentPipeline:
    defaults = {
        "name": "Digest",
        "prompt_template": "Summarize {source_messages}",
    }
    defaults.update(kwargs)
    return ContentPipeline(**defaults)


async def test_add_and_read_with_relations(repo):
    pipeline_id = await repo.add(
        make_pipeline(),
        [1001, 1002],
        [
            PipelineTarget(
                pipeline_id=0,
                phone="+100",
                dialog_id=77,
                title="Target A",
                dialog_type="channel",
            ),
            PipelineTarget(
                pipeline_id=0,
                phone="+100",
                dialog_id=88,
                title="Target B",
                dialog_type="group",
            ),
        ],
    )

    pipeline = await repo.get_by_id(pipeline_id)
    assert pipeline is not None
    assert pipeline.name == "Digest"

    sources = await repo.list_sources(pipeline_id)
    targets = await repo.list_targets(pipeline_id)
    assert [source.channel_id for source in sources] == [1001, 1002]
    assert [(target.phone, target.dialog_id) for target in targets] == [("+100", 77), ("+100", 88)]


async def test_update_replaces_sources_and_targets(repo):
    pipeline_id = await repo.add(
        make_pipeline(),
        [1001],
        [
            PipelineTarget(
                pipeline_id=0,
                phone="+100",
                dialog_id=77,
                title="Target A",
                dialog_type="channel",
            )
        ],
    )

    ok = await repo.update(
        pipeline_id,
        make_pipeline(name="Updated", generate_interval_minutes=15),
        [1002],
        [
            PipelineTarget(
                pipeline_id=0,
                phone="+200",
                dialog_id=99,
                title="Target B",
                dialog_type="group",
            )
        ],
    )

    assert ok is True
    pipeline = await repo.get_by_id(pipeline_id)
    assert pipeline is not None
    assert pipeline.name == "Updated"
    assert pipeline.generate_interval_minutes == 15
    assert [source.channel_id for source in await repo.list_sources(pipeline_id)] == [1002]
    targets = await repo.list_targets(pipeline_id)
    assert [(target.phone, target.dialog_id) for target in targets] == [("+200", 99)]


async def test_delete_cascades_relations(repo):
    pipeline_id = await repo.add(
        make_pipeline(),
        [1001],
        [
            PipelineTarget(
                pipeline_id=0,
                phone="+100",
                dialog_id=77,
                title="Target A",
                dialog_type="channel",
            )
        ],
    )

    await repo.delete(pipeline_id)

    assert await repo.get_by_id(pipeline_id) is None
    assert await repo.list_sources(pipeline_id) == []
    assert await repo.list_targets(pipeline_id) == []
