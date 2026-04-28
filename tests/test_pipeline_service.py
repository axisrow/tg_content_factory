from __future__ import annotations

import pytest

from src.database.bundles import PipelineBundle
from src.models import Account, Channel
from src.services.pipeline_service import (
    PipelineService,
    PipelineTargetRef,
    PipelineValidationError,
)


@pytest.fixture
async def svc(db):
    await db.add_account(Account(phone="+100", session_string="sess"))
    await db.add_channel(Channel(channel_id=1001, title="Source A"))
    await db.add_channel(Channel(channel_id=1002, title="Source B"))
    await db.repos.dialog_cache.replace_dialogs(
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
    return PipelineService(PipelineBundle.from_database(db))


@pytest.mark.anyio
async def test_add_and_list_pipeline(svc):
    pipeline_id = await svc.add(
        name="Digest",
        prompt_template="Summarize {source_messages}",
        source_channel_ids=[1001, 1002],
        target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
        generate_interval_minutes=30,
    )

    items = await svc.get_with_relations()
    assert pipeline_id > 0
    assert len(items) == 1
    assert items[0]["pipeline"].name == "Digest"
    assert items[0]["source_ids"] == [1001, 1002]
    assert items[0]["target_refs"] == ["+100|77"]


@pytest.mark.anyio
async def test_invalid_prompt_is_rejected(svc):
    with pytest.raises(PipelineValidationError):
        await svc.add(
            name="Broken",
            prompt_template="Use {unknown_var}",
            source_channel_ids=[1001],
            target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
        )


@pytest.mark.anyio
async def test_missing_dialog_cache_is_rejected(svc):
    with pytest.raises(PipelineValidationError):
        await svc.add(
            name="Digest",
            prompt_template="Summarize {source_messages}",
            source_channel_ids=[1001],
            target_refs=[PipelineTargetRef(phone="+100", dialog_id=999)],
        )
