from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import Channel
from src.search.persistence import SearchPersistence


@pytest.mark.anyio
async def test_cache_search_results_enqueues_stats_for_new_channels():
    bundle = MagicMock()
    bundle.channels.get_channel_by_channel_id = AsyncMock(return_value=None)
    bundle.add_channel = AsyncMock(return_value=1)
    bundle.log_search = AsyncMock()
    create_stats_task = AsyncMock(return_value=7)
    fetch_channel_meta = AsyncMock(
        return_value={"about": "about text", "linked_chat_id": 123, "has_comments": True}
    )

    persistence = SearchPersistence(bundle, create_stats_task, fetch_channel_meta)

    await persistence.cache_search_results(
        {
            100: Channel(channel_id=100, title="A"),
            200: Channel(channel_id=200, title="B"),
        },
        [],
        "+1",
        "query",
    )

    create_stats_task.assert_awaited_once()
    payload = create_stats_task.await_args.args[0]
    assert payload.channel_ids == [100, 200]
    added_channels = [call.args[0] for call in bundle.add_channel.await_args_list]
    assert [channel.about for channel in added_channels] == ["about text", "about text"]
    assert [channel.linked_chat_id for channel in added_channels] == [123, 123]


@pytest.mark.anyio
async def test_cache_search_results_skips_stats_for_existing_channels():
    bundle = MagicMock()
    bundle.channels.get_channel_by_channel_id = AsyncMock(
        return_value=Channel(channel_id=100, title="Existing")
    )
    bundle.add_channel = AsyncMock(return_value=1)
    bundle.log_search = AsyncMock()
    create_stats_task = AsyncMock(return_value=7)
    fetch_channel_meta = AsyncMock()

    persistence = SearchPersistence(bundle, create_stats_task, fetch_channel_meta)

    await persistence.cache_search_results(
        {100: Channel(channel_id=100, title="Existing")},
        [],
        "+1",
        "query",
    )

    create_stats_task.assert_not_awaited()
    fetch_channel_meta.assert_not_awaited()
