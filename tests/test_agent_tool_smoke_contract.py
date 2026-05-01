"""Smoke tests for read-only agent tool contracts."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta

import pytest

from src.agent.tools import _PIPELINE_SAFE_TOOLS, build_agent_tools_dict
from src.agent.tools.deepagents_sync import build_deepagents_tools
from src.models import Channel, ChannelStats, Message
from tests.agent_tools_helpers import _get_tool_handlers, _text


def _assert_no_formatter_contract_error(tool_name: str, text: str) -> None:
    lowered = text.lower()
    assert "object is not subscriptable" not in lowered, tool_name
    assert "has no attribute" not in lowered, tool_name
    assert "keyerror" not in lowered, tool_name
    assert "required positional" not in lowered, tool_name


async def _seed_read_only_contract_data(db) -> None:
    now = datetime.now()
    await db.add_channel(Channel(channel_id=1001, title="Named Contract", username="named_contract"))
    await db.add_channel(Channel(channel_id=1002, title=None, username=None))
    await db.save_channel_stats(ChannelStats(channel_id=1001, subscriber_count=101, avg_views=12.5))
    await db.save_channel_stats(ChannelStats(channel_id=1002, subscriber_count=202))
    await db.insert_messages_batch(
        [
            Message(channel_id=1001, message_id=1, text="quantum рынок https://example.com", date=now),
            Message(
                channel_id=1001,
                message_id=2,
                text="<b>quantum</b> рынок &amp; news",
                date=now - timedelta(minutes=1),
            ),
            Message(channel_id=1002, message_id=1, text="quantum рынок after before", date=now - timedelta(minutes=2)),
        ]
    )


@pytest.mark.anyio
async def test_pipeline_safe_agent_tools_smoke_on_empty_db(db):
    tools = build_agent_tools_dict(db, client_pool=None)

    assert _PIPELINE_SAFE_TOOLS - set(tools) == set()
    for tool_name in sorted(_PIPELINE_SAFE_TOOLS):
        result = await tools[tool_name]()
        assert isinstance(result, str), tool_name
        _assert_no_formatter_contract_error(tool_name, result)


@pytest.mark.anyio
async def test_pipeline_safe_agent_tools_smoke_on_seeded_db(db):
    await _seed_read_only_contract_data(db)
    tools = build_agent_tools_dict(db, client_pool=None)

    for tool_name in ["get_channel_stats", "get_trending_topics"]:
        result = await tools[tool_name]()
        assert isinstance(result, str), tool_name
        _assert_no_formatter_contract_error(tool_name, result)

    handlers = _get_tool_handlers(db, client_pool=None)
    result = await handlers["analyze_filters"]({})
    text = _text(result)
    assert isinstance(text, str)
    _assert_no_formatter_contract_error("analyze_filters", text)


def test_deepagents_read_only_tools_smoke_on_empty_db(cli_db):
    tool_map: dict[str, Callable] = {tool.__name__: tool for tool in build_deepagents_tools(cli_db)}
    calls: dict[str, dict] = {
        "search_messages": {"query_text": "smoke"},
        "semantic_search": {"query_text": "smoke"},
        "list_channels": {},
        "get_channel_stats": {},
        "list_pipelines": {},
        "get_pipeline_detail": {"pipeline_id": 1},
        "list_pipeline_runs": {"pipeline_id": 1},
        "get_pipeline_run": {"run_id": 1},
        "list_pending_moderation": {},
        "list_search_queries": {},
        "list_accounts": {},
        "get_flood_status": {},
        "get_account_info": {},
        "analyze_filters": {},
        "get_analytics_summary": {},
        "get_pipeline_stats": {},
        "get_trending_topics": {},
        "get_trending_channels": {},
        "get_message_velocity": {},
        "get_peak_hours": {},
        "get_calendar": {},
        "get_daily_stats": {},
        "get_scheduler_status": {},
        "get_notification_status": {},
        "list_image_providers": {},
        "get_settings": {},
        "get_system_info": {},
        "list_agent_threads": {},
        "get_thread_messages": {"thread_id": 1},
    }

    missing = set(calls) - set(tool_map)
    assert missing == set()
    for tool_name, kwargs in calls.items():
        result = tool_map[tool_name](**kwargs)
        assert isinstance(result, str), tool_name
        _assert_no_formatter_contract_error(tool_name, result)


def test_deepagents_read_only_tools_smoke_on_seeded_db(cli_db):
    import asyncio

    asyncio.run(_seed_read_only_contract_data(cli_db))
    tool_map: dict[str, Callable] = {tool.__name__: tool for tool in build_deepagents_tools(cli_db)}

    calls: dict[str, dict] = {
        "get_channel_stats": {},
        "analyze_filters": {},
        "get_trending_topics": {},
        "get_notification_status": {},
    }
    for tool_name, kwargs in calls.items():
        result = tool_map[tool_name](**kwargs)
        assert isinstance(result, str), tool_name
        _assert_no_formatter_contract_error(tool_name, result)
