"""Smoke tests for read-only agent tool contracts."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from src.agent.tools import _PIPELINE_SAFE_TOOLS, build_agent_tools_dict
from src.agent.tools.deepagents_sync import build_deepagents_tools


def _assert_no_formatter_contract_error(tool_name: str, text: str) -> None:
    lowered = text.lower()
    assert "object is not subscriptable" not in lowered, tool_name
    assert "has no attribute" not in lowered, tool_name


@pytest.mark.anyio
async def test_pipeline_safe_agent_tools_smoke_on_empty_db(db):
    tools = build_agent_tools_dict(db, client_pool=None)

    assert _PIPELINE_SAFE_TOOLS - set(tools) == set()
    for tool_name in sorted(_PIPELINE_SAFE_TOOLS):
        result = await tools[tool_name]()
        assert isinstance(result, str), tool_name
        _assert_no_formatter_contract_error(tool_name, result)


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
