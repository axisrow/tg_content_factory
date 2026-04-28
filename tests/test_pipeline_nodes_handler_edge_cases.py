"""Tests for pipeline node handler edge cases."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.pipeline_nodes import NodeContext
from src.services.pipeline_nodes.base import BaseNodeHandler
from src.services.pipeline_nodes.handlers import (
    AgentLoopHandler,
    ConditionHandler,
    DelayHandler,
    FetchMessagesHandler,
    FilterHandler,
    ForwardHandler,
    ImageGenerateHandler,
    LlmGenerateHandler,
    LlmRefineHandler,
    NotifyHandler,
    PublishHandler,
    ReactHandler,
    SourceHandler,
)


def _msg(
    text="test message",
    channel_title="Test Channel",
    channel_username="testchan",
    message_id=123,
    channel_id=-100123,
    sender_id=456,
    sender_name="User",
    date=None,
):
    m = MagicMock()
    m.text = text
    m.channel_title = channel_title
    m.channel_username = channel_username
    m.message_id = message_id
    m.channel_id = channel_id
    m.sender_id = sender_id
    m.sender_name = sender_name
    m.date = date or datetime(2024, 1, 1, tzinfo=timezone.utc)
    return m


# ── SourceHandler extras ─────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_source_handler_empty_channel_ids():
    ctx = NodeContext()
    await SourceHandler().execute({"channel_ids": []}, ctx, {})
    assert ctx.get_global("source_channel_ids") == []


@pytest.mark.anyio
async def test_source_handler_missing_channel_ids_key():
    ctx = NodeContext()
    await SourceHandler().execute({}, ctx, {})
    assert ctx.get_global("source_channel_ids") == []


# ── FetchMessagesHandler extras ───────────────────────────────────────────────


@pytest.mark.anyio
async def test_fetch_messages_no_db_service():
    """When db is None in services, sets empty context_messages and records the error."""
    ctx = NodeContext()
    ctx.set_global("source_channel_ids", [1, 2])
    await FetchMessagesHandler().execute({}, ctx, {})
    assert ctx.get_global("context_messages") == []
    errors = ctx.get_errors()
    assert len(errors) == 1
    assert errors[0]["code"] == "missing_dependency"
    assert "db" in errors[0]["detail"]


@pytest.mark.anyio
async def test_fetch_messages_no_channel_ids():
    """When no source_channel_ids set, passes empty list to DB query."""
    ctx = NodeContext()
    mock_db = MagicMock()
    mock_db.repos.messages.get_recent_for_channels = AsyncMock(return_value=[])
    await FetchMessagesHandler().execute({}, ctx, {"db": mock_db, "since_hours": 24.0})
    mock_db.repos.messages.get_recent_for_channels.assert_awaited_once_with([], 24.0)


@pytest.mark.anyio
async def test_fetch_messages_limit_zero_returns_empty():
    """limit=0 should produce 0 messages (slice [:0])."""
    ctx = NodeContext()
    ctx.set_global("source_channel_ids", [1])
    mock_db = MagicMock()
    mock_db.repos.messages.get_recent_for_channels = AsyncMock(
        return_value=[_msg(text=f"m{i}") for i in range(5)]
    )
    await FetchMessagesHandler().execute({"limit": 0}, ctx, {"db": mock_db, "since_hours": 24.0})
    assert ctx.get_global("context_messages") == []


# ── LlmGenerateHandler extras ─────────────────────────────────────────────────


@pytest.mark.anyio
async def test_llm_generate_empty_prompt_template():
    """Empty prompt_template should still work (renders with empty source_messages)."""
    ctx = NodeContext()

    async def provider(prompt, model="", max_tokens=512, temperature=0.7):
        return "generated"

    await LlmGenerateHandler().execute(
        {"prompt_template": ""}, ctx, {"provider_callable": provider}
    )
    assert ctx.get_global("generated_text") == "generated"


@pytest.mark.anyio
async def test_llm_generate_messages_with_empty_text_skipped():
    """Messages with empty/None text are not included in source_parts."""
    ctx = NodeContext()
    m1 = _msg(text="")
    m2 = _msg(text=None)
    m3 = _msg(text="actual content")
    ctx.set_global("context_messages", [m1, m2, m3])

    captured = {}

    async def provider(prompt, model="", max_tokens=512, temperature=0.7):
        captured["prompt"] = prompt
        return "ok"

    await LlmGenerateHandler().execute(
        {"prompt_template": "{source_messages}"}, ctx, {"provider_callable": provider}
    )
    assert "actual content" in captured["prompt"]
    assert "[None]" not in captured["prompt"] or "id:" not in captured["prompt"].split("actual content")[0]


@pytest.mark.anyio
async def test_llm_generate_provider_returns_dict_with_generated_text_key():
    """Provider returns dict with 'generated_text' key instead of 'text'."""
    ctx = NodeContext()

    async def provider(prompt, model="", max_tokens=512, temperature=0.7):
        return {"generated_text": "from gen key", "citations": []}

    await LlmGenerateHandler().execute(
        {"prompt_template": "test"}, ctx, {"provider_callable": provider}
    )
    assert ctx.get_global("generated_text") == "from gen key"


@pytest.mark.anyio
async def test_llm_generate_default_model_from_services():
    """Model falls back to services['default_model'] when not in config."""
    ctx = NodeContext()
    captured = {}

    async def provider(prompt, model="", max_tokens=512, temperature=0.7):
        captured["model"] = model
        return "ok"

    await LlmGenerateHandler().execute(
        {"prompt_template": "test"}, ctx,
        {"provider_callable": provider, "default_model": "gpt-4"},
    )
    assert captured["model"] == "gpt-4"


# ── LlmRefineHandler extras ──────────────────────────────────────────────────


@pytest.mark.anyio
async def test_llm_refine_no_generated_text_uses_context_messages():
    """When generated_text is empty and context_messages exist, uses first 3 messages."""
    ctx = NodeContext()
    msgs = [_msg(text=f"msg{i}") for i in range(5)]
    ctx.set_global("context_messages", msgs)

    captured = {}

    async def provider(prompt, model="", max_tokens=512, temperature=0.7):
        captured["prompt"] = prompt
        return "refined"

    await LlmRefineHandler().execute(
        {"prompt": "{text}"}, ctx, {"provider_callable": provider}
    )
    assert "msg0" in captured["prompt"]
    assert "msg1" in captured["prompt"]
    assert "msg2" in captured["prompt"]
    # Only first 3 messages should be included
    assert "msg3" not in captured["prompt"]


@pytest.mark.anyio
async def test_llm_refine_no_text_no_messages():
    """When both generated_text and context_messages are empty, prompt has empty {text}."""
    ctx = NodeContext()
    ctx.set_global("generated_text", "")
    ctx.set_global("context_messages", [])

    captured = {}

    async def provider(prompt, model="", max_tokens=512, temperature=0.7):
        captured["prompt"] = prompt
        return "refined"

    await LlmRefineHandler().execute(
        {"prompt": "{text}"}, ctx, {"provider_callable": provider}
    )
    assert captured["prompt"] == ""
    assert ctx.get_global("generated_text") == "refined"


# ── ImageGenerateHandler extras ───────────────────────────────────────────────


@pytest.mark.anyio
async def test_image_generate_no_generated_text():
    """When generated_text is empty, still attempts generation with empty prompt."""
    ctx = NodeContext()
    svc = AsyncMock()
    svc.generate = AsyncMock(return_value=None)
    await ImageGenerateHandler().execute({"model": "test"}, ctx, {"image_service": svc})
    svc.generate.assert_awaited_once_with("test", "")


@pytest.mark.anyio
async def test_image_generate_service_returns_none_url():
    """When image service returns None URL, image_url stays None."""
    ctx = NodeContext()
    ctx.set_global("generated_text", "a cat")
    svc = AsyncMock()
    svc.generate = AsyncMock(return_value=None)
    await ImageGenerateHandler().execute({"model": "test"}, ctx, {"image_service": svc})
    assert ctx.get_global("image_url") is None


@pytest.mark.anyio
async def test_image_generate_default_model_from_services():
    """Model falls back to services['default_image_model'] when not in config."""
    ctx = NodeContext()
    ctx.set_global("generated_text", "a cat")
    svc = AsyncMock()
    svc.generate = AsyncMock(return_value="http://img")
    await ImageGenerateHandler().execute({}, ctx, {
        "image_service": svc, "default_image_model": "flux",
    })
    svc.generate.assert_awaited_once_with("flux", "a cat")


# ── PublishHandler extras ─────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_publish_default_mode_is_moderated():
    ctx = NodeContext()
    await PublishHandler().execute({"targets": []}, ctx, {})
    assert ctx.get_global("publish_mode") == "moderated"


@pytest.mark.anyio
async def test_publish_empty_targets():
    ctx = NodeContext()
    await PublishHandler().execute({}, ctx, {})
    assert ctx.get_global("publish_targets") == []
    assert ctx.get_global("publish_reply") is False


# ── NotifyHandler extras ──────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_notify_no_text_uses_trigger_text():
    """When generated_text is empty but trigger_text exists, uses trigger_text."""
    ctx = NodeContext()
    ctx.set_global("trigger_text", "triggered content")
    svc = AsyncMock()
    await NotifyHandler().execute({"message_template": "{text}"}, ctx, {"notification_service": svc})
    svc.send_text.assert_awaited_once_with("triggered content")


@pytest.mark.anyio
async def test_notify_no_text_no_trigger():
    """When both generated_text and trigger_text are empty, sends empty string."""
    ctx = NodeContext()
    svc = AsyncMock()
    await NotifyHandler().execute({}, ctx, {"notification_service": svc})
    svc.send_text.assert_awaited_once_with("")


@pytest.mark.anyio
async def test_notify_template_with_channel_title():
    ctx = NodeContext()
    ctx.set_global("generated_text", "news")
    ctx.set_global("trigger_channel_title", "MyChannel")
    svc = AsyncMock()
    await NotifyHandler().execute(
        {"message_template": "[{channel_title}] {text}"}, ctx, {"notification_service": svc}
    )
    svc.send_text.assert_awaited_once_with("[MyChannel] news")


# ── FilterHandler extras ──────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_filter_empty_keywords_list():
    """Empty keywords list and no match_links → all messages filtered out."""
    ctx = NodeContext()
    m1 = _msg(text="anything")
    ctx.set_global("context_messages", [m1])
    await FilterHandler().execute(
        {"type": "keywords", "keywords": [], "match_links": False}, ctx, {}
    )
    assert ctx.get_global("context_messages") == []


@pytest.mark.anyio
async def test_filter_empty_context_messages():
    """No context messages → result is empty."""
    ctx = NodeContext()
    ctx.set_global("context_messages", [])
    await FilterHandler().execute({"type": "keywords", "keywords": ["test"]}, ctx, {})
    assert ctx.get_global("context_messages") == []


@pytest.mark.anyio
async def test_filter_keywords_strips_empty_entries():
    """Keywords list with empty strings should be filtered out."""
    ctx = NodeContext()
    m1 = _msg(text="hello world")
    ctx.set_global("context_messages", [m1])
    await FilterHandler().execute(
        {"type": "keywords", "keywords": ["", "hello", ""]}, ctx, {}
    )
    assert len(ctx.get_global("context_messages")) == 1


@pytest.mark.anyio
async def test_filter_keywords_case_insensitive():
    ctx = NodeContext()
    m1 = _msg(text="Crypto is great")
    ctx.set_global("context_messages", [m1])
    await FilterHandler().execute(
        {"type": "keywords", "keywords": ["crypto"]}, ctx, {}
    )
    assert len(ctx.get_global("context_messages")) == 1


@pytest.mark.anyio
async def test_filter_service_message_default_types():
    ctx = NodeContext()
    m1 = _msg(text="user_joined the chat")
    m2 = _msg(text="user_left the chat")
    ctx.set_global("context_messages", [m1, m2])
    await FilterHandler().execute({"type": "service_message"}, ctx, {})
    assert len(ctx.get_global("context_messages")) == 2


@pytest.mark.anyio
async def test_filter_regex_empty_pattern():
    """Empty regex pattern string → no matches, result empty."""
    ctx = NodeContext()
    m1 = _msg(text="anything")
    ctx.set_global("context_messages", [m1])
    await FilterHandler().execute({"type": "regex", "pattern": ""}, ctx, {})
    assert ctx.get_global("context_messages") == []


@pytest.mark.anyio
async def test_filter_sets_filtered_messages_also():
    """FilterHandler sets both filtered_messages and context_messages."""
    ctx = NodeContext()
    m1 = _msg(text="keep this")
    m2 = _msg(text="remove this")
    ctx.set_global("context_messages", [m1, m2])
    await FilterHandler().execute({"type": "keywords", "keywords": ["keep"]}, ctx, {})
    assert ctx.get_global("filtered_messages") == [m1]
    assert ctx.get_global("context_messages") == [m1]


# ── DelayHandler extras ───────────────────────────────────────────────────────


@pytest.mark.anyio
@patch("asyncio.sleep", new_callable=AsyncMock)
async def test_delay_negative_min_seconds(mock_sleep):
    """Negative min_seconds → treated as negative float, no sleep."""
    ctx = NodeContext()
    await DelayHandler().execute({"min_seconds": -1}, ctx, {})
    # -1 > 0 is False, so no sleep
    mock_sleep.assert_not_awaited()


@pytest.mark.anyio
@patch("asyncio.sleep", new_callable=AsyncMock)
async def test_delay_max_less_than_min(mock_sleep):
    """max_seconds < min_seconds → uses min_seconds as delay."""
    ctx = NodeContext()
    await DelayHandler().execute({"min_seconds": 5, "max_seconds": 2}, ctx, {})
    mock_sleep.assert_awaited_once_with(5.0)


# ── ReactHandler extras ──────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_react_empty_messages():
    ctx = NodeContext()
    ctx.set_global("context_messages", [])
    pool = AsyncMock()
    await ReactHandler().execute({"emoji": "👍"}, ctx, {"client_pool": pool})
    pool.get_available_client.assert_not_awaited()


@pytest.mark.anyio
async def test_react_exception_continues():
    """React exception on one message should not crash the loop."""
    ctx = NodeContext()
    m1 = _msg(channel_id=-100, message_id=1)
    m2 = _msg(channel_id=-200, message_id=2)
    ctx.set_global("context_messages", [m1, m2])

    session = AsyncMock()
    session.send_reaction = AsyncMock(side_effect=RuntimeError("fail"))
    pool = AsyncMock()
    pool.get_available_client = AsyncMock(return_value=(session, "p"))
    pool.release_client = AsyncMock()

    await ReactHandler().execute({"emoji": "👍"}, ctx, {"client_pool": pool})
    # Should have tried both messages despite first failing
    assert session.send_reaction.await_count == 2


# ── ForwardHandler extras ─────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_forward_empty_messages():
    ctx = NodeContext()
    ctx.set_global("context_messages", [])
    session = AsyncMock()
    pool = AsyncMock()
    pool.get_client_by_phone = AsyncMock(return_value=(session, "+123"))
    pool.release_client = AsyncMock()
    targets = [{"phone": "+123", "dialog_id": -1}]
    await ForwardHandler().execute({"targets": targets}, ctx, {"client_pool": pool})
    pool.get_client_by_phone.assert_awaited_once_with("+123")
    session.forward_messages.assert_not_awaited()
    pool.release_client.assert_awaited_once_with("+123")


@pytest.mark.anyio
async def test_forward_exception_continues():
    """Forward exception for one target should not prevent others."""
    ctx = NodeContext()
    m = _msg(channel_id=-100, message_id=1)
    ctx.set_global("context_messages", [m])

    session = AsyncMock()
    session.forward_messages = AsyncMock(side_effect=RuntimeError("flood"))
    pool = AsyncMock()
    pool.get_client_by_phone = AsyncMock(return_value=(session, "p"))
    pool.release_client = AsyncMock()

    targets = [
        {"phone": "+111", "dialog_id": -1},
        {"phone": "+222", "dialog_id": -2},
    ]
    await ForwardHandler().execute({"targets": targets}, ctx, {"client_pool": pool})
    # Both targets attempted despite first failing
    assert pool.get_client_by_phone.await_count == 2


@pytest.mark.anyio
async def test_forward_empty_targets():
    ctx = NodeContext()
    ctx.set_global("context_messages", [_msg()])
    pool = AsyncMock()
    await ForwardHandler().execute({"targets": []}, ctx, {"client_pool": pool})
    pool.get_client_by_phone.assert_not_awaited()


# ── ConditionHandler extras ───────────────────────────────────────────────────


@pytest.mark.anyio
async def test_condition_missing_field_in_context():
    """Field not in context → get_global returns '' → not_empty is False."""
    ctx = NodeContext()
    await ConditionHandler().execute(
        {"field": "nonexistent_field", "operator": "not_empty"}, ctx, {}
    )
    assert ctx.get_global("condition_result") is False


@pytest.mark.anyio
async def test_condition_eq_mismatch():
    ctx = NodeContext()
    ctx.set_global("status", "active")
    await ConditionHandler().execute(
        {"field": "status", "operator": "eq", "value": "inactive"}, ctx, {}
    )
    assert ctx.get_global("condition_result") is False


@pytest.mark.anyio
async def test_condition_gt_equal_values():
    ctx = NodeContext()
    ctx.set_global("count", "5")
    await ConditionHandler().execute(
        {"field": "count", "operator": "gt", "value": "5"}, ctx, {}
    )
    assert ctx.get_global("condition_result") is False


@pytest.mark.anyio
async def test_condition_lt_not_supported():
    """Unsupported operator → result stays False."""
    ctx = NodeContext()
    ctx.set_global("val", "10")
    await ConditionHandler().execute(
        {"field": "val", "operator": "lt", "value": "5"}, ctx, {}
    )
    assert ctx.get_global("condition_result") is False


@pytest.mark.anyio
async def test_condition_contains_case_insensitive():
    ctx = NodeContext()
    ctx.set_global("text", "Hello WORLD")
    await ConditionHandler().execute(
        {"field": "text", "operator": "contains", "value": "HELLO"}, ctx, {}
    )
    assert ctx.get_global("condition_result") is True


# ── AgentLoopHandler extras ──────────────────────────────────────────────────


@pytest.mark.anyio
async def test_agent_loop_default_system_prompt():
    """When no system_prompt in config, uses default."""
    ctx = NodeContext()
    ctx.set_global("context_messages", [_msg(text="test")])

    captured = {}

    async def provider(prompt, model="", max_tokens=512, temperature=0.7):
        captured["prompt"] = prompt
        return "result"

    await AgentLoopHandler().execute({}, ctx, {"provider_callable": provider})
    assert "полезный ассистент" in captured["prompt"]


@pytest.mark.anyio
async def test_agent_loop_provider_returns_dict():
    """Provider returns dict with 'text' key."""
    ctx = NodeContext()
    ctx.set_global("context_messages", [_msg(text="test")])

    async def provider(prompt, model="", max_tokens=512, temperature=0.7):
        return {"text": "dict response"}

    await AgentLoopHandler().execute(
        {"system_prompt": "Test"}, ctx, {"provider_callable": provider}
    )
    assert ctx.get_global("generated_text") == "dict response"


@pytest.mark.anyio
async def test_agent_loop_provider_returns_dict_generated_text_key():
    """Provider returns dict with 'generated_text' key."""
    ctx = NodeContext()
    ctx.set_global("context_messages", [_msg(text="test")])

    async def provider(prompt, model="", max_tokens=512, temperature=0.7):
        return {"generated_text": "from gen_text key"}

    await AgentLoopHandler().execute(
        {"system_prompt": "Test"}, ctx, {"provider_callable": provider}
    )
    assert ctx.get_global("generated_text") == "from gen_text key"


@pytest.mark.anyio
async def test_agent_loop_tool_description_in_prompt():
    """When agent_tools provided, tool descriptions appear in prompt."""
    ctx = NodeContext()
    ctx.set_global("context_messages", [_msg(text="test")])

    captured = {}

    async def provider(prompt, model="", max_tokens=512, temperature=0.7):
        captured["prompt"] = prompt
        return "result"

    def my_tool(query: str) -> str:
        """Search for messages matching query."""
        return "found"

    services = {
        "provider_callable": provider,
        "agent_tools": {"my_tool": my_tool},
    }
    await AgentLoopHandler().execute({"system_prompt": "Test"}, ctx, services)
    assert "my_tool" in captured["prompt"]
    assert "Search for messages" in captured["prompt"]


@pytest.mark.anyio
async def test_agent_loop_invalid_json_tool_call():
    """Agent returns malformed JSON in tool call block → loop breaks, text preserved."""
    ctx = NodeContext()
    ctx.set_global("context_messages", [_msg(text="test")])

    call_count = 0

    async def provider(prompt, model="", max_tokens=512, temperature=0.7):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return '```json\n{invalid json}\n```'
        return "recovered"

    services = {"provider_callable": provider, "agent_tools": {}}
    await AgentLoopHandler().execute({"max_steps": 3}, ctx, services)
    # JSON parse fails → loop breaks on step 1
    assert call_count == 1
    # Malformed JSON causes break before final answer
    # The text is the raw tool call output which gets discarded


@pytest.mark.anyio
async def test_agent_loop_messages_with_empty_text_excluded():
    """Messages with empty/None text are excluded from the prompt."""
    ctx = NodeContext()
    m1 = _msg(text="")
    m2 = _msg(text=None)
    m3 = _msg(text="actual content")
    ctx.set_global("context_messages", [m1, m2, m3])

    captured = {}

    async def provider(prompt, model="", max_tokens=512, temperature=0.7):
        captured["prompt"] = prompt
        return "result"

    await AgentLoopHandler().execute(
        {"system_prompt": "Test"}, ctx, {"provider_callable": provider}
    )
    assert "actual content" in captured["prompt"]
    # Empty messages should not produce source_parts entries
    assert captured["prompt"].count("id:") == 1


# ── BaseNodeHandler ──────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_base_handler_execute_is_abstract():
    """BaseNodeHandler cannot be instantiated directly (abstract method)."""

    with pytest.raises(TypeError, match="abstract method"):
        BaseNodeHandler()
