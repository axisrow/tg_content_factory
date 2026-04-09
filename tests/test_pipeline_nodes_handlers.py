"""Tests for all 14 pipeline node handlers."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.pipeline_nodes import NodeContext
from src.services.pipeline_nodes.handlers import (
    AgentLoopHandler,
    ConditionHandler,
    DelayHandler,
    DeleteMessageHandler,
    FilterHandler,
    ForwardHandler,
    ImageGenerateHandler,
    LlmGenerateHandler,
    LlmRefineHandler,
    NotifyHandler,
    PublishHandler,
    ReactHandler,
    RetrieveContextHandler,
    SearchQueryTriggerHandler,
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


def _search_engine(messages=None, semantic_available=True):
    result = MagicMock()
    result.messages = messages if messages is not None else [_msg()]
    engine = AsyncMock()
    engine.semantic_available = semantic_available
    engine.search_hybrid = AsyncMock(return_value=result)
    engine.search_semantic = AsyncMock(return_value=result)
    engine.search_local = AsyncMock(return_value=result)
    return engine


# ── SourceHandler ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_source_handler_sets_channel_ids():
    ctx = NodeContext()
    await SourceHandler().execute({"channel_ids": [1, 2, 3]}, ctx, {})
    assert ctx.get_global("source_channel_ids") == [1, 2, 3]


# ── RetrieveContextHandler ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_retrieve_context_no_search_engine():
    ctx = NodeContext()
    await RetrieveContextHandler().execute({}, ctx, {})
    assert ctx.get_global("context_messages") == []


@pytest.mark.asyncio
async def test_retrieve_context_hybrid_with_semantic():
    ctx = NodeContext()
    engine = _search_engine([_msg("hybrid")])
    await RetrieveContextHandler().execute({"method": "hybrid", "limit": 5}, ctx, {"search_engine": engine})
    engine.search_hybrid.assert_awaited_once()
    assert len(ctx.get_global("context_messages")) == 1
    assert ctx.get_global("context_messages")[0].text == "hybrid"


@pytest.mark.asyncio
async def test_retrieve_context_semantic_method():
    ctx = NodeContext()
    engine = _search_engine([_msg("semantic")])
    await RetrieveContextHandler().execute({"method": "semantic"}, ctx, {"search_engine": engine})
    engine.search_semantic.assert_awaited_once()
    assert ctx.get_global("context_messages")[0].text == "semantic"


@pytest.mark.asyncio
async def test_retrieve_context_fallback_to_local():
    ctx = NodeContext()
    engine = _search_engine([_msg("local")], semantic_available=False)
    await RetrieveContextHandler().execute({"method": "hybrid"}, ctx, {"search_engine": engine})
    engine.search_local.assert_awaited_once()
    assert ctx.get_global("context_messages")[0].text == "local"


@pytest.mark.asyncio
async def test_retrieve_context_search_exception():
    ctx = NodeContext()
    engine = AsyncMock()
    engine.semantic_available = True
    engine.search_hybrid = AsyncMock(side_effect=RuntimeError("boom"))
    await RetrieveContextHandler().execute({"method": "hybrid"}, ctx, {"search_engine": engine})
    assert ctx.get_global("context_messages") == []


# ── LlmGenerateHandler ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_llm_generate_no_provider_raises():
    ctx = NodeContext()
    with pytest.raises(RuntimeError, match="no provider_callable"):
        await LlmGenerateHandler().execute({}, ctx, {})


@pytest.mark.asyncio
async def test_llm_generate_provider_returns_str():
    ctx = NodeContext()
    ctx.set_global("context_messages", [])

    async def provider(prompt, model="", max_tokens=512, temperature=0.7):
        return "hello world"

    await LlmGenerateHandler().execute(
        {"prompt_template": "say: {source_messages}"}, ctx, {"provider_callable": provider}
    )
    assert ctx.get_global("generated_text") == "hello world"
    assert ctx.get_global("citations") == []


@pytest.mark.asyncio
async def test_llm_generate_provider_returns_dict():
    ctx = NodeContext()
    m = _msg(text="source text", channel_title="Chan", date=datetime(2024, 3, 15, tzinfo=timezone.utc))
    ctx.set_global("context_messages", [m])

    async def provider(prompt, model="", max_tokens=512, temperature=0.7):
        return {"text": "generated", "citations": ["a"]}

    await LlmGenerateHandler().execute(
        {"prompt_template": "write: {source_messages}"}, ctx, {"provider_callable": provider}
    )
    assert ctx.get_global("generated_text") == "generated"
    assert ctx.get_global("citations") == ["a"]


@pytest.mark.asyncio
async def test_llm_generate_source_parts_format():
    ctx = NodeContext()
    m = _msg(text="body", channel_title="Title", date=datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc))
    m.message_id = 99
    ctx.set_global("context_messages", [m])

    captured = {}

    async def provider(prompt, model="", max_tokens=512, temperature=0.7):
        captured["prompt"] = prompt
        return "ok"

    await LlmGenerateHandler().execute(
        {"prompt_template": "{source_messages}"}, ctx, {"provider_callable": provider}
    )
    assert "[Title] body (id:99 date:2024-06-01T12:00:00+00:00)" in captured["prompt"]


# ── LlmRefineHandler ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_llm_refine_no_provider_raises():
    ctx = NodeContext()
    with pytest.raises(RuntimeError, match="no provider_callable"):
        await LlmRefineHandler().execute({}, ctx, {})


@pytest.mark.asyncio
async def test_llm_refine_with_generated_text():
    ctx = NodeContext()
    ctx.set_global("generated_text", "original")

    provider = AsyncMock(return_value="refined")
    await LlmRefineHandler().execute({"prompt": "Rewrite: {text}"}, ctx, {"provider_callable": provider})
    assert ctx.get_global("generated_text") == "refined"
    provider.assert_awaited_once()


@pytest.mark.asyncio
async def test_llm_refine_fallback_to_context_messages():
    ctx = NodeContext()
    msgs = [_msg(text="first"), _msg(text="second"), _msg(text="third"), _msg(text="fourth")]
    ctx.set_global("context_messages", msgs)

    captured = {}

    async def provider(prompt, model="", max_tokens=512, temperature=0.7):
        captured["prompt"] = prompt
        return "refined from messages"

    await LlmRefineHandler().execute({"prompt": "{text}"}, ctx, {"provider_callable": provider})
    assert "first" in captured["prompt"]
    assert "second" in captured["prompt"]
    assert "third" in captured["prompt"]
    assert "fourth" not in captured["prompt"]
    assert ctx.get_global("generated_text") == "refined from messages"


@pytest.mark.asyncio
async def test_llm_refine_provider_returns_empty_no_update():
    ctx = NodeContext()
    ctx.set_global("generated_text", "keep this")

    async def provider(prompt, model="", max_tokens=512, temperature=0.7):
        return ""

    await LlmRefineHandler().execute({"prompt": "{text}"}, ctx, {"provider_callable": provider})
    assert ctx.get_global("generated_text") == "keep this"


# ── ImageGenerateHandler ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_image_generate_no_service():
    ctx = NodeContext()
    ctx.set_global("generated_text", "a cat")
    await ImageGenerateHandler().execute({"model": "test"}, ctx, {})
    assert ctx.get_global("image_url") is None


@pytest.mark.asyncio
async def test_image_generate_no_model():
    ctx = NodeContext()
    svc = AsyncMock()
    await ImageGenerateHandler().execute({}, ctx, {"image_service": svc})
    svc.generate.assert_not_awaited()


@pytest.mark.asyncio
async def test_image_generate_success():
    ctx = NodeContext()
    ctx.set_global("generated_text", "a sunset")
    svc = AsyncMock()
    svc.generate = AsyncMock(return_value="https://img.example/1.png")
    await ImageGenerateHandler().execute({"model": "flux"}, ctx, {"image_service": svc})
    assert ctx.get_global("image_url") == "https://img.example/1.png"


@pytest.mark.asyncio
async def test_image_generate_exception_no_crash():
    ctx = NodeContext()
    ctx.set_global("generated_text", "x")
    svc = AsyncMock()
    svc.generate = AsyncMock(side_effect=RuntimeError("img fail"))
    await ImageGenerateHandler().execute({"model": "flux"}, ctx, {"image_service": svc})
    assert ctx.get_global("image_url") is None


# ── PublishHandler ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_publish_without_reply():
    ctx = NodeContext()
    await PublishHandler().execute(
        {"targets": [{"dialog_id": -100}], "mode": "auto"}, ctx, {}
    )
    assert ctx.get_global("publish_targets") == [{"dialog_id": -100}]
    assert ctx.get_global("publish_mode") == "auto"
    assert ctx.get_global("publish_reply") is False


@pytest.mark.asyncio
async def test_publish_with_reply_and_messages():
    ctx = NodeContext()
    m = _msg(message_id=42)
    ctx.set_global("context_messages", [m])
    await PublishHandler().execute({"targets": [], "reply": True}, ctx, {})
    assert ctx.get_global("publish_reply") is True
    assert ctx.get_global("reply_to_message_id") == 42


@pytest.mark.asyncio
async def test_publish_with_reply_empty_messages():
    ctx = NodeContext()
    ctx.set_global("context_messages", [])
    await PublishHandler().execute({"targets": [], "reply": True}, ctx, {})
    assert ctx.get_global("reply_to_message_id") is None


# ── NotifyHandler ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_notify_no_service():
    ctx = NodeContext()
    ctx.set_global("generated_text", "hello")
    await NotifyHandler().execute({}, ctx, {})
    # No crash


@pytest.mark.asyncio
async def test_notify_success():
    ctx = NodeContext()
    ctx.set_global("generated_text", "news")
    ctx.set_global("trigger_channel_title", "Chan")
    svc = AsyncMock()
    await NotifyHandler().execute(
        {"message_template": "[{channel_title}] {text}"}, ctx, {"notification_service": svc}
    )
    svc.send_text.assert_awaited_once_with("[Chan] news")


@pytest.mark.asyncio
async def test_notify_send_raises():
    ctx = NodeContext()
    ctx.set_global("generated_text", "x")
    svc = AsyncMock()
    svc.send_text = AsyncMock(side_effect=RuntimeError("fail"))
    await NotifyHandler().execute({}, ctx, {"notification_service": svc})
    # No crash


# ── FilterHandler ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_filter_keywords_match():
    ctx = NodeContext()
    m1 = _msg(text="buy cheap crypto")
    m2 = _msg(text="nice weather")
    ctx.set_global("context_messages", [m1, m2])
    await FilterHandler().execute({"type": "keywords", "keywords": ["crypto"]}, ctx, {})
    assert ctx.get_global("context_messages") == [m1]


@pytest.mark.asyncio
async def test_filter_match_links():
    ctx = NodeContext()
    m1 = _msg(text="visit https://example.com now")
    m2 = _msg(text="plain text")
    ctx.set_global("context_messages", [m1, m2])
    await FilterHandler().execute({"type": "keywords", "keywords": [], "match_links": True}, ctx, {})
    assert ctx.get_global("context_messages") == [m1]


@pytest.mark.asyncio
async def test_filter_service_message():
    ctx = NodeContext()
    m1 = _msg(text="user joined the group")
    m2 = _msg(text="normal msg")
    ctx.set_global("context_messages", [m1, m2])
    await FilterHandler().execute(
        {"type": "service_message", "service_types": ["user joined"]}, ctx, {}
    )
    assert ctx.get_global("context_messages") == [m1]


@pytest.mark.asyncio
async def test_filter_anonymous_sender():
    ctx = NodeContext()
    m1 = _msg(sender_id=None, sender_name=None)
    m2 = _msg(sender_id=123, sender_name="User")
    ctx.set_global("context_messages", [m1, m2])
    await FilterHandler().execute({"type": "anonymous_sender"}, ctx, {})
    assert ctx.get_global("context_messages") == [m1]


@pytest.mark.asyncio
async def test_filter_regex():
    ctx = NodeContext()
    m1 = _msg(text="price: $100")
    m2 = _msg(text="no price here")
    ctx.set_global("context_messages", [m1, m2])
    await FilterHandler().execute({"type": "regex", "pattern": r"price.*\$\d+"}, ctx, {})
    assert ctx.get_global("context_messages") == [m1]


@pytest.mark.asyncio
async def test_filter_invalid_regex():
    ctx = NodeContext()
    m1 = _msg(text="anything")
    ctx.set_global("context_messages", [m1])
    await FilterHandler().execute({"type": "regex", "pattern": "[invalid"}, ctx, {})
    assert ctx.get_global("context_messages") == []


@pytest.mark.asyncio
async def test_filter_unknown_type_returns_all():
    ctx = NodeContext()
    m1 = _msg(text="a")
    m2 = _msg(text="b")
    ctx.set_global("context_messages", [m1, m2])
    await FilterHandler().execute({"type": "nonexistent"}, ctx, {})
    assert len(ctx.get_global("context_messages")) == 2


# ── DelayHandler ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@patch("asyncio.sleep", new_callable=AsyncMock)
async def test_delay_fixed(mock_sleep):
    ctx = NodeContext()
    await DelayHandler().execute({"min_seconds": 5}, ctx, {})
    mock_sleep.assert_awaited_once_with(5.0)


@pytest.mark.asyncio
@patch("asyncio.sleep", new_callable=AsyncMock)
async def test_delay_zero(mock_sleep):
    ctx = NodeContext()
    await DelayHandler().execute({"min_seconds": 0}, ctx, {})
    mock_sleep.assert_not_awaited()


@pytest.mark.asyncio
@patch("asyncio.sleep", new_callable=AsyncMock)
async def test_delay_range(mock_sleep):
    ctx = NodeContext()
    with patch("random.uniform", return_value=3.5):
        await DelayHandler().execute({"min_seconds": 2, "max_seconds": 5}, ctx, {})
    mock_sleep.assert_awaited_once_with(3.5)


# ── ReactHandler ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_react_no_client_pool():
    ctx = NodeContext()
    ctx.set_global("context_messages", [_msg()])
    await ReactHandler().execute({"emoji": "👍"}, ctx, {})
    # No crash


@pytest.mark.asyncio
async def test_react_success():
    ctx = NodeContext()
    m = _msg(channel_id=-100, message_id=7)
    ctx.set_global("context_messages", [m])
    session = AsyncMock()
    pool = AsyncMock()
    pool.get_available_client = AsyncMock(return_value=(session, "phone1"))
    pool.release_client = AsyncMock()
    await ReactHandler().execute({"emoji": "🔥"}, ctx, {"client_pool": pool})
    session.send_reaction.assert_awaited_once_with(-100, 7, "🔥")
    pool.release_client.assert_awaited_once_with("phone1")


@pytest.mark.asyncio
async def test_react_client_none_breaks_loop():
    ctx = NodeContext()
    m1 = _msg()
    m2 = _msg()
    ctx.set_global("context_messages", [m1, m2])
    pool = AsyncMock()
    pool.get_available_client = AsyncMock(return_value=None)
    await ReactHandler().execute({"emoji": "👍"}, ctx, {"client_pool": pool})
    pool.get_available_client.assert_awaited_once()


@pytest.mark.asyncio
async def test_react_random_emojis():
    ctx = NodeContext()
    m = _msg(channel_id=-100, message_id=1)
    ctx.set_global("context_messages", [m])
    session = AsyncMock()
    pool = AsyncMock()
    pool.get_available_client = AsyncMock(return_value=(session, "p"))
    pool.release_client = AsyncMock()
    with patch("random.choice", return_value="🎉"):
        await ReactHandler().execute({"random_emojis": ["🎉", "😎"]}, ctx, {"client_pool": pool})
    session.send_reaction.assert_awaited_once_with(-100, 1, "🎉")


# ── ForwardHandler ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_forward_no_client_pool():
    ctx = NodeContext()
    ctx.set_global("context_messages", [_msg()])
    await ForwardHandler().execute({"targets": []}, ctx, {})
    # No crash


@pytest.mark.asyncio
async def test_forward_success():
    ctx = NodeContext()
    m = _msg(channel_id=-200, message_id=10)
    ctx.set_global("context_messages", [m])
    session = AsyncMock()
    pool = AsyncMock()
    pool.get_client_by_phone = AsyncMock(return_value=(session, "p"))
    pool.release_client = AsyncMock()
    targets = [{"phone": "+123", "dialog_id": -300}]
    await ForwardHandler().execute({"targets": targets}, ctx, {"client_pool": pool})
    session.forward_messages.assert_awaited_once_with(-300, 10, -200)


@pytest.mark.asyncio
async def test_forward_missing_phone_or_dialog_skips():
    ctx = NodeContext()
    ctx.set_global("context_messages", [_msg()])
    pool = AsyncMock()
    pool.get_client_by_phone = AsyncMock(return_value=None)
    await ForwardHandler().execute(
        {"targets": [{"phone": "", "dialog_id": -1}, {"phone": "p", "dialog_id": None}]},
        ctx,
        {"client_pool": pool},
    )
    pool.get_client_by_phone.assert_not_awaited()


@pytest.mark.asyncio
async def test_forward_get_client_returns_none_skips():
    ctx = NodeContext()
    m = _msg()
    ctx.set_global("context_messages", [m])
    pool = AsyncMock()
    pool.get_client_by_phone = AsyncMock(return_value=None)
    targets = [{"phone": "+123", "dialog_id": -1}]
    await ForwardHandler().execute({"targets": targets}, ctx, {"client_pool": pool})
    pool.get_client_by_phone.assert_awaited_once_with("+123")


# ── DeleteMessageHandler ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_delete_no_client_pool():
    ctx = NodeContext()
    ctx.set_global("context_messages", [_msg()])
    await DeleteMessageHandler().execute({}, ctx, {})
    # No crash


@pytest.mark.asyncio
async def test_delete_success():
    ctx = NodeContext()
    m = _msg(channel_id=-500, message_id=22)
    ctx.set_global("context_messages", [m])
    session = AsyncMock()
    pool = AsyncMock()
    pool.get_available_client = AsyncMock(return_value=(session, "p"))
    pool.release_client = AsyncMock()
    await DeleteMessageHandler().execute({}, ctx, {"client_pool": pool})
    session.delete_messages.assert_awaited_once_with(-500, [22])


@pytest.mark.asyncio
async def test_delete_client_none_breaks():
    ctx = NodeContext()
    m1 = _msg()
    m2 = _msg()
    ctx.set_global("context_messages", [m1, m2])
    pool = AsyncMock()
    pool.get_available_client = AsyncMock(return_value=None)
    await DeleteMessageHandler().execute({}, ctx, {"client_pool": pool})
    pool.get_available_client.assert_awaited_once()


# ── ConditionHandler ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_condition_not_empty_with_value():
    ctx = NodeContext()
    ctx.set_global("generated_text", "exists")
    await ConditionHandler().execute({"field": "generated_text", "operator": "not_empty"}, ctx, {})
    assert ctx.get_global("condition_result") is True


@pytest.mark.asyncio
async def test_condition_not_empty_with_empty():
    ctx = NodeContext()
    ctx.set_global("generated_text", "")
    await ConditionHandler().execute({"field": "generated_text", "operator": "not_empty"}, ctx, {})
    assert ctx.get_global("condition_result") is False


@pytest.mark.asyncio
async def test_condition_empty():
    ctx = NodeContext()
    ctx.set_global("val", "")
    await ConditionHandler().execute({"field": "val", "operator": "empty"}, ctx, {})
    assert ctx.get_global("condition_result") is True


@pytest.mark.asyncio
async def test_condition_contains():
    ctx = NodeContext()
    ctx.set_global("text", "Hello World")
    await ConditionHandler().execute({"field": "text", "operator": "contains", "value": "hello"}, ctx, {})
    assert ctx.get_global("condition_result") is True


@pytest.mark.asyncio
async def test_condition_eq():
    ctx = NodeContext()
    ctx.set_global("status", "ok")
    await ConditionHandler().execute({"field": "status", "operator": "eq", "value": "ok"}, ctx, {})
    assert ctx.get_global("condition_result") is True


@pytest.mark.asyncio
async def test_condition_gt():
    ctx = NodeContext()
    ctx.set_global("count", "10")
    await ConditionHandler().execute({"field": "count", "operator": "gt", "value": "5"}, ctx, {})
    assert ctx.get_global("condition_result") is True


@pytest.mark.asyncio
async def test_condition_gt_type_error():
    ctx = NodeContext()
    ctx.set_global("count", "not_a_number")
    await ConditionHandler().execute({"field": "count", "operator": "gt", "value": "5"}, ctx, {})
    assert ctx.get_global("condition_result") is False


# ── SearchQueryTriggerHandler ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_search_trigger_no_engine():
    ctx = NodeContext()
    await SearchQueryTriggerHandler().execute({"query": "test"}, ctx, {})
    # No crash, no trigger_matched set


@pytest.mark.asyncio
async def test_search_trigger_empty_query():
    ctx = NodeContext()
    engine = _search_engine()
    await SearchQueryTriggerHandler().execute({"query": ""}, ctx, {"search_engine": engine})
    engine.search_local.assert_not_awaited()


@pytest.mark.asyncio
async def test_search_trigger_messages_found():
    ctx = NodeContext()
    m = _msg(text="matched text", channel_title="Chan")
    engine = _search_engine([m])
    await SearchQueryTriggerHandler().execute({"query": "text"}, ctx, {"search_engine": engine})
    assert ctx.get_global("trigger_text") == "matched text"
    assert ctx.get_global("trigger_channel_title") == "Chan"
    assert ctx.get_global("trigger_matched") is True


@pytest.mark.asyncio
async def test_search_trigger_empty_result():
    ctx = NodeContext()
    engine = _search_engine([])
    await SearchQueryTriggerHandler().execute({"query": "nothing"}, ctx, {"search_engine": engine})
    assert ctx.get_global("trigger_matched") is False


@pytest.mark.asyncio
async def test_search_trigger_exception():
    ctx = NodeContext()
    engine = AsyncMock()
    engine.search_local = AsyncMock(side_effect=RuntimeError("db down"))
    await SearchQueryTriggerHandler().execute({"query": "test"}, ctx, {"search_engine": engine})
    assert ctx.get_global("trigger_matched") is False


# --- AgentLoopHandler ---


@pytest.mark.asyncio
async def test_agent_loop_generates_text():
    ctx = NodeContext()
    ctx.set_global("context_messages", [
        _msg(text="Hello world", channel_title="Chan1", message_id=1),
        _msg(text="Second post", channel_title="Chan2", message_id=2),
    ])
    provider = AsyncMock(return_value="Analyzed: 2 messages")
    services = {"provider_callable": provider, "default_model": "test-model"}

    await AgentLoopHandler().execute(
        {"system_prompt": "Analyze", "max_tokens": 500, "temperature": 0.5},
        ctx, services,
    )

    assert ctx.get_global("generated_text") == "Analyzed: 2 messages"
    provider.assert_called_once()
    call_kwargs = provider.call_args
    assert "Analyze" in call_kwargs.kwargs["prompt"]
    assert call_kwargs.kwargs["max_tokens"] == 500


@pytest.mark.asyncio
async def test_agent_loop_no_provider_raises():
    ctx = NodeContext()
    with pytest.raises(RuntimeError, match="no provider_callable"):
        await AgentLoopHandler().execute({}, ctx, {})


@pytest.mark.asyncio
async def test_agent_loop_empty_messages():
    ctx = NodeContext()
    ctx.set_global("context_messages", [])
    provider = AsyncMock(return_value="Nothing to analyze")
    services = {"provider_callable": provider}

    await AgentLoopHandler().execute({"system_prompt": "Summarize"}, ctx, services)

    assert ctx.get_global("generated_text") == "Nothing to analyze"
