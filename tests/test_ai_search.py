import sys
import types
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import LLMConfig
from src.database.repositories.messages import MessageSearchPage
from src.models import Message
from src.search.ai_search import AISearchEngine


class _FakeAIMessage:
    """Minimal stand-in for a langchain AIMessage — only ``.content``."""

    def __init__(self, content):
        self.content = content


class _FakeCompiledStateGraph:
    """Fake with the real ``create_deep_agent`` return signature.

    ``create_deep_agent`` returns a langgraph ``CompiledStateGraph`` that
    exposes ``.invoke`` and has **no** ``.run`` method. Using this instead of a
    bare ``MagicMock`` (which auto-creates ``.run``) means the test regresses if
    anyone reintroduces the ``.run`` call path from #1237.
    """

    def __init__(self, content):
        self._content = content
        self.invoke_calls: list[dict] = []

    def invoke(self, state):
        self.invoke_calls.append(state)
        return {"messages": [{"role": "user", "content": "q"}, _FakeAIMessage(self._content)]}


@pytest.fixture
def llm_config():
    return LLMConfig(enabled=True, api_key="test_key", provider="anthropic", model="claude-3")


@pytest.fixture
def mock_search_bundle():
    bundle = MagicMock()
    bundle.search_messages = AsyncMock(return_value=MessageSearchPage(messages=[], total=0))
    return bundle


@pytest.fixture
def fake_deepagents():
    create_deep_agent = MagicMock()
    module = types.SimpleNamespace(create_deep_agent=create_deep_agent)
    with patch.dict(sys.modules, {"deepagents": module}):
        yield create_deep_agent


@pytest.mark.anyio
async def test_ai_search_disabled(mock_search_bundle):
    config = LLMConfig(enabled=False)
    engine = AISearchEngine(config, mock_search_bundle)
    engine.initialize()
    assert engine._agent is None

    res = await engine.search("test")
    assert "AI search is not available" in res.ai_summary


@pytest.mark.anyio
async def test_ai_search_initialization_success(llm_config, mock_search_bundle):
    mock_create = MagicMock()
    mock_agent = MagicMock()
    mock_create.return_value = mock_agent
    module = types.SimpleNamespace(create_deep_agent=mock_create)

    with patch.dict(sys.modules, {"deepagents": module}):
        engine = AISearchEngine(llm_config, mock_search_bundle)
        engine.initialize()

    assert engine._agent == mock_agent
    mock_create.assert_called_once()


@pytest.mark.anyio
async def test_ai_search_initialization_import_error(llm_config, mock_search_bundle):
    with patch.dict(sys.modules, {"deepagents": None}):
        engine = AISearchEngine(llm_config, mock_search_bundle)
        engine.initialize()
        assert engine._agent is None


@pytest.mark.anyio
async def test_ai_search_initialization_general_error(llm_config, mock_search_bundle):
    mock_create = MagicMock(side_effect=Exception("unexpected"))
    module = types.SimpleNamespace(create_deep_agent=mock_create)

    with patch.dict(sys.modules, {"deepagents": module}):
        engine = AISearchEngine(llm_config, mock_search_bundle)
        engine.initialize()

    assert engine._agent is None


@pytest.mark.anyio
async def test_ai_search_run_success(llm_config, mock_search_bundle, fake_deepagents):
    # Real CompiledStateGraph shape: only .invoke, returns {"messages": [...]}.
    # #1237 regressed here because the code called the non-existent .run.
    agent = _FakeCompiledStateGraph("AI Summary Result")

    mock_search_bundle.search_messages.return_value = MessageSearchPage(
        messages=[Message(channel_id=1, message_id=1, text="hello", date=datetime.now(timezone.utc))],
        total=1,
    )

    fake_deepagents.return_value = agent
    engine = AISearchEngine(llm_config, mock_search_bundle)
    engine.initialize()

    res = await engine.search("hello")
    assert res.ai_summary == "AI Summary Result"
    assert res.total == 1
    # The query must actually reach the graph via .invoke({"messages": [...]}).
    assert agent.invoke_calls == [{"messages": [{"role": "user", "content": "hello"}]}]


@pytest.mark.anyio
async def test_ai_search_invoke_not_run_regression(llm_config, mock_search_bundle, fake_deepagents):
    """Guard against reintroducing the #1237 ``.run`` call.

    A langgraph ``CompiledStateGraph`` has no ``.run`` — accessing it raises
    ``AttributeError``, which ``search`` swallows into a degraded summary. This
    fake mirrors that: if the code ever calls ``.run`` again, the summary flips
    back to the "AI search error" fallback and this assertion fails.
    """
    agent = _FakeCompiledStateGraph("Structured answer")
    assert not hasattr(agent, "run")  # sanity: the fake has no .run, like the real graph

    fake_deepagents.return_value = agent
    engine = AISearchEngine(llm_config, mock_search_bundle)
    engine.initialize()

    res = await engine.search("hello")
    assert res.ai_summary == "Structured answer"
    assert "AI search error" not in res.ai_summary


@pytest.mark.anyio
async def test_ai_search_run_success_list_content(llm_config, mock_search_bundle, fake_deepagents):
    # Content blocks (Anthropic-style) must be flattened to text.
    agent = _FakeCompiledStateGraph([{"type": "text", "text": "Block one"}, {"type": "text", "text": "Block two"}])

    fake_deepagents.return_value = agent
    engine = AISearchEngine(llm_config, mock_search_bundle)
    engine.initialize()

    res = await engine.search("hello")
    assert res.ai_summary == "Block one\nBlock two"


@pytest.mark.anyio
async def test_ai_search_legacy_run_agent_still_supported(llm_config, mock_search_bundle, fake_deepagents):
    # hasattr-guard: backends/fakes that still expose .run keep working.
    legacy_agent = MagicMock()
    legacy_agent.run.return_value = "Legacy Result"

    fake_deepagents.return_value = legacy_agent
    engine = AISearchEngine(llm_config, mock_search_bundle)
    engine.initialize()

    res = await engine.search("hello")
    assert res.ai_summary == "Legacy Result"
    legacy_agent.run.assert_called_once_with("hello")
    legacy_agent.invoke.assert_not_called()


@pytest.mark.anyio
async def test_ai_search_run_error(llm_config, mock_search_bundle, fake_deepagents):
    agent = MagicMock(spec=["invoke"])
    agent.invoke.side_effect = Exception("AI Fail")

    fake_deepagents.return_value = agent
    engine = AISearchEngine(llm_config, mock_search_bundle)
    engine.initialize()

    res = await engine.search("hello")
    assert res.ai_summary is not None
    assert "AI search error: AI Fail" in res.ai_summary


@pytest.mark.anyio
async def test_search_posts_tool_logic(llm_config, mock_search_bundle, fake_deepagents):
    # This tests the tool function defined inside initialize
    mock_search_bundle.search_messages.return_value = MessageSearchPage(
        messages=[Message(channel_id=1, message_id=1, text="content", date=datetime.now(timezone.utc))],
        total=1,
    )

    engine = AISearchEngine(llm_config, mock_search_bundle)
    engine.initialize()

    # Extract the tool function from mock_create call
    _, kwargs = fake_deepagents.call_args
    tools = kwargs["tools"]
    search_tool = tools[0]

    # Test tool without running loop (simulating thread pool behavior)
    result = search_tool("query")
    assert "Found 1 results" in result
    assert "content" in result


@pytest.mark.anyio
async def test_search_posts_tool_no_results(llm_config, mock_search_bundle, fake_deepagents):
    mock_search_bundle.search_messages.return_value = MessageSearchPage(messages=[], total=0)

    engine = AISearchEngine(llm_config, mock_search_bundle)
    engine.initialize()
    search_tool = fake_deepagents.call_args[1]["tools"][0]

    result = search_tool("nothing")
    assert "No results found" in result
