import sys
import types
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import LLMConfig
from src.models import Message
from src.search.ai_search import AISearchEngine


@pytest.fixture
def llm_config():
    return LLMConfig(enabled=True, api_key="test_key", provider="anthropic", model="claude-3")


@pytest.fixture
def mock_search_bundle():
    bundle = MagicMock()
    bundle.search_messages = AsyncMock(return_value=([], 0))
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
    mock_agent = MagicMock()
    mock_agent.run.return_value = "AI Summary Result"

    mock_search_bundle.search_messages.return_value = (
        [Message(channel_id=1, message_id=1, text="hello", date=datetime.now())],
        1,
    )

    fake_deepagents.return_value = mock_agent
    engine = AISearchEngine(llm_config, mock_search_bundle)
    engine.initialize()

    res = await engine.search("hello")
    assert res.ai_summary == "AI Summary Result"
    assert res.total == 1


@pytest.mark.anyio
async def test_ai_search_run_error(llm_config, mock_search_bundle, fake_deepagents):
    mock_agent = MagicMock()
    mock_agent.run.side_effect = Exception("AI Fail")

    fake_deepagents.return_value = mock_agent
    engine = AISearchEngine(llm_config, mock_search_bundle)
    engine.initialize()

    res = await engine.search("hello")
    assert "AI search error: AI Fail" in res.ai_summary


@pytest.mark.anyio
async def test_search_posts_tool_logic(llm_config, mock_search_bundle, fake_deepagents):
    # This tests the tool function defined inside initialize
    mock_search_bundle.search_messages.return_value = (
        [Message(channel_id=1, message_id=1, text="content", date=datetime.now())],
        1,
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
    mock_search_bundle.search_messages.return_value = ([], 0)

    engine = AISearchEngine(llm_config, mock_search_bundle)
    engine.initialize()
    search_tool = fake_deepagents.call_args[1]["tools"][0]

    result = search_tool("nothing")
    assert "No results found" in result
