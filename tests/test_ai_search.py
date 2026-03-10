import sys
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

@pytest.mark.asyncio
async def test_ai_search_disabled(mock_search_bundle):
    config = LLMConfig(enabled=False)
    engine = AISearchEngine(config, mock_search_bundle)
    engine.initialize()
    assert engine._agent is None

    res = await engine.search("test")
    assert "AI search is not available" in res.ai_summary

@pytest.mark.asyncio
async def test_ai_search_initialization_success(llm_config, mock_search_bundle):
    with patch("deepagents.create_deep_agent") as mock_create:
        mock_agent = MagicMock()
        mock_create.return_value = mock_agent

        engine = AISearchEngine(llm_config, mock_search_bundle)
        engine.initialize()

        assert engine._agent == mock_agent
        mock_create.assert_called_once()

@pytest.mark.asyncio
async def test_ai_search_initialization_import_error(llm_config, mock_search_bundle):
    with patch.dict(sys.modules, {"deepagents": None}):
        engine = AISearchEngine(llm_config, mock_search_bundle)
        engine.initialize()
        assert engine._agent is None

@pytest.mark.asyncio
async def test_ai_search_initialization_general_error(llm_config, mock_search_bundle):
    with patch("deepagents.create_deep_agent", side_effect=Exception("unexpected")):
         engine = AISearchEngine(llm_config, mock_search_bundle)
         engine.initialize()
         assert engine._agent is None

@pytest.mark.asyncio
async def test_ai_search_run_success(llm_config, mock_search_bundle):
    mock_agent = MagicMock()
    mock_agent.run.return_value = "AI Summary Result"

    mock_search_bundle.search_messages.return_value = ([
        Message(channel_id=1, message_id=1, text="hello", date=datetime.now())
    ], 1)

    with patch("deepagents.create_deep_agent", return_value=mock_agent):
        engine = AISearchEngine(llm_config, mock_search_bundle)
        engine.initialize()

        res = await engine.search("hello")
        assert res.ai_summary == "AI Summary Result"
        assert res.total == 1

@pytest.mark.asyncio
async def test_ai_search_run_error(llm_config, mock_search_bundle):
    mock_agent = MagicMock()
    mock_agent.run.side_effect = Exception("AI Fail")

    with patch("deepagents.create_deep_agent", return_value=mock_agent):
        engine = AISearchEngine(llm_config, mock_search_bundle)
        engine.initialize()

        res = await engine.search("hello")
        assert "AI search error: AI Fail" in res.ai_summary

@pytest.mark.asyncio
async def test_search_posts_tool_logic(llm_config, mock_search_bundle):
    # This tests the tool function defined inside initialize
    mock_search_bundle.search_messages.return_value = ([
        Message(channel_id=1, message_id=1, text="content", date=datetime.now())
    ], 1)

    with patch("deepagents.create_deep_agent") as mock_create:
        engine = AISearchEngine(llm_config, mock_search_bundle)
        engine.initialize()

        # Extract the tool function from mock_create call
        args, kwargs = mock_create.call_args
        tools = kwargs['tools']
        search_tool = tools[0]

        # Test tool without running loop (simulating thread pool behavior)
        result = search_tool("query")
        assert "Found 1 results" in result
        assert "content" in result

@pytest.mark.asyncio
async def test_search_posts_tool_no_results(llm_config, mock_search_bundle):
    mock_search_bundle.search_messages.return_value = ([], 0)

    with patch("deepagents.create_deep_agent") as mock_create:
        engine = AISearchEngine(llm_config, mock_search_bundle)
        engine.initialize()
        search_tool = mock_create.call_args[1]['tools'][0]

        result = search_tool("nothing")
        assert "No results found" in result
