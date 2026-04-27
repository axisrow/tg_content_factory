"""Tests for ReAct-style agent fallback logic and optional S3 storage."""

from __future__ import annotations

import json
import os
import urllib.error
from unittest.mock import MagicMock, patch

import pytest

from src.agent.react_agent import (
    _REACT_SUFFIX,
    _TOOL_CALL_RE,
    _chat_sync,
    _describe_tools,
    _MockMessage,
    _try_call_tool,
)
from src.services.s3_store import S3Store

# ---------------------------------------------------------------------------
# Helper functions for react_agent tests
# ---------------------------------------------------------------------------


def _make_response_with_json_in_markdown(json_content: str) -> str:
    """Create response with JSON in markdown code block."""
    return f"```json\n{json_content}\n```"


def _make_response_with_json_in_tg_block(json_content: str) -> str:
    """Create response with JSON in TG-style block."""
    return f"```json\n{json_content}\n```"


# ---------------------------------------------------------------------------
# Tests for react_agent module
# ---------------------------------------------------------------------------


class TestDescribeTools:
    """Tests for _describe_tools function."""

    def test_empty_tools_list(self):
        """Empty tools list returns empty string."""
        result = _describe_tools([])
        assert result == ""

    def test_single_tool_no_params(self):
        """Tool without parameters is described correctly."""
        def simple_tool():
            """A simple tool."""
            pass

        result = _describe_tools([simple_tool])
        assert "simple_tool()" in result
        assert "A simple tool" in result

    def test_tool_with_params(self):
        """Tool with parameters shows parameter names."""
        def tool_with_params(a: int, b: str):
            """Tool with params."""
            pass

        result = _describe_tools([tool_with_params])
        assert "tool_with_params(a, b)" in result

    def test_tool_with_self_param_excluded(self):
        """Self parameter is excluded from description."""
        def method_like(self, x: int):
            """Method-like function."""
            pass

        result = _describe_tools([method_like])
        assert "self" not in result
        assert "x" in result

    def test_tool_without_docstring(self):
        """Tool without docstring still shows name and params."""
        def no_doc_tool(x: int):
            pass

        result = _describe_tools([no_doc_tool])
        assert "no_doc_tool(x)" in result

    def test_multiple_tools(self):
        """Multiple tools are separated by newlines."""
        def tool_a():
            """Tool A."""
            pass

        def tool_b():
            """Tool B."""
            pass

        result = _describe_tools([tool_a, tool_b])
        assert "tool_a()" in result
        assert "tool_b()" in result
        assert "\n" in result


class TestTryCallTool:
    """Tests for _try_call_tool function."""

    def test_unknown_tool(self):
        """Unknown tool returns error message."""
        result = _try_call_tool("unknown", {}, {})
        assert result == "[Unknown tool: unknown]"

    def test_successful_tool_call(self):
        """Successful tool call returns stringified result."""
        def my_tool(x: int) -> str:
            return f"result: {x}"

        tool_map = {"my_tool": my_tool}
        result = _try_call_tool("my_tool", {"x": 42}, tool_map)
        assert result == "result: 42"

    def test_tool_call_with_exception(self):
        """Tool exception is caught and returned as error message."""
        def failing_tool():
            raise ValueError("something went wrong")

        tool_map = {"failing_tool": failing_tool}
        result = _try_call_tool("failing_tool", {}, tool_map)
        assert "[Tool error:" in result
        assert "something went wrong" in result

    def test_tool_call_with_various_args(self):
        """Tool called with various argument types."""
        def multi_arg_tool(a: int, b: str, c: list) -> str:
            return f"{a}-{b}-{len(c)}"

        tool_map = {"multi_arg_tool": multi_arg_tool}
        result = _try_call_tool("multi_arg_tool", {"a": 1, "b": "test", "c": [1, 2, 3]}, tool_map)
        assert result == "1-test-3"


class TestToolCallRegex:
    """Tests for _TOOL_CALL_RE regex pattern."""

    def test_match_json_in_markdown_block(self):
        """Matches JSON in markdown code block."""
        response = _make_response_with_json_in_markdown('{"tool": "test", "args": {"x": 1}}')
        match = _TOOL_CALL_RE.search(response)
        assert match is not None
        json_str = match.group(1) or match.group(2)
        data = json.loads(json_str)
        assert data["tool"] == "test"
        assert data["args"] == {"x": 1}

    def test_no_match_without_block(self):
        """No match when JSON is not in a code block."""
        response = "plain text without tool call"
        match = _TOOL_CALL_RE.search(response)
        assert match is None

    def test_match_multiline_json(self):
        """Matches multiline JSON in block."""
        response = "```json\n{\"tool\": \"test\",\n\"args\": {\"x\": 1}}\n```"
        match = _TOOL_CALL_RE.search(response)
        assert match is not None


class TestChatSync:
    """Tests for _chat_sync function."""

    def test_successful_request(self):
        """Successful request returns message content."""
        mock_response = MagicMock()
        mock_response.read.return_value = b'{"message": {"content": "Hello!"}}'
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response):
            result = _chat_sync("http://localhost", "model", [{"role": "user", "content": "hi"}])
            assert result == "Hello!"

    def test_successful_request_missing_content(self):
        """Request with missing content field returns stringified data."""
        mock_response = MagicMock()
        mock_response.read.return_value = b'{"other": "data"}'
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response):
            result = _chat_sync("http://localhost", "model", [{"role": "user", "content": "hi"}])
            assert "other" in result

    def test_api_key_in_headers(self):
        """API key is included in Authorization header when provided."""
        mock_response = MagicMock()
        mock_response.read.return_value = b'{"message": {"content": "ok"}}'
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response) as mock_urlopen:
            _chat_sync("http://localhost", "model", [{"role": "user", "content": "hi"}], api_key="secret")

            # Check that Request was created with auth header
            call_args = mock_urlopen.call_args
            request = call_args[0][0]
            assert request.headers.get("Authorization") == "Bearer secret"

    def test_http_error_raises_runtime_error(self):
        """HTTP error is raised as RuntimeError with status code."""
        http_error = urllib.error.HTTPError(
            url="http://localhost/api/chat",
            code=500,
            hdrs=None,
            fp=None,
            msg="Internal Server Error",
        )
        http_error.read = MagicMock(return_value=b'{"error": "server error"}')

        with patch("urllib.request.urlopen", side_effect=http_error):
            with pytest.raises(RuntimeError, match="500"):
                _chat_sync("http://localhost", "model", [{"role": "user", "content": "hi"}])

    def test_base_url_trailing_slash_stripped(self):
        """Trailing slash in base_url is handled correctly."""
        mock_response = MagicMock()
        mock_response.read.return_value = b'{"message": {"content": "ok"}}'
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response) as mock_urlopen:
            _chat_sync("http://localhost/", "model", [{"role": "user", "content": "hi"}])

            call_args = mock_urlopen.call_args
            request = call_args[0][0]
            assert request.full_url == "http://localhost/api/chat"


class TestMockMessage:
    """Tests for _MockMessage class."""

    def test_content_attribute(self):
        """_MockMessage stores content in attribute."""
        msg = _MockMessage("test content")
        assert msg.content == "test content"

    def test_content_various_types(self):
        """_MockMessage can store various content types."""
        msg = _MockMessage("")
        assert msg.content == ""

        msg2 = _MockMessage("multi\nline\ncontent")
        assert msg2.content == "multi\nline\ncontent"


class TestOllamaReActAgent:
    """Tests for OllamaReActAgent class."""

    def test_init_system_prompt_includes_tools(self):
        """System prompt includes tool descriptions."""
        from src.agent.react_agent import OllamaReActAgent

        def tool_a():
            """Tool A."""
            pass

        agent = OllamaReActAgent(
            base_url="http://localhost",
            model="test",
            tools=[tool_a],
            system_prompt="Base prompt",
        )

        assert "Base prompt" in agent._system
        assert "tool_a" in agent._system
        assert _REACT_SUFFIX in agent._system

    def test_init_empty_tools(self):
        """Agent initializes with empty tools list."""
        from src.agent.react_agent import OllamaReActAgent

        agent = OllamaReActAgent(
            base_url="http://localhost",
            model="test",
            tools=[],
            system_prompt="Base prompt",
        )

        assert "Base prompt" in agent._system
        assert _REACT_SUFFIX in agent._system

    def test_invoke_no_tool_call(self):
        """Invoke without tool call returns final response."""
        from src.agent.react_agent import OllamaReActAgent

        mock_response = MagicMock()
        mock_response.read.return_value = b'{"message": {"content": "Final answer"}}'
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response):
            agent = OllamaReActAgent(
                base_url="http://localhost",
                model="test",
                tools=[],
                system_prompt="test",
            )
            result = agent.invoke({"messages": [{"role": "user", "content": "hi"}]})

            assert result["messages"][0].content == "Final answer"

    def test_invoke_with_tool_call(self):
        """Invoke with tool call executes tool and continues."""
        from src.agent.react_agent import OllamaReActAgent

        def test_tool(x: int) -> str:
            return f"tool result: {x}"

        # First call returns tool call, second returns final answer
        responses = [
            b'{"message": {"content": "```json\\n{\\"tool\\": \\"test_tool\\", \\"args\\": {\\"x\\": 42}}\\n```"}}',
            b'{"message": {"content": "Final after tool"}}',
        ]
        response_iter = iter(responses)

        mock_response = MagicMock()
        mock_response.read.side_effect = lambda: next(response_iter)
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response):
            agent = OllamaReActAgent(
                base_url="http://localhost",
                model="test",
                tools=[test_tool],
                system_prompt="test",
            )
            result = agent.invoke({"messages": [{"role": "user", "content": "hi"}]})

            assert result["messages"][0].content == "Final after tool"

    def test_invoke_unknown_tool(self):
        """Invoke with unknown tool returns error message."""
        from src.agent.react_agent import OllamaReActAgent

        responses = [
            b'{"message": {"content": "```json\\n{\\"tool\\": \\"unknown\\", \\"args\\": {}}\\n```"}}',
            b'{"message": {"content": "Done"}}',
        ]
        response_iter = iter(responses)

        mock_response = MagicMock()
        mock_response.read.side_effect = lambda: next(response_iter)
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response):
            agent = OllamaReActAgent(
                base_url="http://localhost",
                model="test",
                tools=[],
                system_prompt="test",
            )
            result = agent.invoke({"messages": [{"role": "user", "content": "hi"}]})

            assert result["messages"][0].content == "Done"

    def test_invoke_max_steps_limit(self):
        """Max steps limit is enforced."""
        from src.agent.react_agent import OllamaReActAgent

        def dummy_tool() -> str:
            return "ok"

        # Always return tool call - but max_steps should stop it
        mock_response = MagicMock()
        mock_response.read.return_value = (
            b'{"message": {"content": "```json\\n'
            b'{\\"tool\\": \\"dummy_tool\\", \\"args\\": {}}\\n```"}}'
        )
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response) as mock_urlopen:
            agent = OllamaReActAgent(
                base_url="http://localhost",
                model="test",
                tools=[dummy_tool],
                system_prompt="test",
                max_steps=2,
            )
            agent.invoke({"messages": [{"role": "user", "content": "hi"}]})

            # max_steps=2 means 2 tool calls + 1 final call = 3 total
            assert mock_urlopen.call_count == 3

    def test_invoke_invalid_json_in_block(self):
        """Invalid JSON in tool call block returns response as-is."""
        from src.agent.react_agent import OllamaReActAgent

        mock_response = MagicMock()
        mock_response.read.return_value = b'{"message": {"content": "```json\\n{invalid json}\\n```"}}'
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response):
            agent = OllamaReActAgent(
                base_url="http://localhost",
                model="test",
                tools=[],
                system_prompt="test",
            )
            result = agent.invoke({"messages": [{"role": "user", "content": "hi"}]})

            # Invalid JSON should cause return of the response
            assert "invalid json" in result["messages"][0].content


# ---------------------------------------------------------------------------
# Tests for s3_store module
# ---------------------------------------------------------------------------


class TestS3StoreFromEnv:
    """Tests for S3Store.from_env class method."""

    def test_from_env_missing_all_vars(self):
        """from_env returns None when no S3 vars are set."""
        with patch.dict(os.environ, {}, clear=True):
            # Remove any existing S3 vars
            for key in ["S3_ENDPOINT", "S3_BUCKET", "S3_ACCESS_KEY", "S3_SECRET_KEY"]:
                os.environ.pop(key, None)
            result = S3Store.from_env()
            assert result is None

    def test_from_env_partial_config(self):
        """from_env returns None when only some S3 vars are set."""
        with patch.dict(
            os.environ,
            {"S3_ENDPOINT": "https://example.com", "S3_BUCKET": "bucket"},
            clear=True,
        ):
            result = S3Store.from_env()
            assert result is None

    def test_from_env_full_config(self):
        """from_env returns S3Store when all S3 vars are set."""
        env = {
            "S3_ENDPOINT": "https://s3.example.com",
            "S3_BUCKET": "test-bucket",
            "S3_ACCESS_KEY": "access123",
            "S3_SECRET_KEY": "secret456",
        }
        with patch.dict(os.environ, env, clear=True):
            result = S3Store.from_env()
            assert result is not None
            assert result._endpoint == "https://s3.example.com"
            assert result._bucket == "test-bucket"
            assert result._access_key == "access123"
            assert result._secret_key == "secret456"

    def test_from_env_trailing_slash_stripped(self):
        """Endpoint trailing slash is removed."""
        env = {
            "S3_ENDPOINT": "https://s3.example.com/",
            "S3_BUCKET": "bucket",
            "S3_ACCESS_KEY": "key",
            "S3_SECRET_KEY": "secret",
        }
        with patch.dict(os.environ, env, clear=True):
            result = S3Store.from_env()
            assert result is not None
            assert result._endpoint == "https://s3.example.com"


class TestS3StoreInit:
    """Tests for S3Store initialization."""

    def test_init_stores_all_params(self):
        """All parameters are stored correctly."""
        store = S3Store(
            endpoint="https://example.com",
            bucket="my-bucket",
            access_key="key",
            secret_key="secret",
        )
        assert store._endpoint == "https://example.com"
        assert store._bucket == "my-bucket"
        assert store._access_key == "key"
        assert store._secret_key == "secret"

    def test_init_strips_trailing_slash(self):
        """Trailing slash in endpoint is stripped."""
        store = S3Store(
            endpoint="https://example.com/",
            bucket="bucket",
            access_key="key",
            secret_key="secret",
        )
        assert store._endpoint == "https://example.com"


class TestS3StoreUploadFile:
    """Tests for S3Store.upload_file method."""

    @pytest.mark.asyncio
    async def test_upload_file_import_error(self):
        """upload_file returns None when boto3 is not installed."""
        store = S3Store(
            endpoint="https://example.com",
            bucket="bucket",
            access_key="key",
            secret_key="secret",
        )

        with patch.dict("sys.modules", {"boto3": None, "botocore.config": None}):
            result = await store.upload_file("/tmp/test.jpg")
            assert result is None

    @pytest.mark.asyncio
    async def test_upload_file_boto3_exception(self):
        """upload_file returns None on boto3 exception."""
        store = S3Store(
            endpoint="https://example.com",
            bucket="bucket",
            access_key="key",
            secret_key="secret",
        )

        mock_boto3 = MagicMock()
        mock_client = MagicMock()
        mock_client.upload_file.side_effect = Exception("S3 error")
        mock_boto3.client.return_value = mock_client

        mock_config = MagicMock()

        with patch.dict(
            "sys.modules",
            {"boto3": mock_boto3, "botocore.config": mock_config, "botocore": MagicMock()},
        ):
            result = await store.upload_file("/tmp/test.jpg")
            assert result is None

    @pytest.mark.asyncio
    async def test_upload_file_success(self):
        """upload_file returns URL on successful upload."""
        store = S3Store(
            endpoint="https://s3.example.com",
            bucket="my-bucket",
            access_key="key",
            secret_key="secret",
        )

        mock_boto3 = MagicMock()
        mock_client = MagicMock()
        mock_client.upload_file.return_value = None
        mock_boto3.client.return_value = mock_client

        mock_config = MagicMock()

        modules = {
            "boto3": mock_boto3,
            "botocore": MagicMock(),
            "botocore.config": mock_config,
        }

        with patch.dict("sys.modules", modules):
            result = await store.upload_file("/tmp/test_image.jpg")
            assert result == "https://s3.example.com/my-bucket/test_image.jpg"
            mock_client.upload_file.assert_called_once()

    @pytest.mark.asyncio
    async def test_upload_file_uses_basename(self):
        """upload_file uses basename of path as S3 key."""
        store = S3Store(
            endpoint="https://example.com",
            bucket="bucket",
            access_key="key",
            secret_key="secret",
        )

        mock_boto3 = MagicMock()
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client

        modules = {
            "boto3": mock_boto3,
            "botocore": MagicMock(),
            "botocore.config": MagicMock(),
        }

        with patch.dict("sys.modules", modules):
            await store.upload_file("/some/deep/path/to/file.png")
            # Check that only basename was used as key
            call_args = mock_client.upload_file.call_args
            assert call_args[0][2] == "file.png"
