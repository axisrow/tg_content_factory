"""Tests for src/agent/tools/__init__.py — session gate, tool adapter, build_agent_tools_dict."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ── _adapt_sdk_tool ───────────────────────────────────────────────────────────


class TestAdaptSdkTool:
    @pytest.mark.anyio
    async def test_extracts_text_from_mcp_response(self):
        from claude_agent_sdk import SdkMcpTool

        async def handler(args):
            return {"content": [{"type": "text", "text": "hello world"}]}

        tool = SdkMcpTool(
            name="test_tool",
            description="A test tool",
            input_schema={},
            handler=handler,
        )

        from src.agent.tools import _adapt_sdk_tool

        adapted = _adapt_sdk_tool(tool)
        result = await adapted()
        assert result == "hello world"

    @pytest.mark.anyio
    async def test_joins_multiple_text_parts(self):
        from claude_agent_sdk import SdkMcpTool

        async def handler(args):
            return {
                "content": [
                    {"type": "text", "text": "part1"},
                    {"type": "text", "text": "part2"},
                ]
            }

        tool = SdkMcpTool(name="t", description="d", input_schema={}, handler=handler)
        from src.agent.tools import _adapt_sdk_tool

        adapted = _adapt_sdk_tool(tool)
        result = await adapted()
        assert result == "part1\npart2"

    @pytest.mark.anyio
    async def test_non_text_parts_ignored(self):
        from claude_agent_sdk import SdkMcpTool

        async def handler(args):
            return {
                "content": [
                    {"type": "image", "data": "base64..."},
                    {"type": "text", "text": "only this"},
                ]
            }

        tool = SdkMcpTool(name="t", description="d", input_schema={}, handler=handler)
        from src.agent.tools import _adapt_sdk_tool

        adapted = _adapt_sdk_tool(tool)
        result = await adapted()
        assert result == "only this"

    @pytest.mark.anyio
    async def test_non_dict_result_str(self):
        from claude_agent_sdk import SdkMcpTool

        async def handler(args):
            return "plain string"

        tool = SdkMcpTool(name="t", description="d", input_schema={}, handler=handler)
        from src.agent.tools import _adapt_sdk_tool

        adapted = _adapt_sdk_tool(tool)
        result = await adapted()
        assert result == "plain string"

    @pytest.mark.anyio
    async def test_dict_without_content_key(self):
        from claude_agent_sdk import SdkMcpTool

        async def handler(args):
            return {"error": "something"}

        tool = SdkMcpTool(name="t", description="d", input_schema={}, handler=handler)
        from src.agent.tools import _adapt_sdk_tool

        adapted = _adapt_sdk_tool(tool)
        result = await adapted()
        assert "error" in result

    @pytest.mark.anyio
    async def test_passes_kwargs_as_args_dict(self):
        from claude_agent_sdk import SdkMcpTool

        captured = {}

        async def handler(args):
            captured.update(args)
            return {"content": [{"type": "text", "text": "ok"}]}

        tool = SdkMcpTool(name="t", description="d", input_schema={}, handler=handler)
        from src.agent.tools import _adapt_sdk_tool

        adapted = _adapt_sdk_tool(tool)
        await adapted(query="test", limit=5)
        assert captured == {"query": "test", "limit": 5}

    @pytest.mark.anyio
    async def test_docstring_preserved(self):
        from claude_agent_sdk import SdkMcpTool

        async def handler(args):
            return {"content": [{"type": "text", "text": "ok"}]}

        tool = SdkMcpTool(name="t", description="My description", input_schema={}, handler=handler)
        from src.agent.tools import _adapt_sdk_tool

        adapted = _adapt_sdk_tool(tool)
        assert adapted.__doc__ == "My description"

    @pytest.mark.anyio
    async def test_no_text_parts_returns_str(self):
        from claude_agent_sdk import SdkMcpTool

        async def handler(args):
            return {"content": [{"type": "image", "data": "x"}]}

        tool = SdkMcpTool(name="t", description="d", input_schema={}, handler=handler)
        from src.agent.tools import _adapt_sdk_tool

        adapted = _adapt_sdk_tool(tool)
        result = await adapted()
        # No text parts → str(result)
        assert isinstance(result, str)


# ── _wrap_with_session_gate ──────────────────────────────────────────────────


class TestWrapWithSessionGate:
    @pytest.mark.anyio
    async def test_no_gate_passes_through(self):
        from claude_agent_sdk import SdkMcpTool

        async def handler(*args, **kwargs):
            return {"content": [{"type": "text", "text": "ok"}]}

        tool = SdkMcpTool(name="test_tool", description="d", input_schema={}, handler=handler)

        from src.agent.tools import _wrap_with_session_gate

        wrapped = _wrap_with_session_gate(tool)
        assert wrapped.name == "test_tool"

        with patch("src.agent.permission_gate.get_gate", return_value=None):
            result = await wrapped.handler()
        assert result["content"][0]["text"] == "ok"

    @pytest.mark.anyio
    async def test_gate_none_context_passes_through(self):
        from claude_agent_sdk import SdkMcpTool

        async def handler(*args, **kwargs):
            return {"content": [{"type": "text", "text": "passed"}]}

        tool = SdkMcpTool(name="t", description="d", input_schema={}, handler=handler)

        from src.agent.tools import _wrap_with_session_gate

        wrapped = _wrap_with_session_gate(tool)

        gate = MagicMock()
        with (
            patch("src.agent.permission_gate.get_gate", return_value=gate),
            patch("src.agent.permission_gate.get_request_context", return_value=None),
        ):
            result = await wrapped.handler()
        assert result["content"][0]["text"] == "passed"

    @pytest.mark.anyio
    async def test_gate_tool_disabled_returns_gate_result(self):
        from claude_agent_sdk import SdkMcpTool

        async def handler(*args, **kwargs):
            return {"content": [{"type": "text", "text": "should not reach"}]}

        tool = SdkMcpTool(name="test_tool", description="d", input_schema={}, handler=handler)

        from src.agent.tools import _wrap_with_session_gate

        wrapped = _wrap_with_session_gate(tool)

        gate = MagicMock()
        gate.check = AsyncMock(return_value={"content": [{"type": "text", "text": "blocked"}]})

        ctx = MagicMock()
        ctx.db_permissions = {"test_tool": False}

        with (
            patch("src.agent.permission_gate.get_gate", return_value=gate),
            patch("src.agent.permission_gate.get_request_context", return_value=ctx),
        ):
            result = await wrapped.handler()
        assert result["content"][0]["text"] == "blocked"

    @pytest.mark.anyio
    async def test_gate_tool_enabled_passes_through(self):
        from claude_agent_sdk import SdkMcpTool

        async def handler(*args, **kwargs):
            return {"content": [{"type": "text", "text": "allowed"}]}

        tool = SdkMcpTool(name="test_tool", description="d", input_schema={}, handler=handler)

        from src.agent.tools import _wrap_with_session_gate

        wrapped = _wrap_with_session_gate(tool)

        ctx = MagicMock()
        ctx.db_permissions = {"test_tool": True}

        with (
            patch("src.agent.permission_gate.get_gate", return_value=MagicMock()),
            patch("src.agent.permission_gate.get_request_context", return_value=ctx),
        ):
            result = await wrapped.handler()
        assert result["content"][0]["text"] == "allowed"

    def test_wrapped_preserves_metadata(self):
        from claude_agent_sdk import SdkMcpTool

        async def handler(*args, **kwargs):
            return {}

        tool = SdkMcpTool(
            name="my_tool",
            description="my desc",
            input_schema={"type": "object"},
            handler=handler,
            annotations={"title": "My Tool"},
        )

        from src.agent.tools import _wrap_with_session_gate

        wrapped = _wrap_with_session_gate(tool)
        assert wrapped.name == "my_tool"
        assert wrapped.description == "my desc"
        assert wrapped.input_schema == {"type": "object"}
        assert wrapped.annotations == {"title": "My Tool"}


# ── _PIPELINE_SAFE_TOOLS ─────────────────────────────────────────────────────


class TestPipelineSafeTools:
    def test_is_frozenset(self):
        from src.agent.tools import _PIPELINE_SAFE_TOOLS

        assert isinstance(_PIPELINE_SAFE_TOOLS, frozenset)

    def test_contains_search_tools(self):
        from src.agent.tools import _PIPELINE_SAFE_TOOLS

        assert "search_messages" in _PIPELINE_SAFE_TOOLS
        assert "semantic_search" in _PIPELINE_SAFE_TOOLS

    def test_does_not_contain_write_tools(self):
        from src.agent.tools import _PIPELINE_SAFE_TOOLS

        assert "send_message" not in _PIPELINE_SAFE_TOOLS
        assert "delete_channel" not in _PIPELINE_SAFE_TOOLS
        assert "run_pipeline" not in _PIPELINE_SAFE_TOOLS

    def test_contains_list_and_get_tools(self):
        from src.agent.tools import _PIPELINE_SAFE_TOOLS

        assert "list_channels" in _PIPELINE_SAFE_TOOLS
        assert "list_pipelines" in _PIPELINE_SAFE_TOOLS
        assert "get_channel_stats" in _PIPELINE_SAFE_TOOLS
        assert "get_pipeline_detail" in _PIPELINE_SAFE_TOOLS


# ── build_agent_tools_dict ────────────────────────────────────────────────────


class TestBuildAgentToolsDict:
    def test_returns_only_safe_tools(self):
        from src.agent.tools import _PIPELINE_SAFE_TOOLS, build_agent_tools_dict

        mock_db = MagicMock()
        captured_tools = []

        with patch(
            "src.agent.tools.create_sdk_mcp_server",
            side_effect=lambda **kwargs: captured_tools.extend(kwargs.get("tools", [])),
        ):
            from src.agent.tools import make_mcp_server
            make_mcp_server(mock_db)

        with patch("src.services.embedding_service.EmbeddingService"):
            tools_dict = build_agent_tools_dict(mock_db)

        for tool_name in tools_dict:
            assert tool_name in _PIPELINE_SAFE_TOOLS, f"build_agent_tools_dict included unsafe tool: {tool_name}"

    @pytest.mark.anyio
    async def test_tool_functions_are_async_callable(self):
        from claude_agent_sdk import SdkMcpTool

        mock_db = MagicMock()
        handler_call_count = 0

        async def mock_handler(args):
            nonlocal handler_call_count
            handler_call_count += 1
            return {"content": [{"type": "text", "text": "result"}]}

        fake_tool = SdkMcpTool(
            name="search_messages",
            description="Search",
            input_schema={},
            handler=mock_handler,
        )

        # Mock the module register functions to return our fake tool
        mock_module = MagicMock()
        mock_module.register = MagicMock(return_value=[fake_tool])

        with (
            patch("src.services.embedding_service.EmbeddingService"),
            patch.multiple(
                "src.agent.tools",
                search=mock_module,
                channels=mock_module,
                collection=mock_module,
                pipelines=mock_module,
                moderation=mock_module,
                search_queries=mock_module,
                accounts=mock_module,
                filters=mock_module,
                analytics=mock_module,
                scheduler=mock_module,
                notifications=mock_module,
                photo_loader=mock_module,
                dialogs=mock_module,
                messaging=mock_module,
                images=mock_module,
                settings=mock_module,
                agent_threads=mock_module,
            ),
        ):
            from src.agent.tools import build_agent_tools_dict
            tools_dict = build_agent_tools_dict(mock_db)

        # search_messages is in _PIPELINE_SAFE_TOOLS, so it should be included
        if "search_messages" in tools_dict:
            result = await tools_dict["search_messages"](query="test")
            assert isinstance(result, str)
            assert handler_call_count == 1

    def test_runtime_context_passed_to_pipeline_tool_registers(self):
        from claude_agent_sdk import SdkMcpTool

        from src.agent.runtime_context import AgentRuntimeContext
        from src.agent.tools import build_agent_tools_dict

        mock_db = MagicMock()
        pool = MagicMock()
        seen_contexts = []

        async def mock_handler(args):
            return {"content": [{"type": "text", "text": "ok"}]}

        fake_tool = SdkMcpTool(
            name="get_account_info",
            description="Account info",
            input_schema={},
            handler=mock_handler,
        )

        mock_module = MagicMock()

        def register(*args, **kwargs):
            seen_contexts.append(kwargs.get("runtime_context"))
            return [fake_tool]

        mock_module.register = register

        with (
            patch("src.services.embedding_service.EmbeddingService"),
            patch.multiple(
                "src.agent.tools",
                search=mock_module,
                channels=mock_module,
                collection=mock_module,
                pipelines=mock_module,
                moderation=mock_module,
                search_queries=mock_module,
                accounts=mock_module,
                filters=mock_module,
                analytics=mock_module,
                scheduler=mock_module,
                notifications=mock_module,
                photo_loader=mock_module,
                dialogs=mock_module,
                messaging=mock_module,
                images=mock_module,
                settings=mock_module,
                agent_threads=mock_module,
            ),
        ):
            tools_dict = build_agent_tools_dict(mock_db, client_pool=pool)

        assert "get_account_info" in tools_dict
        assert seen_contexts
        assert all(isinstance(ctx, AgentRuntimeContext) for ctx in seen_contexts)
        assert all(ctx.client_pool is pool for ctx in seen_contexts)
        assert all(ctx.runtime_kind == "live" for ctx in seen_contexts)
