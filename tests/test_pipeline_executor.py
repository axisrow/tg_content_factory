"""Tests for pipeline_executor: topological sort, downstream BFS, and execute()."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.models import (
    ContentPipeline,
    PipelineEdge,
    PipelineGraph,
    PipelineNode,
    PipelineNodeType,
    PipelinePublishMode,
)
from src.services.pipeline_executor import PipelineExecutor, _topological_sort
from src.services.pipeline_nodes.base import NodeContext

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _node(nid: str, ntype: PipelineNodeType = PipelineNodeType.LLM_GENERATE) -> PipelineNode:
    return PipelineNode(id=nid, type=ntype, name=nid, config={})


def _edge(fr: str, to: str) -> PipelineEdge:
    return PipelineEdge(from_node=fr, to_node=to)


def _pipeline(**overrides) -> ContentPipeline:
    defaults = dict(
        name="test-pipeline",
        prompt_template="write something",
        llm_model="test-model",
        publish_mode=PipelinePublishMode.MODERATED,
    )
    defaults.update(overrides)
    return ContentPipeline(**defaults)


def _make_handler(side_effect=None):
    """Return a fake handler with an async execute method."""
    handler = AsyncMock()
    if side_effect is not None:
        handler.execute.side_effect = side_effect
    return handler


# ---------------------------------------------------------------------------
# 1. _topological_sort — linear chain A -> B -> C
# ---------------------------------------------------------------------------

class TestTopologicalSortLinear:
    def test_linear_chain(self):
        graph = PipelineGraph(
            nodes=[_node("a"), _node("b"), _node("c")],
            edges=[_edge("a", "b"), _edge("b", "c")],
        )
        order = _topological_sort(graph)
        ids = [n.id for n in order]
        assert ids.index("a") < ids.index("b") < ids.index("c")


# ---------------------------------------------------------------------------
# 2. _topological_sort — diamond A->B, A->C, B->D, C->D
# ---------------------------------------------------------------------------

class TestTopologicalSortDiamond:
    def test_diamond(self):
        graph = PipelineGraph(
            nodes=[_node("a"), _node("b"), _node("c"), _node("d")],
            edges=[_edge("a", "b"), _edge("a", "c"), _edge("b", "d"), _edge("c", "d")],
        )
        order = _topological_sort(graph)
        ids = [n.id for n in order]
        assert ids.index("a") < ids.index("b")
        assert ids.index("a") < ids.index("c")
        assert ids.index("b") < ids.index("d")
        assert ids.index("c") < ids.index("d")


# ---------------------------------------------------------------------------
# 3. _topological_sort — cycle falls back to original order
# ---------------------------------------------------------------------------

class TestTopologicalSortCycle:
    def test_cycle_returns_original_order(self):
        graph = PipelineGraph(
            nodes=[_node("x"), _node("y"), _node("z")],
            edges=[_edge("x", "y"), _edge("y", "z"), _edge("z", "x")],
        )
        order = _topological_sort(graph)
        ids = [n.id for n in order]
        # Should fall back to the original node list
        assert ids == ["x", "y", "z"]


# ---------------------------------------------------------------------------
# 4. _topological_sort — single node
# ---------------------------------------------------------------------------

class TestTopologicalSortSingleNode:
    def test_single_node(self):
        graph = PipelineGraph(nodes=[_node("solo")], edges=[])
        order = _topological_sort(graph)
        assert len(order) == 1
        assert order[0].id == "solo"


# ---------------------------------------------------------------------------
# 5. _topological_sort — disconnected nodes
# ---------------------------------------------------------------------------

class TestTopologicalSortDisconnected:
    def test_disconnected_nodes(self):
        graph = PipelineGraph(
            nodes=[_node("a"), _node("b"), _node("c")],
            edges=[_edge("a", "b")],  # c is disconnected
        )
        order = _topological_sort(graph)
        ids = [n.id for n in order]
        assert set(ids) == {"a", "b", "c"}
        assert ids.index("a") < ids.index("b")
        # c has no constraint — can be anywhere


# ---------------------------------------------------------------------------
# 6. _downstream_nodes — basic
# ---------------------------------------------------------------------------

class TestDownstreamNodesBasic:
    def test_downstream_chain(self):
        graph = PipelineGraph(
            nodes=[_node("a"), _node("b"), _node("c"), _node("d")],
            edges=[_edge("a", "b"), _edge("b", "c"), _edge("c", "d")],
        )
        result = PipelineExecutor._downstream_nodes(graph, "b")
        assert result == {"c", "d"}

    def test_downstream_branching(self):
        graph = PipelineGraph(
            nodes=[_node("a"), _node("b"), _node("c"), _node("d")],
            edges=[_edge("a", "b"), _edge("a", "c"), _edge("b", "d"), _edge("c", "d")],
        )
        result = PipelineExecutor._downstream_nodes(graph, "a")
        assert result == {"b", "c", "d"}


# ---------------------------------------------------------------------------
# 7. _downstream_nodes — no edges
# ---------------------------------------------------------------------------

class TestDownstreamNodesNoEdges:
    def test_no_edges_returns_empty(self):
        graph = PipelineGraph(
            nodes=[_node("a"), _node("b")],
            edges=[],
        )
        result = PipelineExecutor._downstream_nodes(graph, "a")
        assert result == set()

    def test_disconnected_start(self):
        graph = PipelineGraph(
            nodes=[_node("a"), _node("b"), _node("c")],
            edges=[_edge("b", "c")],
        )
        result = PipelineExecutor._downstream_nodes(graph, "a")
        assert result == set()


# ---------------------------------------------------------------------------
# 8. execute — happy path with mock handlers
# ---------------------------------------------------------------------------

class TestExecuteHappyPath:
    @pytest.mark.asyncio
    async def test_execute_sets_context_from_handlers(self):
        graph = PipelineGraph(
            nodes=[_node("n1"), _node("n2")],
            edges=[_edge("n1", "n2")],
        )
        pipeline = _pipeline()

        def fake_get_handler(node_type):
            # We return a different handler depending on which node type is requested.
            # Since both nodes are LLM_GENERATE, use a shared handler that checks calls.
            h = AsyncMock()

            async def _execute(config, ctx, services):
                ctx.set_global("generated_text", "hello world")
                ctx.set_global("image_url", "https://example.com/img.png")

            h.execute.side_effect = _execute
            return h

        with patch("src.services.pipeline_executor.get_handler", side_effect=fake_get_handler):
            executor = PipelineExecutor()
            result = await executor.execute(pipeline, graph, {})

        assert result["generated_text"] == "hello world"
        assert result["image_url"] == "https://example.com/img.png"
        assert isinstance(result["context"], NodeContext)


# ---------------------------------------------------------------------------
# 9. execute — condition node returns False, downstream skipped
# ---------------------------------------------------------------------------

class TestExecuteConditionSkip:
    @pytest.mark.asyncio
    async def test_condition_false_skips_downstream(self):
        graph = PipelineGraph(
            nodes=[
                _node("cond", PipelineNodeType.CONDITION),
                _node("gen", PipelineNodeType.LLM_GENERATE),
            ],
            edges=[_edge("cond", "gen")],
        )
        pipeline = _pipeline()

        call_log: list[str] = []

        def fake_get_handler(node_type):
            handler = AsyncMock()

            if node_type == PipelineNodeType.CONDITION:

                async def _cond(config, ctx, services):
                    call_log.append("cond")
                    ctx.set_global("condition_result", False)

                handler.execute.side_effect = _cond

            else:

                async def _gen(config, ctx, services):
                    call_log.append("gen")
                    ctx.set_global("generated_text", "should not run")

                handler.execute.side_effect = _gen

            return handler

        with patch("src.services.pipeline_executor.get_handler", side_effect=fake_get_handler):
            executor = PipelineExecutor()
            result = await executor.execute(pipeline, graph, {})

        assert "cond" in call_log
        assert "gen" not in call_log
        # generated_text stays at default ""
        assert result["generated_text"] == ""


# ---------------------------------------------------------------------------
# 10. execute — node failure propagates exception
# ---------------------------------------------------------------------------

class TestExecuteNodeFailure:
    @pytest.mark.asyncio
    async def test_exception_propagates(self):
        graph = PipelineGraph(
            nodes=[_node("bad")],
            edges=[],
        )
        pipeline = _pipeline()

        def fake_get_handler(node_type):
            handler = AsyncMock()

            async def _fail(config, ctx, services):
                raise RuntimeError("node exploded")

            handler.execute.side_effect = _fail
            return handler

        with patch("src.services.pipeline_executor.get_handler", side_effect=fake_get_handler):
            executor = PipelineExecutor()
            with pytest.raises(RuntimeError, match="node exploded"):
                await executor.execute(pipeline, graph, {})


# ---------------------------------------------------------------------------
# 11. execute — seeds initial values from pipeline model
# ---------------------------------------------------------------------------

class TestExecuteSeedsContext:
    @pytest.mark.asyncio
    async def test_pipeline_fields_seeded_into_context(self):
        graph = PipelineGraph(
            nodes=[_node("n1")],
            edges=[],
        )
        pipeline = _pipeline(
            prompt_template="my prompt",
            llm_model="gpt-4o",
        )

        captured_context: NodeContext | None = None

        def fake_get_handler(node_type):
            handler = AsyncMock()

            async def _inspect(config, ctx, services):
                nonlocal captured_context
                captured_context = ctx

            handler.execute.side_effect = _inspect
            return handler

        with patch("src.services.pipeline_executor.get_handler", side_effect=fake_get_handler):
            executor = PipelineExecutor()
            result = await executor.execute(pipeline, graph, {})

        assert captured_context is not None
        assert captured_context.get_global("prompt_template") == "my prompt"
        assert captured_context.get_global("generation_query") == "my prompt"
        assert captured_context.get_global("default_model") == "gpt-4o"
        # publish_mode falls back to pipeline's value
        assert result["publish_mode"] == PipelinePublishMode.MODERATED.value
