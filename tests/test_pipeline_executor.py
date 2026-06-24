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
# 8. execute — happy path with mock handlers
# ---------------------------------------------------------------------------

class TestExecuteHappyPath:
    @pytest.mark.anyio
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
    @pytest.mark.anyio
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

    @pytest.mark.anyio
    async def test_condition_false_does_not_skip_merge_reachable_via_live_branch(self):
        """Diamond DAG: cond->merge, src->fetch, fetch->merge, merge->publish with
        cond False. merge/publish must still run via the live src branch — the old
        flood-fill skip wrongly removed the whole subtree (audit #837/3)."""
        graph = PipelineGraph(
            nodes=[
                _node("cond", PipelineNodeType.CONDITION),
                _node("src"),
                _node("fetch"),
                _node("merge"),
                _node("publish"),
            ],
            edges=[
                _edge("cond", "merge"),
                _edge("src", "fetch"),
                _edge("fetch", "merge"),
                _edge("merge", "publish"),
            ],
        )
        pipeline = _pipeline()
        call_log: list[str] = []

        def fake_get_handler(node_type):
            handler = AsyncMock()

            async def _run(config, ctx, services):
                nid = services.get("_current_node_id")
                call_log.append(nid)
                if node_type == PipelineNodeType.CONDITION:
                    ctx.set_global("condition_result", False)

            handler.execute.side_effect = _run
            return handler

        with patch("src.services.pipeline_executor.get_handler", side_effect=fake_get_handler):
            result = await PipelineExecutor().execute(pipeline, graph, {})

        assert "cond" in call_log
        assert "merge" in call_log  # reachable via src->fetch->merge
        assert "publish" in call_log
        assert isinstance(result["context"], NodeContext)


# ---------------------------------------------------------------------------
# 10. execute — node failure propagates exception
# ---------------------------------------------------------------------------

class TestExecuteNodeFailure:
    @pytest.mark.anyio
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
    @pytest.mark.anyio
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
        assert captured_context.get_global("generation_query") == "test-pipeline"
        assert captured_context.get_global("channel_id") is None
        assert captured_context.get_global("default_model") == "gpt-4o"
        # publish_mode falls back to pipeline's value
        assert result["publish_mode"] == PipelinePublishMode.MODERATED.value


# ---------------------------------------------------------------------------
# 12. Issue #463 — result semantics per pipeline shape
# ---------------------------------------------------------------------------


class TestExecutorResultSemantics:
    """Verify executor.execute() returns correct result_kind/result_count for
    each pipeline shape: generation-only, action-only, mixed, empty-success.
    """

    @pytest.mark.anyio
    async def test_generation_only_graph_returns_generated_items(self):
        graph = PipelineGraph(
            nodes=[
                _node("src", PipelineNodeType.SOURCE),
                _node("gen", PipelineNodeType.LLM_GENERATE),
            ],
            edges=[_edge("src", "gen")],
        )
        pipeline = _pipeline()

        def fake_get_handler(node_type):
            h = AsyncMock()

            async def _execute(config, ctx, services):
                if node_type == PipelineNodeType.LLM_GENERATE:
                    ctx.set_global("generated_text", "draft text")
                    ctx.set_global("citations", [{"id": 1}, {"id": 2}])

            h.execute.side_effect = _execute
            return h

        with patch("src.services.pipeline_executor.get_handler", side_effect=fake_get_handler):
            executor = PipelineExecutor()
            result = await executor.execute(pipeline, graph, {})

        assert result["result_kind"] == "generated_items"
        assert result["result_count"] == 2
        assert result["generated_text"] == "draft text"

    @pytest.mark.anyio
    async def test_mixed_graph_generation_wins_but_action_counts_preserved(self):
        from src.services.pipeline_result import increment_action_count

        graph = PipelineGraph(
            nodes=[
                _node("src", PipelineNodeType.SOURCE),
                _node("gen", PipelineNodeType.LLM_GENERATE),
                _node("react", PipelineNodeType.REACT),
            ],
            edges=[_edge("src", "gen"), _edge("gen", "react")],
        )
        pipeline = _pipeline()

        def fake_get_handler(node_type):
            h = AsyncMock()

            async def _execute(config, ctx, services):
                if node_type == PipelineNodeType.LLM_GENERATE:
                    ctx.set_global("generated_text", "mixed draft")
                    ctx.set_global("citations", [{"id": 1}])
                if node_type == PipelineNodeType.REACT:
                    increment_action_count(ctx, "react", amount=3)

            h.execute.side_effect = _execute
            return h

        with patch("src.services.pipeline_executor.get_handler", side_effect=fake_get_handler):
            executor = PipelineExecutor()
            result = await executor.execute(pipeline, graph, {})

        # Generation wins result_kind/result_count per issue #463.
        assert result["result_kind"] == "generated_items"
        assert result["result_count"] == 1
        # But action counts remain visible for UI/metadata consumers.
        assert result["action_counts"] == {"react": 3}

    @pytest.mark.anyio
    async def test_action_only_graph_with_empty_text_is_not_zero_result(self):
        """Regression: empty generated_text MUST NOT collapse result_count to 0."""
        from src.services.pipeline_result import increment_action_count

        graph = PipelineGraph(
            nodes=[
                _node("src", PipelineNodeType.SOURCE),
                _node("react", PipelineNodeType.REACT),
            ],
            edges=[_edge("src", "react")],
        )
        pipeline = _pipeline()

        def fake_get_handler(node_type):
            h = AsyncMock()

            async def _execute(config, ctx, services):
                if node_type == PipelineNodeType.REACT:
                    increment_action_count(ctx, "react", amount=5)

            h.execute.side_effect = _execute
            return h

        with patch("src.services.pipeline_executor.get_handler", side_effect=fake_get_handler):
            executor = PipelineExecutor()
            result = await executor.execute(pipeline, graph, {})

        assert result["result_kind"] == "processed_messages"
        assert result["result_count"] == 5
        assert (result.get("generated_text") or "") == ""

    @pytest.mark.anyio
    async def test_empty_successful_graph_returns_zero_processed(self):
        graph = PipelineGraph(
            nodes=[_node("src", PipelineNodeType.SOURCE)],
            edges=[],
        )
        pipeline = _pipeline()

        def fake_get_handler(node_type):
            h = AsyncMock()
            h.execute.return_value = None
            return h

        with patch("src.services.pipeline_executor.get_handler", side_effect=fake_get_handler):
            executor = PipelineExecutor()
            result = await executor.execute(pipeline, graph, {})

        assert result["result_kind"] == "processed_messages"
        assert result["result_count"] == 0

    @pytest.mark.anyio
    async def test_executor_propagates_node_errors_from_context(self):
        """Issue #463: errors recorded via ctx.record_error() must appear in result['node_errors']."""
        graph = PipelineGraph(
            nodes=[_node("react", PipelineNodeType.REACT)],
            edges=[],
        )
        pipeline = _pipeline()

        def fake_get_handler(node_type):
            h = AsyncMock()

            async def _execute(config, ctx, services):
                ctx.record_error(
                    node_id="react",
                    code="no_available_client",
                    detail="all accounts are flood-waited",
                )

            h.execute.side_effect = _execute
            return h

        with patch("src.services.pipeline_executor.get_handler", side_effect=fake_get_handler):
            executor = PipelineExecutor()
            result = await executor.execute(pipeline, graph, {})

        assert result["node_errors"] == [
            {
                "node_id": "react",
                "code": "no_available_client",
                "detail": "all accounts are flood-waited",
            }
        ]

    @pytest.mark.anyio
    async def test_executor_node_errors_default_empty(self):
        graph = PipelineGraph(nodes=[_node("src", PipelineNodeType.SOURCE)], edges=[])
        pipeline = _pipeline()

        def fake_get_handler(node_type):
            h = AsyncMock()
            h.execute.return_value = None
            return h

        with patch("src.services.pipeline_executor.get_handler", side_effect=fake_get_handler):
            result = await PipelineExecutor().execute(pipeline, graph, {})

        assert result["node_errors"] == []

    @pytest.mark.anyio
    async def test_mixed_action_types_all_preserved(self):
        """Multiple action types (react + forward + delete) all surface in action_counts."""
        from src.services.pipeline_result import increment_action_count

        graph = PipelineGraph(
            nodes=[
                _node("src", PipelineNodeType.SOURCE),
                _node("react", PipelineNodeType.REACT),
                _node("fwd", PipelineNodeType.FORWARD),
                _node("del", PipelineNodeType.DELETE_MESSAGE),
            ],
            edges=[
                _edge("src", "react"),
                _edge("src", "fwd"),
                _edge("src", "del"),
            ],
        )
        pipeline = _pipeline()

        def fake_get_handler(node_type):
            h = AsyncMock()

            async def _execute(config, ctx, services):
                if node_type == PipelineNodeType.REACT:
                    increment_action_count(ctx, "react", amount=2)
                elif node_type == PipelineNodeType.FORWARD:
                    increment_action_count(ctx, "forward", amount=3)
                elif node_type == PipelineNodeType.DELETE_MESSAGE:
                    increment_action_count(ctx, "delete_message", amount=1)

            h.execute.side_effect = _execute
            return h

        with patch("src.services.pipeline_executor.get_handler", side_effect=fake_get_handler):
            executor = PipelineExecutor()
            result = await executor.execute(pipeline, graph, {})

        assert result["result_kind"] == "processed_messages"
        assert result["result_count"] == 6  # 2+3+1
        assert result["action_counts"] == {"react": 2, "forward": 3, "delete_message": 1}


# ---------------------------------------------------------------------------
# 13. execute() DAG run-through edge cases (#1037, epic #1024 tier-2).
#
# _topological_sort is unit-tested above; these exercise the full execute()
# loop where the order interacts with inbound-edge suppression, cycle fallback,
# and diamond fan-in (does the merge node run exactly once?).
# ---------------------------------------------------------------------------


def _counting_handler_factory(call_log: list[str], condition_false: set[str] | None = None):
    """get_handler stand-in that logs every node run by its _current_node_id and
    can mark named CONDITION nodes False to suppress their outgoing edges."""
    condition_false = condition_false or set()

    def fake_get_handler(node_type):
        handler = AsyncMock()

        async def _run(config, ctx, services):
            nid = services.get("_current_node_id")
            call_log.append(nid)
            if node_type == PipelineNodeType.CONDITION and nid in condition_false:
                ctx.set_global("condition_result", False)

        handler.execute.side_effect = _run
        return handler

    return fake_get_handler


class TestExecuteDiamondRunsMergeOnce:
    @pytest.mark.anyio
    async def test_diamond_merge_node_executes_exactly_once(self):
        """Diamond A->B, A->C, B->D, C->D: D has two inbound edges but must run
        exactly once — the executor iterates the topo order, it does not re-run a
        node per inbound edge."""
        graph = PipelineGraph(
            nodes=[_node("a"), _node("b"), _node("c"), _node("d")],
            edges=[_edge("a", "b"), _edge("a", "c"), _edge("b", "d"), _edge("c", "d")],
        )
        pipeline = _pipeline()
        call_log: list[str] = []

        with patch(
            "src.services.pipeline_executor.get_handler",
            side_effect=_counting_handler_factory(call_log),
        ):
            await PipelineExecutor().execute(pipeline, graph, {})

        assert call_log.count("d") == 1
        # Every node ran, fan-in node last.
        assert sorted(call_log) == ["a", "b", "c", "d"]
        assert call_log.index("a") == 0
        assert call_log[-1] == "d"


class TestExecuteCycleRunsAllNodes:
    @pytest.mark.anyio
    async def test_cycle_fallback_still_runs_every_node_once(self):
        """A cyclic graph (x->y->z->x) can't be topo-sorted; the executor falls
        back to original node order and must still run each node exactly once
        (no infinite loop, no dropped node) — issue #1037 names the cycle
        fallback's *end-to-end* behaviour as uncovered."""
        graph = PipelineGraph(
            nodes=[_node("x"), _node("y"), _node("z")],
            edges=[_edge("x", "y"), _edge("y", "z"), _edge("z", "x")],
        )
        pipeline = _pipeline()
        call_log: list[str] = []

        with patch(
            "src.services.pipeline_executor.get_handler",
            side_effect=_counting_handler_factory(call_log),
        ):
            result = await PipelineExecutor().execute(pipeline, graph, {})

        # Original order, each once. NOTE: in the cycle fallback every node has a
        # live predecessor that already ran, so none are suppressed.
        assert call_log == ["x", "y", "z"]
        assert isinstance(result["context"], NodeContext)


class TestExecutePartialInboundSuppression:
    @pytest.mark.anyio
    async def test_merge_runs_when_only_some_inbound_edges_suppressed(self):
        """Partial inbound suppression: merge has two inbound edges, one from a
        False CONDITION (suppressed) and one from a live branch. Because NOT
        every inbound path is dead, merge must still run (audit #837/3 semantics,
        the partial case the issue calls out as uncovered)."""
        graph = PipelineGraph(
            nodes=[
                _node("cond", PipelineNodeType.CONDITION),
                _node("live"),
                _node("merge"),
            ],
            edges=[_edge("cond", "merge"), _edge("live", "merge")],
        )
        pipeline = _pipeline()
        call_log: list[str] = []

        with patch(
            "src.services.pipeline_executor.get_handler",
            side_effect=_counting_handler_factory(call_log, condition_false={"cond"}),
        ):
            await PipelineExecutor().execute(pipeline, graph, {})

        assert "cond" in call_log
        assert "live" in call_log
        assert "merge" in call_log  # reachable via the live branch
        assert call_log.count("merge") == 1

    @pytest.mark.anyio
    async def test_merge_skipped_when_all_inbound_edges_suppressed(self):
        """Complement: when EVERY inbound edge of merge originates from a False
        condition, merge (and its downstream) is skipped — confirms the partial
        case above is genuinely about *partial*, not blanket, suppression."""
        graph = PipelineGraph(
            nodes=[
                _node("cond1", PipelineNodeType.CONDITION),
                _node("cond2", PipelineNodeType.CONDITION),
                _node("merge"),
                _node("tail"),
            ],
            edges=[
                _edge("cond1", "merge"),
                _edge("cond2", "merge"),
                _edge("merge", "tail"),
            ],
        )
        pipeline = _pipeline()
        call_log: list[str] = []

        with patch(
            "src.services.pipeline_executor.get_handler",
            side_effect=_counting_handler_factory(
                call_log, condition_false={"cond1", "cond2"}
            ),
        ):
            await PipelineExecutor().execute(pipeline, graph, {})

        assert "cond1" in call_log and "cond2" in call_log
        assert "merge" not in call_log
        assert "tail" not in call_log  # suppression propagates downstream


class TestExecuteFullSourceToPublishChain:
    @pytest.mark.anyio
    async def test_source_filter_llm_image_publish_run_in_order(self):
        """A realistic linear content pipeline (source -> filter -> llm -> image
        -> publish) runs every node in dependency order and threads context
        values end-to-end."""
        graph = PipelineGraph(
            nodes=[
                _node("src", PipelineNodeType.SOURCE),
                _node("flt", PipelineNodeType.FILTER),
                _node("llm", PipelineNodeType.LLM_GENERATE),
                _node("img", PipelineNodeType.IMAGE_GENERATE),
                _node("pub", PipelineNodeType.PUBLISH),
            ],
            edges=[
                _edge("src", "flt"),
                _edge("flt", "llm"),
                _edge("llm", "img"),
                _edge("img", "pub"),
            ],
        )
        pipeline = _pipeline()
        call_log: list[str] = []

        def fake_get_handler(node_type):
            handler = AsyncMock()

            async def _run(config, ctx, services):
                call_log.append(services.get("_current_node_id"))
                if node_type == PipelineNodeType.LLM_GENERATE:
                    ctx.set_global("generated_text", "drafted")
                if node_type == PipelineNodeType.IMAGE_GENERATE:
                    ctx.set_global("image_url", "https://example.com/x.png")

            handler.execute.side_effect = _run
            return handler

        with patch("src.services.pipeline_executor.get_handler", side_effect=fake_get_handler):
            result = await PipelineExecutor().execute(pipeline, graph, {})

        assert call_log == ["src", "flt", "llm", "img", "pub"]
        assert result["generated_text"] == "drafted"
        assert result["image_url"] == "https://example.com/x.png"
