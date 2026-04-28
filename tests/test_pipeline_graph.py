"""Tests for pipeline graph models, executor, and node handlers."""
from __future__ import annotations

import pytest

from src.models import (
    ContentPipeline,
    PipelineEdge,
    PipelineGraph,
    PipelineNode,
    PipelineNodeType,
)
from src.services.pipeline_executor import PipelineExecutor, _topological_sort
from src.services.pipeline_nodes import NodeContext, get_handler
from src.services.pipeline_nodes.handlers import (
    ConditionHandler,
    DelayHandler,
    FilterHandler,
    LlmGenerateHandler,
    ReactHandler,
    SourceHandler,
)

# ── Model tests ──────────────────────────────────────────────────────────────


def _edge(from_node: str, to_node: str) -> PipelineEdge:
    return PipelineEdge.model_validate({"from": from_node, "to": to_node})


def _node(node_id: str, ntype: PipelineNodeType) -> PipelineNode:
    return PipelineNode(id=node_id, type=ntype, name=node_id, config={})


def test_pipeline_graph_round_trip():
    graph = PipelineGraph(
        nodes=[
            _node("src", PipelineNodeType.SOURCE),
            _node("llm", PipelineNodeType.LLM_GENERATE),
        ],
        edges=[_edge("src", "llm")],
    )
    raw = graph.to_json()
    restored = PipelineGraph.from_json(raw)
    assert len(restored.nodes) == 2
    assert restored.nodes[0].type == PipelineNodeType.SOURCE
    assert restored.edges[0].from_node == "src"
    assert restored.edges[0].to_node == "llm"


def test_pipeline_graph_from_dict():
    data = {
        "nodes": [{"id": "n1", "type": "filter", "name": "F", "config": {}}],
        "edges": [],
    }
    graph = PipelineGraph.from_json(data)
    assert graph.nodes[0].type == PipelineNodeType.FILTER


def test_pipeline_graph_empty():
    g = PipelineGraph()
    raw = g.to_json()
    restored = PipelineGraph.from_json(raw)
    assert restored.nodes == []
    assert restored.edges == []


# ── Topological sort ─────────────────────────────────────────────────────────


def test_topological_sort_linear():
    graph = PipelineGraph(
        nodes=[
            _node("a", PipelineNodeType.SOURCE),
            _node("b", PipelineNodeType.RETRIEVE_CONTEXT),
            _node("c", PipelineNodeType.LLM_GENERATE),
        ],
        edges=[_edge("a", "b"), _edge("b", "c")],
    )
    order = _topological_sort(graph)
    ids = [n.id for n in order]
    assert ids.index("a") < ids.index("b") < ids.index("c")


def test_topological_sort_single_node():
    graph = PipelineGraph(nodes=[_node("x", PipelineNodeType.PUBLISH)], edges=[])
    order = _topological_sort(graph)
    assert len(order) == 1
    assert order[0].id == "x"


def test_topological_sort_diamond():
    graph = PipelineGraph(
        nodes=[
            _node("a", PipelineNodeType.SOURCE),
            _node("b", PipelineNodeType.FILTER),
            _node("c", PipelineNodeType.LLM_GENERATE),
            _node("d", PipelineNodeType.PUBLISH),
        ],
        edges=[_edge("a", "b"), _edge("a", "c"), _edge("b", "d"), _edge("c", "d")],
    )
    order = _topological_sort(graph)
    ids = [n.id for n in order]
    assert ids.index("a") < ids.index("b")
    assert ids.index("a") < ids.index("c")
    assert ids.index("b") < ids.index("d")
    assert ids.index("c") < ids.index("d")


# ── NodeContext ───────────────────────────────────────────────────────────────


def test_node_context_set_get():
    ctx = NodeContext()
    ctx.set("node1", "text", "hello")
    assert ctx.get("node1", "text") == "hello"
    assert ctx.get("node1", "missing") is None
    assert ctx.get("node1", "missing", "default") == "default"


def test_node_context_global():
    ctx = NodeContext()
    ctx.set_global("key", 42)
    assert ctx.get_global("key") == 42
    assert ctx.get_global("missing") is None


def test_node_context_get_last():
    ctx = NodeContext()
    ctx.set("n1", "text", "first")
    ctx.set("n2", "text", "second")
    assert ctx.get_last("text") == "second"


# ── Handler tests ─────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_source_handler():
    handler = SourceHandler()
    ctx = NodeContext()
    await handler.execute({"channel_ids": [1, 2, 3]}, ctx, {})
    assert ctx.get_global("source_channel_ids") == [1, 2, 3]


@pytest.mark.anyio
async def test_filter_handler_keywords():
    from unittest.mock import MagicMock

    handler = FilterHandler()
    ctx = NodeContext()
    m1 = MagicMock()
    m1.text = "Buy cheap crypto now"
    m2 = MagicMock()
    m2.text = "Today is a great day"
    ctx.set_global("context_messages", [m1, m2])
    await handler.execute({"type": "keywords", "keywords": ["crypto", "cheap"]}, ctx, {})
    filtered = ctx.get_global("context_messages")
    assert len(filtered) == 1
    assert filtered[0] is m1


@pytest.mark.anyio
async def test_filter_handler_anonymous():
    from unittest.mock import MagicMock

    handler = FilterHandler()
    ctx = NodeContext()
    m1 = MagicMock()
    m1.text = "anon"
    m1.sender_id = None
    m1.sender_name = None
    m2 = MagicMock()
    m2.text = "normal"
    m2.sender_id = 123
    m2.sender_name = "User"
    ctx.set_global("context_messages", [m1, m2])
    await handler.execute({"type": "anonymous_sender"}, ctx, {})
    filtered = ctx.get_global("context_messages")
    assert len(filtered) == 1
    assert filtered[0] is m1


@pytest.mark.anyio
async def test_condition_handler_not_empty():
    handler = ConditionHandler()
    ctx = NodeContext()
    ctx.set_global("generated_text", "some text")
    await handler.execute({"field": "generated_text", "operator": "not_empty"}, ctx, {})
    assert ctx.get_global("condition_result") is True


@pytest.mark.anyio
async def test_condition_handler_empty():
    handler = ConditionHandler()
    ctx = NodeContext()
    ctx.set_global("generated_text", "")
    await handler.execute({"field": "generated_text", "operator": "not_empty"}, ctx, {})
    assert ctx.get_global("condition_result") is False


@pytest.mark.anyio
async def test_delay_handler_zero():
    handler = DelayHandler()
    ctx = NodeContext()
    # Should not raise, effectively a no-op
    await handler.execute({"min_seconds": 0, "max_seconds": 0}, ctx, {})


@pytest.mark.anyio
async def test_llm_generate_handler():
    handler = LlmGenerateHandler()
    ctx = NodeContext()
    ctx.set_global("context_messages", [])
    ctx.set_global("prompt_template", "Write something: {source_messages}")

    async def mock_provider(prompt, model="", max_tokens=512, temperature=0.7):
        return {"text": f"generated:{prompt[:20]}"}

    await handler.execute(
        {"prompt_template": "Write: {source_messages}", "max_tokens": 100},
        ctx,
        {"provider_callable": mock_provider, "default_model": "test"},
    )
    assert ctx.get_global("generated_text").startswith("generated:")


# ── PipelineExecutor integration ──────────────────────────────────────────────


@pytest.mark.anyio
async def test_executor_linear_pipeline():
    graph = PipelineGraph(
        nodes=[
            _node("src", PipelineNodeType.SOURCE),
            PipelineNode(
                id="llm",
                type=PipelineNodeType.LLM_GENERATE,
                name="LLM",
                config={"prompt_template": "Say hello: {source_messages}"},
            ),
        ],
        edges=[_edge("src", "llm")],
    )
    pipeline = ContentPipeline(
        name="test",
        prompt_template="Say hello",
        pipeline_json=graph,
    )

    async def mock_provider(prompt, model="", max_tokens=512, temperature=0.7):
        return {"text": "Hello from LLM"}

    executor = PipelineExecutor()
    result = await executor.execute(
        pipeline, graph, {"provider_callable": mock_provider}
    )
    assert result["generated_text"] == "Hello from LLM"


@pytest.mark.anyio
async def test_executor_condition_stops_on_false():
    graph = PipelineGraph(
        nodes=[
            PipelineNode(
                id="cond",
                type=PipelineNodeType.CONDITION,
                name="Cond",
                config={"field": "generated_text", "operator": "not_empty"},
            ),
            _node("pub", PipelineNodeType.PUBLISH),
        ],
        edges=[_edge("cond", "pub")],
    )
    pipeline = ContentPipeline(name="test", prompt_template="x", pipeline_json=graph)
    executor = PipelineExecutor()
    # generated_text is empty → condition False → publish never runs
    result = await executor.execute(pipeline, graph, {})
    # condition_result should be False, publish_targets not set
    assert result["context"].get_global("condition_result") is False
    assert result["context"].get_global("publish_targets") is None


@pytest.mark.anyio
async def test_react_handler_tracks_successful_reactions():
    class FakeSession:
        def __init__(self):
            self.calls = []

        async def send_reaction(self, channel_id, message_id, emoji):
            self.calls.append((channel_id, message_id, emoji))

    class FakePool:
        def __init__(self, session):
            self._session = session

        async def get_client_by_phone(self, phone):
            return self._session, phone

        async def release_client(self, phone):
            return None

    msg = type("Msg", (), {"channel_id": 1001, "message_id": 42})()
    session = FakeSession()
    handler = ReactHandler()
    ctx = NodeContext()
    ctx.set_global("context_messages", [msg])

    await handler.execute(
        {"emoji": "🔥"},
        ctx,
        {"client_pool": FakePool(session), "account_phone": "+100"},
    )

    assert session.calls == [(1001, 42, "🔥")]
    assert ctx.get_global("action_counts") == {"react": 1}


@pytest.mark.anyio
async def test_executor_returns_processed_message_result_for_action_only_pipeline(monkeypatch):
    class FakeHandler:
        async def execute(self, node_config, context, services):
            context.set_global("action_counts", {"react": 3})

    graph = PipelineGraph(
        nodes=[_node("react", PipelineNodeType.REACT)],
        edges=[],
    )
    pipeline = ContentPipeline(name="reaction", prompt_template=".", pipeline_json=graph)
    executor = PipelineExecutor()

    monkeypatch.setattr("src.services.pipeline_executor.get_handler", lambda _node_type: FakeHandler())

    result = await executor.execute(pipeline, graph, {})

    assert result["generated_text"] == ""
    assert result["result_kind"] == "processed_messages"
    assert result["result_count"] == 3
    assert result["action_counts"] == {"react": 3}


# ── Pipeline model with pipeline_json ────────────────────────────────────────


def test_content_pipeline_with_graph():
    graph = PipelineGraph(
        nodes=[_node("s", PipelineNodeType.SOURCE)],
        edges=[],
    )
    p = ContentPipeline(name="test", prompt_template="x", pipeline_json=graph)
    assert p.pipeline_json is not None
    assert len(p.pipeline_json.nodes) == 1


def test_content_pipeline_without_graph():
    p = ContentPipeline(name="test", prompt_template="x")
    assert p.pipeline_json is None


# ── get_handler registry ─────────────────────────────────────────────────────


def test_get_handler_all_types():
    for node_type in PipelineNodeType:
        handler = get_handler(node_type)
        assert handler is not None


# ── PipelineTemplates ─────────────────────────────────────────────────────────


def test_builtin_templates_valid():
    from src.services.pipeline_templates_builtin import get_builtin_templates

    templates = get_builtin_templates()
    assert len(templates) > 0
    for tpl in templates:
        assert tpl.name
        assert tpl.is_builtin
        assert len(tpl.template_json.nodes) > 0
        # Round-trip JSON
        raw = tpl.template_json.to_json()
        restored = PipelineGraph.from_json(raw)
        assert len(restored.nodes) == len(tpl.template_json.nodes)
