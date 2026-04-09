"""Helper to decide whether executing a pipeline actually requires an LLM provider.

Some pipelines are pure forward/publish flows (e.g. SOURCE → PUBLISH DAG) and
can run without any LLM provider configured. Others (legacy chain backend,
agent/deep_agents backends, or DAGs containing LLM_GENERATE/LLM_REFINE nodes)
definitely need a provider.

This helper centralizes that decision so web routes, CLI commands, and the
pipelines template can all gate UI/server checks on the same logic.
"""
from __future__ import annotations

from src.models import ContentPipeline, PipelineGenerationBackend, PipelineNodeType

_LLM_NODE_TYPES: frozenset[PipelineNodeType] = frozenset(
    {PipelineNodeType.LLM_GENERATE, PipelineNodeType.LLM_REFINE, PipelineNodeType.AGENT_LOOP}
)

_PUBLISH_NODE_TYPES: frozenset[PipelineNodeType] = frozenset(
    {PipelineNodeType.PUBLISH, PipelineNodeType.LLM_GENERATE}
)


def pipeline_needs_llm(pipeline: ContentPipeline) -> bool:
    """Return True if running ``pipeline`` requires a registered LLM provider.

    Rules (any one matching ⇒ needs LLM):

    * ``generation_backend`` is not ``CHAIN`` — AGENT/DEEP_AGENTS always invoke
      an LLM regardless of node graph.
    * ``pipeline_json`` (DAG mode) contains at least one ``LLM_GENERATE`` or
      ``LLM_REFINE`` node.
    * Legacy chain mode (``pipeline_json is None``) — the ContentGenerationService
      always calls ``generate_with_provider`` on this path, so it needs an LLM.
    * Legacy chain mode (``pipeline_json is None``) — the ContentGenerationService
      always calls ``generate_with_provider`` on this path, so it needs an LLM.
    """
    if pipeline.generation_backend != PipelineGenerationBackend.CHAIN:
        return True

    if pipeline.pipeline_json is not None:
        # DAG mode — look for explicit LLM nodes.
        nodes = getattr(pipeline.pipeline_json, "nodes", []) or []
        for node in nodes:
            node_type = getattr(node, "type", None)
            if node_type in _LLM_NODE_TYPES:
                return True
        # DAG with no LLM nodes — non-LLM flow (forward/publish/notify).
        return False

    # Legacy chain path always calls the provider.
    return True


def pipeline_is_dag(pipeline: ContentPipeline) -> bool:
    """Return True if the pipeline uses a node-based DAG (pipeline_json is set)."""
    return pipeline.pipeline_json is not None


def pipeline_needs_publish_mode(pipeline: ContentPipeline) -> bool:
    """Return True if publish_mode is relevant for this pipeline.

    False for DAG pipelines that only react/forward/delete without publishing.
    """
    if pipeline.pipeline_json is None:
        return True  # Legacy chain always publishes
    node_types = frozenset(node.type for node in pipeline.pipeline_json.nodes)
    return bool(node_types & _PUBLISH_NODE_TYPES)


def get_react_emoji_config(pipeline: ContentPipeline) -> str | None:
    """Return the emoji config string for the react node, or None if no react node.

    Format: single emoji like "👍", or comma-separated list "👍,❤️,🔥" for random choice.
    """
    if pipeline.pipeline_json is None:
        return None
    for node in pipeline.pipeline_json.nodes:
        if node.type == PipelineNodeType.REACT:
            random_emojis = node.config.get("random_emojis", [])
            if random_emojis:
                return ",".join(random_emojis)
            return node.config.get("emoji", "👍")
    return None


def get_dag_source_channel_ids(pipeline: ContentPipeline) -> list[int] | None:
    """Return channel_ids from the DAG source node, or None for legacy pipelines.

    Returns an empty list if the pipeline is a DAG but has no source node.
    """
    if pipeline.pipeline_json is None:
        return None
    for node in pipeline.pipeline_json.nodes:
        if node.type == PipelineNodeType.SOURCE:
            return [int(c) for c in node.config.get("channel_ids", [])]
    return []  # DAG pipeline but no source node
