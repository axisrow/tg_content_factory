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
    {PipelineNodeType.LLM_GENERATE, PipelineNodeType.LLM_REFINE}
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
