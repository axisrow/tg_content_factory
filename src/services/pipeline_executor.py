"""DAG-based pipeline executor (issue #343)."""
from __future__ import annotations

import logging
from collections import defaultdict, deque
from typing import Any

from src.models import ContentPipeline, PipelineGraph, PipelineNode, PipelineNodeType
from src.services.pipeline_nodes import NodeContext, get_handler

logger = logging.getLogger(__name__)


def _topological_sort(graph: PipelineGraph) -> list[PipelineNode]:
    """Return nodes in topological order (Kahn's algorithm)."""
    nodes_by_id = {n.id: n for n in graph.nodes}
    in_degree: dict[str, int] = {n.id: 0 for n in graph.nodes}
    adj: dict[str, list[str]] = defaultdict(list)

    for edge in graph.edges:
        if edge.from_node in nodes_by_id and edge.to_node in nodes_by_id:
            adj[edge.from_node].append(edge.to_node)
            in_degree[edge.to_node] += 1

    queue: deque[str] = deque(n_id for n_id, deg in in_degree.items() if deg == 0)
    order: list[PipelineNode] = []

    while queue:
        n_id = queue.popleft()
        order.append(nodes_by_id[n_id])
        for neighbor in adj[n_id]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(order) != len(graph.nodes):
        logger.warning("Pipeline graph has a cycle; using original node order as fallback")
        return list(graph.nodes)

    return order


class PipelineExecutor:
    """Executes a PipelineGraph (node-based DAG).

    Accepts a ``services`` dict that is passed to each node handler. Expected keys:
    - ``search_engine``: SearchEngine instance
    - ``provider_callable``: async callable for LLM generation
    - ``image_service``: ImageGenerationService (optional)
    - ``notification_service``: DraftNotificationService (optional)
    - ``client_pool``: ClientPool (optional, for Telegram-side nodes)
    - ``default_model``: str (optional)
    - ``default_image_model``: str (optional)
    - ``db``: Database (optional)
    """

    async def execute(
        self,
        pipeline: ContentPipeline,
        graph: PipelineGraph,
        services: dict[str, Any],
    ) -> dict[str, Any]:
        """Execute the graph and return a result dict.

        Returns keys:
        - ``generated_text``: str
        - ``image_url``: str | None
        - ``citations``: list
        - ``publish_mode``: str
        - ``context``: NodeContext (the full execution context)
        """
        context = NodeContext()

        # Seed initial values from pipeline model (for legacy-compat)
        context.set_global("prompt_template", pipeline.prompt_template or "")
        context.set_global("generation_query", pipeline.prompt_template or pipeline.name or "")
        context.set_global("default_model", pipeline.llm_model or "")

        ordered = _topological_sort(graph)

        for node in ordered:
            handler = get_handler(node.type)
            try:
                logger.debug("Executing node %s (%s)", node.id, node.type)
                await handler.execute(node.config, context, services)

                # Short-circuit condition nodes: skip subtree if condition is False
                if node.type == PipelineNodeType.CONDITION:
                    if not context.get_global("condition_result", True):
                        logger.debug("Condition node %s evaluated False; stopping execution", node.id)
                        break

                # Short-circuit trigger nodes: skip if not matched
                if node.type == PipelineNodeType.SEARCH_QUERY_TRIGGER:
                    if not context.get_global("trigger_matched", False):
                        logger.debug("Trigger node %s did not match; stopping execution", node.id)
                        break
            except Exception:
                logger.exception("Node %s (%s) failed during pipeline execution", node.id, node.type)
                raise

        return {
            "generated_text": context.get_global("generated_text", ""),
            "image_url": context.get_global("image_url"),
            "citations": context.get_global("citations", []),
            "publish_mode": context.get_global("publish_mode", pipeline.publish_mode.value),
            "context": context,
        }
