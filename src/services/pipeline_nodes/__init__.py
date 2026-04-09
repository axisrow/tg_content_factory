"""Pipeline node handler registry."""
from __future__ import annotations

from src.models import PipelineNodeType
from src.services.pipeline_nodes.base import BaseNodeHandler, NodeContext
from src.services.pipeline_nodes.handlers import (
    AgentLoopHandler,
    ConditionHandler,
    DelayHandler,
    DeleteMessageHandler,
    FetchMessagesHandler,
    FilterHandler,
    ForwardHandler,
    ImageGenerateHandler,
    LlmGenerateHandler,
    LlmRefineHandler,
    NotifyHandler,
    PublishHandler,
    ReactHandler,
    RetrieveContextHandler,
    SearchQueryTriggerHandler,
    SourceHandler,
)

HANDLER_REGISTRY: dict[PipelineNodeType, type[BaseNodeHandler]] = {
    PipelineNodeType.SOURCE: SourceHandler,
    PipelineNodeType.RETRIEVE_CONTEXT: RetrieveContextHandler,
    PipelineNodeType.LLM_GENERATE: LlmGenerateHandler,
    PipelineNodeType.LLM_REFINE: LlmRefineHandler,
    PipelineNodeType.IMAGE_GENERATE: ImageGenerateHandler,
    PipelineNodeType.PUBLISH: PublishHandler,
    PipelineNodeType.NOTIFY: NotifyHandler,
    PipelineNodeType.FILTER: FilterHandler,
    PipelineNodeType.DELAY: DelayHandler,
    PipelineNodeType.REACT: ReactHandler,
    PipelineNodeType.FORWARD: ForwardHandler,
    PipelineNodeType.DELETE_MESSAGE: DeleteMessageHandler,
    PipelineNodeType.FETCH_MESSAGES: FetchMessagesHandler,
    PipelineNodeType.CONDITION: ConditionHandler,
    PipelineNodeType.SEARCH_QUERY_TRIGGER: SearchQueryTriggerHandler,
    PipelineNodeType.AGENT_LOOP: AgentLoopHandler,
}


def get_handler(node_type: PipelineNodeType) -> BaseNodeHandler:
    handler_class = HANDLER_REGISTRY.get(node_type)
    if handler_class is None:
        raise ValueError(f"No handler registered for node type: {node_type}")
    return handler_class()


__all__ = ["HANDLER_REGISTRY", "get_handler", "NodeContext", "BaseNodeHandler"]
