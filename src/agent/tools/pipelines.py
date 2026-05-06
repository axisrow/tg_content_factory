"""Agent tools for content pipeline management."""

from __future__ import annotations

from src.agent.tools._registry import get_tool_context
from src.agent.tools.pipeline_read import register_pipeline_read_tools
from src.agent.tools.pipeline_runs import register_pipeline_run_tools
from src.agent.tools.pipeline_templates import register_pipeline_template_tools
from src.agent.tools.pipeline_write import register_pipeline_write_tools, register_refinement_write_tools


def register(db, client_pool, embedding_service, **kwargs):
    config = kwargs.get("config")
    ctx = get_tool_context(kwargs, db=db, client_pool=client_pool, embedding_service=embedding_service)
    tools = []
    tools.extend(register_pipeline_read_tools(db, ctx))
    tools.extend(register_pipeline_write_tools(db, ctx))
    tools.extend(register_pipeline_run_tools(db, client_pool, config, ctx))
    tools.extend(register_refinement_write_tools(db))
    tools.extend(register_pipeline_template_tools(db, config))
    return tools
