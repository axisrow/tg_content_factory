"""Agent tools for content pipeline management."""

from __future__ import annotations

from src.agent.tools._categories import ToolCategory, ToolMeta
from src.agent.tools._registry import get_tool_context
from src.agent.tools.pipeline_read import register_pipeline_read_tools
from src.agent.tools.pipeline_runs import register_pipeline_run_tools
from src.agent.tools.pipeline_templates import register_pipeline_template_tools
from src.agent.tools.pipeline_write import register_pipeline_write_tools, register_refinement_write_tools

# Permission metadata for this module's tools (#245). Single source of
# truth: permissions.py derives TOOL_CATEGORIES / MODULE_GROUPS /
# PHONE_BINDED_TOOLS from these declarations; invariants in
# tests/test_tool_permissions_autoderive.py keep them in sync with the
# @tool() definitions.
TOOL_GROUPS: list[tuple[str, dict[str, ToolMeta]]] = [
    ("Пайплайны", {
        "list_pipelines": ToolMeta(ToolCategory.READ),
        "get_pipeline_detail": ToolMeta(ToolCategory.READ),
        "add_pipeline": ToolMeta(ToolCategory.WRITE),
        "edit_pipeline": ToolMeta(ToolCategory.WRITE),
        "toggle_pipeline": ToolMeta(ToolCategory.WRITE),
        "delete_pipeline": ToolMeta(ToolCategory.DELETE),
        "run_pipeline": ToolMeta(ToolCategory.WRITE),
        "generate_draft": ToolMeta(ToolCategory.WRITE),
        "list_pipeline_runs": ToolMeta(ToolCategory.READ),
        "get_pipeline_run": ToolMeta(ToolCategory.READ),
        "publish_pipeline_run": ToolMeta(ToolCategory.WRITE),
        "get_pipeline_queue": ToolMeta(ToolCategory.READ),
        "get_refinement_steps": ToolMeta(ToolCategory.READ),
        "set_refinement_steps": ToolMeta(ToolCategory.WRITE),
        "export_pipeline_json": ToolMeta(ToolCategory.READ),
        "import_pipeline_json": ToolMeta(ToolCategory.WRITE),
        "list_pipeline_templates": ToolMeta(ToolCategory.READ),
        "create_pipeline_from_template": ToolMeta(ToolCategory.WRITE),
        "ai_edit_pipeline": ToolMeta(ToolCategory.WRITE),
        "get_pipeline_dry_run_count": ToolMeta(ToolCategory.READ),
    }),
]

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
