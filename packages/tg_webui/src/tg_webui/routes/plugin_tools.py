"""Authenticated plugin calls through the current web container."""
from typing import Annotated

from fastapi import APIRouter, Body, HTTPException, Request

from tg_agent.services.plugin_tools import PluginToolError, PluginTools
from tg_webui import deps

router = APIRouter()


@router.post('/api/tools/{tool}')
async def invoke_plugin_tool(request: Request, tool: str, arguments: Annotated[dict, Body()]):
    container = deps.get_container(request)
    try:
        return await PluginTools(container.plugins).invoke(tool, arguments)
    except PluginToolError as exc:
        raise HTTPException(exc.status, exc.detail) from exc
