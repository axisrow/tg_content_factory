from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.web import deps
from src.web.agent import handlers
from src.web.agent.responses import agent_response

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/threads/{thread_id}/messages", response_class=JSONResponse)
async def get_thread_messages(request: Request, thread_id: int):
    """List messages of an agent thread as JSON (parity with CLI `agent messages`)."""
    db = deps.get_db(request)
    thread = await db.get_agent_thread(thread_id)
    if thread is None:
        return JSONResponse({"error": "thread_not_found"}, status_code=404)
    messages = await db.get_agent_messages(thread_id)
    return JSONResponse({"thread_id": thread_id, "title": thread.get("title"), "messages": messages})


@router.get("", response_class=HTMLResponse)
async def agent_page(request: Request, thread_id: int | None = None):
    return agent_response(request, await handlers.agent_page(request, thread_id))


@router.post("/threads", response_class=HTMLResponse)
async def create_thread(request: Request):
    return agent_response(request, await handlers.create_thread(request))


@router.delete("/threads/{thread_id}")
async def delete_thread(request: Request, thread_id: int):
    return agent_response(request, await handlers.delete_thread(request, thread_id))


@router.post("/threads/{thread_id}/rename")
async def rename_thread(request: Request, thread_id: int):
    return agent_response(request, await handlers.rename_thread(request, thread_id))


@router.get("/channels-json")
async def get_channels_json(request: Request):
    return agent_response(request, await handlers.get_channels_json(request))


@router.get("/forum-topics")
async def get_forum_topics(request: Request, channel_id: int):
    return agent_response(request, await handlers.get_forum_topics(request, channel_id))


@router.post("/threads/{thread_id}/context")
async def inject_context(request: Request, thread_id: int):
    return agent_response(request, await handlers.inject_context(request, thread_id))


@router.post("/threads/{thread_id}/permission/{request_id}")
async def resolve_permission(request: Request, thread_id: int, request_id: str):
    return agent_response(request, await handlers.resolve_permission(request, thread_id, request_id))


@router.post("/threads/{thread_id}/stop")
async def stop_chat(request: Request, thread_id: int):
    return agent_response(request, await handlers.stop_chat(request, thread_id))


@router.post("/threads/{thread_id}/chat")
async def chat(request: Request, thread_id: int):
    return agent_response(request, await handlers.chat(request, thread_id))
