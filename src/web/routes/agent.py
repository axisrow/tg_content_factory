from __future__ import annotations

import json
import logging

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from pydantic import BaseModel

from src.web import deps

router = APIRouter()
logger = logging.getLogger(__name__)


class ChatRequest(BaseModel):
    message: str


@router.get("", response_class=HTMLResponse)
async def agent_page(request: Request, thread_id: int | None = None):
    db = deps.get_db(request)
    threads = await db.get_agent_threads()

    messages = []
    active_thread = None

    if thread_id is not None:
        active_thread = await db.get_agent_thread(thread_id)
        if active_thread is None and threads:
            # Redirect to first existing thread
            return RedirectResponse(url=f"/agent?thread_id={threads[0]['id']}", status_code=303)
        if active_thread is not None:
            messages = await db.get_agent_messages(thread_id)
    elif threads:
        return RedirectResponse(url=f"/agent?thread_id={threads[0]['id']}", status_code=303)

    return deps.get_templates(request).TemplateResponse(
        request,
        "agent.html",
        {
            "threads": threads,
            "active_thread": active_thread,
            "messages": messages,
        },
    )


@router.post("/threads", response_class=HTMLResponse)
async def create_thread(request: Request):
    db = deps.get_db(request)
    thread_id = await db.create_agent_thread("Новый тред")
    return RedirectResponse(url=f"/agent?thread_id={thread_id}", status_code=303)


@router.delete("/threads/{thread_id}")
async def delete_thread(request: Request, thread_id: int):
    db = deps.get_db(request)
    await db.delete_agent_thread(thread_id)
    return {"ok": True}


@router.post("/threads/{thread_id}/rename")
async def rename_thread(request: Request, thread_id: int):
    body = await request.json()
    title = (body.get("title") or "").strip()
    if not title:
        raise HTTPException(status_code=400, detail="Title cannot be empty")
    db = deps.get_db(request)
    await db.rename_agent_thread(thread_id, title[:100])
    return {"ok": True}


@router.post("/threads/{thread_id}/chat")
async def chat(request: Request, thread_id: int):
    body = await request.json()
    message = (body.get("message") or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    db = deps.get_db(request)

    # Verify thread exists
    thread = await db.get_agent_thread(thread_id)
    if thread is None:
        raise HTTPException(status_code=404, detail="Thread not found")

    # Save user message
    await db.save_agent_message(thread_id, "user", message)

    # Auto-rename thread from first message
    if thread["title"] == "Новый тред":
        await db.rename_agent_thread(thread_id, message[:60])

    agent_manager = deps.get_agent_manager(request)
    if agent_manager is None:
        raise HTTPException(status_code=503, detail="AgentManager not initialized")

    async def generate():
        async for chunk in agent_manager.chat_stream(thread_id, message):
            # Save before yielding so disconnect doesn't lose the message
            try:
                data_str = chunk.removeprefix("data: ").strip()
                data = json.loads(data_str)
                if data.get("done") and data.get("full_text"):
                    await db.save_agent_message(thread_id, "assistant", data["full_text"])
            except json.JSONDecodeError:
                pass
            except Exception:
                logger.exception("Failed to save agent message for thread %d", thread_id)
            yield chunk

    return StreamingResponse(generate(), media_type="text/event-stream")
