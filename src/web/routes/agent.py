from __future__ import annotations

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
        # Validate thread exists
        for t in threads:
            if t["id"] == thread_id:
                active_thread = t
                break
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
    threads = await db.get_agent_threads()
    if not any(t["id"] == thread_id for t in threads):
        raise HTTPException(status_code=404, detail="Thread not found")

    # Save user message
    await db.save_agent_message(thread_id, "user", message)

    # Auto-rename thread from first message
    for t in threads:
        if t["id"] == thread_id and t["title"] == "Новый тред":
            await db.rename_agent_thread(thread_id, message[:60])
            break

    agent_manager = deps.get_agent_manager(request)
    if agent_manager is None:
        raise HTTPException(status_code=503, detail="AgentManager not initialized")

    async def generate():
        full_text_parts = []
        had_error = False

        async for chunk in agent_manager.chat_stream(thread_id, message):
            yield chunk
            # Collect full_text from done event
            import json

            try:
                data_str = chunk.removeprefix("data: ").strip()
                data = json.loads(data_str)
                if data.get("done") and data.get("full_text"):
                    full_text_parts.append(data["full_text"])
                elif data.get("error"):
                    had_error = True
                    full_text_parts.append(data["error"])
            except Exception:
                pass

        # Save assistant response
        if full_text_parts:
            role = "assistant" if not had_error else "assistant"
            await db.save_agent_message(thread_id, role, full_text_parts[-1])

    return StreamingResponse(generate(), media_type="text/event-stream")
