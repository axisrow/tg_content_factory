from __future__ import annotations

import json
import logging

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from pydantic import BaseModel

from src.web import deps

router = APIRouter()
logger = logging.getLogger(__name__)


ALLOWED_MODELS = {
    "claude-sonnet-4-5",
    "claude-opus-4-6",
    "claude-haiku-4-5-20251001",
}


class ChatRequest(BaseModel):
    message: str
    model: str | None = None


@router.get("", response_class=HTMLResponse)
async def agent_page(request: Request, thread_id: int | None = None):
    db = deps.get_db(request)
    agent_manager = deps.get_agent_manager(request)
    threads = await db.get_agent_threads()
    agent_status = None
    if agent_manager is not None:
        runtime_status = await agent_manager.get_runtime_status()
        agent_status = {
            "claude_available": runtime_status.claude_available,
            "deepagents_available": runtime_status.deepagents_available,
            "dev_mode_enabled": runtime_status.dev_mode_enabled,
            "backend_override": runtime_status.backend_override,
            "selected_backend": runtime_status.selected_backend,
            "fallback_model": runtime_status.fallback_model,
            "fallback_provider": runtime_status.fallback_provider,
            "using_override": runtime_status.using_override,
            "error": runtime_status.error,
        }

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
    else:
        # Auto-create first thread
        thread_id = await db.create_agent_thread("Новый тред")
        return RedirectResponse(url=f"/agent?thread_id={thread_id}", status_code=303)

    return deps.get_templates(request).TemplateResponse(
        request,
        "agent.html",
        {
            "threads": threads,
            "active_thread": active_thread,
            "messages": messages,
            "agent_status": agent_status,
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


@router.get("/channels-json")
async def get_channels_json(request: Request):
    db = deps.get_db(request)
    channels = await db.get_channels(active_only=True, include_filtered=False)
    return JSONResponse([
        {
            "id": ch.channel_id,
            "title": ch.title or str(ch.channel_id),
            "channel_type": ch.channel_type,
        }
        for ch in channels
    ])


@router.get("/forum-topics")
async def get_forum_topics(request: Request, channel_id: int):
    db = deps.get_db(request)
    pool = deps.get_pool(request)

    # Try to refresh from Telegram API (topics may be renamed)
    fresh_topics = await pool.get_forum_topics(channel_id)
    if fresh_topics:
        await db.upsert_forum_topics(channel_id, fresh_topics)
        await db.set_channel_type(channel_id, "forum")
        return JSONResponse(fresh_topics)

    # Fallback: serve from DB if API is unavailable (flood wait, etc.)
    cached = await db.get_forum_topics(channel_id)
    if cached:
        return JSONResponse(cached)

    return JSONResponse([])


@router.post("/threads/{thread_id}/context")
async def inject_context(request: Request, thread_id: int):
    data = await request.json()
    channel_id_raw = data.get("channel_id")
    if not channel_id_raw:
        raise HTTPException(status_code=400, detail="channel_id is required")
    channel_id = int(channel_id_raw)
    raw_limit = int(data.get("limit", 0))
    limit = min(raw_limit, 10_000) if raw_limit > 0 else 10_000
    topic_id = data.get("topic_id")
    if topic_id is not None:
        topic_id = int(topic_id) if str(topic_id).strip() else None

    db = deps.get_db(request)
    thread = await db.get_agent_thread(thread_id)
    if thread is None:
        raise HTTPException(status_code=404, detail="Thread not found")

    messages, _ = await db.search_messages(
        query="",
        channel_id=channel_id,
        limit=limit,
        topic_id=topic_id,
    )
    ch = await db.get_channel_by_channel_id(channel_id)
    title = ch.title if ch else str(channel_id)

    topics = await db.get_forum_topics(channel_id)
    topics_map = {t["id"]: t["title"] for t in topics}

    from src.agent.context import format_context

    content = format_context(messages, title, topic_id, topics_map)

    await db.save_agent_message(thread_id=thread_id, role="user", content=content)
    logger.info(
        "Context loaded for thread %d: %d messages, %d chars",
        thread_id, len(messages), len(content),
    )
    if len(content) > 200_000:
        logger.warning(
            "Large context for thread %d: %d chars (>200K) — may cause prompt overflow",
            thread_id, len(content),
        )
    return JSONResponse({"content": content})


@router.post("/threads/{thread_id}/stop")
async def stop_chat(request: Request, thread_id: int):
    db = deps.get_db(request)
    agent_manager = deps.get_agent_manager(request)
    cancelled = False
    if agent_manager is not None:
        cancelled = await agent_manager.cancel_stream(thread_id)
    await db.delete_last_agent_exchange(thread_id)
    return JSONResponse({"ok": True, "cancelled": cancelled})


@router.post("/threads/{thread_id}/chat")
async def chat(request: Request, thread_id: int):
    body = await request.json()
    message = (body.get("message") or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    raw_model = (body.get("model") or "").strip()
    model = raw_model if raw_model in ALLOWED_MODELS else None

    db = deps.get_db(request)
    agent_manager = deps.get_agent_manager(request)
    if agent_manager is None:
        raise HTTPException(status_code=503, detail="AgentManager not initialized")
    runtime_status = await agent_manager.get_runtime_status()
    if runtime_status.selected_backend is None:
        raise HTTPException(status_code=503, detail=runtime_status.error or "Agent backend unavailable")
    if runtime_status.using_override and runtime_status.error:
        raise HTTPException(status_code=503, detail=runtime_status.error)

    # Verify thread exists
    thread = await db.get_agent_thread(thread_id)
    if thread is None:
        raise HTTPException(status_code=404, detail="Thread not found")

    # Check prompt size before saving
    estimated = await agent_manager.estimate_prompt_tokens(thread_id, message)
    if estimated > 100_000:
        raise HTTPException(
            status_code=400,
            detail="Контекст слишком длинный. Создайте новый тред.",
        )

    # Save user message
    await db.save_agent_message(thread_id, "user", message)

    # Auto-rename thread from first message
    if thread["title"] == "Новый тред":
        await db.rename_agent_thread(thread_id, message[:60])

    async def generate():
        async for chunk in agent_manager.chat_stream(thread_id, message, model=model):
            try:
                data_str = chunk.removeprefix("data: ").strip()
                data = json.loads(data_str)
                if data.get("done") and data.get("full_text"):
                    await db.save_agent_message(thread_id, "assistant", data["full_text"])
                elif data.get("error"):
                    await db.delete_last_agent_exchange(thread_id)
            except json.JSONDecodeError:
                pass
            except Exception:
                logger.exception("Failed to process agent message for thread %d", thread_id)
            yield chunk

    return StreamingResponse(generate(), media_type="text/event-stream")
