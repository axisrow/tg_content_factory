"""Application orchestration for the agent web domain."""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3

from fastapi import HTTPException, Request

from src.agent.models import CLAUDE_MODELS
from src.web import deps
from src.web.agent.forms import select_model
from src.web.agent.responses import AgentJson, AgentRedirect, AgentStream, AgentTemplate

logger = logging.getLogger(__name__)

# Max seconds the agent SSE stream may go without emitting a chunk before we
# abort it. This prevents a hung LLM/backend from holding the connection open.
_SSE_IDLE_TIMEOUT = 180.0


async def _json_object_body(request: Request) -> dict:
    """Return the request body as a JSON object, or raise HTTP 400."""
    try:
        body = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise HTTPException(status_code=400, detail="Request body must be valid JSON") from None
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Request body must be a JSON object")
    return body


def _coerce_int(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise HTTPException(status_code=400, detail=f"{field} must be an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text and (text.isdecimal() or (text[0] in "+-" and text[1:].isdecimal())):
            return int(text)
    raise HTTPException(status_code=400, detail=f"{field} must be an integer")


def _agent_unavailable_copy(runtime_state: deps.AgentRuntimeState) -> tuple[str, str]:
    if runtime_state.state == "starting":
        return (
            "Агент запускается.",
            "Рабочий процесс запускает чат. Обновите страницу через несколько секунд.",
        )
    if runtime_state.state == "failed":
        return (
            "Агент не запустился.",
            runtime_state.error or "Рабочий процесс не запустился. Проверьте логи сервера.",
        )
    return (
        "Чат недоступен в web-процессе.",
        "Чат работает только в рабочем процессе. "
        "Запустите рядом с web-сервером команду `python -m src.main worker`.",
    )


def _agent_unavailable_result(runtime_state: deps.AgentRuntimeState) -> AgentJson:
    _, detail = _agent_unavailable_copy(runtime_state)
    headers = {"Retry-After": "3"} if runtime_state.state == "starting" else None
    return AgentJson(
        {"detail": detail, "runtime_state": runtime_state.state},
        status_code=503,
        headers=headers,
    )


async def agent_page(request: Request, thread_id: int | None = None):
    db = deps.get_db(request)
    agent_runtime_state = deps.get_agent_runtime_state(request)
    agent_manager = agent_runtime_state.manager
    threads = await db.get_agent_threads()
    agent_status = None
    agent_disabled_title = None
    agent_disabled_reason = None
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
    else:
        agent_disabled_title, agent_disabled_reason = _agent_unavailable_copy(agent_runtime_state)

    messages = []
    active_thread = None

    if thread_id is not None:
        active_thread = await db.get_agent_thread(thread_id)
        if active_thread is None and threads:
            logger.debug("Thread %s not found, redirect to first thread", thread_id)
            return AgentRedirect(f"/agent?thread_id={threads[0]['id']}")
        if active_thread is not None:
            messages = await db.get_agent_messages(thread_id)
    elif threads:
        logger.debug("No thread_id param, redirect to first thread %s", threads[0]["id"])
        return AgentRedirect(f"/agent?thread_id={threads[0]['id']}")
    else:
        thread_id = await db.create_agent_thread("Новый тред")
        logger.debug("No threads exist, auto-created thread %s", thread_id)
        return AgentRedirect(f"/agent?thread_id={thread_id}")

    return AgentTemplate(
        "agent.html",
        {
            "threads": threads,
            "active_thread": active_thread,
            "messages": messages,
            "agent_status": agent_status,
            "agent_runtime_state": agent_runtime_state.state,
            "agent_disabled_title": agent_disabled_title,
            "agent_disabled_reason": agent_disabled_reason,
            "model_options": CLAUDE_MODELS,
        },
    )


async def create_thread(request: Request) -> AgentRedirect:
    db = deps.get_db(request)
    thread_id = await db.create_agent_thread("Новый тред")
    return AgentRedirect(f"/agent?thread_id={thread_id}")


async def delete_thread(request: Request, thread_id: int) -> AgentJson:
    db = deps.get_db(request)
    agent_manager = deps.get_agent_manager(request)
    cancelled = False
    if agent_manager is not None:
        cancelled = await agent_manager.cancel_stream(thread_id, wait_timeout=5.0)
    await db.delete_agent_thread(thread_id)
    if agent_manager is not None and agent_manager.permission_gate is not None:
        session_id = request.cookies.get("session", "web")
        agent_manager.permission_gate.clear_thread(session_id, thread_id)
        agent_manager.permission_gate.clear_session(session_id)
    return AgentJson({"ok": True, "cancelled": cancelled})


async def rename_thread(request: Request, thread_id: int) -> AgentJson:
    body = await _json_object_body(request)
    title_raw = body.get("title")
    title = title_raw.strip() if isinstance(title_raw, str) else ""
    if not title:
        raise HTTPException(status_code=400, detail="Title cannot be empty")
    db = deps.get_db(request)
    await db.rename_agent_thread(thread_id, title[:100])
    return AgentJson({"ok": True})


async def get_channels_json(request: Request) -> AgentJson:
    db = deps.get_db(request)
    channels = await db.get_channels(active_only=True, include_filtered=False)
    return AgentJson(
        [
            {
                "id": ch.channel_id,
                "title": ch.title or str(ch.channel_id),
                "channel_type": ch.channel_type,
            }
            for ch in channels
        ]
    )


async def get_forum_topics(request: Request, channel_id: int) -> AgentJson:
    db = deps.get_db(request)
    cached = await db.get_forum_topics(channel_id)
    if cached:
        return AgentJson(cached)
    command_id = await deps.telegram_command_service(request).enqueue(
        "agent.forum_topics_refresh",
        payload={"channel_id": channel_id},
        requested_by="web:agent",
    )
    return AgentJson({"status": "queued", "command_id": command_id}, status_code=202)


async def inject_context(request: Request, thread_id: int) -> AgentJson:
    data = await _json_object_body(request)
    channel_id_raw = data.get("channel_id")
    if not channel_id_raw:
        raise HTTPException(status_code=400, detail="channel_id is required")
    channel_id = _coerce_int(channel_id_raw, "channel_id")
    raw_limit = _coerce_int(data.get("limit", 0), "limit")
    limit = min(raw_limit, 10_000) if raw_limit > 0 else 10_000
    topic_id_raw = data.get("topic_id")
    if topic_id_raw is None or not str(topic_id_raw).strip():
        topic_id = None
    else:
        topic_id = _coerce_int(topic_id_raw, "topic_id")

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
        thread_id,
        len(messages),
        len(content),
    )
    if len(content) > 200_000:
        logger.warning(
            "Large context for thread %d: %d chars (>200K) — may cause prompt overflow",
            thread_id,
            len(content),
        )
    return AgentJson({"content": content})


async def resolve_permission(request: Request, thread_id: int, request_id: str):
    body = await _json_object_body(request)
    choice = body.get("choice", "deny")
    if choice not in ("once", "session", "deny"):
        raise HTTPException(status_code=400, detail="Invalid choice")
    agent_runtime_state = deps.get_agent_runtime_state(request)
    agent_manager = agent_runtime_state.manager
    if agent_manager is None:
        return _agent_unavailable_result(agent_runtime_state)
    ok = agent_manager.permission_gate.resolve(request_id, choice)
    logger.info(
        "Permission resolve from web: thread_id=%s request_id=%s choice=%s ok=%s",
        thread_id,
        request_id,
        choice,
        ok,
    )
    if not ok:
        raise HTTPException(status_code=404, detail="Permission request not found or already resolved")
    return AgentJson({"ok": True})


async def stop_chat(request: Request, thread_id: int) -> AgentJson:
    db = deps.get_db(request)
    agent_manager = deps.get_agent_manager(request)
    cancelled = False
    if agent_manager is not None:
        cancelled = await agent_manager.cancel_stream(thread_id)
    await db.delete_last_agent_exchange(thread_id)
    return AgentJson({"ok": True, "cancelled": cancelled})


async def chat(request: Request, thread_id: int):
    body = await _json_object_body(request)
    message_raw = body.get("message")
    message = message_raw.strip() if isinstance(message_raw, str) else ""
    if not message:
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    model = select_model(body.get("model"))

    db = deps.get_db(request)
    agent_runtime_state = deps.get_agent_runtime_state(request)
    agent_manager = agent_runtime_state.manager
    if agent_manager is None:
        return _agent_unavailable_result(agent_runtime_state)
    runtime_status = await agent_manager.get_runtime_status()
    if runtime_status.selected_backend is None:
        raise HTTPException(
            status_code=503, detail=runtime_status.error or "Agent backend unavailable"
        )
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

    # Use HTTP session cookie as session_id so per-user overrides are isolated
    session_id = request.cookies.get("session", "web")

    async def generate():
        stream = agent_manager.chat_stream(
            thread_id,
            message,
            model=model,
            session_id=session_id,
            interactive_permissions=True,
        )
        agen = stream.__aiter__()
        waiting_for_permission = False
        try:
            while True:
                try:
                    if waiting_for_permission:
                        chunk = await agen.__anext__()
                    else:
                        chunk = await asyncio.wait_for(agen.__anext__(), timeout=_SSE_IDLE_TIMEOUT)
                except StopAsyncIteration:
                    break
                except asyncio.TimeoutError:
                    logger.warning(
                        "Agent SSE stream for thread %d idle >%.0fs; aborting",
                        thread_id,
                        _SSE_IDLE_TIMEOUT,
                    )
                    try:
                        await agent_manager.cancel_stream(thread_id, wait_timeout=5.0)
                    except Exception:
                        logger.debug("cancel_stream after SSE timeout failed", exc_info=True)
                    yield 'data: {"error": "Agent response timed out."}\n\n'
                    break
                try:
                    data_str = chunk.removeprefix("data: ").strip()
                    data = json.loads(data_str)
                    waiting_for_permission = data.get("type") == "permission_request"
                    if data.get("done") and data.get("full_text"):
                        try:
                            await db.save_agent_message(thread_id, "assistant", data["full_text"])
                        except sqlite3.IntegrityError:
                            logger.debug("Thread %d deleted during response; skipping save", thread_id)
                    elif data.get("error"):
                        try:
                            await db.delete_last_agent_exchange(thread_id)
                        except sqlite3.IntegrityError:
                            pass
                except json.JSONDecodeError:
                    waiting_for_permission = False
                    pass
                except Exception:
                    logger.exception("Failed to process agent message for thread %d", thread_id)
                yield chunk
        finally:
            try:
                await agen.aclose()
            except Exception:
                logger.debug("Error closing agent stream for thread %d", thread_id, exc_info=True)

    return AgentStream(generate())
