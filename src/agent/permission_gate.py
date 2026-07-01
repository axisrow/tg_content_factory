"""Permission gate for agent tool access — interactive TUI/web dialogs instead of text errors.

Flow:
1. AgentManager.chat_stream() sets AgentRequestContext via ContextVar before spawning the backend task.
2. Tool handlers (session-level wrapper in __init__.py, phone-level in _registry.py) call
   PermissionGate.check() when a tool is blocked.
3. check() puts a "permission_request" SSE event in ctx.queue and awaits an asyncio.Future.
4. TUI: _stream_response() intercepts the event, shows PermissionDialog, resolves the future.
   Web: JS intercepts the event, shows a menu, POSTs to /agent/threads/.../permission/<request_id>.
5. choice "once"  → allow this single call (no override stored)
   choice "session" → store in _session_overrides[session_id], allow
   choice "deny"  → return _text_response error to the LLM

Permission prompts are serialized per agent session.  This prevents parallel
tool calls from opening a burst of dialogs before the user answers the first
one.  There is intentionally no timeout: the agent pauses until the user
chooses once/session/deny, or until the agent task is cancelled.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import uuid
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.agent.tools.permissions import ToolAccessState

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Per-request context (set by AgentManager.chat_stream, inherited by the
# backend task via asyncio.create_task which copies the current context).
# ---------------------------------------------------------------------------


@dataclass
class PermissionWaitTracker:
    """Thread-safe marker for sync bridges blocked on interactive permissions."""

    _event: threading.Event = field(default_factory=threading.Event)
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _count: int = 0

    def begin(self) -> None:
        with self._lock:
            self._count += 1
            self._event.set()

    def end(self) -> None:
        with self._lock:
            self._count = max(0, self._count - 1)
            if self._count == 0:
                self._event.clear()

    def is_waiting(self) -> bool:
        return self._event.is_set()


@dataclass
class AgentRequestContext:
    session_id: str                   # UUID — unique per TUI launch / web session
    thread_id: int
    queue: asyncio.Queue              # SSE queue for this request
    db_permissions: dict[str, bool] | None = None  # legacy boolean view
    tool_access_policy: dict[str, ToolAccessState] | None = None
    permission_gate: PermissionGate | None = None
    permission_timeout: int | None = None  # legacy config field; prompts no longer time out
    cancel_event: threading.Event | None = None
    permission_wait_tracker: PermissionWaitTracker | None = None


_request_ctx: ContextVar[AgentRequestContext | None] = ContextVar(
    "agent_request_ctx", default=None
)


def get_request_context() -> AgentRequestContext | None:
    """Return the current request context (None outside of agent chat_stream)."""
    return _request_ctx.get()


def set_request_context(ctx: AgentRequestContext):
    """Set the request context and return the reset token."""
    return _request_ctx.set(ctx)


def reset_request_context(token) -> None:
    _request_ctx.reset(token)


# ---------------------------------------------------------------------------
# Module-level gate accessor (set by AgentManager after construction).
# Allows _registry.py to access the gate without circular imports.
# ---------------------------------------------------------------------------

_active_gate: PermissionGate | None = None


def get_gate() -> PermissionGate | None:
    """Return the active PermissionGate, or None if not in use."""
    ctx = get_request_context()
    if ctx is not None and ctx.permission_gate is not None:
        return ctx.permission_gate
    return _active_gate


def set_gate(gate: PermissionGate | None) -> None:
    global _active_gate  # noqa: PLW0603
    _active_gate = gate


# ---------------------------------------------------------------------------
# PermissionGate
# ---------------------------------------------------------------------------


@dataclass
class PermissionGate:
    """Manages runtime permission overrides for agent tool access.

    Session overrides are in-memory only, never persisted to DB.
    Each session_id (UUID) has its own set of approved (tool, phone) pairs.

    Phone scoping: a session grant is keyed on ``(tool_name, phone)`` so a
    "session" allow on one phone does not leak into a phone-level ACL check
    for a different phone.  Session-level (global) prompts use ``phone=""``.
    """

    # session_id → set of (tool_name, phone) approved for the session
    _session_overrides: dict[str, set[tuple[str, str]]] = field(default_factory=dict)
    # request_id (UUID str) → Future that resolves with "once"|"session"|"deny"
    _pending: dict[str, asyncio.Future] = field(default_factory=dict)
    # request_id → session_id, so clear_thread/cancel_all can unblock visible prompts
    _pending_sessions: dict[str, str] = field(default_factory=dict)
    # request_id → thread_id, so thread deletion cancels only its own visible prompt
    _pending_threads: dict[str, int] = field(default_factory=dict)
    # (session_id, event_loop_id) → lock.  A lock is loop-bound once contended,
    # so tests and embedded runtimes that create fresh loops need separate locks.
    _session_prompt_locks: dict[tuple[str, int], asyncio.Lock] = field(default_factory=dict)

    def is_session_approved(self, tool_name: str, session_id: str, phone: str = "") -> bool:
        """True if (tool, phone) was previously approved for this session_id."""
        return (tool_name, phone) in self._session_overrides.get(session_id, set())

    def _prompt_lock(self, session_id: str) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        key = (session_id, id(loop))
        lock = self._session_prompt_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._session_prompt_locks[key] = lock
        return lock

    async def check(self, tool_name: str, phone: str) -> dict | None:
        """Check if tool/phone is allowed; show permission dialog if not.

        Returns None (proceed) or a _text_response dict (deny).
        Reads context from ContextVar — must be called inside a backend task.

        Args:
            tool_name: bare tool name (e.g. "refresh_dialogs")
            phone: phone number for phone-level checks, or "" for session-level
        """
        ctx = get_request_context()
        if ctx is None:
            # No context set (e.g. one-shot CLI mode) — fall through to caller
            return None

        # Already approved for this (session, tool, phone)?
        if self.is_session_approved(tool_name, ctx.session_id, phone):
            return None

        lock = self._prompt_lock(ctx.session_id)
        tracker = ctx.permission_wait_tracker
        if tracker is not None:
            tracker.begin()
        try:
            async with lock:
                # Another request may have received "session" while we were queued.
                if self.is_session_approved(tool_name, ctx.session_id, phone):
                    return None
                return await self._ask_user(ctx, tool_name, phone)
        finally:
            if tracker is not None:
                tracker.end()

    async def _ask_user(self, ctx: AgentRequestContext, tool_name: str, phone: str) -> dict | None:
        """Emit one permission prompt and wait indefinitely for the user's choice."""
        request_id = str(uuid.uuid4())
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        self._pending_sessions[request_id] = ctx.session_id
        self._pending_threads[request_id] = ctx.thread_id

        event = json.dumps(
            {
                "type": "permission_request",
                "request_id": request_id,
                "tool": tool_name,
                "phone": phone,
                "timeout": None,
            },
            ensure_ascii=False,
        )
        await ctx.queue.put(f"data: {event}\n\n")
        logger.info(
            "Permission request emitted: request_id=%s tool=%s phone=%s thread=%d session=%s timeout=none",
            request_id,
            tool_name,
            phone or "(none)",
            ctx.thread_id,
            ctx.session_id,
        )

        try:
            choice: str = await future
        except asyncio.CancelledError:
            self._pending.pop(request_id, None)
            self._pending_sessions.pop(request_id, None)
            self._pending_threads.pop(request_id, None)
            if not future.done():
                future.cancel()
            raise
        finally:
            self._pending.pop(request_id, None)
            self._pending_sessions.pop(request_id, None)
            self._pending_threads.pop(request_id, None)

        if choice == "session":
            self._session_overrides.setdefault(ctx.session_id, set()).add((tool_name, phone))
            logger.info(
                "Session permission granted: %s (phone=%s) for session %s",
                tool_name, phone or "(none)", ctx.session_id,
            )
            return None
        elif choice == "once":
            logger.info("One-time permission granted: %s", tool_name)
            return None
        else:
            logger.info("Permission denied by user: %s", tool_name)
            return _text_response(f"❌ Доступ к '{tool_name}' запрещён пользователем.")

    def resolve(self, request_id: str, choice: str) -> bool:
        """Resolve a pending permission request.

        Args:
            request_id: UUID from the "permission_request" SSE event
            choice: "once", "session", or "deny"

        Returns True if the request was found and resolved, False otherwise.
        """
        future = self._pending.get(request_id)
        if future is None:
            logger.warning("resolve() called for unknown request_id=%s", request_id)
            return False
        if not future.done():
            future.set_result(choice)
            logger.info("Permission request resolved: request_id=%s choice=%s", request_id, choice)
        return True

    def clear_session(self, session_id: str) -> None:
        """Clear session overrides for a specific session_id."""
        self._session_overrides.pop(session_id, None)
        for key, lock in list(self._session_prompt_locks.items()):
            if key[0] == session_id and not lock.locked():
                self._session_prompt_locks.pop(key, None)

    def clear_thread(self, session_id: str, thread_id: int) -> None:
        """Cancel pending permission prompts for one thread in one session."""
        for request_id, pending_session_id in list(self._pending_sessions.items()):
            if pending_session_id != session_id:
                continue
            if self._pending_threads.get(request_id) != thread_id:
                continue
            future = self._pending.get(request_id)
            if future is not None and not future.done():
                future.cancel()
            self._pending.pop(request_id, None)
            self._pending_sessions.pop(request_id, None)
            self._pending_threads.pop(request_id, None)
        for key, lock in list(self._session_prompt_locks.items()):
            if key[0] == session_id and not lock.locked():
                self._session_prompt_locks.pop(key, None)

    def cancel_all(self) -> None:
        """Cancel all pending permission prompts and clear in-memory grants."""
        for future in list(self._pending.values()):
            if not future.done():
                future.cancel()
        self._pending.clear()
        self._pending_sessions.clear()
        self._pending_threads.clear()
        self._session_overrides.clear()
        self._session_prompt_locks.clear()


def _text_response(text: str) -> dict:
    return {"content": [{"type": "text", "text": text}]}
