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
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from contextvars import ContextVar
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Per-request context (set by AgentManager.chat_stream, inherited by the
# backend task via asyncio.create_task which copies the current context).
# ---------------------------------------------------------------------------


@dataclass
class AgentRequestContext:
    session_id: str                   # UUID — unique per TUI launch / web session
    thread_id: int
    queue: asyncio.Queue              # SSE queue for this request
    db_permissions: dict[str, bool]   # from load_tool_permissions_union at call time
    permission_timeout: int = 120     # seconds to wait for user response


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
    Each session_id (UUID) has its own set of approved tools.
    """

    # session_id → set of tool bare names approved for the session
    _session_overrides: dict[str, set[str]] = field(default_factory=dict)
    # request_id (UUID str) → Future that resolves with "once"|"session"|"deny"
    _pending: dict[str, asyncio.Future] = field(default_factory=dict)

    def is_session_approved(self, tool_name: str, session_id: str) -> bool:
        """True if tool was previously approved for this session_id."""
        return tool_name in self._session_overrides.get(session_id, set())

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

        # Already approved for this session?
        if self.is_session_approved(tool_name, ctx.session_id):
            return None

        # Ask user
        request_id = str(uuid.uuid4())
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future

        timeout = ctx.permission_timeout
        event = json.dumps(
            {
                "type": "permission_request",
                "request_id": request_id,
                "tool": tool_name,
                "phone": phone,
                "timeout": timeout,
            },
            ensure_ascii=False,
        )
        await ctx.queue.put(f"data: {event}\n\n")

        try:
            choice: str = await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending.pop(request_id, None)
            logger.warning(
                "Permission timeout %ds fired for tool '%s' (thread %d, session %s)",
                timeout, tool_name, ctx.thread_id, ctx.session_id,
            )
            mins = timeout // 60
            return _text_response(f"❌ Таймаут запроса разрешения для '{tool_name}' ({mins} мин).")
        except asyncio.CancelledError:
            self._pending.pop(request_id, None)
            raise
        finally:
            self._pending.pop(request_id, None)

        if choice == "session":
            self._session_overrides.setdefault(ctx.session_id, set()).add(tool_name)
            logger.info("Session permission granted: %s for session %s", tool_name, ctx.session_id)
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
        return True

    def clear_session(self, session_id: str) -> None:
        """Clear session overrides for a specific session_id."""
        self._session_overrides.pop(session_id, None)


def _text_response(text: str) -> dict:
    return {"content": [{"type": "text", "text": text}]}
