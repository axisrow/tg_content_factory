"""Agent tool floodgate boundary (#1851): gate + breaker around tool calls.

Port of the upstream chigwell/telegram-mcp#363 ``with_account`` integration
(Apache-2.0, see THIRD_PARTY_NOTICES.md), composed over the pool-owned
``telethon-floodgate`` primitives — no limiter instances are created here.

Protocol per tool call (mirrors upstream's defer/settle):

* **defer** — before any client work: the breaker refuses a suspended
  ``(tool, phone)`` pair first, then the gate is *peeked* for the tool's
  flood category. A deferral returns a rate-limit reply the agent can
  schedule around, without acquiring a client or burning dispatch budget.
  The gate is peeked, not charged: #1840's dispatch guard already charges
  the same buckets one boundary down, and a second charge would halve the
  calibrated throughput (send 30/60s, history 24/30s).
* **settle** — after the completion: a granted half-open breaker trial is
  released as a success unless a real flood was attributed to this call
  through the pool reporters (``tg_agent.telegram.tool_flood_scope``) —
  upstream's success-clears-counter rule, including "a result that merely
  quotes flood text is not a flood".
* **kill switch** — ``TG_AGENT_FLOODGATE=0/false/no/off`` disables the
  boundary; anything else (including an invalid value) keeps it enabled.

Every phone-bound tool passes through this boundary via
``build_agent_tool_registry``; the flood category comes from the tool's
``ToolMeta.flood_category`` (#245 registry next to the declaration,
derived as ``permissions.TOOL_FLOOD_CATEGORIES``). Unannotated tools take
the gate's permissive ``default`` bucket (1000/60s).
"""

from __future__ import annotations

import functools
import logging
import os
import zlib
from typing import TYPE_CHECKING, Any

from claude_agent_sdk import SdkMcpTool
from telethon_floodgate import (
    FloodCircuitBreaker,
    TelegramOperationSuspendedError,
    TelegramRateLimitGate,
)

from tg_agent.agent.tools._registry import normalize_phone, resolve_phone
from tg_agent.telegram.tool_flood_scope import tool_flood_scope

if TYPE_CHECKING:
    from tg_agent.database import Database

logger = logging.getLogger(__name__)

_DEFAULT_CATEGORY = "default"


def floodgate_enabled() -> bool:
    """Read TG_AGENT_FLOODGATE (default: on; 0/false/no/off disables)."""
    raw = os.getenv("TG_AGENT_FLOODGATE", "1").strip().lower()
    if raw in {"0", "false", "no", "off"}:
        return False
    if raw in {"", "1", "true", "yes", "on"}:
        return True
    logger.warning("Invalid TG_AGENT_FLOODGATE value; keeping the floodgate enabled")
    return True


def category_for_tool(tool_name: str) -> str:
    """Flood category for a tool name; unannotated tools take ``default``."""
    from tg_agent.agent.tools.permissions import TOOL_FLOOD_CATEGORIES

    return TOOL_FLOOD_CATEGORIES.get(tool_name, _DEFAULT_CATEGORY)


def floodgate_rate_limited_message(tool_name: str, retry_after: float) -> dict:
    """Stable rate-limit reply in the shape agents already honor."""
    wait_clause = (
        f"{retry_after:.0f} сек" if retry_after < 90 else f"{retry_after / 60:.1f} мин"
    )
    code = f"FLOODGATE-ERR-{zlib.crc32(tool_name.encode()) % 1000:03d}"
    return {
        "content": [
            {
                "type": "text",
                "text": (
                    f"⏳ Локальный floodgate откладывает «{tool_name}»: повторите через "
                    f"{wait_clause}. НЕ повторяйте вызов немедленно (код: {code})."
                ),
            }
        ]
    }


def _pool_floodgate(pool: Any) -> tuple[TelegramRateLimitGate, FloodCircuitBreaker] | None:
    """The pool's shared gate+breaker pair, or None when unavailable.

    isinstance guards keep mock pools (and older fakes) outside the boundary:
    a fake that happens to expose unrelated attributes must not gate tools.
    """
    gate = getattr(pool, "_rate_limit_gate", None)
    breaker = getattr(pool, "_flood_breaker", None)
    if isinstance(gate, TelegramRateLimitGate) and isinstance(breaker, FloodCircuitBreaker):
        return gate, breaker
    return None


def floodgate_defer(
    tool_name: str,
    phone: str,
    *,
    gate: TelegramRateLimitGate,
    breaker: FloodCircuitBreaker,
) -> tuple[float, bool] | None:
    """Seconds the boundary defers this tool call, or None to proceed.

    The bool separates a breaker suspension from a gate deferral: a
    suspension granted nothing, so the caller must not settle the pair as a
    success — that would close an OPEN breaker after a single refusal.

    Never raises: a broken boundary must never take a tool down.
    """
    try:
        # The breaker refuses first (no slots burned while suspended), then
        # the proactive window is peeked. Peeking matches the physical
        # boundary's own charge decision without spending a slot.
        breaker.check(tool_name, phone)
        delay = gate.peek_batch(phone, [(category_for_tool(tool_name), None)])
        return (delay, False) if delay > 0 else None
    except TelegramOperationSuspendedError as exc:
        return exc.retry_after_sec, True
    except Exception:
        logger.warning("Floodgate check failed; letting the tool call through.", exc_info=True)
        return None


def wrap_tool_with_floodgate(
    tool: SdkMcpTool, *, db: Database, client_pool: Any | None
) -> SdkMcpTool:
    """Wrap a SdkMcpTool with the defer/settle floodgate protocol.

    Non-phone-bound tools (DB-only surface) skip the boundary entirely; a
    pool without real floodgate primitives (CLI mode, fakes) passes through.
    """
    # Lazy: the derivation imports every tool module — package-import paths
    # must not pay for it (mirrors the permissions.py laziness contract).
    from tg_agent.agent.tools.permissions import PHONE_BINDED_TOOLS

    tool_name = tool.name
    if tool_name not in PHONE_BINDED_TOOLS:
        return tool
    original_handler = tool.handler

    @functools.wraps(original_handler)
    async def wrapped_handler(*args: Any, **kwargs: Any):
        if not floodgate_enabled() or client_pool is None:
            return await original_handler(*args, **kwargs)
        pair = _pool_floodgate(client_pool)
        if pair is None:
            return await original_handler(*args, **kwargs)
        gate, breaker = pair
        phone = normalize_phone(kwargs.get("phone"))
        if not phone:
            try:
                phone, _err = await resolve_phone(db, "")
                if _err is not None:
                    phone = ""
            except Exception:
                phone = ""
        if not phone:
            return await original_handler(*args, **kwargs)

        defer = floodgate_defer(tool_name, phone, gate=gate, breaker=breaker)
        if defer is not None:
            seconds, suspended = defer
            logger.info(
                "Floodgate defers %s on %s for %.1fs; returning a rate-limit reply.",
                tool_name,
                phone,
                seconds,
            )
            if not suspended:
                # No call was made: release a breaker trial this check granted.
                _settle(breaker, tool_name, phone, flooded=False)
            return floodgate_rate_limited_message(tool_name, seconds)

        with tool_flood_scope(tool_name, phone) as scope:
            try:
                result = await original_handler(*args, **kwargs)
            except BaseException:
                _settle(breaker, tool_name, phone, flooded=scope.flooded)
                raise
            _settle(breaker, tool_name, phone, flooded=scope.flooded)
            return result

    return SdkMcpTool(
        name=tool.name,
        description=tool.description,
        input_schema=tool.input_schema,
        handler=wrapped_handler,
        annotations=tool.annotations,
    )


def _settle(breaker: FloodCircuitBreaker, tool_name: str, phone: str, *, flooded: bool) -> None:
    """Resolve the breaker trial after a completion; never raises.

    A flood-attributed completion keeps the record the pool reporter already
    wrote; every other completion — non-flood errors included, and deferrals
    that made no call — releases the trial, so a granted probe can never
    wedge the pair.
    """
    if flooded:
        return
    try:
        breaker.record_success(tool_name, phone)
    except Exception:
        logger.debug("Floodgate settle failed for %s on %s", tool_name, phone, exc_info=True)
