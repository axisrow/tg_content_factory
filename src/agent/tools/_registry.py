"""Tool registry and confirmation helpers for agent tools."""

from __future__ import annotations

import json as _json
import logging

logger = logging.getLogger(__name__)


def _text_response(text: str) -> dict:
    """Wrap text into MCP tool response format."""
    return {"content": [{"type": "text", "text": text}]}


def normalize_phone(phone: str) -> str:
    """Ensure phone starts with '+' — models sometimes omit it."""
    phone = phone.strip()
    if phone and not phone.startswith("+"):
        phone = "+" + phone
    return phone


def require_confirmation(action_description: str, args: dict) -> dict | None:
    """Return a warning response if confirm is not True, else None (proceed).

    Usage in a tool handler::

        gate = require_confirmation("удалит канал 'X'", args)
        if gate:
            return gate
        # ... execute dangerous action
    """
    if args.get("confirm"):
        return None
    return _text_response(
        f"⚠️ Эта операция {action_description}. "
        f"Подтвердите, вызвав tool повторно с confirm=true."
    )


def require_pool(client_pool: object | None, action: str = "Эта операция") -> dict | None:
    """Return an error response if client_pool is None (CLI mode), else None."""
    if client_pool is not None:
        return None
    return _text_response(
        f"❌ {action} требует Telegram-клиент, который недоступен в CLI-режиме. "
        f"Используйте web-интерфейс."
    )


async def require_phone_permission(db: object, phone: str, tool_name: str) -> dict | None:
    """Return helpful response with allowed phones if not permitted, else None.

    If db has no phone permissions configured, returns None (all phones allowed).
    If phone is in allowed list for this tool, returns None (proceed).
    Otherwise returns a message with list of allowed phones so agent can retry.
    """
    try:
        raw = await db.get_setting("agent_phone_tool_permissions")
    except Exception:
        return None  # DB error → allow all
    if not raw:
        return None  # no restrictions configured → allow all
    try:
        perms = _json.loads(raw)
    except (ValueError, TypeError):
        return None  # malformed → allow all
    # Collect phones allowed for this tool
    allowed_phones = [p for p, tools in perms.items() if tool_name in tools]
    if not allowed_phones:
        return None  # tool not restricted for any phone → allow all
    if phone in allowed_phones:
        return None  # phone is allowed
    # Phone not allowed — return list of allowed phones so agent can retry
    phones_str = ", ".join(allowed_phones)
    if not phone:
        msg = (
            f"ℹ️ Для инструмента '{tool_name}' укажи параметр phone. "
            f"Разрешённые телефоны: {phones_str}"
        )
    else:
        msg = (
            f"❌ Телефон {phone} не разрешён для '{tool_name}'. "
            f"Разрешённые телефоны: {phones_str}"
        )
    return _text_response(msg)
