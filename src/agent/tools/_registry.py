"""Tool registry and confirmation helpers for agent tools."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def _text_response(text: str) -> dict:
    """Wrap text into MCP tool response format."""
    return {"content": [{"type": "text", "text": text}]}


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
