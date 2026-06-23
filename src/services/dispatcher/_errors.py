"""Dispatcher control-flow exceptions (#1047).

Lives in the subpackage so both the facade and the mixins can raise/catch it
without a circular import. The facade re-exports
:class:`TelegramCommandRetryLaterError` so existing
``from src.services.telegram_command_dispatcher import TelegramCommandRetryLaterError``
imports keep working.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class TelegramCommandRetryLaterError(RuntimeError):
    """Raised by a handler to requeue its command for a later run.

    The dispatcher loop catches this, resets the command to PENDING with the
    given ``run_after`` and ``result_payload``, and does not mark it failed.
    """

    run_after: datetime
    reason: str
    result_payload: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.reason
