from __future__ import annotations

from inspect import isawaitable
from typing import Any

from src.models import Account


def _has_explicit_attr(target: object, name: str) -> bool:
    try:
        if name in vars(target):
            return True
    except TypeError:
        pass
    return hasattr(type(target), name)


async def load_live_usable_accounts(db: Any, *, active_only: bool = False) -> list[Account]:
    """Load decryptable accounts when the DB facade supports it.

    Some tests use unspecced MagicMock databases, where getattr() fabricates
    attributes. Require an explicit instance or class attribute before using the
    safe loader, then fall back to the historical strict get_accounts() method.
    """
    getter = None
    if _has_explicit_attr(db, "get_live_usable_accounts"):
        getter = getattr(db, "get_live_usable_accounts", None)
    if not callable(getter):
        getter = getattr(db, "get_accounts")

    result = getter(active_only=active_only)
    if isawaitable(result):
        result = await result
    return list(result or [])
