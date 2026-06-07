from __future__ import annotations

from inspect import isawaitable
from typing import Any

from src.models import Account
from src.utils.introspection import explicit_pool_method


async def load_live_usable_accounts(db: Any, *, active_only: bool = False) -> list[Account]:
    """Load decryptable accounts when the DB facade supports it.

    Some tests use unspecced MagicMock databases, where getattr() fabricates
    attributes. Require an explicit instance or class attribute before using the
    safe loader, then fall back to the historical strict get_accounts() method.
    """
    getter = explicit_pool_method(db, "get_live_usable_accounts")
    if getter is None:
        getter = getattr(db, "get_accounts")

    result = getter(active_only=active_only)
    if isawaitable(result):
        result = await result
    return list(result or [])
