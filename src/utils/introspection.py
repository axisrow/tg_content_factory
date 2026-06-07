from __future__ import annotations

from typing import Any


def explicit_pool_method(target: Any, name: str) -> Any | None:
    """Return ``target.name`` only if it is an explicitly-defined callable.

    Test doubles built on ``MagicMock`` auto-fabricate a callable child for any
    attribute name, so a plain ``getattr(target, name, None)`` would treat
    every unimplemented method as present. Require either a real instance
    attribute (``target.__dict__``) or a class-level definition before
    returning it; otherwise return ``None`` so callers can fall back.
    """
    instance_attrs = getattr(target, "__dict__", {})
    if isinstance(instance_attrs, dict) and name in instance_attrs:
        candidate = instance_attrs[name]
    elif callable(getattr(type(target), name, None)):
        candidate = getattr(target, name)
    else:
        return None
    return candidate if callable(candidate) else None
