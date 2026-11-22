"""Flood-wait family scoping policy (#1832).

Classifies a transport operation tag into its flood family (the gate
category) and splits flood reports into:

* **write families** — the ``send``, ``channel_lifecycle`` and ``admin_action``
  buckets plus the raw-invoke wildcard. A FloodWait there keeps the historical
  account-wide behavior (``accounts.flood_wait_until``): Telegram refused a
  mutating RPC.
* **read families** — everything else (``resolve``, ``history``, ``default``,
  ``reaction``, ...). A FloodWait there records a per-(phone, family) deadline
  (``account_family_floods``) and leaves the account writable, so a history
  read or a username resolve cannot freeze publishing (#1832).

Unattributed reports (floodgate's 2-arg reporter calls) stay account-wide: the
report must carry the underlying RPC before it can be scoped narrower
(floodgate#34, app #1828). This module never edits the classification tables —
it only reads them (``_GATE_OPERATION_CATEGORIES``, ``gate.category_for``).
"""

from __future__ import annotations

import inspect
from typing import Any

# Categories whose FloodWait blocks the whole account (Telegram refused a
# mutating RPC). Everything else is a read family and gets per-family scoping.
WRITE_FAMILY_CATEGORIES = {"send", "channel_lifecycle", "admin_action"}

# Raw invoke is a wildcard: the underlying RPC may be a write, so its flood
# can never be scoped narrower than the account.
UNSCOPED_OPERATIONS = {"telegram_invoke_request"}

# Families whose flood still lets an account collect (#1879): a channel with a
# cached access key needs no username lookup, and the collector keeps such an
# account in cache-only mode, so no lookup is sent on it.
COLLECTION_ALLOWED_FAMILIES = frozenset({"resolve"})


def resolve_operation_family(
    operation: str,
    gate: Any | None = None,
    app_categories: dict[str, str] | None = None,
) -> str:
    """Resolve an operation tag to its flood family (the gate category).

    Same resolution order as the proactive gate slot reservation: the
    access-layer gap table, then the floodgate gate, then the permissive
    ``default`` bucket. Never raises (a strict-categories gate refusal is
    treated as ``default`` — scoping may only ever be conservative, and
    ``default`` is a read family that still blocks its own family).
    """
    category: str | None = None
    if app_categories:
        category = app_categories.get(operation)
    if category is None and gate is not None:
        category_for = getattr(gate, "category_for", None)
        if callable(category_for):
            try:
                resolved = category_for(operation)
            except Exception:
                resolved = None
            # isinstance guard doubles as AsyncMock/stray-object tolerance.
            category = resolved if isinstance(resolved, str) else None
    return category or "default"


def is_read_family(operation: str, family: str) -> bool:
    """True when a flood on this operation scopes to the family only."""
    return family not in WRITE_FAMILY_CATEGORIES and operation not in UNSCOPED_OPERATIONS


def family_floods_repo(db: Any) -> Any:
    """The accounts repo that implements the family-flood slice, or None.

    Both real stores implement it: the project database
    (``AccountsRepository``) and the gateway's ``GatewayPoolStorage`` (#1844).
    A store without the slice — and a test double that stubs only legacy
    methods (plain MagicMock repos) — reads as None; flood scoping then
    degrades to the historical account-wide report instead of dropping the
    flood signal.
    """
    repo = getattr(getattr(db, "repos", None), "accounts", None)
    if not inspect.iscoroutinefunction(getattr(repo, "list_active_family_floods", None)):
        return None
    return repo
