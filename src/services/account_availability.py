"""Single source of truth for per-account availability state (#529).

The Settings UI and the agent tools must agree on whether an account is
"available", "disconnected", needs interactive re-auth, etc. Previously the
Settings page computed this inline while the agent had no equivalent and would
wrongly tell the user an account was unavailable / needed re-auth even when
Settings showed "Availability: OK". This helper is the one place that decides,
so both surfaces stay in lock-step.
"""

from __future__ import annotations

from datetime import datetime, timezone

from src.models import AccountSessionStatus

# Ordered roughly by severity. Mirrors the states rendered by the Settings page.
AVAILABILITY_STATES = (
    "available",
    "flood",
    "disconnected",
    "inactive",
    "session_unavailable",
)


def compute_account_availability(
    account: object,
    *,
    connected: bool,
    now: datetime | None = None,
) -> dict[str, object]:
    """Return ``{"state", "remaining_seconds", "remaining_minutes"}`` for one account.

    ``connected`` is whether the account's phone is currently in the live/worker
    client pool. The precedence — session validity, then active flag, then
    connection, then flood — matches ``handle_settings_page`` exactly.
    """
    now = now or datetime.now(timezone.utc)

    session_status = getattr(account, "session_status", AccountSessionStatus.OK)
    if session_status != AccountSessionStatus.OK:
        return _state("session_unavailable")
    if not getattr(account, "is_active", False):
        return _state("inactive")
    if not connected:
        return _state("disconnected")

    flood_until = getattr(account, "flood_wait_until", None)
    if flood_until is not None:
        if flood_until.tzinfo is None:
            flood_until = flood_until.replace(tzinfo=timezone.utc)
        remaining_seconds = max(0, int((flood_until - now).total_seconds()))
        if remaining_seconds > 0:
            return {
                "state": "flood",
                "remaining_seconds": remaining_seconds,
                "remaining_minutes": max(1, remaining_seconds // 60),
            }
    return _state("available")


def _state(state: str) -> dict[str, object]:
    return {"state": state, "remaining_seconds": 0, "remaining_minutes": 0}
