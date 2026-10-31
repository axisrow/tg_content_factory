"""REST response schemas for account endpoints (#1070)."""

from __future__ import annotations

from pydantic import BaseModel, Field


class FloodStatusItem(BaseModel):
    """Flood-wait status of one account (``GET /settings/flood-status``)."""

    phone: str
    flood_wait_until: str = Field(
        ...,
        description="'ok' when not flooded, otherwise the flood-wait expiry as 'YYYY-MM-DD HH:MM:SS UTC'.",
    )
    remaining_seconds: int = Field(..., description="Seconds until the flood-wait clears; 0 when not flooded.")


class AccountInfoResponse(BaseModel):
    """Account summary plus live Telegram diagnostics (``GET /settings/{id}/info``).

    The account-summary fields come from ``Account.model_dump`` and vary by
    schema version, so they are accepted as additional properties; ``live_info``
    is always present.
    """

    model_config = {"extra": "allow"}

    live_info: str = Field(..., description="Live Telegram account diagnostics, or an error string.")

    # Common summary fields (subset of Account) surfaced for documentation;
    # the full Account.model_dump payload is merged in via extra='allow'.
    id: int | None = None
    phone: str | None = None
    is_active: bool | None = None
    is_primary: bool | None = None
