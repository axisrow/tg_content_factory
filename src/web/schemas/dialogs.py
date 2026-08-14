"""REST response schemas for dialog endpoints (#1070).

The participants/broadcast-stats endpoints serve a cached worker snapshot when
available (200) or enqueue a worker command and return
``QueuedCommandResponse`` (202). These models document the 200 success shape.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class DialogParticipant(BaseModel):
    """A single chat participant."""

    id: int
    first_name: str = ""
    last_name: str = ""
    username: str = ""


class ParticipantsResponse(BaseModel):
    """Chat participants snapshot (``GET /dialogs/participants``, HTTP 200).

    When no snapshot is cached the endpoint instead returns HTTP 202 with a
    ``QueuedCommandResponse`` body.
    """

    participants: list[DialogParticipant] = Field(default_factory=list)
    total: int = 0


class DialogHistoryMessage(BaseModel):
    """A single message from a dialog's live history."""

    id: int
    sender_id: int | None = None
    sender_name: str | None = None
    out: bool = False
    date: str
    text: str | None = None
    media_type: str | None = None
    reply_to_id: int | None = None
    is_forward: bool = False


class DialogHistoryResponse(BaseModel):
    """Dialog message history snapshot (``GET /dialogs/history``, HTTP 200).

    When no snapshot is cached the endpoint instead returns HTTP 202 with a
    ``QueuedCommandResponse`` body.
    """

    messages: list[DialogHistoryMessage] = Field(default_factory=list)
    total: int = 0
    dialog_id: int | None = None


class BroadcastStatsResponse(BaseModel):
    """Channel broadcast statistics snapshot (``GET /dialogs/broadcast-stats``, HTTP 200).

    The ``stats`` payload is heterogeneous (per-metric current/previous deltas,
    period bounds, raw fallback), so it is documented as a free-form object.
    """

    stats: dict[str, Any] = Field(
        default_factory=dict,
        description="Broadcast metrics: followers, views_per_post, period, etc.",
    )
