"""REST response schemas for agent endpoints (#1070)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ThreadMessagesResponse(BaseModel):
    """Messages of an agent thread (``GET /agent/threads/{id}/messages``)."""

    thread_id: int
    title: str | None = None
    messages: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Chronological agent messages (id, role, content, created_at, …).",
    )


class AgentChannelItem(BaseModel):
    """A channel offered to the agent channel picker (``GET /agent/channels-json``)."""

    id: int = Field(..., description="Telegram channel id.")
    title: str
    channel_type: str
