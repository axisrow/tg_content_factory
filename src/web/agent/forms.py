"""Request body parsing for the agent web domain."""

from __future__ import annotations

from pydantic import BaseModel

from src.agent.models import CLAUDE_MODEL_IDS


class ChatRequest(BaseModel):
    message: str
    model: str | None = None


def select_model(raw_model: object) -> str | None:
    """Return the requested model id only if it is a known Claude model, else None."""
    model = (str(raw_model) if raw_model is not None else "").strip()
    return model if model in CLAUDE_MODEL_IDS else None
