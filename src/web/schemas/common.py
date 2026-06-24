"""Shared REST response schemas (#1070)."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ErrorResponse(BaseModel):
    """Generic error envelope returned with a non-2xx status code."""

    error: str = Field(..., description="Machine-readable error code, e.g. 'not_found'.")


class QueuedCommandResponse(BaseModel):
    """Returned with HTTP 202 when a live-Telegram action is enqueued for the worker.

    The web process never opens Telegram connections itself; data-fetching dialog
    endpoints either serve a cached runtime snapshot (200) or enqueue a command
    for the worker and return this envelope (202).
    """

    status: str = Field("queued", description="Always 'queued' for an enqueued command.")
    command_id: int | str = Field(..., description="Identifier of the enqueued worker command.")


class HealthResponse(BaseModel):
    """Liveness/readiness probe payload (``GET /health``)."""

    status: str = Field(..., description="'healthy' when the DB probe succeeds, else 'degraded'.")
    db: bool = Field(..., description="Whether the SQLite probe (SELECT 1) succeeded.")
    accounts_connected: int = Field(..., description="Number of live Telegram clients in the pool.")
