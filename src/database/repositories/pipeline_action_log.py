"""Леджер обработанных нодами пайплайна сообщений (дедуп react/forward/delete)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import aiosqlite

if TYPE_CHECKING:
    from src.database.facade import Database


class PipelineActionLogRepository:
    """Tracks which messages a pipeline node already acted on, so re-runs over
    the same time window do not re-react/re-forward/re-delete them (issue #471)."""

    def __init__(
        self,
        db: aiosqlite.Connection,
        *,
        database: "Database | None" = None,
    ):
        self._db = db
        self._database = database

    async def processed_message_ids(
        self,
        pipeline_id: int,
        node_id: str,
        action: str,
        since_hours: float | None = None,
    ) -> set[tuple[int, int]]:
        """Return already-processed ``(channel_id, message_id)`` pairs for this
        pipeline+node+action.

        The pair — not the bare message_id — is the identity: Telegram message ids
        are per-channel (both start at 1), so a pipeline pulling from several source
        channels could otherwise dedup a fresh message in channel B against an
        already-processed message with the same id in channel A.

        When ``since_hours`` is given, only rows newer than that window are loaded.
        Messages older than the fetch window can never match the current run's
        candidates anyway, so this caps memory/scan growth on long-lived pipelines
        instead of reading the whole unbounded log every run.
        """
        if since_hours is not None:
            cur = await self._db.execute(
                "SELECT channel_id, message_id FROM pipeline_action_log "
                "WHERE pipeline_id = ? AND node_id = ? AND action = ? "
                "AND created_at > datetime('now', ?)",
                (pipeline_id, node_id, action, f"-{float(since_hours)} hours"),
            )
        else:
            cur = await self._db.execute(
                "SELECT channel_id, message_id FROM pipeline_action_log "
                "WHERE pipeline_id = ? AND node_id = ? AND action = ?",
                (pipeline_id, node_id, action),
            )
        rows = await cur.fetchall()
        return {(int(r["channel_id"]), int(r["message_id"])) for r in rows}

    async def log_action(
        self,
        pipeline_id: int,
        node_id: str,
        action: str,
        channel_id: int,
        message_id: int,
    ) -> None:
        """Record that (pipeline, node, action) acted on a message. Idempotent."""
        assert self._database is not None, (
            "PipelineActionLogRepository.log_action requires a Database reference"
        )
        await self._database.execute_write(
            "INSERT OR IGNORE INTO pipeline_action_log "
            "(pipeline_id, node_id, action, channel_id, message_id) VALUES (?, ?, ?, ?, ?)",
            (pipeline_id, node_id, action, channel_id, message_id),
        )
