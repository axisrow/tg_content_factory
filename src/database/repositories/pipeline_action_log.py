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
    ) -> set[int]:
        """Return the set of already-processed message_ids for this pipeline+node+action."""
        cur = await self._db.execute(
            "SELECT message_id FROM pipeline_action_log "
            "WHERE pipeline_id = ? AND node_id = ? AND action = ?",
            (pipeline_id, node_id, action),
        )
        rows = await cur.fetchall()
        return {int(r["message_id"]) for r in rows}

    async def log_action(
        self,
        pipeline_id: int,
        node_id: str,
        action: str,
        channel_id: int,
        message_id: int,
    ) -> None:
        """Record that (pipeline, node, action) acted on a message. Idempotent."""
        await self._database.execute_write(
            "INSERT OR IGNORE INTO pipeline_action_log "
            "(pipeline_id, node_id, action, channel_id, message_id) VALUES (?, ?, ?, ?, ?)",
            (pipeline_id, node_id, action, channel_id, message_id),
        )
