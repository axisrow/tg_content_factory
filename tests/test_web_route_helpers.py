"""Tests for web route helpers and small module behaviors."""
from __future__ import annotations

# --- collection queue behavior ---


async def test_collection_queue_status_enum():
    from tg_agent.collection_queue import CollectionTaskStatus

    assert CollectionTaskStatus.PENDING.value == "pending"
    assert CollectionTaskStatus.COMPLETED.value == "completed"
