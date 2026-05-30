"""Tests for PipelineActionLogRepository (issue #471)."""

from __future__ import annotations

import pytest

from src.database.repositories.pipeline_action_log import PipelineActionLogRepository


@pytest.fixture
async def action_log_repo(db):
    return PipelineActionLogRepository(db.db, database=db)


async def test_empty_returns_no_processed_ids(action_log_repo):
    assert await action_log_repo.processed_message_ids(1, "react", "react") == set()


async def test_log_and_read_back(action_log_repo):
    await action_log_repo.log_action(1, "react", "react", -100, 5)
    await action_log_repo.log_action(1, "react", "react", -100, 6)
    assert await action_log_repo.processed_message_ids(1, "react", "react") == {(-100, 5), (-100, 6)}


async def test_log_action_is_idempotent(action_log_repo):
    await action_log_repo.log_action(1, "react", "react", -100, 5)
    await action_log_repo.log_action(1, "react", "react", -100, 5)
    assert await action_log_repo.processed_message_ids(1, "react", "react") == {(-100, 5)}


async def test_scoped_by_pipeline_node_and_action(action_log_repo):
    await action_log_repo.log_action(1, "react", "react", -100, 5)
    await action_log_repo.log_action(2, "react", "react", -100, 5)
    await action_log_repo.log_action(1, "forward", "forward", -100, 5)
    await action_log_repo.log_action(1, "react", "delete_message", -100, 5)

    assert await action_log_repo.processed_message_ids(1, "react", "react") == {(-100, 5)}
    assert await action_log_repo.processed_message_ids(2, "react", "react") == {(-100, 5)}
    assert await action_log_repo.processed_message_ids(1, "forward", "forward") == {(-100, 5)}
    assert await action_log_repo.processed_message_ids(99, "react", "react") == set()


async def test_processed_ids_are_scoped_by_channel(action_log_repo):
    """Regression: Telegram message ids are per-channel, so the same id in two
    channels must not collapse — otherwise a fresh message in channel B gets
    silently skipped because channel A already used that id."""
    await action_log_repo.log_action(1, "react", "react", -100, 42)
    processed = await action_log_repo.processed_message_ids(1, "react", "react")
    assert (-100, 42) in processed
    # A different channel with the same bare message id must NOT be considered processed.
    assert (-200, 42) not in processed


async def test_since_hours_window_excludes_old_rows(action_log_repo, db):
    """processed_message_ids(since_hours=...) must drop rows older than the window
    so the log does not grow into an unbounded per-run scan."""
    # One fresh row via the normal path, one stamped 1000h in the past directly.
    await action_log_repo.log_action(1, "react", "react", -100, 5)
    await db.execute_write(
        "INSERT INTO pipeline_action_log "
        "(pipeline_id, node_id, action, channel_id, message_id, created_at) "
        "VALUES (?, ?, ?, ?, ?, datetime('now', '-1000 hours'))",
        (1, "react", "react", -100, 999),
    )
    windowed = await action_log_repo.processed_message_ids(1, "react", "react", since_hours=24.0)
    assert (-100, 5) in windowed
    assert (-100, 999) not in windowed
    # Without the window both rows are returned.
    unbounded = await action_log_repo.processed_message_ids(1, "react", "react")
    assert {(-100, 5), (-100, 999)} == unbounded
