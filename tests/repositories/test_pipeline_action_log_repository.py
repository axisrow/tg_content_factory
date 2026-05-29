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
    assert await action_log_repo.processed_message_ids(1, "react", "react") == {5, 6}


async def test_log_action_is_idempotent(action_log_repo):
    await action_log_repo.log_action(1, "react", "react", -100, 5)
    await action_log_repo.log_action(1, "react", "react", -100, 5)
    assert await action_log_repo.processed_message_ids(1, "react", "react") == {5}


async def test_scoped_by_pipeline_node_and_action(action_log_repo):
    await action_log_repo.log_action(1, "react", "react", -100, 5)
    await action_log_repo.log_action(2, "react", "react", -100, 5)
    await action_log_repo.log_action(1, "forward", "forward", -100, 5)
    await action_log_repo.log_action(1, "react", "delete_message", -100, 5)

    assert await action_log_repo.processed_message_ids(1, "react", "react") == {5}
    assert await action_log_repo.processed_message_ids(2, "react", "react") == {5}
    assert await action_log_repo.processed_message_ids(1, "forward", "forward") == {5}
    assert await action_log_repo.processed_message_ids(99, "react", "react") == set()
