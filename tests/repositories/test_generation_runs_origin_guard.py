"""Provenance of ``generation_runs.moderation_status`` (#1315)."""

from __future__ import annotations

import pytest

from tg_agent.database.repositories.generation_runs import GenerationRunsRepository


@pytest.fixture
async def runs_repo(db):
    return GenerationRunsRepository(db.db, database=db)


async def _status(runs_repo, run_id: int) -> tuple[str, str]:
    run = await runs_repo.get(run_id)
    assert run is not None
    return run.moderation_status, run.moderation_origin


async def test_auto_approve_cannot_override_human_rejection(runs_repo):
    run_id = await runs_repo.create_run(None, "prompt")
    assert await runs_repo.set_moderation_status(run_id, "rejected", origin="human", actor="cli") == 1

    assert await runs_repo.set_moderation_status(run_id, "approved", origin="auto", actor="agent") == 0

    assert await _status(runs_repo, run_id) == ("rejected", "human")
    history = await runs_repo._database.repos.decisions.history(
        "generation_run", run_id, field="moderation_status"
    )
    assert [(d.origin, d.actor, d.old_value, d.new_value) for d in history] == [
        ("auto", "agent", "rejected", "approved"),
        ("human", "cli", "pending", "rejected"),
    ]


async def test_bulk_auto_approve_skips_human_rejection_and_journals_attempt(runs_repo):
    rejected = await runs_repo.create_run(None, "a")
    fresh = await runs_repo.create_run(None, "b")
    await runs_repo.set_moderation_status(rejected, "rejected", origin="human")

    skipped = await runs_repo.set_moderation_status_bulk(
        [rejected, fresh], "approved", origin="auto", actor="agent"
    )

    assert skipped == [rejected]
    assert await _status(runs_repo, rejected) == ("rejected", "human")
    assert await _status(runs_repo, fresh) == ("approved", "auto")
    attempts = await runs_repo._database.repos.decisions.history(
        "generation_run", rejected, field="moderation_status"
    )
    assert attempts[0].origin == "auto" and attempts[0].new_value == "approved"


async def test_human_can_revise_own_decision(runs_repo):
    run_id = await runs_repo.create_run(None, "prompt")
    await runs_repo.set_moderation_status(run_id, "rejected", origin="human")

    assert await runs_repo.set_moderation_status(run_id, "approved", origin="human") == 1

    assert await _status(runs_repo, run_id) == ("approved", "human")


async def test_auto_write_without_human_decision_still_applies(runs_repo):
    run_id = await runs_repo.create_run(None, "prompt")

    assert await runs_repo.set_moderation_status(run_id, "approved", origin="auto", actor="pipeline") == 1

    assert await _status(runs_repo, run_id) == ("approved", "auto")
    # Successful automatic writes are routine and stay out of the journal.
    assert await runs_repo._database.repos.decisions.history("generation_run", run_id) == []


async def test_set_published_at_never_flips_human_rejection(runs_repo):
    run_id = await runs_repo.create_run(None, "prompt")
    await runs_repo.set_moderation_status(run_id, "rejected", origin="human")

    await runs_repo.set_published_at(run_id)

    run = await runs_repo.get(run_id)
    assert run is not None
    assert run.moderation_status == "rejected"
    assert run.published_at is None
