"""CLI scheduler mutations must reach the live worker (parity with web).

The worker has no periodic scheduler re-sync: the only way a settings mutation
reaches the running APScheduler is a ``scheduler.reconcile`` telegram command.
Web routes enqueue it after every mutation (``src/web/scheduler/handlers.py``),
but the CLI ``stop`` / ``job-toggle`` / ``set-interval`` impls wrote the setting
only — the live scheduler kept the old state until a worker restart (found live
2026-10-02: ``collect_all`` kept firing every 30 min after a CLI job-toggle).
``queue-pause`` / ``queue-resume`` already follow the command pattern
(audit #835/5); these tests pin the same parity for the remaining three.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from src.cli.commands import scheduler as scheduler_cmd
from src.config import AppConfig
from src.database import Database
from src.models import TelegramCommandStatus

pytestmark = pytest.mark.aiosqlite_serial

# (impl suffix, kwargs) — every CLI mutation the live scheduler consumes.
MUTATIONS = [
    ("stop", {}),
    ("job-toggle", {"job_id": "collect_all"}),
    ("set-interval", {"job_id": "collect_all", "minutes": 45}),
]

_SECRET = "test-session-encryption-key"


@contextmanager
def _patched_init_db(fake_init_db: Callable[..., Coroutine[Any, Any, tuple[AppConfig, Database]]]):
    with patch("src.cli.runtime.init_db", side_effect=fake_init_db):
        yield


async def _run_impl(
    tmp_path: Path, impl: Callable[..., Coroutine[Any, Any, None]], kwargs: dict, *, calls: int = 1
) -> Database:
    """Run a scheduler ``*_impl`` against a throwaway DB; return an open handle.

    The impl closes the DB it receives (``finally: await db.close()``), so each
    call opens a fresh connection on the same file and the assertions go through
    a second connection opened after the last call.
    """
    db_path = str(tmp_path / "impl.db")

    async def fake_init_db(config_path: str):
        db = Database(db_path, session_encryption_secret=_SECRET)
        await db.initialize()
        return AppConfig(), db

    with _patched_init_db(fake_init_db):
        for _ in range(calls):
            await impl("config.yaml", **kwargs)

    verify = Database(db_path, session_encryption_secret=_SECRET)
    await verify.initialize()
    return verify


@pytest.mark.parametrize(("impl_suffix", "kwargs"), MUTATIONS)
async def test_cli_scheduler_mutation_enqueues_reconcile(tmp_path, impl_suffix, kwargs):
    impl = getattr(scheduler_cmd, f"{impl_suffix.replace('-', '_')}_impl")

    verify = await _run_impl(tmp_path, impl, kwargs)
    try:
        commands = await verify.repos.telegram_commands.list_commands(
            command_type="scheduler.reconcile",
            limit=10,
        )
        assert commands, (
            f"CLI scheduler {impl_suffix} must enqueue scheduler.reconcile — "
            "the running worker has no other way to pick up the new setting"
        )
        assert all(c.status == TelegramCommandStatus.PENDING for c in commands)
        assert all(c.requested_by == f"cli:scheduler.{impl_suffix}" for c in commands)
    finally:
        await verify.close()


@pytest.mark.parametrize(("impl_suffix", "kwargs"), MUTATIONS)
async def test_cli_scheduler_mutation_reconcile_is_deduplicated(tmp_path, impl_suffix, kwargs):
    """Repeated mutations collapse into one pending reconcile (web parity)."""
    impl = getattr(scheduler_cmd, f"{impl_suffix.replace('-', '_')}_impl")

    verify = await _run_impl(tmp_path, impl, kwargs, calls=2)
    try:
        commands = await verify.repos.telegram_commands.list_commands(
            command_type="scheduler.reconcile",
            limit=10,
        )
        assert len(commands) == 1
    finally:
        await verify.close()
