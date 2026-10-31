"""Fixtures for the tg_tui package tests (the core's tests/conftest.py is out of scope here)."""

from __future__ import annotations

import pytest

from tg_agent.database import Database


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """Asyncio only — the TUI runs on Textual's asyncio loop."""
    return "asyncio"


@pytest.fixture
async def db(anyio_backend):
    """In-memory test database (mirrors the core suite's ``db`` fixture)."""
    database = Database(":memory:", session_encryption_secret="test-session-encryption-key")
    await database.initialize()
    yield database
    await database.close()
