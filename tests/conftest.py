from __future__ import annotations

import pytest

from src.config import AppConfig
from src.database import Database


@pytest.fixture
async def db(tmp_path):
    """In-memory-like test database (uses temp file)."""
    db_path = str(tmp_path / "test.db")
    database = Database(db_path)
    await database.initialize()
    yield database
    await database.close()


@pytest.fixture
def config():
    return AppConfig()
