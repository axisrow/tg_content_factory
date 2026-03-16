"""Tests for SettingsRepository."""

from __future__ import annotations

import pytest

from src.database.repositories.settings import SettingsRepository


@pytest.fixture
async def repo(db):
    """Create repository instance."""
    return SettingsRepository(db.db)


async def test_get_setting_not_found(repo):
    """Test getting non-existent setting returns None."""
    result = await repo.get_setting("nonexistent_key")
    assert result is None


async def test_set_and_get_setting(repo):
    """Test setting and getting a value."""
    await repo.set_setting("test_key", "test_value")
    result = await repo.get_setting("test_key")
    assert result == "test_value"


async def test_set_setting_upsert(repo):
    """Test that set_setting updates existing value."""
    await repo.set_setting("key1", "value1")
    await repo.set_setting("key1", "value2")
    result = await repo.get_setting("key1")
    assert result == "value2"


async def test_set_setting_empty_string(repo):
    """Test setting empty string value."""
    await repo.set_setting("empty_key", "")
    result = await repo.get_setting("empty_key")
    assert result == ""


async def test_set_setting_unicode(repo):
    """Test setting unicode value."""
    await repo.set_setting("unicode_key", "Значение на русском 🎉")
    result = await repo.get_setting("unicode_key")
    assert result == "Значение на русском 🎉"


async def test_set_setting_long_value(repo):
    """Test setting long value."""
    long_value = "x" * 10000
    await repo.set_setting("long_key", long_value)
    result = await repo.get_setting("long_key")
    assert result == long_value


async def test_roundtrip_multiple_settings(repo):
    """Test multiple settings roundtrip."""
    settings = {
        "key1": "value1",
        "key2": "value2",
        "key3": "value3",
    }
    for k, v in settings.items():
        await repo.set_setting(k, v)

    for k, v in settings.items():
        result = await repo.get_setting(k)
        assert result == v


async def test_get_setting_case_sensitive(repo):
    """Test that keys are case-sensitive."""
    await repo.set_setting("TestKey", "value1")
    await repo.set_setting("testkey", "value2")

    assert await repo.get_setting("TestKey") == "value1"
    assert await repo.get_setting("testkey") == "value2"
