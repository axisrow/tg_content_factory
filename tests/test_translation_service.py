"""Tests for TranslationService: language detection, translation, and repository methods."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import Message
from src.services.translation_service import TranslationService

# ── detect_language ──────────────────────────────────────────────────

def test_detect_language_russian():
    assert TranslationService.detect_language("Привет, как дела? Всё хорошо.") == "ru"


def test_detect_language_english():
    assert TranslationService.detect_language("Hello, how are you doing today?") == "en"


def test_detect_language_chinese():
    result = TranslationService.detect_language("你好世界，这是一个测试消息。")
    assert result == "zh-cn" or result == "zh-tw" or result == "zh"


def test_detect_language_none_for_empty():
    assert TranslationService.detect_language(None) is None
    assert TranslationService.detect_language("") is None


def test_detect_language_none_for_short():
    assert TranslationService.detect_language("hi") is None


# ── translate_message ────────────────────────────────────────────────

@pytest.mark.anyio
async def test_translate_skips_same_language():
    svc = TranslationService(db=AsyncMock())
    result = await svc.translate_message("Привет", "ru", "ru")
    assert result is None


@pytest.mark.anyio
async def test_translate_message_calls_provider():
    mock_provider = AsyncMock(return_value="Hello, how are you?")
    mock_provider_service = MagicMock()
    mock_provider_service.get_provider_callable.return_value = mock_provider

    svc = TranslationService(db=AsyncMock(), provider_service=mock_provider_service)
    result = await svc.translate_message("Привет, как дела?", "ru", "en", provider_name="openai", model="gpt-4o-mini")
    assert result == "Hello, how are you?"
    mock_provider.assert_called_once()


@pytest.mark.anyio
async def test_translate_message_no_provider():
    svc = TranslationService(db=AsyncMock(), provider_service=None)
    result = await svc.translate_message("你好", "zh", "en")
    assert result is None


# ── translate_batch ──────────────────────────────────────────────────

@pytest.mark.anyio
async def test_translate_batch():
    mock_provider = AsyncMock(return_value="1: Hello\n2: Good morning")
    mock_provider_service = MagicMock()
    mock_provider_service.get_provider_callable.return_value = mock_provider

    svc = TranslationService(db=AsyncMock(), provider_service=mock_provider_service)
    messages = [
        Message(id=1, channel_id=100, message_id=1, text="Привет", detected_lang="ru", date=datetime.now(timezone.utc)),
        Message(
            id=2, channel_id=100, message_id=2, text="Доброе утро",
            detected_lang="ru", date=datetime.now(timezone.utc),
        ),
    ]
    results = await svc.translate_batch(messages, "en")
    assert len(results) == 2
    assert results[0] == (1, "Hello")
    assert results[1] == (2, "Good morning")


@pytest.mark.anyio
async def test_translate_batch_skips_same_lang():
    mock_provider_service = MagicMock()
    svc = TranslationService(db=AsyncMock(), provider_service=mock_provider_service)
    messages = [
        Message(id=1, channel_id=100, message_id=1, text="Hello", detected_lang="en", date=datetime.now(timezone.utc)),
    ]
    results = await svc.translate_batch(messages, "en")
    assert results == []


# ── _parse_numbered_response ─────────────────────────────────────────

def test_parse_numbered_response():
    response = "1: Hello world\n2: Good morning\n3: How are you"
    result = TranslationService._parse_numbered_response(response, 3)
    assert result == {0: "Hello world", 1: "Good morning", 2: "How are you"}


def test_parse_numbered_response_with_dots():
    response = "1. Hello world\n2. Good morning"
    result = TranslationService._parse_numbered_response(response, 2)
    assert result == {0: "Hello world", 1: "Good morning"}


def test_parse_numbered_response_multiline():
    response = "1: First paragraph.\nSecond paragraph continues.\n2: Next message."
    result = TranslationService._parse_numbered_response(response, 2)
    assert result == {0: "First paragraph.\nSecond paragraph continues.", 1: "Next message."}


def test_parse_numbered_response_partial():
    response = "1: Hello world\n\n3: How are you"
    result = TranslationService._parse_numbered_response(response, 3)
    assert result == {0: "Hello world", 2: "How are you"}


# ── get_source_filter ────────────────────────────────────────────────

def test_get_source_filter():
    svc = TranslationService(db=AsyncMock())
    assert svc.get_source_filter("zh,ko,ja") == ["zh", "ko", "ja"]
    assert svc.get_source_filter("") == []
    assert svc.get_source_filter(None) == []
    assert svc.get_source_filter(" zh , ko ") == ["zh", "ko"]


# ── DB repository methods ───────────────────────────────────────────

@pytest.mark.anyio
async def test_language_stats(db):
    # Insert messages with detected_lang
    await db.repos.messages._db.execute(
        "INSERT INTO messages (channel_id, message_id, text, date, detected_lang) VALUES (?, ?, ?, ?, ?)",
        (1, 1, "Hello", "2024-01-01T00:00:00", "en"),
    )
    await db.repos.messages._db.execute(
        "INSERT INTO messages (channel_id, message_id, text, date, detected_lang) VALUES (?, ?, ?, ?, ?)",
        (1, 2, "Привет", "2024-01-01T00:00:00", "ru"),
    )
    await db.repos.messages._db.execute(
        "INSERT INTO messages (channel_id, message_id, text, date, detected_lang) VALUES (?, ?, ?, ?, ?)",
        (1, 3, "Hi there", "2024-01-01T00:00:00", "en"),
    )
    await db.repos.messages._db.commit()

    stats = await db.repos.messages.get_language_stats()
    lang_map = dict(stats)
    assert lang_map["en"] == 2
    assert lang_map["ru"] == 1


@pytest.mark.anyio
async def test_update_translation(db):
    await db.repos.messages._db.execute(
        "INSERT INTO messages (channel_id, message_id, text, date, detected_lang) VALUES (?, ?, ?, ?, ?)",
        (1, 1, "Привет", "2024-01-01T00:00:00", "ru"),
    )
    await db.repos.messages._db.commit()

    # Get the id
    cur = await db.repos.messages._db.execute("SELECT id FROM messages WHERE message_id = 1")
    try:
        row = await cur.fetchone()
        msg_id = row["id"]
    finally:
        await cur.close()

    await db.repos.messages.update_translation(msg_id, "en", "Hello")
    msg = await db.repos.messages.get_message_by_id(msg_id)
    assert msg is not None
    assert msg.translation_en == "Hello"
    assert msg.translation_custom is None

    await db.repos.messages.update_translation(msg_id, "custom", "Hallo")
    msg = await db.repos.messages.get_message_by_id(msg_id)
    assert msg.translation_custom == "Hallo"


@pytest.mark.anyio
async def test_get_untranslated_messages(db):
    # Insert channels first for JOIN
    await db.repos.messages._db.execute(
        "INSERT OR IGNORE INTO channels (channel_id, title, username) VALUES (?, ?, ?)",
        (1, "Test Channel", "test"),
    )
    await db.repos.messages._db.execute(
        "INSERT INTO messages (channel_id, message_id, text, date, detected_lang) VALUES (?, ?, ?, ?, ?)",
        (1, 1, "你好", "2024-01-01T00:00:00", "zh-cn"),
    )
    await db.repos.messages._db.execute(
        "INSERT INTO messages (channel_id, message_id, text, date, detected_lang, translation_en)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (1, 2, "Привет", "2024-01-01T00:00:00", "ru", "Hello"),
    )
    await db.repos.messages._db.commit()

    msgs = await db.repos.messages.get_untranslated_messages(target="en")
    assert len(msgs) == 1
    assert msgs[0].detected_lang == "zh-cn"


@pytest.mark.anyio
async def test_get_untranslated_with_source_filter(db):
    await db.repos.messages._db.execute(
        "INSERT OR IGNORE INTO channels (channel_id, title, username) VALUES (?, ?, ?)",
        (1, "Test Channel", "test"),
    )
    await db.repos.messages._db.execute(
        "INSERT INTO messages (channel_id, message_id, text, date, detected_lang) VALUES (?, ?, ?, ?, ?)",
        (1, 1, "你好", "2024-01-01T00:00:00", "zh-cn"),
    )
    await db.repos.messages._db.execute(
        "INSERT INTO messages (channel_id, message_id, text, date, detected_lang) VALUES (?, ?, ?, ?, ?)",
        (1, 2, "Bonjour", "2024-01-01T00:00:00", "fr"),
    )
    await db.repos.messages._db.commit()

    # Filter only zh-cn
    msgs = await db.repos.messages.get_untranslated_messages(target="en", source_langs=["zh-cn"])
    assert len(msgs) == 1
    assert msgs[0].detected_lang == "zh-cn"


@pytest.mark.anyio
async def test_backfill_language_detection(db):
    await db.repos.messages._db.execute(
        "INSERT INTO messages (channel_id, message_id, text, date) VALUES (?, ?, ?, ?)",
        (1, 1, "Hello, this is a test message in English.", "2024-01-01T00:00:00"),
    )
    await db.repos.messages._db.commit()

    updated = await db.repos.messages.backfill_language_detection(batch_size=100)
    assert updated == 1

    cur = await db.repos.messages._db.execute("SELECT detected_lang FROM messages WHERE message_id = 1")
    try:
        row = await cur.fetchone()
        assert row["detected_lang"] == "en"
    finally:
        await cur.close()


# ── detect_language exception fallback ──────────────────────────────


def test_detect_language_exception_returns_none():
    """detect_language returns None for text shorter than 8 chars after strip."""
    # Text shorter than 8 chars triggers early return
    assert TranslationService.detect_language("short") is None


def test_detect_language_with_none_text():
    assert TranslationService.detect_language(None) is None


# ── translate_message with stub default provider ────────────────────


@pytest.mark.anyio
async def test_translate_message_skips_stub_default_provider():
    """When get_provider_callable returns the stub default, translate skips."""
    svc = TranslationService(db=AsyncMock())

    mock_default = AsyncMock(return_value="stub garbage")
    mock_provider_service = MagicMock()
    mock_provider_service.get_provider_callable.return_value = mock_default
    mock_provider_service._registry = {"default": mock_default}
    svc._provider_service = mock_provider_service

    result = await svc.translate_message("text", "ru", "en")
    assert result is None


@pytest.mark.anyio
async def test_translate_message_exception_returns_none():
    """When provider raises, translate_message returns None."""
    mock_provider = AsyncMock(side_effect=RuntimeError("API down"))
    mock_provider_service = MagicMock()
    mock_provider_service.get_provider_callable.return_value = mock_provider
    # Make sure it's not the default stub
    mock_provider_service._registry = {}

    svc = TranslationService(db=AsyncMock(), provider_service=mock_provider_service)
    result = await svc.translate_message("text", "ru", "en")
    assert result is None


# ── translate_batch edge cases ──────────────────────────────────────


@pytest.mark.anyio
async def test_translate_batch_empty_messages():
    svc = TranslationService(db=AsyncMock(), provider_service=MagicMock())
    result = await svc.translate_batch([], "en")
    assert result == []


@pytest.mark.anyio
async def test_translate_batch_no_provider():
    svc = TranslationService(db=AsyncMock(), provider_service=None)
    msgs = [Message(id=1, channel_id=100, message_id=1, text="hi", detected_lang="ru", date=datetime.now(timezone.utc))]
    result = await svc.translate_batch(msgs, "en")
    assert result == []


@pytest.mark.anyio
async def test_translate_batch_skips_stub_default_provider():
    """Batch translation should skip when only stub default provider available."""
    mock_default = AsyncMock(return_value="stub")
    mock_ps = MagicMock()
    mock_ps.get_provider_callable.return_value = mock_default
    mock_ps._registry = {"default": mock_default}

    svc = TranslationService(db=AsyncMock(), provider_service=mock_ps)
    msgs = [
        Message(id=1, channel_id=100, message_id=1, text="Привет", detected_lang="ru", date=datetime.now(timezone.utc)),
    ]
    result = await svc.translate_batch(msgs, "en")
    assert result == []


@pytest.mark.anyio
async def test_translate_batch_provider_exception():
    """Batch translation returns [] when provider raises."""
    mock_provider = AsyncMock(side_effect=RuntimeError("API error"))
    mock_ps = MagicMock()
    mock_ps.get_provider_callable.return_value = mock_provider
    mock_ps._registry = {}

    svc = TranslationService(db=AsyncMock(), provider_service=mock_ps)
    msgs = [
        Message(id=1, channel_id=100, message_id=1, text="Привет", detected_lang="ru", date=datetime.now(timezone.utc)),
    ]
    result = await svc.translate_batch(msgs, "en")
    assert result == []


@pytest.mark.anyio
async def test_translate_batch_provider_returns_none():
    """Batch translation returns [] when provider returns None."""
    mock_provider = AsyncMock(return_value=None)
    mock_ps = MagicMock()
    mock_ps.get_provider_callable.return_value = mock_provider
    mock_ps._registry = {}

    svc = TranslationService(db=AsyncMock(), provider_service=mock_ps)
    msgs = [
        Message(id=1, channel_id=100, message_id=1, text="Привет", detected_lang="ru", date=datetime.now(timezone.utc)),
    ]
    result = await svc.translate_batch(msgs, "en")
    assert result == []


# ── get_settings ────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_get_settings():
    mock_db = AsyncMock()
    mock_db.get_setting = AsyncMock(side_effect=lambda k: f"value_for_{k}")
    svc = TranslationService(db=mock_db)
    settings = await svc.get_settings()
    assert "translation_provider" in settings
    assert settings["translation_provider"] == "value_for_translation_provider"


# ── _parse_numbered_response edge cases ─────────────────────────────


def test_parse_numbered_response_empty():
    result = TranslationService._parse_numbered_response("", 3)
    assert result == {}


def test_parse_numbered_response_out_of_range():
    response = "1: Hello\n5: Out of range"
    result = TranslationService._parse_numbered_response(response, 3)
    assert 0 in result
    assert result[0] == "Hello"
    # 5 > expected_count(3) so it's ignored
    assert 4 not in result


def test_parse_numbered_response_empty_lines_with_current_block():
    """Empty lines between numbered entries with a current block."""
    response = "1: Hello\n\n2: World"
    result = TranslationService._parse_numbered_response(response, 2)
    assert result[0] == "Hello"
    assert result[1] == "World"


def test_parse_numbered_response_last_block_empty_text():
    """Last block with only whitespace text is dropped."""
    response = "1: Hello\n2:   "
    result = TranslationService._parse_numbered_response(response, 2)
    assert 0 in result
    assert 1 not in result  # empty text is dropped
