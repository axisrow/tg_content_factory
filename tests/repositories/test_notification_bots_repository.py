"""Tests for NotificationBotsRepository."""

from __future__ import annotations

from datetime import datetime

import pytest

from src.models import NotificationBot


@pytest.fixture
def sample_bot():
    """Create sample NotificationBot."""
    return NotificationBot(
        tg_user_id=123456789,
        tg_username="test_user",
        bot_id=987654321,
        bot_username="test_bot",
        bot_token="123456789:ABCDEF",
    )


async def test_get_bot_not_found(notification_bots_repo):
    """Test getting non-existent bot returns None."""
    result = await notification_bots_repo.get_bot(999999999)
    assert result is None


async def test_save_and_get_bot(notification_bots_repo, sample_bot):
    """Test saving and retrieving a bot."""
    await notification_bots_repo.save_bot(sample_bot)

    result = await notification_bots_repo.get_bot(sample_bot.tg_user_id)
    assert result is not None
    assert result.tg_user_id == sample_bot.tg_user_id
    assert result.tg_username == sample_bot.tg_username
    assert result.bot_id == sample_bot.bot_id
    assert result.bot_username == sample_bot.bot_username
    assert result.bot_token == sample_bot.bot_token


async def test_save_bot_upsert(notification_bots_repo, sample_bot):
    """Test that save_bot updates existing bot."""
    await notification_bots_repo.save_bot(sample_bot)

    # Update bot
    updated_bot = NotificationBot(
        tg_user_id=sample_bot.tg_user_id,
        tg_username="updated_user",
        bot_id=111111111,
        bot_username="updated_bot",
        bot_token="new_token",
    )
    await notification_bots_repo.save_bot(updated_bot)

    result = await notification_bots_repo.get_bot(sample_bot.tg_user_id)
    assert result is not None
    assert result.tg_username == "updated_user"
    assert result.bot_id == 111111111
    assert result.bot_username == "updated_bot"
    assert result.bot_token == "new_token"


async def test_delete_bot(notification_bots_repo, sample_bot):
    """Test deleting a bot."""
    await notification_bots_repo.save_bot(sample_bot)
    await notification_bots_repo.delete_bot(sample_bot.tg_user_id)

    result = await notification_bots_repo.get_bot(sample_bot.tg_user_id)
    assert result is None


async def test_delete_nonexistent_bot(notification_bots_repo):
    """Test deleting non-existent bot doesn't raise."""
    # Should not raise
    await notification_bots_repo.delete_bot(999999999)


async def test_row_to_model_with_valid_created_at(notification_bots_repo, sample_bot):
    """Test that _row_to_model parses created_at correctly."""
    await notification_bots_repo.save_bot(sample_bot)

    result = await notification_bots_repo.get_bot(sample_bot.tg_user_id)
    assert result is not None
    # created_at should be set by DB
    assert result.created_at is not None
    assert isinstance(result.created_at, datetime)


async def test_row_to_model_with_none_fields(notification_bots_repo):
    """Test bot with None optional fields."""
    bot = NotificationBot(
        tg_user_id=555555555,
        tg_username=None,
        bot_id=None,
        bot_username="minimal_bot",
        bot_token="token123",
    )
    await notification_bots_repo.save_bot(bot)

    result = await notification_bots_repo.get_bot(555555555)
    assert result is not None
    assert result.tg_username is None
    assert result.bot_id is None


async def test_multiple_bots(notification_bots_repo):
    """Test storing multiple bots for different users."""
    bots = [
        NotificationBot(
            tg_user_id=111111111,
            tg_username="user1",
            bot_id=222222222,
            bot_username="bot1",
            bot_token="token1",
        ),
        NotificationBot(
            tg_user_id=333333333,
            tg_username="user2",
            bot_id=444444444,
            bot_username="bot2",
            bot_token="token2",
        ),
    ]

    for bot in bots:
        await notification_bots_repo.save_bot(bot)

    for bot in bots:
        result = await notification_bots_repo.get_bot(bot.tg_user_id)
        assert result is not None
        assert result.bot_username == bot.bot_username
