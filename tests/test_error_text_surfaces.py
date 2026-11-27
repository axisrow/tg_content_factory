"""A surfaced failure reason is never an empty string (#1982).

Follow-up to #1968: the stored-reason convention guard sees ``error=``/
``error_text=``/``fallback_error=`` keywords only. These surfaces are result
fields and event payloads outside that scope — an exception raised without a
message must surface its class name there too, via :func:`error_text`, never
``""``.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

from tg_agent.cli.commands.test import (
    CheckResult,
    Status,
    _check_get_stats,
    _run_account_toggle_check,
)
from tg_agent.database import Database
from tg_agent.services.telegram_actions import TelegramActionService
from tg_agent.telegram.reactions import fetch_message_reaction_users


async def test_reaction_users_unavailable_names_a_message_less_exception():
    with patch(
        "tg_agent.telegram.backends.fetch_message_reaction_users_raw",
        AsyncMock(side_effect=RuntimeError()),
    ):
        result = await fetch_message_reaction_users(MagicMock(), "@chan", 5)
    assert result.unavailable == "RuntimeError"


def _pool_with_client(client):
    pool = MagicMock()
    pool.get_native_client_by_phone = AsyncMock(return_value=(client, "+1"))
    pool.get_available_client = AsyncMock(return_value=(client, "+1"))
    pool.get_client_by_phone = AsyncMock(return_value=(client, "+1"))
    pool.release_client = AsyncMock()
    return pool


async def test_create_channel_username_error_names_a_message_less_exception():
    client = AsyncMock()
    client.create_channel = AsyncMock(
        return_value=SimpleNamespace(chats=[SimpleNamespace(id=1, username="")])
    )
    client.update_channel_username = AsyncMock(side_effect=RuntimeError())

    result = await TelegramActionService(_pool_with_client(client)).create_channel(
        phone="+1",
        title="Title",
        username="public_name",
    )

    assert result.username_error == "RuntimeError"


async def test_return_style_check_detail_names_a_message_less_exception():
    class Boom:
        async def get_stats(self):
            raise TimeoutError()

    result = await _check_get_stats(Boom())
    assert result.status is Status.FAIL
    assert result.detail == "TimeoutError"


async def test_append_style_check_detail_names_a_message_less_exception():
    class Boom:
        async def get_accounts(self):
            raise TimeoutError()

    results: list[CheckResult] = []
    await _run_account_toggle_check(cast(Database, Boom()), results)
    assert results[-1].status is Status.FAIL
    assert results[-1].detail == "TimeoutError"
