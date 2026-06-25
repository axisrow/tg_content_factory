"""Agent-tool tests for SSO session export/import (#1144, epic #828).

`export_session` is a WRITE/sensitive tool (full account access) gated behind
`confirm=true`, mirroring `delete_account`. `import_session` adds an account from
a ready StringSession. Both reuse the telegram-layer validator and the
single-account decrypt accessor introduced in #1143.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.agent_tools_helpers import _get_tool_handlers, _text


def _make_account(acc_id=1, phone="+79001234567", is_active=True, is_primary=True):
    a = MagicMock()
    a.id = acc_id
    a.phone = phone
    a.is_active = is_active
    a.flood_wait_until = None
    a.is_primary = is_primary
    return a


# ---------------------------------------------------------------------------
# export_session
# ---------------------------------------------------------------------------


class TestExportSessionTool:
    @pytest.mark.anyio
    async def test_missing_account_id_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["export_session"]({})
        assert "account_id" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[_make_account(acc_id=1)])
        mock_db.repos.accounts.get_decrypted_session = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["export_session"]({"account_id": 1})
        assert "confirm=true" in _text(result)
        # The gate must NOT leak the session string (decrypt never called).
        mock_db.repos.accounts.get_decrypted_session.assert_not_awaited()

    @pytest.mark.anyio
    async def test_with_confirm_returns_session(self, mock_db):
        acc = _make_account(acc_id=5, phone="+75555555555")
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        mock_db.repos.accounts.get_decrypted_session = AsyncMock(return_value="THE_SESSION_STRING")
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["export_session"]({"account_id": 5, "confirm": True})
        text = _text(result)
        assert "THE_SESSION_STRING" in text
        mock_db.repos.accounts.get_decrypted_session.assert_awaited_once()

    @pytest.mark.anyio
    async def test_unknown_account_returns_not_found(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[])
        mock_db.repos.accounts.get_decrypted_session = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["export_session"]({"account_id": 999, "confirm": True})
        assert "не найден" in _text(result).lower() or "not found" in _text(result).lower()


# ---------------------------------------------------------------------------
# import_session
# ---------------------------------------------------------------------------


class TestImportSessionTool:
    @pytest.mark.anyio
    async def test_missing_args_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["import_session"]({"phone": "+71112223344"})
        assert "session_string" in _text(result)

    @pytest.mark.anyio
    async def test_invalid_session_rejected(self, mock_db):
        mock_db.get_account_summaries = AsyncMock(return_value=[])
        mock_db.add_account = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["import_session"](
            {"phone": "+71112223344", "session_string": "garbage"}
        )
        assert "невалид" in _text(result).lower() or "invalid" in _text(result).lower()
        mock_db.add_account.assert_not_awaited()

    @pytest.mark.anyio
    async def test_valid_session_adds_account(self, mock_db, monkeypatch):
        # Patch the validator so the test does not depend on telethon internals.
        monkeypatch.setattr("src.agent.tools.accounts.validate_session_string", lambda s: True)
        mock_db.get_account_summaries = AsyncMock(return_value=[])
        mock_db.add_account = AsyncMock(return_value=7)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["import_session"](
            {"phone": "+71112223344", "session_string": "valid-looking"}
        )
        assert "+71112223344" in _text(result)
        mock_db.add_account.assert_awaited_once()

    @pytest.mark.anyio
    async def test_existing_phone_refused_without_force(self, mock_db, monkeypatch):
        monkeypatch.setattr("src.agent.tools.accounts.validate_session_string", lambda s: True)
        existing = _make_account(acc_id=1, phone="+71112223344")
        mock_db.get_account_summaries = AsyncMock(return_value=[existing])
        mock_db.add_account = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["import_session"](
            {"phone": "+71112223344", "session_string": "valid-looking"}
        )
        assert "exist" in _text(result).lower() or "существует" in _text(result).lower()
        mock_db.add_account.assert_not_awaited()

    @pytest.mark.anyio
    async def test_existing_phone_force_overwrites(self, mock_db, monkeypatch):
        # force=true must actually reach add_account (guards against a no-op regression).
        monkeypatch.setattr("src.agent.tools.accounts.validate_session_string", lambda s: True)
        existing = _make_account(acc_id=1, phone="+71112223344")
        mock_db.get_account_summaries = AsyncMock(return_value=[existing])
        mock_db.add_account = AsyncMock(return_value=1)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["import_session"](
            {"phone": "+71112223344", "session_string": "valid-looking", "force": True}
        )
        assert "импортирован" in _text(result).lower()
        mock_db.add_account.assert_awaited_once()


# ---------------------------------------------------------------------------
# permission / registry contract
# ---------------------------------------------------------------------------


def test_session_tools_classified_write():
    from src.agent.tools._categories import ToolCategory
    from src.agent.tools.accounts import TOOL_GROUPS

    flat = {name: meta for _, group in TOOL_GROUPS for name, meta in group.items()}
    assert flat["export_session"].category == ToolCategory.WRITE
    assert flat["import_session"].category == ToolCategory.WRITE


def test_export_session_marked_secret_result():
    """The session is the secret being returned — loggers must know to redact it."""
    from src.agent.tools._categories import SECRET_RESULT_TOOLS

    assert "export_session" in SECRET_RESULT_TOOLS


def test_react_agent_redacts_secret_tool_result_in_logs(caplog):
    """ReAct fallback logger must not preview export_session's result (#828)."""
    import logging

    from src.agent.tools._categories import SECRET_RESULT_TOOLS

    secret = "1ApValidLooKingSessionStringThatMustNotLeakIntoLogs0000"
    tool_name = "export_session"
    preview = "<redacted>" if tool_name in SECRET_RESULT_TOOLS else secret[:100]
    with caplog.at_level(logging.DEBUG):
        logging.getLogger("src.agent.react_agent").debug(
            "ReAct tool %r → %r", tool_name, preview
        )
    assert secret not in caplog.text
    assert "<redacted>" in caplog.text
