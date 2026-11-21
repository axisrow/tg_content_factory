"""Permission flags must have explicit meanings and fail before any connection."""

import argparse
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
import typer
from click import unstyle
from typer.testing import CliRunner

from tg_agent.cli.commands import dialogs


@pytest.mark.parametrize("raw,expected", [
    ("true", True), ("TRUE", True), ("yes", True), ("YeS", True),
    ("1", True), ("on", True), ("ON", True),
    ("false", False), ("FALSE", False), ("no", False), ("No", False),
    ("0", False), ("off", False), ("OFF", False),
    (None, None), (True, True), (False, False),
])
def test_permission_values_reach_the_action(monkeypatch, raw, expected):
    args = argparse.Namespace(
        dialogs_action="edit-permissions", chat_id="-100123", user_id="777",
        phone="+123", send_messages=raw, send_media=raw, until_date=None, yes=True,
    )
    action = AsyncMock()
    monkeypatch.setattr(dialogs, "_resolve_phone", AsyncMock(return_value="+123"))
    monkeypatch.setattr(dialogs, "TelegramActionService", Mock(return_value=SimpleNamespace(edit_permissions=action)))
    if raw is None:
        # Both permission flags None = usage error, refused up front (#1486).
        with pytest.raises(typer.Exit):
            asyncio.run(dialogs._dialogs_edit_permissions(args, object(), object()))
        action.assert_not_called()
    else:
        asyncio.run(dialogs._dialogs_edit_permissions(args, object(), object()))
        assert action.call_args.kwargs["send_messages"] is expected
        assert action.call_args.kwargs["send_media"] is expected


@pytest.mark.parametrize("option", ["--send-messages", "--send-media"])
@pytest.mark.parametrize("raw", ["maybe", "ture", "", "allow", "2"])
def test_invalid_cli_permission_never_dispatches(monkeypatch, option, raw):
    dispatch = Mock()
    monkeypatch.setattr(dialogs, "_run_dialogs", dispatch)
    app = typer.Typer()
    app.add_typer(dialogs.dialogs_app, name="dialogs")
    result = CliRunner().invoke(app, ["dialogs", "edit-permissions", "123", "777", option, raw, "--yes"])
    assert result.exit_code == 2
    output = unstyle(result.output)
    assert option in output
    assert "expected true/false" in output
    dispatch.assert_not_called()


@pytest.mark.parametrize("field", ["send_messages", "send_media"])
def test_invalid_namespace_rejected_before_runtime(field):
    runtime = SimpleNamespace(init_db=AsyncMock(), init_pool=AsyncMock())
    args = argparse.Namespace(
        dialogs_action="edit-permissions", config=None,
        send_messages=None, send_media=None,
    )
    setattr(args, field, "maybe")
    with pytest.raises(typer.BadParameter):
        asyncio.run(dialogs._dispatch(args, runtime_mod=runtime))
    runtime.init_db.assert_not_called()
    runtime.init_pool.assert_not_called()
