"""Tests for CLI filter command helpers and core logic paths."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import typer

from tg_agent.cli import runtime
from tg_agent.cli.commands import account as account_cli
from tg_agent.cli.commands.filter import _parse_pks, _print_result
from tg_agent.gateway.client import TelegramGatewayClient


def test_gateway_toggle_inverts_the_live_state_not_a_stale_mirror(cli_db, monkeypatch):
    """The account was disabled on the gateway since the mirror was taken: toggle enables it."""
    row = {"id": "acc-p", "phone": "+10000000001", "active": True, "flood_until": 0, "is_primary": True}
    asyncio.run(cli_db.repos.accounts.sync_gateway_catalog([row]))  # the mirror: still active
    local_id = asyncio.run(cli_db.get_account_summaries(active_only=False))[0].id
    updates = []

    class Gateway:
        async def accounts(self):
            return [{**row, "active": False}]  # `gateway account disable --shared`

        async def update_account(self, account_id, *, active=None, primary=False):
            updates.append((account_id, active))

        async def close(self):
            pass

    async def from_config(cls, config):
        return Gateway()

    async def init_db(_path):
        return SimpleNamespace(gateway={"project_id": "project-a"}), cli_db

    monkeypatch.setattr(TelegramGatewayClient, "from_config", classmethod(from_config))
    monkeypatch.setattr(runtime, "init_db", init_db)

    asyncio.run(account_cli.toggle_impl("cfg", account_id=local_id))

    assert updates == [("acc-p", True)]


def test_parse_pks_single():
    assert _parse_pks("1") == [1]


def test_parse_pks_multiple():
    assert _parse_pks("1,2,3") == [1, 2, 3]


def test_parse_pks_with_spaces():
    assert _parse_pks(" 1 , 2 , 3 ") == [1, 2, 3]


def test_parse_pks_empty():
    assert _parse_pks("") == []


def test_parse_pks_invalid():
    assert _parse_pks("abc,1,xyz,2") == [1, 2]


def test_parse_pks_commas_only():
    assert _parse_pks(",,,") == []


def test_print_result_nothing():
    result = MagicMock()
    result.purged_count = 0
    result.purged_titles = []
    result.skipped_count = 0
    _print_result(result)


def test_print_result_with_purged(capsys):
    result = MagicMock()
    result.purged_count = 3
    result.purged_titles = ["Ch1", "Ch2", "Ch3"]
    result.skipped_count = 1
    _print_result(result, "Deleted")
    captured = capsys.readouterr()
    assert "Deleted 3 channels" in captured.out
    assert "Ch1" in captured.out
    assert "Skipped: 1" in captured.out


@pytest.mark.parametrize(
    "call",
    [
        lambda account_id: account_cli.set_primary_impl("cfg", account_id=account_id),
        lambda account_id: account_cli.delete_impl("cfg", account_id=account_id),
    ],
    ids=["set-primary", "delete"],
)
def test_gateway_commands_name_a_missing_account(cli_db, monkeypatch, capsys, call):
    """Neither command looked the id up: a missing one reached the user as a raw traceback (#1955).

    Harness only — neither command is run against the real store."""
    # The real store has the gateway's reference table; an empty mirror makes one.
    asyncio.run(cli_db.repos.accounts.sync_gateway_catalog([]))
    calls = []

    class Gateway:
        async def accounts(self):
            return []

        async def update_account(self, account_id, *, active=None, primary=False):
            calls.append(("update", account_id))

        async def detach(self, account_id):
            calls.append(("detach", account_id))

        async def close(self):
            pass

    async def from_config(cls, config):
        return Gateway()

    async def init_db(_path):
        return SimpleNamespace(gateway={"project_id": "project-a"}), cli_db

    monkeypatch.setattr(TelegramGatewayClient, "from_config", classmethod(from_config))
    monkeypatch.setattr(runtime, "init_db", init_db)

    with pytest.raises(typer.Exit) as excinfo:
        asyncio.run(call(999))

    assert excinfo.value.exit_code == 1
    captured = capsys.readouterr()
    assert "Account id=999 not found" in captured.err
    assert captured.out == ""
    assert calls == []
