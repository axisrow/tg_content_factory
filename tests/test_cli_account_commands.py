"""Tests for CLI filter command helpers and core logic paths."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

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
