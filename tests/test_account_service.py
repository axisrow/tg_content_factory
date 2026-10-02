from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import Account, AccountSessionStatus, AccountSummary
from src.services.account_service import (
    AccountService,
    pick_default_account_phone,
    resolve_default_phone,
)


@pytest.fixture
def mock_bundle():
    bundle = MagicMock()
    bundle.list_accounts = AsyncMock()
    bundle.set_active = AsyncMock()
    bundle.delete_account = AsyncMock()
    return bundle


@pytest.fixture
def mock_pool():
    pool = AsyncMock()
    pool.add_client = AsyncMock()
    pool.remove_client = AsyncMock()
    return pool


@pytest.mark.anyio
async def test_account_service_list(mock_bundle):
    mock_bundle.list_accounts.return_value = [Account(id=1, phone="+7999", session_string="sess")]
    svc = AccountService(mock_bundle)
    results = await svc.list()
    assert len(results) == 1
    assert results[0].phone == "+7999"


@pytest.mark.anyio
async def test_account_service_toggle_no_pool(mock_bundle):
    acc = Account(id=1, phone="+7999", session_string="sess", is_active=True)
    mock_bundle.list_accounts.return_value = [acc]
    svc = AccountService(mock_bundle)

    await svc.toggle(1)
    mock_bundle.set_active.assert_called_once_with(1, False)


@pytest.mark.anyio
async def test_account_service_toggle_activate_with_pool(mock_bundle, mock_pool):
    acc = Account(id=1, phone="+7999", session_string="sess", is_active=False)
    mock_bundle.list_accounts.return_value = [acc]
    svc = AccountService(mock_bundle, mock_pool)

    await svc.toggle(1)
    mock_bundle.set_active.assert_called_once_with(1, True)
    mock_pool.add_client.assert_called_once_with("+7999", "sess")


@pytest.mark.anyio
async def test_account_service_toggle_deactivate_with_pool(mock_bundle, mock_pool):
    acc = Account(id=1, phone="+7999", session_string="sess", is_active=True)
    mock_bundle.list_accounts.return_value = [acc]
    svc = AccountService(mock_bundle, mock_pool)

    await svc.toggle(1)
    mock_bundle.set_active.assert_called_once_with(1, False)
    mock_pool.remove_client.assert_called_once_with("+7999")


@pytest.mark.anyio
async def test_account_service_toggle_add_client_error(mock_bundle, mock_pool):
    acc = Account(id=1, phone="+7999", session_string="sess", is_active=False)
    mock_bundle.list_accounts.return_value = [acc]
    mock_pool.add_client.side_effect = Exception("failed")
    svc = AccountService(mock_bundle, mock_pool)

    # Should not raise exception, just log it
    await svc.toggle(1)
    mock_bundle.set_active.assert_called_once_with(1, True)


@pytest.mark.anyio
async def test_account_service_toggle_deactivate_remove_client_error(mock_bundle, mock_pool):
    """#1029: deactivating an account already wrote set_active to the DB, so a
    pool.remove_client failure must NOT propagate — otherwise the DB says the
    account is inactive while the in-memory client lingers AND the caller sees an
    error. Mirror the activate branch (and delete()): log, don't raise."""
    acc = Account(id=1, phone="+7999", session_string="sess", is_active=True)
    mock_bundle.list_accounts.return_value = [acc]
    mock_pool.remove_client.side_effect = Exception("pool removal failed")
    svc = AccountService(mock_bundle, mock_pool)

    # Must not raise — the pool error is logged, not propagated.
    await svc.toggle(1)

    mock_bundle.set_active.assert_called_once_with(1, False)
    mock_pool.remove_client.assert_called_once_with("+7999")


@pytest.mark.anyio
async def test_account_service_toggle_not_found(mock_bundle):
    mock_bundle.list_accounts.return_value = []
    svc = AccountService(mock_bundle)
    await svc.toggle(999)
    mock_bundle.set_active.assert_not_called()


@pytest.mark.anyio
async def test_account_service_delete_with_pool(mock_bundle, mock_pool):
    acc = Account(id=1, phone="+7999", session_string="sess")
    mock_bundle.list_accounts.return_value = [acc]
    svc = AccountService(mock_bundle, mock_pool)

    await svc.delete(1)
    mock_pool.remove_client.assert_called_once_with("+7999")
    mock_bundle.delete_account.assert_called_once_with(1)


@pytest.mark.anyio
async def test_account_service_delete_no_pool(mock_bundle):
    svc = AccountService(mock_bundle)
    await svc.delete(1)
    mock_bundle.delete_account.assert_called_once_with(1)


@pytest.mark.anyio
async def test_account_service_delete_proceeds_when_pool_remove_fails(mock_bundle, mock_pool):
    """#1029 consistency regression: if pool.remove_client raises, the account
    must STILL be deleted from the DB. The DB is the source of truth — the pool is
    rebuilt from it on restart, so letting a pool failure abort delete_account
    leaves a 'ghost' account the operator believes is gone but that reappears
    (and an orphaned in-memory client). This mirrors toggle(), which already
    swallows add_client failures (test_account_service_toggle_add_client_error)."""
    acc = Account(id=1, phone="+7999", session_string="sess")
    mock_bundle.list_accounts.return_value = [acc]
    mock_pool.remove_client.side_effect = Exception("pool removal failed")
    svc = AccountService(mock_bundle, mock_pool)

    # Must not raise — the pool error is logged, not propagated.
    await svc.delete(1)

    mock_pool.remove_client.assert_called_once_with("+7999")
    mock_bundle.delete_account.assert_called_once_with(1)


@pytest.mark.anyio
async def test_account_service_init_with_db():
    from src.database import Database

    db = MagicMock(spec=Database)
    # This just tests that it doesn't crash during init
    svc = AccountService(db)
    assert svc._accounts is not None


# --------------------------------------------------------------------------- #
# Default-account pick (#1480): primary first, deterministic.
# --------------------------------------------------------------------------- #


def _summary(phone, *, is_primary=False, is_active=True,
             session_status: AccountSessionStatus = AccountSessionStatus.OK):
    return AccountSummary(
        phone=phone,
        is_primary=is_primary,
        is_active=is_active,
        session_status=session_status,
    )


def test_pick_default_account_phone_primary_wins_over_sort_order():
    """Sorted-first must NOT win: the DB primary does (#1480 regression)."""
    accounts = [_summary("+20000000001"), _summary("+10000000002", is_primary=True)]
    assert pick_default_account_phone(accounts) == "+10000000002"


def test_pick_default_account_phone_falls_back_to_first_usable():
    accounts = [_summary("+20000000001"), _summary("+10000000002")]
    assert pick_default_account_phone(accounts) == "+20000000001"


def test_pick_default_account_phone_skips_inactive_and_broken_sessions():
    accounts = [
        _summary("+10000000001", session_status=AccountSessionStatus.DECRYPT_FAILED),
        _summary("+20000000002", is_active=False),
        _summary("+30000000003"),
    ]
    assert pick_default_account_phone(accounts) == "+30000000003"


def test_pick_default_account_phone_empty_returns_none():
    assert pick_default_account_phone([]) is None
    assert pick_default_account_phone([_summary("+1", is_active=False)]) is None


@pytest.mark.anyio
async def test_resolve_default_phone_uses_db_pick():
    db = MagicMock()
    db.get_account_summaries = AsyncMock(
        return_value=[_summary("+10000000001"), _summary("+20000000002", is_primary=True)]
    )
    assert await resolve_default_phone(db) == "+20000000002"


@pytest.mark.anyio
async def test_resolve_default_phone_connected_falls_back_when_primary_down():
    db = MagicMock()
    db.get_account_summaries = AsyncMock(return_value=[_summary("+90000000009", is_primary=True)])
    connected = {"+10000000001", "+20000000002"}
    assert await resolve_default_phone(db, connected=connected) == "+10000000001"


@pytest.mark.anyio
async def test_resolve_default_phone_prefers_connected_primary():
    db = MagicMock()
    db.get_account_summaries = AsyncMock(return_value=[_summary("+90000000009", is_primary=True)])
    connected = {"+10000000001", "+90000000009"}
    assert await resolve_default_phone(db, connected=connected) == "+90000000009"


@pytest.mark.anyio
async def test_resolve_default_phone_nothing_usable_uses_connected():
    """No DB accounts at all — any connected phone still beats failing."""
    db = MagicMock()
    db.get_account_summaries = AsyncMock(return_value=[])
    assert await resolve_default_phone(db, connected={"+10000000001"}) == "+10000000001"
    assert await resolve_default_phone(db, connected=set()) is None
