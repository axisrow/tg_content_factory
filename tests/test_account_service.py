from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import Account
from src.services.account_service import AccountService


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


@pytest.mark.asyncio
async def test_account_service_list(mock_bundle):
    mock_bundle.list_accounts.return_value = [Account(id=1, phone="+7999", session_string="sess")]
    svc = AccountService(mock_bundle)
    results = await svc.list()
    assert len(results) == 1
    assert results[0].phone == "+7999"


@pytest.mark.asyncio
async def test_account_service_toggle_no_pool(mock_bundle):
    acc = Account(id=1, phone="+7999", session_string="sess", is_active=True)
    mock_bundle.list_accounts.return_value = [acc]
    svc = AccountService(mock_bundle)

    await svc.toggle(1)
    mock_bundle.set_active.assert_called_once_with(1, False)


@pytest.mark.asyncio
async def test_account_service_toggle_activate_with_pool(mock_bundle, mock_pool):
    acc = Account(id=1, phone="+7999", session_string="sess", is_active=False)
    mock_bundle.list_accounts.return_value = [acc]
    svc = AccountService(mock_bundle, mock_pool)

    await svc.toggle(1)
    mock_bundle.set_active.assert_called_once_with(1, True)
    mock_pool.add_client.assert_called_once_with("+7999", "sess")


@pytest.mark.asyncio
async def test_account_service_toggle_deactivate_with_pool(mock_bundle, mock_pool):
    acc = Account(id=1, phone="+7999", session_string="sess", is_active=True)
    mock_bundle.list_accounts.return_value = [acc]
    svc = AccountService(mock_bundle, mock_pool)

    await svc.toggle(1)
    mock_bundle.set_active.assert_called_once_with(1, False)
    mock_pool.remove_client.assert_called_once_with("+7999")


@pytest.mark.asyncio
async def test_account_service_toggle_add_client_error(mock_bundle, mock_pool):
    acc = Account(id=1, phone="+7999", session_string="sess", is_active=False)
    mock_bundle.list_accounts.return_value = [acc]
    mock_pool.add_client.side_effect = Exception("failed")
    svc = AccountService(mock_bundle, mock_pool)

    # Should not raise exception, just log it
    await svc.toggle(1)
    mock_bundle.set_active.assert_called_once_with(1, True)


@pytest.mark.asyncio
async def test_account_service_toggle_not_found(mock_bundle):
    mock_bundle.list_accounts.return_value = []
    svc = AccountService(mock_bundle)
    await svc.toggle(999)
    mock_bundle.set_active.assert_not_called()


@pytest.mark.asyncio
async def test_account_service_delete_with_pool(mock_bundle, mock_pool):
    acc = Account(id=1, phone="+7999", session_string="sess")
    mock_bundle.list_accounts.return_value = [acc]
    svc = AccountService(mock_bundle, mock_pool)

    await svc.delete(1)
    mock_pool.remove_client.assert_called_once_with("+7999")
    mock_bundle.delete_account.assert_called_once_with(1)


@pytest.mark.asyncio
async def test_account_service_delete_no_pool(mock_bundle):
    svc = AccountService(mock_bundle)
    await svc.delete(1)
    mock_bundle.delete_account.assert_called_once_with(1)


@pytest.mark.asyncio
async def test_account_service_init_with_db():
    from src.database import Database

    db = MagicMock(spec=Database)
    # This just tests that it doesn't crash during init
    svc = AccountService(db)
    assert svc._accounts is not None
