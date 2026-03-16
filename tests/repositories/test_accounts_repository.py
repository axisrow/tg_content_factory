"""Tests for AccountsRepository."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.database.repositories.accounts import AccountsRepository
from src.models import Account
from src.security.session_cipher import SessionCipher


@pytest.fixture
async def repo(db):
    """Create repository instance without cipher."""
    return AccountsRepository(db.db)


@pytest.fixture
def cipher():
    """Create a SessionCipher for encryption tests."""
    return SessionCipher("test-secret-key-12345")


@pytest.fixture
async def repo_with_cipher(db, cipher):
    """Create repository instance with cipher."""
    return AccountsRepository(db.db, session_cipher=cipher)


def make_account(phone: str, session: str = "session_string_123", **kwargs) -> Account:
    """Create a test Account."""
    return Account(phone=phone, session_string=session, **kwargs)


# add_account tests

async def test_add_account_insert(repo):
    """Test inserting a new account."""
    account = make_account("+1234567890")
    pk = await repo.add_account(account)
    assert pk > 0

    accounts = await repo.get_accounts()
    assert len(accounts) == 1
    assert accounts[0].phone == "+1234567890"


async def test_add_account_upsert_on_conflict(repo):
    """Test that add_account updates existing account on phone conflict."""
    account1 = make_account("+1234567890", is_active=True)
    await repo.add_account(account1)

    account2 = make_account("+1234567890", is_active=False, is_premium=True)
    await repo.add_account(account2)

    accounts = await repo.get_accounts()
    assert len(accounts) == 1
    assert accounts[0].is_active is False
    assert accounts[0].is_premium is True


async def test_add_account_with_cipher_encrypts_session(repo_with_cipher, cipher):
    """Test that session is encrypted when cipher is provided."""
    account = make_account("+1234567890", session="plaintext_session")
    await repo_with_cipher.add_account(account)

    # Check that DB contains encrypted value
    cur = await repo_with_cipher._db.execute(
        "SELECT session_string FROM accounts WHERE phone = ?", ("+1234567890",)
    )
    row = await cur.fetchone()
    assert row is not None
    assert cipher.is_encrypted(row["session_string"])


# get_accounts tests

async def test_get_accounts_empty(repo):
    """Test getting accounts when none exist."""
    accounts = await repo.get_accounts()
    assert accounts == []


async def test_get_accounts_active_only(repo):
    """Test filtering by active_only."""
    await repo.add_account(make_account("+1111111111", is_active=True))
    await repo.add_account(make_account("+2222222222", is_active=False))
    await repo.add_account(make_account("+3333333333", is_active=True))

    all_accounts = await repo.get_accounts()
    assert len(all_accounts) == 3

    active_accounts = await repo.get_accounts(active_only=True)
    assert len(active_accounts) == 2
    phones = {a.phone for a in active_accounts}
    assert phones == {"+1111111111", "+3333333333"}


async def test_get_accounts_ordering(repo):
    """Test that accounts are ordered by is_primary DESC, id ASC."""
    await repo.add_account(make_account("+1111111111", is_primary=False))
    await repo.add_account(make_account("+2222222222", is_primary=True))
    await repo.add_account(make_account("+3333333333", is_primary=False))

    accounts = await repo.get_accounts()
    assert accounts[0].phone == "+2222222222"  # primary first
    assert accounts[1].phone == "+1111111111"
    assert accounts[2].phone == "+3333333333"


async def test_get_accounts_decrypts_sessions(repo_with_cipher, cipher):
    """Test that get_accounts decrypts encrypted sessions."""
    await repo_with_cipher.add_account(make_account("+1234567890", session="my_secret"))

    accounts = await repo_with_cipher.get_accounts()
    assert len(accounts) == 1
    assert accounts[0].session_string == "my_secret"


async def test_get_accounts_handles_plaintext_when_cipher_set(repo_with_cipher):
    """Test that plaintext sessions work when cipher is configured."""
    # Insert plaintext directly
    await repo_with_cipher._db.execute(
        "INSERT INTO accounts (phone, session_string, is_primary, is_active) VALUES (?, ?, 0, 1)",
        ("+9999999999", "plaintext_value"),
    )
    await repo_with_cipher._db.commit()

    accounts = await repo_with_cipher.get_accounts()
    assert len(accounts) == 1
    assert accounts[0].session_string == "plaintext_value"


# update_account_flood tests

async def test_update_account_flood_set(repo):
    """Test setting flood_wait_until."""
    await repo.add_account(make_account("+1234567890"))
    until = datetime(2026, 3, 16, 12, 0, 0, tzinfo=timezone.utc)
    await repo.update_account_flood("+1234567890", until)

    accounts = await repo.get_accounts()
    assert accounts[0].flood_wait_until == until


async def test_update_account_flood_clear(repo):
    """Test clearing flood_wait_until."""
    await repo.add_account(make_account("+1234567890"))
    until = datetime(2026, 3, 16, 12, 0, 0, tzinfo=timezone.utc)
    await repo.update_account_flood("+1234567890", until)

    # Clear it
    await repo.update_account_flood("+1234567890", None)

    accounts = await repo.get_accounts()
    assert accounts[0].flood_wait_until is None


# update_account_premium tests

async def test_update_account_premium_set_true(repo):
    """Test setting is_premium to True."""
    await repo.add_account(make_account("+1234567890", is_premium=False))
    await repo.update_account_premium("+1234567890", True)

    accounts = await repo.get_accounts()
    assert accounts[0].is_premium is True


async def test_update_account_premium_set_false(repo):
    """Test setting is_premium to False."""
    await repo.add_account(make_account("+1234567890", is_premium=True))
    await repo.update_account_premium("+1234567890", False)

    accounts = await repo.get_accounts()
    assert accounts[0].is_premium is False


# set_account_active tests

async def test_set_account_active_deactivate(repo):
    """Test deactivating an account."""
    await repo.add_account(make_account("+1234567890", is_active=True))
    accounts = await repo.get_accounts()
    pk = accounts[0].id

    await repo.set_account_active(pk, False)

    accounts = await repo.get_accounts()
    assert accounts[0].is_active is False


async def test_set_account_active_activate(repo):
    """Test activating an account."""
    await repo.add_account(make_account("+1234567890", is_active=False))
    accounts = await repo.get_accounts()
    pk = accounts[0].id

    await repo.set_account_active(pk, True)

    accounts = await repo.get_accounts()
    assert accounts[0].is_active is True


# delete_account tests

async def test_delete_account(repo):
    """Test deleting an account."""
    await repo.add_account(make_account("+1234567890"))
    accounts = await repo.get_accounts()
    pk = accounts[0].id

    await repo.delete_account(pk)

    accounts = await repo.get_accounts()
    assert len(accounts) == 0


async def test_delete_account_nonexistent(repo):
    """Test deleting non-existent account does not raise."""
    await repo.delete_account(999)  # Should not raise


# migrate_sessions tests

async def test_migrate_sessions_no_cipher(repo):
    """Test migrate_sessions returns 0 when no cipher is configured."""
    await repo.add_account(make_account("+1234567890", session="plaintext"))
    count = await repo.migrate_sessions()
    assert count == 0


async def test_migrate_sessions_plaintext_to_encrypted(repo_with_cipher, cipher):
    """Test migrating plaintext sessions to encrypted."""
    # Insert plaintext directly
    await repo_with_cipher._db.execute(
        "INSERT INTO accounts (phone, session_string, is_primary, is_active) VALUES (?, ?, 0, 1)",
        ("+1234567890", "plaintext_session"),
    )
    await repo_with_cipher._db.commit()

    count = await repo_with_cipher.migrate_sessions()
    assert count == 1

    # Verify it's now encrypted
    cur = await repo_with_cipher._db.execute(
        "SELECT session_string FROM accounts WHERE phone = ?", ("+1234567890",)
    )
    row = await cur.fetchone()
    assert cipher.is_encrypted(row["session_string"])


async def test_migrate_sessions_already_encrypted_v2(repo_with_cipher, cipher):
    """Test that already encrypted v2 sessions are not re-encrypted."""
    encrypted = cipher.encrypt("already_encrypted")
    await repo_with_cipher._db.execute(
        "INSERT INTO accounts (phone, session_string, is_primary, is_active) VALUES (?, ?, 0, 1)",
        ("+1234567890", encrypted),
    )
    await repo_with_cipher._db.commit()

    count = await repo_with_cipher.migrate_sessions()
    assert count == 0

    # Verify it's unchanged
    cur = await repo_with_cipher._db.execute(
        "SELECT session_string FROM accounts WHERE phone = ?", ("+1234567890",)
    )
    row = await cur.fetchone()
    assert row["session_string"] == encrypted


async def test_migrate_sessions_v1_to_v2(repo_with_cipher, cipher):
    """Test migrating v1 encrypted sessions to v2."""
    # Create v1 encrypted value manually

    from cryptography.fernet import Fernet

    from src.security.session_cipher import _derive_fernet_key_v1

    fernet_v1 = Fernet(_derive_fernet_key_v1("test-secret-key-12345"))
    v1_token = fernet_v1.encrypt(b"session_data").decode("ascii")
    v1_encrypted = f"enc:v1:{v1_token}"

    await repo_with_cipher._db.execute(
        "INSERT INTO accounts (phone, session_string, is_primary, is_active) VALUES (?, ?, 0, 1)",
        ("+1234567890", v1_encrypted),
    )
    await repo_with_cipher._db.commit()

    count = await repo_with_cipher.migrate_sessions()
    assert count == 1

    # Verify it's now v2
    cur = await repo_with_cipher._db.execute(
        "SELECT session_string FROM accounts WHERE phone = ?", ("+1234567890",)
    )
    row = await cur.fetchone()
    assert row["session_string"].startswith("enc:v2:")


async def test_migrate_sessions_empty_table(repo_with_cipher):
    """Test migrate_sessions returns 0 when no accounts exist."""
    count = await repo_with_cipher.migrate_sessions()
    assert count == 0


async def test_get_accounts_decrypts_v1_session(db):
    """Test that v1 encrypted sessions can be decrypted."""
    secret = "test-secret-key-12345"
    cipher = SessionCipher(secret)

    from cryptography.fernet import Fernet

    from src.security.session_cipher import _derive_fernet_key_v1

    fernet_v1 = Fernet(_derive_fernet_key_v1(secret))
    v1_token = fernet_v1.encrypt(b"original_session").decode("ascii")
    v1_encrypted = f"enc:v1:{v1_token}"

    await db.db.execute(
        "INSERT INTO accounts (phone, session_string, is_primary, is_active) VALUES (?, ?, 0, 1)",
        ("+1234567890", v1_encrypted),
    )
    await db.db.commit()

    repo = AccountsRepository(db.db, session_cipher=cipher)
    accounts = await repo.get_accounts()
    assert len(accounts) == 1
    assert accounts[0].session_string == "original_session"
