"""Tests for AccountsRepository."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import pytest

from src.database.repositories.accounts import AccountSessionDecryptError, AccountsRepository
from src.models import Account, AccountSessionStatus
from src.security.session_cipher import SessionCipher


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


async def test_add_account_insert(accounts_repo):
    """Test inserting a new account."""
    account = make_account("+1234567890")
    pk = await accounts_repo.add_account(account)
    assert pk > 0

    accounts = await accounts_repo.get_accounts()
    assert len(accounts) == 1
    assert accounts[0].phone == "+1234567890"


async def test_add_account_upsert_on_conflict(accounts_repo):
    """Test that add_account updates existing account on phone conflict."""
    account1 = make_account("+1234567890", is_active=True)
    await accounts_repo.add_account(account1)

    account2 = make_account("+1234567890", is_active=False, is_premium=True)
    await accounts_repo.add_account(account2)

    accounts = await accounts_repo.get_accounts()
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


async def test_get_accounts_empty(accounts_repo):
    """Test getting accounts when none exist."""
    accounts = await accounts_repo.get_accounts()
    assert accounts == []


async def test_get_accounts_active_only(accounts_repo):
    """Test filtering by active_only."""
    await accounts_repo.add_account(make_account("+1111111111", is_active=True))
    await accounts_repo.add_account(make_account("+2222222222", is_active=False))
    await accounts_repo.add_account(make_account("+3333333333", is_active=True))

    all_accounts = await accounts_repo.get_accounts()
    assert len(all_accounts) == 3

    active_accounts = await accounts_repo.get_accounts(active_only=True)
    assert len(active_accounts) == 2
    phones = {a.phone for a in active_accounts}
    assert phones == {"+1111111111", "+3333333333"}


async def test_get_accounts_ordering(accounts_repo):
    """Test that accounts are ordered by is_primary DESC, id ASC."""
    await accounts_repo.add_account(make_account("+1111111111", is_primary=False))
    await accounts_repo.add_account(make_account("+2222222222", is_primary=True))
    await accounts_repo.add_account(make_account("+3333333333", is_primary=False))

    accounts = await accounts_repo.get_accounts()
    assert accounts[0].phone == "+2222222222"  # primary first
    assert accounts[1].phone == "+1111111111"
    assert accounts[2].phone == "+3333333333"


async def test_get_accounts_decrypts_sessions(repo_with_cipher, cipher):
    """Test that get_accounts decrypts encrypted sessions."""
    await repo_with_cipher.add_account(make_account("+1234567890", session="my_secret"))

    accounts = await repo_with_cipher.get_accounts()
    assert len(accounts) == 1
    assert accounts[0].session_string == "my_secret"


async def test_get_account_summaries_reports_decrypt_failed_without_session(repo_with_cipher, caplog):
    writer_cipher = SessionCipher("correct-key")
    await repo_with_cipher._db.execute(
        "INSERT INTO accounts (phone, session_string, is_primary, is_active) VALUES (?, ?, 0, 1)",
        ("+1234567890", writer_cipher.encrypt("live-session")),
    )
    await repo_with_cipher._db.commit()

    wrong_key_repo = AccountsRepository(
        repo_with_cipher._db,
        session_cipher=SessionCipher("wrong-key"),
    )

    with caplog.at_level(logging.DEBUG, logger="src.database.repositories.accounts"):
        summaries = await wrong_key_repo.get_account_summaries()

    assert len(summaries) == 1
    assert summaries[0].phone == "+1234567890"
    assert summaries[0].session_status == AccountSessionStatus.DECRYPT_FAILED
    assert any(
        record.levelno == logging.DEBUG
        and "decrypt failed: resource=telegram_account identifier=+1234567890 status=key_mismatch" in record.message
        for record in caplog.records
    )
    assert not any(record.levelno >= logging.ERROR for record in caplog.records)


async def test_get_accounts_wrong_key_still_fails_for_live_use(repo_with_cipher):
    writer_cipher = SessionCipher("correct-key")
    await repo_with_cipher._db.execute(
        "INSERT INTO accounts (phone, session_string, is_primary, is_active) VALUES (?, ?, 0, 1)",
        ("+1234567890", writer_cipher.encrypt("live-session")),
    )
    await repo_with_cipher._db.commit()

    wrong_key_repo = AccountsRepository(
        repo_with_cipher._db,
        session_cipher=SessionCipher("wrong-key"),
    )

    with pytest.raises(AccountSessionDecryptError, match="status=key_mismatch"):
        await wrong_key_repo.get_accounts()


async def test_get_live_usable_accounts_skips_degraded_rows(repo_with_cipher):
    runtime_cipher = SessionCipher("runtime-key")
    bad_cipher = SessionCipher("other-key")
    await repo_with_cipher._db.execute(
        "INSERT INTO accounts (phone, session_string, is_primary, is_active) VALUES (?, ?, 0, 1)",
        ("+10000000001", bad_cipher.encrypt("bad-session")),
    )
    await repo_with_cipher._db.execute(
        "INSERT INTO accounts (phone, session_string, is_primary, is_active) VALUES (?, ?, 1, 1)",
        ("+10000000002", runtime_cipher.encrypt("good-session")),
    )
    await repo_with_cipher._db.execute(
        "INSERT INTO accounts (phone, session_string, is_primary, is_active) VALUES (?, ?, 0, 1)",
        ("+10000000003", "enc:v999:unsupported"),
    )
    await repo_with_cipher._db.execute(
        "INSERT INTO accounts (phone, session_string, is_primary, is_active) VALUES (?, ?, 0, 1)",
        ("+10000000004", "plaintext-session"),
    )
    await repo_with_cipher._db.commit()

    runtime_repo = AccountsRepository(repo_with_cipher._db, session_cipher=runtime_cipher)

    accounts = await runtime_repo.get_live_usable_accounts(active_only=True)

    assert [account.phone for account in accounts] == ["+10000000002", "+10000000004"]
    assert accounts[0].session_string == "good-session"
    assert accounts[1].session_string == "plaintext-session"


async def test_get_live_usable_accounts_returns_empty_when_all_degraded(repo_with_cipher):
    bad_cipher = SessionCipher("other-key")
    await repo_with_cipher._db.execute(
        "INSERT INTO accounts (phone, session_string, is_primary, is_active) VALUES (?, ?, 0, 1)",
        ("+10000000001", bad_cipher.encrypt("bad-session")),
    )
    await repo_with_cipher._db.execute(
        "INSERT INTO accounts (phone, session_string, is_primary, is_active) VALUES (?, ?, 0, 1)",
        ("+10000000002", "enc:v999:unsupported"),
    )
    await repo_with_cipher._db.commit()

    runtime_repo = AccountsRepository(
        repo_with_cipher._db,
        session_cipher=SessionCipher("runtime-key"),
    )

    assert await runtime_repo.get_live_usable_accounts(active_only=True) == []


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


async def test_update_account_flood_set(accounts_repo):
    """Test setting flood_wait_until."""
    await accounts_repo.add_account(make_account("+1234567890"))
    until = datetime(2026, 3, 16, 12, 0, 0, tzinfo=timezone.utc)
    await accounts_repo.update_account_flood("+1234567890", until)

    accounts = await accounts_repo.get_accounts()
    assert accounts[0].flood_wait_until == until


async def test_update_account_flood_clear(accounts_repo):
    """Test clearing flood_wait_until."""
    await accounts_repo.add_account(make_account("+1234567890"))
    until = datetime(2026, 3, 16, 12, 0, 0, tzinfo=timezone.utc)
    await accounts_repo.update_account_flood("+1234567890", until)

    # Clear it
    await accounts_repo.update_account_flood("+1234567890", None)

    accounts = await accounts_repo.get_accounts()
    assert accounts[0].flood_wait_until is None


# update_account_premium tests


async def test_update_account_premium_set_true(accounts_repo):
    """Test setting is_premium to True."""
    await accounts_repo.add_account(make_account("+1234567890", is_premium=False))
    await accounts_repo.update_account_premium("+1234567890", True)

    accounts = await accounts_repo.get_accounts()
    assert accounts[0].is_premium is True


async def test_update_account_premium_set_false(accounts_repo):
    """Test setting is_premium to False."""
    await accounts_repo.add_account(make_account("+1234567890", is_premium=True))
    await accounts_repo.update_account_premium("+1234567890", False)

    accounts = await accounts_repo.get_accounts()
    assert accounts[0].is_premium is False


# set_account_active tests


async def test_set_account_active_deactivate(accounts_repo):
    """Test deactivating an account."""
    await accounts_repo.add_account(make_account("+1234567890", is_active=True))
    accounts = await accounts_repo.get_accounts()
    pk = accounts[0].id

    await accounts_repo.set_account_active(pk, False)

    accounts = await accounts_repo.get_accounts()
    assert accounts[0].is_active is False


async def test_set_account_active_activate(accounts_repo):
    """Test activating an account."""
    await accounts_repo.add_account(make_account("+1234567890", is_active=False))
    accounts = await accounts_repo.get_accounts()
    pk = accounts[0].id

    await accounts_repo.set_account_active(pk, True)

    accounts = await accounts_repo.get_accounts()
    assert accounts[0].is_active is True


# delete_account tests


async def test_delete_account(accounts_repo):
    """Test deleting an account."""
    await accounts_repo.add_account(make_account("+1234567890"))
    accounts = await accounts_repo.get_accounts()
    pk = accounts[0].id

    await accounts_repo.delete_account(pk)

    accounts = await accounts_repo.get_accounts()
    assert len(accounts) == 0


async def test_delete_account_nonexistent(accounts_repo):
    """Test deleting non-existent account does not raise."""
    await accounts_repo.delete_account(999)  # Should not raise


async def test_delete_primary_promotes_lowest_id_remaining_account(accounts_repo):
    """Deleting the primary promotes the next account in repository order."""
    primary_id = await accounts_repo.add_account(make_account("+1111111111", is_primary=True))
    next_id = await accounts_repo.add_account(make_account("+2222222222", is_primary=False))
    later_id = await accounts_repo.add_account(make_account("+3333333333", is_primary=False))

    await accounts_repo.delete_account(primary_id)

    summaries = await accounts_repo.get_account_summaries(active_only=False)
    assert [acc.id for acc in summaries] == [next_id, later_id]
    assert summaries[0].is_primary is True
    assert summaries[1].is_primary is False


async def test_delete_non_primary_keeps_existing_primary(accounts_repo):
    """Deleting a secondary account leaves the current primary unchanged."""
    primary_id = await accounts_repo.add_account(make_account("+1111111111", is_primary=True))
    secondary_id = await accounts_repo.add_account(make_account("+2222222222", is_primary=False))
    later_id = await accounts_repo.add_account(make_account("+3333333333", is_primary=False))

    await accounts_repo.delete_account(secondary_id)

    summaries = await accounts_repo.get_account_summaries(active_only=False)
    assert [acc.id for acc in summaries] == [primary_id, later_id]
    assert summaries[0].is_primary is True
    assert summaries[1].is_primary is False


async def test_delete_last_account_leaves_empty_table(accounts_repo):
    """Deleting the only account does not attempt promotion."""
    account_id = await accounts_repo.add_account(make_account("+1111111111", is_primary=True))

    await accounts_repo.delete_account(account_id)

    assert await accounts_repo.get_account_summaries(active_only=False) == []


async def test_delete_missing_account_is_noop(accounts_repo):
    """Deleting a missing ID must not repair or otherwise mutate account rows."""
    first_id = await accounts_repo.add_account(make_account("+1111111111", is_primary=False))
    second_id = await accounts_repo.add_account(make_account("+2222222222", is_primary=False))

    await accounts_repo.delete_account(999)

    summaries = await accounts_repo.get_account_summaries(active_only=False)
    assert [(acc.id, acc.is_primary) for acc in summaries] == [
        (first_id, False),
        (second_id, False),
    ]


async def test_delete_primary_promotes_with_wrong_session_key(repo_with_cipher):
    """Promotion uses summaries only and does not require decrypting sessions."""
    writer_cipher = SessionCipher("correct-key")
    primary_cur = await repo_with_cipher._db.execute(
        "INSERT INTO accounts (phone, session_string, is_primary, is_active) VALUES (?, ?, 1, 1)",
        ("+1111111111", writer_cipher.encrypt("primary-session")),
    )
    next_cur = await repo_with_cipher._db.execute(
        "INSERT INTO accounts (phone, session_string, is_primary, is_active) VALUES (?, ?, 0, 1)",
        ("+2222222222", writer_cipher.encrypt("next-session")),
    )
    await repo_with_cipher._db.commit()

    wrong_key_repo = AccountsRepository(
        repo_with_cipher._db,
        session_cipher=SessionCipher("wrong-key"),
    )

    await wrong_key_repo.delete_account(primary_cur.lastrowid)

    summaries = await wrong_key_repo.get_account_summaries(active_only=False)
    assert len(summaries) == 1
    assert summaries[0].id == next_cur.lastrowid
    assert summaries[0].phone == "+2222222222"
    assert summaries[0].is_primary is True
    assert summaries[0].session_status == AccountSessionStatus.DECRYPT_FAILED


# migrate_sessions tests


async def test_migrate_sessions_no_cipher(accounts_repo):
    """Test migrate_sessions returns 0 when no cipher is configured."""
    await accounts_repo.add_account(make_account("+1234567890", session="plaintext"))
    count = await accounts_repo.migrate_sessions()
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
