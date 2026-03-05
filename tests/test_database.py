from datetime import datetime, timezone

import aiosqlite
import pytest

from src.database import Database
from src.models import Account, Channel, Keyword, Message


@pytest.mark.asyncio
async def test_add_and_get_accounts(db):
    acc = Account(phone="+71234567890", session_string="session1", is_primary=True)
    await db.add_account(acc)

    accounts = await db.get_accounts()
    assert len(accounts) == 1
    assert accounts[0].phone == "+71234567890"
    assert accounts[0].is_primary is True


@pytest.mark.asyncio
async def test_account_upsert(db):
    acc = Account(phone="+71234567890", session_string="session1")
    await db.add_account(acc)
    acc2 = Account(phone="+71234567890", session_string="session2")
    await db.add_account(acc2)

    accounts = await db.get_accounts()
    assert len(accounts) == 1
    assert accounts[0].session_string == "session2"


@pytest.mark.asyncio
async def test_account_session_encrypted_at_rest(tmp_path):
    db_path = str(tmp_path / "encrypted.db")
    database = Database(db_path, session_encryption_secret="test-encryption-secret")
    await database.initialize()

    await database.add_account(Account(phone="+71230000000", session_string="session_plain"))

    cur = await database.execute(
        "SELECT session_string FROM accounts WHERE phone = ?",
        ("+71230000000",),
    )
    row = await cur.fetchone()
    assert row is not None
    assert row["session_string"] != "session_plain"
    assert row["session_string"].startswith("enc:v1:")

    accounts = await database.get_accounts()
    assert accounts[0].session_string == "session_plain"
    await database.close()


@pytest.mark.asyncio
async def test_plaintext_sessions_migrate_on_init(tmp_path):
    """Plaintext sessions are encrypted during initialize(), not get_accounts()."""
    db_path = str(tmp_path / "plaintext_migration.db")

    legacy_db = Database(db_path)
    await legacy_db.initialize()
    await legacy_db.add_account(Account(phone="+71230000001", session_string="legacy_plaintext"))
    await legacy_db.close()

    encrypted_db = Database(db_path, session_encryption_secret="migration-secret")
    await encrypted_db.initialize()

    # Migration already happened during initialize(); verify DB state first.
    cur = await encrypted_db.execute(
        "SELECT session_string FROM accounts WHERE phone = ?",
        ("+71230000001",),
    )
    row = await cur.fetchone()
    assert row is not None
    assert row["session_string"].startswith("enc:v1:")

    # get_accounts() returns decrypted value.
    accounts = await encrypted_db.get_accounts()

    assert accounts[0].session_string == "legacy_plaintext"
    await encrypted_db.close()


@pytest.mark.asyncio
async def test_add_and_get_channels(db):
    ch = Channel(channel_id=-1001234567890, title="Test Channel", username="@test")
    await db.add_channel(ch)

    channels = await db.get_channels()
    assert len(channels) == 1
    assert channels[0].channel_id == -1001234567890


@pytest.mark.asyncio
async def test_get_channel_by_pk(db):
    ch = Channel(channel_id=-1002233445566, title="Lookup Channel", username="@lookup")
    await db.add_channel(ch)

    channels = await db.get_channels()
    found = await db.get_channel_by_pk(channels[0].id)
    assert found is not None
    assert found.channel_id == -1002233445566
    assert found.title == "Lookup Channel"


@pytest.mark.asyncio
async def test_get_channel_by_pk_returns_none_for_unknown_id(db):
    found = await db.get_channel_by_pk(999999)
    assert found is None


@pytest.mark.asyncio
async def test_insert_message_deduplication(db):
    msg = Message(
        channel_id=-1001234567890,
        message_id=1,
        text="Hello world",
        date=datetime.now(timezone.utc),
    )
    first = await db.insert_message(msg)
    second = await db.insert_message(msg)
    assert first is True
    assert second is False
    # Second insert should be ignored (dedup)
    messages, total = await db.search_messages()
    assert total == 1


@pytest.mark.asyncio
async def test_batch_insert(db):
    messages = [
        Message(
            channel_id=-100123,
            message_id=i,
            text=f"Message {i}",
            date=datetime.now(timezone.utc),
        )
        for i in range(10)
    ]
    count = await db.insert_messages_batch(messages)
    assert count == 10

    results, total = await db.search_messages()
    assert total == 10


@pytest.mark.asyncio
async def test_search_messages(db):
    messages = [
        Message(
            channel_id=-100123,
            message_id=1,
            text="Bitcoin price is rising",
            date=datetime.now(timezone.utc),
        ),
        Message(
            channel_id=-100123,
            message_id=2,
            text="Ethereum update today",
            date=datetime.now(timezone.utc),
        ),
        Message(
            channel_id=-100123,
            message_id=3,
            text="Bitcoin hits new ATH",
            date=datetime.now(timezone.utc),
        ),
    ]
    await db.insert_messages_batch(messages)

    results, total = await db.search_messages(query="Bitcoin")
    assert total == 2
    assert all("Bitcoin" in (m.text or "") for m in results)


@pytest.mark.asyncio
async def test_keywords_crud(db):
    kw = Keyword(pattern="bitcoin", is_regex=False)
    kid = await db.add_keyword(kw)
    assert kid > 0

    keywords = await db.get_keywords()
    assert len(keywords) == 1
    assert keywords[0].pattern == "bitcoin"

    await db.delete_keyword(keywords[0].id)
    keywords = await db.get_keywords()
    assert len(keywords) == 0


@pytest.mark.asyncio
async def test_stats(db):
    stats = await db.get_stats()
    assert stats["accounts"] == 0
    assert stats["channels"] == 0
    assert stats["messages"] == 0
    assert stats["keywords"] == 0


@pytest.mark.asyncio
async def test_get_set_setting(db):
    assert await db.get_setting("nonexistent") is None
    await db.set_setting("tg_api_id", "12345")
    assert await db.get_setting("tg_api_id") == "12345"
    await db.set_setting("tg_api_id", "99999")  # upsert
    assert await db.get_setting("tg_api_id") == "99999"


@pytest.mark.asyncio
async def test_stats_with_data(db):
    acc = Account(phone="+71234567890", session_string="s1", is_primary=True)
    await db.add_account(acc)
    ch = Channel(channel_id=-100123, title="Test Channel")
    await db.add_channel(ch)
    msgs = [
        Message(
            channel_id=-100123,
            message_id=i,
            text=f"Msg {i}",
            date=datetime.now(timezone.utc),
        )
        for i in range(1, 4)
    ]
    await db.insert_messages_batch(msgs)
    kw = Keyword(pattern="test")
    await db.add_keyword(kw)

    stats = await db.get_stats()
    assert stats["accounts"] == 1
    assert stats["channels"] == 1
    assert stats["messages"] == 3
    assert stats["keywords"] == 1


@pytest.mark.asyncio
async def test_account_premium_field(db):
    acc = Account(phone="+71234567890", session_string="s1", is_premium=True)
    await db.add_account(acc)

    accounts = await db.get_accounts()
    assert len(accounts) == 1
    assert accounts[0].is_premium is True


@pytest.mark.asyncio
async def test_update_account_premium(db):
    acc = Account(phone="+71234567890", session_string="s1", is_premium=False)
    await db.add_account(acc)

    await db.update_account_premium("+71234567890", True)
    accounts = await db.get_accounts()
    assert accounts[0].is_premium is True

    await db.update_account_premium("+71234567890", False)
    accounts = await db.get_accounts()
    assert accounts[0].is_premium is False


@pytest.mark.asyncio
async def test_account_upsert_updates_premium(db):
    acc = Account(phone="+71234567890", session_string="s1", is_premium=False)
    await db.add_account(acc)

    acc2 = Account(phone="+71234567890", session_string="s2", is_premium=True)
    await db.add_account(acc2)

    accounts = await db.get_accounts()
    assert len(accounts) == 1
    assert accounts[0].is_premium is True
    assert accounts[0].session_string == "s2"


@pytest.mark.asyncio
async def test_insert_message_with_media_type(db):
    msg = Message(
        channel_id=-100123,
        message_id=1,
        text=None,
        media_type="photo",
        date=datetime.now(timezone.utc),
    )
    inserted = await db.insert_message(msg)
    assert inserted is True

    messages, total = await db.search_messages()
    assert total == 1
    assert messages[0].media_type == "photo"
    assert messages[0].text is None


@pytest.mark.asyncio
async def test_batch_insert_with_media_type(db):
    messages = [
        Message(
            channel_id=-100123,
            message_id=1,
            text="Hello",
            media_type=None,
            date=datetime.now(timezone.utc),
        ),
        Message(
            channel_id=-100123,
            message_id=2,
            text=None,
            media_type="video",
            date=datetime.now(timezone.utc),
        ),
        Message(
            channel_id=-100123,
            message_id=3,
            text="With photo",
            media_type="photo",
            date=datetime.now(timezone.utc),
        ),
    ]
    count = await db.insert_messages_batch(messages)
    assert count > 0

    results, total = await db.search_messages()
    assert total == 3
    media_types = {m.message_id: m.media_type for m in results}
    assert media_types[1] is None
    assert media_types[2] == "video"
    assert media_types[3] == "photo"


@pytest.mark.asyncio
async def test_migrate_adds_media_type_column(tmp_path):
    """Migration adds media_type column to existing DB without it."""
    db_path = str(tmp_path / "migrate_test.db")

    # Create DB with old schema (no media_type)
    conn = await aiosqlite.connect(db_path)
    await conn.executescript("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY,
            channel_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            sender_id INTEGER,
            sender_name TEXT,
            text TEXT,
            date TEXT NOT NULL,
            collected_at TEXT DEFAULT (datetime('now')),
            UNIQUE(channel_id, message_id)
        );
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY,
            phone TEXT UNIQUE NOT NULL,
            session_string TEXT NOT NULL,
            is_primary INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            flood_wait_until TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY,
            channel_id INTEGER UNIQUE NOT NULL,
            title TEXT,
            username TEXT,
            is_active INTEGER DEFAULT 1,
            last_collected_id INTEGER DEFAULT 0,
            added_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS keywords (
            id INTEGER PRIMARY KEY,
            pattern TEXT NOT NULL,
            is_regex INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
    """)
    await conn.commit()
    await conn.close()

    # Now initialize with Database — should run migration
    database = Database(db_path)
    await database.initialize()

    # Verify media_type column exists
    cur = await database.execute("PRAGMA table_info(messages)")
    columns = {row["name"] for row in await cur.fetchall()}
    assert "media_type" in columns

    # Verify we can insert with media_type
    msg = Message(
        channel_id=-100123,
        message_id=1,
        text="test",
        media_type="sticker",
        date=datetime.now(timezone.utc),
    )
    await database.insert_message(msg)
    messages, total = await database.search_messages()
    assert total == 1
    assert messages[0].media_type == "sticker"

    await database.close()


@pytest.mark.asyncio
async def test_add_channel_with_channel_type(db):
    ch = Channel(channel_id=-100123, title="Test", username="test", channel_type="channel")
    await db.add_channel(ch)

    channels = await db.get_channels()
    assert len(channels) == 1
    assert channels[0].channel_type == "channel"


@pytest.mark.asyncio
async def test_channel_type_upsert(db):
    ch = Channel(channel_id=-100123, title="Test", username="test", channel_type=None)
    await db.add_channel(ch)

    ch2 = Channel(channel_id=-100123, title="Test Updated", username="test", channel_type="group")
    await db.add_channel(ch2)

    channels = await db.get_channels()
    assert len(channels) == 1
    assert channels[0].channel_type == "group"
    assert channels[0].title == "Test Updated"


@pytest.mark.asyncio
async def test_migrate_adds_channel_type_column(tmp_path):
    """Migration adds channel_type column to existing channels table."""
    db_path = str(tmp_path / "migrate_channel_type_test.db")

    conn = await aiosqlite.connect(db_path)
    await conn.executescript("""
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY,
            phone TEXT UNIQUE NOT NULL,
            session_string TEXT NOT NULL,
            is_primary INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            is_premium INTEGER DEFAULT 0,
            flood_wait_until TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY,
            channel_id INTEGER UNIQUE NOT NULL,
            title TEXT,
            username TEXT,
            is_active INTEGER DEFAULT 1,
            last_collected_id INTEGER DEFAULT 0,
            added_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY,
            channel_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            sender_id INTEGER,
            sender_name TEXT,
            text TEXT,
            media_type TEXT,
            date TEXT NOT NULL,
            collected_at TEXT DEFAULT (datetime('now')),
            UNIQUE(channel_id, message_id)
        );
        CREATE TABLE IF NOT EXISTS keywords (
            id INTEGER PRIMARY KEY,
            pattern TEXT NOT NULL,
            is_regex INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
    """)
    await conn.execute(
        "INSERT INTO channels (channel_id, title) VALUES (?, ?)",
        (-100123, "Old Channel"),
    )
    await conn.commit()
    await conn.close()

    database = Database(db_path)
    await database.initialize()

    cur = await database.execute("PRAGMA table_info(channels)")
    columns = {row["name"] for row in await cur.fetchall()}
    assert "channel_type" in columns

    channels = await database.get_channels()
    assert len(channels) == 1
    assert channels[0].channel_type is None

    ch = Channel(channel_id=-100456, title="New", channel_type="group")
    await database.add_channel(ch)
    channels = await database.get_channels()
    assert any(c.channel_type == "group" for c in channels)

    await database.close()


@pytest.mark.asyncio
async def test_migrate_adds_is_premium_column(tmp_path):
    """Migration adds is_premium column to existing accounts table without it."""
    db_path = str(tmp_path / "migrate_premium_test.db")

    conn = await aiosqlite.connect(db_path)
    await conn.executescript("""
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY,
            phone TEXT UNIQUE NOT NULL,
            session_string TEXT NOT NULL,
            is_primary INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            flood_wait_until TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY,
            channel_id INTEGER UNIQUE NOT NULL,
            title TEXT,
            username TEXT,
            is_active INTEGER DEFAULT 1,
            last_collected_id INTEGER DEFAULT 0,
            added_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY,
            channel_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            sender_id INTEGER,
            sender_name TEXT,
            text TEXT,
            media_type TEXT,
            date TEXT NOT NULL,
            collected_at TEXT DEFAULT (datetime('now')),
            UNIQUE(channel_id, message_id)
        );
        CREATE TABLE IF NOT EXISTS keywords (
            id INTEGER PRIMARY KEY,
            pattern TEXT NOT NULL,
            is_regex INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
    """)
    # Insert an account without is_premium column
    await conn.execute(
        "INSERT INTO accounts (phone, session_string) VALUES (?, ?)",
        ("+71111111111", "session_old"),
    )
    await conn.commit()
    await conn.close()

    database = Database(db_path)
    await database.initialize()

    cur = await database.execute("PRAGMA table_info(accounts)")
    columns = {row["name"] for row in await cur.fetchall()}
    assert "is_premium" in columns

    accounts = await database.get_accounts()
    assert len(accounts) == 1
    assert accounts[0].is_premium is False

    await database.close()
