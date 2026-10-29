"""Migrate complete content snapshots without overwriting or exposing sessions."""

import json
import os
import sqlite3

import httpx
import pytest

from tg_agent.config import GLOBAL_DB_NAME, load_config
from tg_agent.database import Database
from tg_agent.gateway import enrollment
from tg_agent.gateway.catalog import GatewayAdministrator
from tg_agent.gateway.client import TelegramGatewayClient
from tg_agent.gateway.server import create_app
from tg_agent.gateway.store import GatewayStore, private_key
from tg_agent.models import Account, Channel, NotificationBot
from tg_agent.telegram.session_lock import SessionLock, _lock_dir

pytestmark = pytest.mark.aiosqlite_serial


class Access:
    async def close(self):
        pass


@pytest.fixture
async def migration(tmp_path, monkeypatch):
    project = tmp_path / "project"
    project.mkdir()
    monkeypatch.chdir(project)
    monkeypatch.setenv("TGCF_SESSION_LOCK_DIR", str(tmp_path / "locks"))
    root = tmp_path / "shared"
    root.mkdir()
    source = root / GLOBAL_DB_NAME
    db = Database(str(source), session_encryption_secret="source-key")
    await db.initialize()
    await db.add_account(Account(phone="+100", session_string="original-session", is_primary=True))
    await db.add_channel(Channel(channel_id=123, title="all historical content"))
    await db.repos.notification_bots.save_bot(NotificationBot(tg_user_id=10, bot_username="legacy_test_bot",
                                                               bot_token="legacy-test-token"))
    await db.close()
    (project / "config.yaml").write_text("security:\n  session_encryption_key: source-key\n")
    marker = project / ".tg-agent-global"
    marker.write_text(str(root))
    store = GatewayStore(root)
    await store.open()
    admin_token = private_key(root, "gateway-admin.token")
    app = create_app(store, Access())
    transport = httpx.ASGITransport(app=app)

    async def administrator(cls, root):
        return GatewayAdministrator(httpx.AsyncClient(base_url="http://gateway", transport=transport,
            headers={"Authorization": f"Bearer {admin_token}"}))

    async def client(cls, config):
        identity = json.loads((project / "data" / "gateway-project.json").read_text())
        return TelegramGatewayClient("http://gateway", identity["token"], transport=transport)

    monkeypatch.setattr(enrollment, "configure", lambda *args, **kwargs: None)
    monkeypatch.setattr(GatewayAdministrator, "connect", classmethod(administrator))
    monkeypatch.setattr(TelegramGatewayClient, "from_config", classmethod(client))
    try:
        async with app.router.lifespan_context(app):
            yield project, root, source, marker, store
    finally:
        await store.close()


async def test_migration_reencrypts_and_retains_complete_project_content(migration):
    project, _, source, marker, store = migration
    destination = project / "independent.db"
    binding = await enrollment.migrate_project("config.yaml", destination)
    assert load_config().database.path == str(destination)
    assert json.loads(marker.read_text())["version"] == 2
    account = await store.account_for_phone("+100")
    assert await store.session(str(binding.project_id), account) == "original-session"
    from tg_agent.gateway.notifications import BotStorage

    bot = await BotStorage(store, str(binding.project_id)).get_bot(10)
    assert bot.bot_token == "legacy-test-token"
    with sqlite3.connect(destination) as db:
        assert db.execute("SELECT title FROM channels WHERE channel_id=123").fetchone()[0] == "all historical content"
        assert db.execute("SELECT session_string FROM accounts").fetchone()[0] == ""
        assert db.execute("SELECT account_id FROM gateway_account_refs").fetchone()[0] == account
        assert db.execute("SELECT bot_token FROM notification_bots").fetchone()[0] == ""
    with sqlite3.connect(source) as db:
        assert db.execute("SELECT session_string FROM accounts").fetchone()[0].startswith("enc:v2:")
    assert await enrollment.migrate_project("config.yaml", destination) == binding
    assert list((project / "data").glob("gateway-source-*.db"))


async def test_migration_refuses_foreign_destination(migration):
    project, root, _, marker, _ = migration
    destination = project / "occupied.db"
    destination.write_bytes(b"belongs to someone else")
    with pytest.raises(ValueError, match="already exists"):
        await enrollment.migrate_project("config.yaml", destination)
    assert destination.read_bytes() == b"belongs to someone else"
    assert marker.read_text() == str(root)


async def test_migration_resumes_after_marker_commit_interruption(migration, monkeypatch):
    project, root, _, marker, store = migration
    destination = project / "independent.db"
    original = enrollment.write_private_json

    def interrupted(path, value):
        if path == marker:
            raise OSError("injected marker replacement failure")
        return original(path, value)

    monkeypatch.setattr(enrollment, "write_private_json", interrupted)
    with pytest.raises(OSError, match="injected"):
        await enrollment.migrate_project("config.yaml", destination)
    assert marker.read_text() == str(root)
    monkeypatch.setattr(enrollment, "write_private_json", original)
    binding = await enrollment.migrate_project("config.yaml", destination)
    assert len(await store.catalog()) == 1
    assert binding.database_path == str(destination)
    assert json.loads((project / "data" / "gateway-migration.json").read_text())["phase"] == "committed"


async def test_migration_blocks_live_session_owner(migration):
    project, root, _, marker, _ = migration
    lock = SessionLock("another-project", "string:original-session", lock_dir=_lock_dir())
    lock.acquire(grace_seconds=0)
    try:
        with pytest.raises(ValueError, match=f"PID {os.getpid()}"):
            await enrollment.migrate_project("config.yaml", project / "independent.db")
        assert marker.read_text() == str(root)
    finally:
        lock.release()


async def test_wrong_source_key_does_not_switch_marker_or_scrub_session(migration):
    project, root, _, marker, store = migration
    (project / "config.yaml").write_text("security:\n  session_encryption_key: wrong-key\n")
    destination = project / "independent.db"
    with pytest.raises(ValueError):
        await enrollment.migrate_project("config.yaml", destination)
    assert marker.read_text() == str(root)
    assert await store.catalog() == []
    with sqlite3.connect(destination) as db:
        assert db.execute("SELECT session_string FROM accounts").fetchone()[0].startswith("enc:v2:")


async def test_disable_preserves_migrated_content_and_other_project_access(migration):
    from uuid import uuid4

    project, _, _, marker, store = migration
    destination = project / "independent.db"
    binding = await enrollment.migrate_project("config.yaml", destination)
    account = await store.account_for_phone("+100")
    other = str(uuid4())
    await store.register_project(other, project.parent / "other", "x" * 48)
    await store.grant(other, account)
    await enrollment.disable_project("config.yaml")
    assert not marker.exists()
    assert load_config().database.path == str(destination)
    assert await store.catalog(str(binding.project_id)) == []
    assert await store.session(other, account) == "original-session"
    with sqlite3.connect(destination) as db:
        assert db.execute("SELECT COUNT(*) FROM channels").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM accounts").fetchone()[0] == 0
