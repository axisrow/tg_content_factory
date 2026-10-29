"""Private gateway storage. This schema never contains project content tables."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
import time
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import aiosqlite

from tg_agent.gateway.action_protocol import READ_ACTIONS
from tg_agent.security import SessionCipher

SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS gateway_project_bots(
 project_id TEXT NOT NULL REFERENCES projects(id),user_id INTEGER NOT NULL,payload TEXT NOT NULL,
 PRIMARY KEY(project_id,user_id)
);
CREATE TABLE IF NOT EXISTS projects (
 id TEXT PRIMARY KEY, root TEXT UNIQUE NOT NULL, token_hash TEXT UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS accounts (
 id TEXT PRIMARY KEY, phone TEXT UNIQUE NOT NULL, session TEXT NOT NULL,
 active INTEGER NOT NULL DEFAULT 1, flood_until REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS grants (
 project_id TEXT NOT NULL REFERENCES projects(id),
 account_id TEXT NOT NULL REFERENCES accounts(id),
 active INTEGER NOT NULL DEFAULT 1, is_primary INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(project_id, account_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS project_primary
 ON grants(project_id) WHERE is_primary=1;
CREATE TABLE IF NOT EXISTS operations (
 project_id TEXT NOT NULL REFERENCES projects(id), request_id TEXT NOT NULL,
 fingerprint TEXT NOT NULL, payload TEXT NOT NULL, state TEXT NOT NULL,
 result TEXT, created_at REAL NOT NULL DEFAULT 0, PRIMARY KEY(project_id,request_id)
);
CREATE TABLE IF NOT EXISTS events (
 id INTEGER PRIMARY KEY AUTOINCREMENT, account_id TEXT NOT NULL REFERENCES accounts(id),
 created_at REAL NOT NULL, payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS acknowledgements (
 project_id TEXT PRIMARY KEY REFERENCES projects(id), event_id INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS reply_owners (
 account_id TEXT NOT NULL REFERENCES accounts(id), dialog_id INTEGER NOT NULL,
 project_id TEXT NOT NULL REFERENCES projects(id), PRIMARY KEY(account_id,dialog_id)
);
CREATE TABLE IF NOT EXISTS event_retention (id INTEGER PRIMARY KEY CHECK(id=1), floor INTEGER NOT NULL);
INSERT OR IGNORE INTO event_retention VALUES (1,0);
CREATE UNIQUE INDEX IF NOT EXISTS event_identity ON events(account_id,json_extract(payload,'$.identity'));
"""


class GatewayConflictError(ValueError):
    """An existing identity or operation cannot be silently replaced."""


class EventCursorExpiredError(ValueError):
    """The client must recover history rather than silently miss old events."""


# Operations whose delivery is side-effect free: safe to recover as pending and
# safe to prune. Everything else keeps its receipt forever so a request ID stays
# an immutable tombstone (fingerprint conflict, uncertain state).
_READ_KINDS = frozenset(READ_ACTIONS | {
    "history", "profile", "inspect_peer", "dialogs", "search", "dm_history", "message_page", "voice_source",
})


def private_key(root: Path, name: str = "gateway.key") -> str:
    """Create once with exclusive creation, including under concurrent setup."""
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, 0o700)
    path = root / name
    if name == "gateway.key" and not path.exists() and (root / "gateway.db").exists():
        raise GatewayConflictError("Gateway key is missing; restore gateway.key before opening the existing store")
    key = secrets.token_urlsafe(48)
    temporary = root / f".gateway-key-{secrets.token_hex(16)}"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(key)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            # Publish a complete file without replacing a concurrently created key.
            os.link(temporary, path)
        except FileExistsError:
            os.chmod(path, 0o600)
            key = path.read_text(encoding="utf-8").strip()
            if not key:
                raise GatewayConflictError("Gateway key is empty") from None
        return key
    finally:
        temporary.unlink(missing_ok=True)


class GatewayStore:
    """One serialized writer for accounts, grants, operation receipts and events."""

    def __init__(self, root: Path):
        self.root = root.resolve()
        self.cipher = SessionCipher(private_key(self.root))
        self._connection: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    async def open(self) -> None:
        path = self.root / "gateway.db"
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT, 0o600)
        os.close(descriptor)
        os.chmod(path, 0o600)
        self._connection = await aiosqlite.connect(path)
        self._connection.row_factory = aiosqlite.Row
        await self._connection.executescript(SCHEMA)
        async with self._connection.execute("PRAGMA table_info(operations)") as cur:
            columns = {row[1] for row in await cur.fetchall()}
        if "created_at" not in columns:
            # Pre-#1715-receipt-prune databases keep working; old receipts age out
            # from their upgrade moment (DEFAULT 0 makes them immediately eligible).
            await self._connection.execute("ALTER TABLE operations ADD COLUMN created_at REAL NOT NULL DEFAULT 0")
        await self._connection.commit()

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None

    @asynccontextmanager
    async def transaction(self):
        async with self._lock:
            if self._connection is None:
                raise RuntimeError("Gateway store is closed")
            await self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield self._connection
                await self._connection.commit()
            except BaseException:
                await self._connection.rollback()
                raise

    async def register_project(self, project_id: str, root: Path, token: str) -> None:
        if len(token) < 32:
            raise ValueError("Project tokens require at least 32 characters")
        digest = hashlib.sha256(token.encode()).hexdigest()
        canonical = str(await asyncio.to_thread(root.resolve))
        async with self.transaction() as db:
            async with db.execute("SELECT * FROM projects WHERE id=? OR root=?", (project_id, canonical)) as cur:
                row = await cur.fetchone()
            if row:
                if (row["id"], row["root"], row["token_hash"]) != (project_id, canonical, digest):
                    raise GatewayConflictError("Project identity is already registered")
                return
            await db.execute("INSERT INTO projects VALUES (?,?,?)", (project_id, canonical, digest))

    async def authenticate(self, token: str) -> str | None:
        digest = hashlib.sha256(token.encode()).hexdigest()
        async with self.transaction() as db:
            async with db.execute("SELECT id FROM projects WHERE token_hash=?", (digest,)) as cur:
                row = await cur.fetchone()
        return row["id"] if row else None

    async def import_account(self, phone: str, session: str, *, source_secret: str | None = None,
                             replace: bool = False) -> str:
        if SessionCipher.is_encrypted(session):
            if not source_secret:
                raise ValueError("Source encryption key is required")
            session = SessionCipher(source_secret).decrypt(session)
        if not phone or not session:
            raise ValueError("Phone and session are required")
        async with self.transaction() as db:
            async with db.execute("SELECT id,session FROM accounts WHERE phone=?", (phone,)) as cur:
                row = await cur.fetchone()
            if row:
                if self.cipher.decrypt(row["session"]) != session:
                    if replace:
                        async with db.execute("SELECT session FROM accounts WHERE id!=?", (row["id"],)) as cur:
                            others = await cur.fetchall()
                        if any(self.cipher.decrypt(other[0]) == session for other in others):
                            raise GatewayConflictError("Session is already registered under another account")
                        await db.execute("UPDATE accounts SET session=?,active=1 WHERE id=?",
                                         (self.cipher.encrypt(session), row["id"]))
                        return row["id"]
                    raise GatewayConflictError("Account already has a different shared session")
                return row["id"]
            async with db.execute("SELECT session FROM accounts") as cur:
                existing = await cur.fetchall()
            if any(self.cipher.decrypt(candidate["session"]) == session for candidate in existing):
                raise GatewayConflictError("Session is already registered under another account")
            account_id = str(uuid4())
            await db.execute("INSERT INTO accounts(id,phone,session) VALUES (?,?,?)",
                             (account_id, phone, self.cipher.encrypt(session)))
            return account_id

    async def grant(self, project: str, account: str, *, primary: bool = False) -> None:
        async with self.transaction() as db:
            if primary:
                await db.execute("UPDATE grants SET is_primary=0 WHERE project_id=?", (project,))
            await db.execute(
                "INSERT INTO grants(project_id,account_id,is_primary) VALUES (?,?,?) "
                "ON CONFLICT(project_id,account_id) DO UPDATE SET active=1,"
                "is_primary=MAX(is_primary,excluded.is_primary)",
                (project, account, int(primary)),
            )

    async def revoke(self, project: str, account: str) -> None:
        async with self.transaction() as db:
            await db.execute("DELETE FROM grants WHERE project_id=? AND account_id=?", (project, account))
            await db.execute("DELETE FROM reply_owners WHERE project_id=? AND account_id=?", (project, account))

    async def accounts(self, project: str) -> list[dict]:
        async with self.transaction() as db:
            async with db.execute(
                "SELECT a.id,a.phone,a.active,a.flood_until,g.is_primary,"
                "COALESCE((SELECT value='1' FROM settings WHERE key='premium:'||a.id),0) AS is_premium "
                "FROM accounts a "
                "JOIN grants g ON g.account_id=a.id WHERE g.project_id=? AND g.active=1 "
                "ORDER BY g.is_primary DESC,a.id", (project,),
            ) as cur:
                return [dict(row) for row in await cur.fetchall()]

    async def catalog(self, project: str | None = None) -> list[dict]:
        async with self.transaction() as db:
            if project is None:
                sql, params = "SELECT id,phone,active,flood_until FROM accounts ORDER BY phone", ()
            else:
                sql = ("SELECT a.id,a.phone,(a.active AND g.active) AS active,a.flood_until,g.is_primary,"
                       "COALESCE((SELECT value='1' FROM settings WHERE key='premium:'||a.id),0) AS is_premium "
                       "FROM accounts a JOIN grants g ON g.account_id=a.id WHERE g.project_id=? ORDER BY a.phone")
                params = (project,)
            async with db.execute(sql, params) as cur:
                return [dict(row) for row in await cur.fetchall()]

    async def account_for_phone(self, phone: str) -> str:
        async with self.transaction() as db:
            async with db.execute("SELECT id FROM accounts WHERE phone=?", (phone,)) as cur:
                row = await cur.fetchone()
        if row is None:
            raise KeyError("Shared account not found")
        return row[0]

    async def update_grant(self, project: str, account: str, *, active: bool | None = None,
                           primary: bool = False) -> None:
        async with self.transaction() as db:
            async with db.execute("SELECT 1 FROM grants WHERE project_id=? AND account_id=?",
                                  (project, account)) as cur:
                if await cur.fetchone() is None:
                    raise KeyError("Project account not found")
            if primary:
                await db.execute("UPDATE grants SET is_primary=0 WHERE project_id=?", (project,))
                await db.execute("UPDATE grants SET is_primary=1 WHERE project_id=? AND account_id=?",
                                 (project, account))
            if active is not None:
                await db.execute("UPDATE grants SET active=? WHERE project_id=? AND account_id=?",
                                 (int(active), project, account))

    async def administer_account(self, phone: str, *, active: bool | None = None, delete: bool = False) -> str:
        account = await self.account_for_phone(phone)
        async with self.transaction() as db:
            if delete:
                await db.execute("DELETE FROM reply_owners WHERE account_id=?", (account,))
                await db.execute("DELETE FROM grants WHERE account_id=?", (account,))
                await db.execute("UPDATE accounts SET active=0,session='' WHERE id=?", (account,))
            elif active is not None:
                await db.execute("UPDATE accounts SET active=? WHERE id=?", (int(active), account))
        return account

    async def export_session(self, phone: str) -> str:
        async with self.transaction() as db:
            async with db.execute("SELECT session FROM accounts WHERE phone=?", (phone,)) as cur:
                row = await cur.fetchone()
        if row is None or not row[0]:
            raise KeyError("Shared session not found")
        return self.cipher.decrypt(row[0])

    async def subscriptions(self) -> list[tuple[str, str]]:
        """One authorized bootstrap context for every shared active account."""
        async with self.transaction() as db:
            async with db.execute("SELECT MIN(g.project_id),a.id FROM accounts a JOIN grants g "
                                  "ON g.account_id=a.id WHERE a.active=1 AND g.active=1 GROUP BY a.id") as cur:
                return [tuple(row) for row in await cur.fetchall()]

    async def session(self, project: str, account: str) -> str:
        async with self.transaction() as db:
            async with db.execute(
                "SELECT a.session FROM accounts a JOIN grants g ON g.account_id=a.id "
                "WHERE g.project_id=? AND a.id=? AND a.active=1 AND g.active=1", (project, account),
            ) as cur:
                row = await cur.fetchone()
        if row is None:
            raise PermissionError("Account access denied")
        return self.cipher.decrypt(row["session"])

    async def flood(self, account: str, until: float) -> None:
        async with self.transaction() as db:
            await db.execute("UPDATE accounts SET flood_until=MAX(flood_until,?) WHERE id=?", (until, account))

    async def submit(self, project: str, request_id: str, payload: dict) -> dict:
        serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        fingerprint = hashlib.sha256(serialized.encode()).hexdigest()
        async with self.transaction() as db:
            async with db.execute(
                "SELECT fingerprint FROM operations WHERE project_id=? AND request_id=?", (project, request_id),
            ) as cur:
                row = await cur.fetchone()
            if row and row["fingerprint"] != fingerprint:
                raise GatewayConflictError("Request ID was already used for another payload")
            if row is None:
                await db.execute("INSERT INTO operations VALUES (?,?,?,?,?,NULL,?)",
                                 (project, request_id, fingerprint, serialized, "pending", time.time()))
        return await self.operation(project, request_id)

    async def operation(self, project: str, request_id: str) -> dict:
        async with self.transaction() as db:
            async with db.execute("SELECT * FROM operations WHERE project_id=? AND request_id=?",
                                  (project, request_id)) as cur:
                row = await cur.fetchone()
        if row is None:
            raise KeyError("Operation not found")
        return {"request_id": request_id, "state": row["state"],
                "result": json.loads(row["result"]) if row["result"] else None}

    async def pending(self) -> list[dict]:
        async with self.transaction() as db:
            async with db.execute("SELECT project_id,request_id,payload FROM operations WHERE state='pending' "
                                  "ORDER BY rowid") as cur:
                return [{**dict(row), "payload": json.loads(row["payload"])} for row in await cur.fetchall()]

    async def transition(self, project: str, request: str, expected: str, state: str, result=None) -> bool:
        async with self.transaction() as db:
            async with db.execute(
                "UPDATE operations SET state=?,result=? WHERE project_id=? AND request_id=? AND state=?",
                (state, json.dumps(result) if result is not None else None, project, request, expected),
            ) as cur:
                return cur.rowcount == 1

    async def recover(self) -> None:
        """A crash can leave writes delivered: never replay an interrupted operation."""
        async with self.transaction() as db:
            async with db.execute("SELECT project_id,request_id,payload FROM operations WHERE state='running'") as cur:
                rows = await cur.fetchall()
            for row in rows:
                payload = json.loads(row["payload"])
                kind = payload.get("kind")
                read = kind in _READ_KINDS or (kind == "notification" and payload.get("action") == "status")
                state = "pending" if read else "uncertain"
                await db.execute("UPDATE operations SET state=? WHERE project_id=? AND request_id=?",
                                 (state, row["project_id"], row["request_id"]))

    async def prune_read_receipts(self, *, now: float, ttl: float = 86400) -> int:
        """Drop aged-out read receipts; write receipts stay forever.

        Read results (message pages, dialogs, avatars) are the bulk of receipt
        storage and are always re-fetchable. Write receipts — including their
        fingerprints and uncertain states — must never disappear, or a reused
        request ID would silently replay an unconfirmed write.
        """
        kinds = sorted(_READ_KINDS)
        placeholders = ",".join("?" for _ in kinds)
        async with self.transaction() as db:
            cur = await db.execute(
                "DELETE FROM operations WHERE created_at>0 AND created_at<? AND state NOT IN ('pending','running') "
                f"AND (json_extract(payload,'$.kind') IN ({placeholders}) "
                "OR (json_extract(payload,'$.kind')='notification' "
                "AND json_extract(payload,'$.action')='status'))",
                (now - ttl, *kinds),
            )
            return cur.rowcount or 0

    async def record_event(self, account: str, identity: str, payload: dict, *, now: float) -> None:
        async with self.transaction() as db:
            await db.execute("INSERT OR IGNORE INTO events(account_id,created_at,payload) VALUES (?,?,?)",
                             (account, now, json.dumps({**payload, "identity": identity})))

    async def expire_events(self, *, now: float) -> None:
        async with self.transaction() as db:
            threshold = now - 7 * 86400
            await db.execute("UPDATE event_retention SET floor=MAX(floor,COALESCE("
                             "(SELECT MAX(id) FROM events WHERE created_at<?),0)) WHERE id=1", (threshold,))
            await db.execute("DELETE FROM events WHERE created_at<?", (threshold,))

    async def events(self, project: str, after: int, *, limit: int = 100) -> list[dict]:
        if after < 0 or not 1 <= limit <= 100:
            raise ValueError("Invalid event cursor or limit")
        async with self.transaction() as db:
            async with db.execute("SELECT floor FROM event_retention WHERE id=1") as cur:
                floor = (await cur.fetchone())[0]
            if after < floor:
                raise EventCursorExpiredError("Event cursor expired; recover Telegram history")
            async with db.execute(
                "SELECT e.* FROM events e JOIN grants g ON g.account_id=e.account_id "
                "WHERE g.project_id=? AND g.active=1 AND e.id>? ORDER BY e.id LIMIT ?",
                (project, after, limit),
            ) as cur:
                return [{"id": row["id"], "account_id": row["account_id"], "payload": json.loads(row["payload"])}
                        for row in await cur.fetchall()]

    async def acknowledge(self, project: str, event_id: int) -> None:
        if event_id < 0:
            raise ValueError("Invalid event ID")
        async with self.transaction() as db:
            await db.execute("INSERT INTO acknowledgements VALUES (?,?) ON CONFLICT(project_id) "
                             "DO UPDATE SET event_id=MAX(event_id,excluded.event_id)", (project, event_id))

    async def assign_reply_owner(self, project: str, account: str, dialog: int) -> None:
        await self.session(project, account)
        async with self.transaction() as db:
            async with db.execute("SELECT project_id FROM reply_owners WHERE account_id=? AND dialog_id=?",
                                  (account, dialog)) as cur:
                row = await cur.fetchone()
            if row is not None and row[0] != project:
                raise GatewayConflictError("Dialog already has an automatic-reply owner")
            await db.execute("INSERT OR IGNORE INTO reply_owners VALUES (?,?,?)", (account, dialog, project))

    async def release_reply_owner(self, project: str, account: str, dialog: int):
        async with self.transaction() as db:
            await db.execute("DELETE FROM reply_owners WHERE project_id=? AND account_id=? AND dialog_id=?",
                             (project, account, dialog))

    async def event_floor(self):
        async with self.transaction() as db:
            async with db.execute("SELECT floor FROM event_retention WHERE id=1") as cur:
                return (await cur.fetchone())[0]

    async def can_auto_reply(self, project: str, account: str, dialog: int) -> bool:
        async with self.transaction() as db:
            async with db.execute("SELECT 1 FROM reply_owners r JOIN grants g ON "
                                  "g.project_id=r.project_id AND g.account_id=r.account_id WHERE "
                                  "r.project_id=? AND r.account_id=? AND r.dialog_id=? AND g.active=1",
                                  (project, account, dialog)) as cur:
                return await cur.fetchone() is not None
