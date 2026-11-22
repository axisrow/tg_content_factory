"""Channel access keys that survive the process (#1877).

Reading a channel the account has not joined takes its access hash, and only a
username lookup returns one: 20 a minute and 60 an hour per account. The pool's
``FreshEntityStringSession`` keeps the keys in memory, so every restart of the
connection's owner looked every channel up again. Telegram's advice is to keep
them (core.telegram.org/api/peers): a key is valid for the account and the login
that received it, so rows carry the auth key id and a new login drops them.

Schema and write path follow Telethon v1 ``sessions/sqlite.py`` (MIT): one row per
peer, ``insert or replace``. The ``(account, id)`` key follows
telethon-session-sqlalchemy (MIT), which keeps many sessions in one table. Only
groups and channels are kept: collection needs them, and users would grow the
table without bound. A username moves between channels: one not written for a day
loads without it — the id and key still work, and the next answer that names the
channel writes it back.

Only the connection's owner loads and saves: a relayed pool gets the owner's keys
over the relay (the ``entities`` op), and file sessions keep their own.
"""

from __future__ import annotations

import logging
import time

from tg_agent.utils.safe_logging import mask_phone
from tg_shared.sessions import FreshEntityStringSession

logger = logging.getLogger(__name__)

USERNAME_TTL_SEC = 24 * 60 * 60


class EntityStoreMixin:
    """Load a session's channel keys when it connects; save the ones that changed."""

    def _owned_entity_session(self, raw_client: object) -> tuple[FreshEntityStringSession, str] | None:
        """The client's session and its login (auth key id), when this pool keeps its keys."""
        if vars(self).get("_relay") is not None:
            return None
        session = getattr(raw_client, "session", None)
        auth_key = getattr(session, "auth_key", None)
        if isinstance(session, FreshEntityStringSession) and auth_key is not None:
            return session, str(auth_key.key_id)
        return None

    def _saved_entities(self, phone: str) -> dict[int, tuple]:
        """The account's rows as this pool last read or wrote them: id -> (hash, username, phone, name)."""
        return vars(self).setdefault("_session_entities_saved", {}).setdefault(phone, {})

    async def restore_session_entities(self, phone: str, raw_client: object) -> None:
        """Load the account's stored keys into a client before the pool hands it out."""
        owned = self._owned_entity_session(raw_client)
        db = getattr(self, "_db", None)
        if owned is None or db is None:
            return
        session, key_id = owned
        try:
            async with db.transaction() as conn:
                dropped = await conn.execute(
                    "DELETE FROM session_entities WHERE account = ? AND auth_key_id != ?", (phone, key_id)
                )
                cursor = await conn.execute(
                    "SELECT id, hash, CASE WHEN date >= ? THEN username END, phone, name "
                    "FROM session_entities WHERE account = ? ORDER BY date",
                    (int(time.time() - USERNAME_TTL_SEC), phone),
                )
                rows = [tuple(row) for row in await cursor.fetchall()]
        except Exception:
            logger.warning("entity store: account %s: keys not loaded", mask_phone(phone), exc_info=True)
            return
        live = {row[0] for row in session._entities}  # a row Telegram already sent is newer
        session.add_entity_rows(row for row in rows if row[0] not in live)
        saved = self._saved_entities(phone)
        saved.clear()
        saved.update((row[0], row[1:]) for row in rows)
        logger.info(
            "entity store: account %s: %d channel key(s) loaded, %d of an older login dropped",
            mask_phone(phone), len(rows), dropped.rowcount,
        )

    async def persist_session_entities(self, phone: str | None = None) -> None:
        """Best-effort: save the channel keys that changed since the last load or save."""
        db = getattr(self, "_db", None)
        clients = getattr(self, "clients", {})
        if db is None:
            return
        for account in [phone] if phone is not None else list(clients):
            owned = self._owned_entity_session(getattr(clients.get(account), "raw_client", None))
            if owned is None:
                continue
            session, key_id = owned
            saved = self._saved_entities(account)
            changed = [row for row in session._entities if row[0] < 0 and saved.get(row[0]) != row[1:]]
            if not changed:
                continue
            now = int(time.time())
            try:
                await db.executemany_write(
                    "INSERT OR REPLACE INTO session_entities VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [(account, key_id, *row, now) for row in changed],
                )
            except Exception:
                logger.warning("entity store: account %s: keys not saved", mask_phone(account), exc_info=True)
                continue
            saved.update((row[0], row[1:]) for row in changed)
