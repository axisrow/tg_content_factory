from __future__ import annotations

import logging
import sqlite3

import aiosqlite

logger = logging.getLogger(__name__)


async def begin_immediate(db: aiosqlite.Connection) -> None:
    """Start a write transaction, recovering from stale nested transactions."""
    if db.in_transaction:
        logger.warning("begin_immediate: rolling back existing transaction (pending writes will be lost)")
        await db.rollback()
    try:
        await db.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError as exc:
        if "cannot start a transaction within a transaction" not in str(exc):
            raise
        logger.warning("begin_immediate: nested-transaction fallback, rolling back")
        await db.rollback()
        await db.execute("BEGIN IMMEDIATE")
