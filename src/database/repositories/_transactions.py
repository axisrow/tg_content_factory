from __future__ import annotations

import sqlite3

import aiosqlite


async def begin_immediate(db: aiosqlite.Connection) -> None:
    """Start a write transaction, recovering from stale nested transactions."""
    if db.in_transaction:
        await db.rollback()
    try:
        await db.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError as exc:
        if "cannot start a transaction within a transaction" not in str(exc):
            raise
        await db.rollback()
        await db.execute("BEGIN IMMEDIATE")
