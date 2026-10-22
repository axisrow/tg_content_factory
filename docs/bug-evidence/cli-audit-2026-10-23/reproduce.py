"""Reproduce four visually confirmed CLI bugs using a temporary, offline database.

Run with the project's Python environment: python reproduce.py
All credentials below are fake. This script does not contact Telegram.
"""
from __future__ import annotations

import asyncio
import os
import shlex
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from tg_agent.database import Database
from tg_agent.models import Channel, Message

AUDIT_ROOT = Path(tempfile.mkdtemp(prefix="tg-cli-issues-"))
CONFIG_PATH = AUDIT_ROOT / "config.yaml"
CUTOFF = datetime.now(timezone.utc) - timedelta(days=1)
DAY_START = CUTOFF.replace(hour=0, minute=0, second=0, microsecond=0)


async def seed() -> None:
    database_path = AUDIT_ROOT / "audit.db"
    CONFIG_PATH.write_text(
        f"database:\n  path: {database_path}\n"
        "security:\n  session_encryption_key: local-audit-only-do-not-use\n",
        encoding="utf-8",
    )
    database = Database(str(database_path), session_encryption_secret="local-audit-only-do-not-use")
    await database.initialize()
    try:
        await database.add_channel(Channel(channel_id=1001, title="Alpha — Проверка", username="audit_alpha"))
        await database.add_channel(Channel(channel_id=1002, title="Beta — Проверка", username="audit_beta"))
        stats_dates = [DAY_START, DAY_START + (CUTOFF - DAY_START) / 2, CUTOFF + timedelta(hours=23)]
        messages = [
            Message(channel_id=1001, message_id=100 + index, text=f"stats fixture {index}", date=value)
            for index, value in enumerate(stats_dates)
        ]
        messages.extend([
            Message(channel_id=1002, message_id=201, text="audit beta morning", date=datetime(2026, 10, 20, 10, tzinfo=timezone.utc)),
            Message(channel_id=1002, message_id=203, text="abcde", date=datetime(2026, 10, 22, 9, tzinfo=timezone.utc)),
        ])
        await database.insert_messages_batch(messages)
        await database.set_setting("tg_api_hash", "0123456789abcdef0123456789abcdef")
        await database.set_setting("demo_api_key", "FAKE_AUDIT_KEY_NOT_A_REAL_CREDENTIAL")
    finally:
        await database.close()


def invoke(arguments: list[str]) -> None:
    command = [sys.executable, "-m", "tg_agent.main", "--config", str(CONFIG_PATH), *arguments]
    print("\n$ " + shlex.join(command), flush=True)
    environment = os.environ.copy()
    environment["TG_AGENT_DATA_DIR"] = str(AUDIT_ROOT / "runtime")
    environment["NO_COLOR"] = "1"
    result = subprocess.run(command, cwd=AUDIT_ROOT, env=environment, check=False)
    if result.returncode:
        raise SystemExit(result.returncode)


def main() -> None:
    asyncio.run(seed())
    print(f"Temporary fixture: {AUDIT_ROOT}", flush=True)
    invoke(["settings", "get", "--key", "demo_api_key"])
    invoke(["settings", "get", "--key", "tg_api_hash"])
    invoke(["settings", "server-time"])
    invoke(["messages", "read", "@audit_alpha", "--date-from", CUTOFF.isoformat()])
    invoke(["analytics", "channel", "1001", "--days", "1"])
    invoke(["search", "morning", "--date-from", "2026-10-20T10:00:00Z"])
    invoke(["search", "morning", "--date-from", "2026-10-20T13:00:00+03:00"])
    invoke(["search", "abcde"])
    invoke(["search", "abcde", "--min-length", "5"])
    invoke(["search", "abcde", "--max-length", "5"])


if __name__ == "__main__":
    main()
