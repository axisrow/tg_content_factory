"""Enroll project directories without redirecting their content databases."""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import sqlite3
from contextlib import closing
from pathlib import Path
from uuid import uuid4

import yaml

from tg_agent.cli.process_control import is_expected_server_process, pid_file_path, read_pid
from tg_agent.config import global_marker_path, global_store_root, load_config
from tg_agent.database import Database
from tg_agent.gateway.catalog import GatewayAdministrator, ProjectAccountCatalog
from tg_agent.gateway.client import TelegramGatewayClient
from tg_agent.gateway.project import ProjectBinding, read_binding, write_private_json
from tg_agent.gateway.runtime import configure, endpoint
from tg_agent.security import SessionCipher
from tg_agent.telegram.session_lock import SessionLock, SessionLockError, _lock_dir


def backup_database(source: Path, target: Path) -> None:
    if target.exists():
        with closing(sqlite3.connect(target.as_uri() + "?mode=ro", uri=True)) as existing:
            if existing.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise ValueError("Existing migration snapshot failed SQLite integrity verification")
        return
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = target.with_name(target.name + f".{uuid4()}.partial")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(descriptor)
    try:
        with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)) as original:
            with closing(sqlite3.connect(temporary)) as destination:
                original.backup(destination)
                if destination.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise ValueError("Migration snapshot failed SQLite integrity verification")
                # Publish a self-contained database, never a hard link to an open
                # WAL database whose sidecars still use the temporary filename.
                destination.execute("PRAGMA journal_mode=DELETE")
        try:
            os.link(temporary, target)
        except FileExistsError:
            pass
    finally:
        temporary.unlink(missing_ok=True)


def check_session_owner(session: str, root: Path):
    """Probe the same OS lock used by native clients, without connecting."""
    lock = SessionLock("gateway-import", f"string:{session}", lock_dir=_lock_dir())
    try:
        lock.acquire(grace_seconds=0)
    except SessionLockError:
        record = endpoint(root)
        if record is None or lock.holder_pid() != record.get("pid"):
            raise ValueError(f"Session is owned by active process PID {lock.holder_pid()}; "
                             "stop its owning project before migration") from None
    else:
        lock.release()


def write_project_database_config(path: str, database_path: str):
    """Persist the independent path so detachment cannot reopen another DB."""
    config_path = Path(path)
    contents = yaml.safe_load(config_path.read_text(encoding="utf-8")) if config_path.exists() else {}
    contents = contents or {}
    contents.setdefault("database", {})["path"] = database_path
    temporary = config_path.with_name(config_path.name + f".{uuid4()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            yaml.safe_dump(contents, handle, allow_unicode=True, sort_keys=False)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(config_path)
    finally:
        temporary.unlink(missing_ok=True)


async def disable_project(config_path: str):
    binding = read_binding(global_marker_path())
    if binding is None:
        raise ValueError("Project is not connected to a version 2 gateway")
    config = load_config(config_path)
    pid = read_pid(pid_file_path(config))
    if pid is not None and is_expected_server_process(pid):
        raise ValueError(f"Stop this project's worker (PID {pid}) before detaching it")
    db = Database(config.database.path, session_encryption_secret=config.security.session_encryption_key or None)
    await db.initialize()
    client = None
    try:
        client = await TelegramGatewayClient.from_config(config)
        for account in await client.accounts():
            await client.detach(account["id"])
        await ProjectAccountCatalog(db, client).refresh()
        if binding.database_path:
            await asyncio.to_thread(write_project_database_config, config_path, binding.database_path)
        await asyncio.to_thread(global_marker_path().unlink)
    finally:
        if client is not None:
            await client.close()
        await db.close()


async def enable_project(config_path: str, phones: list[str], *, database_path: Path | None = None,
                         migration: bool = False, store_root: Path | None = None) -> ProjectBinding:
    marker = global_marker_path()
    existing = read_binding(marker)
    if existing is None and await asyncio.to_thread(marker.exists) and not migration:
        raise ValueError("Legacy global store requires `global migrate --project-db PATH` first")
    config = load_config(config_path, apply_marker=False)
    target = database_path or (Path(existing.database_path) if existing and existing.database_path else None)
    db_path = await asyncio.to_thread(lambda: (target or Path(config.database.path)).expanduser().absolute())
    pid = read_pid(pid_file_path(config))
    if existing is None and pid is not None and is_expected_server_process(pid):
        raise ValueError(f"Stop this project's worker (PID {pid}) before changing its account store")
    root = Path(existing.store_root) if existing is not None else (store_root or global_store_root())
    if root == Path.cwd():
        raise ValueError("Project directory must differ from the gateway store")
    credentials = Path(existing.token_file) if existing else Path.cwd() / "data" / "gateway-project.json"
    if await asyncio.to_thread(credentials.exists):
        identity = json.loads(await asyncio.to_thread(credentials.read_text, encoding="utf-8"))
    else:
        if existing is not None:
            raise ValueError("Project connection token is missing; restore its private credential file")
        identity = {"project_id": str(uuid4()), "token": secrets.token_urlsafe(48)}
        await asyncio.to_thread(write_private_json, credentials, identity)
    binding = existing or ProjectBinding(project_id=identity["project_id"], store_root=str(root),
                                         token_file=str(credentials),
                                         database_path=str(db_path) if database_path is not None else None)
    db = Database(str(db_path), session_encryption_secret=config.security.session_encryption_key or None)
    await db.initialize()
    administrator = None
    client = None
    try:
        source = {account.phone: account for account in await db.repos.accounts.export_accounts_raw()}
        backup = credentials.parent / f"before-gateway-{identity['project_id']}.db"
        await asyncio.to_thread(backup_database, db_path, backup)
        api_id = config.telegram.api_id or int(await db.get_setting("tg_api_id") or 0)
        api_hash = config.telegram.api_hash or await db.get_setting("tg_api_hash") or ""
        await asyncio.to_thread(configure, root, api_id=api_id, api_hash=api_hash,
                                proxy=config.telegram_runtime.proxy)
        administrator = await GatewayAdministrator.connect(root)
        await administrator.call("projects", {**identity, "root": str(Path.cwd())})
        for index, phone in enumerate(dict.fromkeys(phones)):
            if phone in source and source[phone].session_string:
                raw = source[phone].session_string
                secret = config.security.session_encryption_key or None
                if raw.startswith("enc:") and not secret:
                    raise ValueError("Original SESSION_ENCRYPTION_KEY is required to import this account")
                plaintext = SessionCipher(secret).decrypt(raw) if raw.startswith("enc:") else raw
                await asyncio.to_thread(check_session_owner, plaintext, root)
                await administrator.call("accounts/import", {"phone": phone,
                    "session": source[phone].session_string,
                    "source_secret": config.security.session_encryption_key or None})
            await administrator.call("grants", {"project_id": identity["project_id"], "phone": phone,
                                                 "primary": index == 0 and existing is None})
        async with db.db.execute("SELECT tg_user_id,tg_username,bot_id,bot_username,bot_token "
                                 "FROM notification_bots WHERE bot_token!=''") as cursor:
            bots = await cursor.fetchall()
        for bot in bots:
            await administrator.call("notification-bots/import", {"project_id": identity["project_id"], **dict(bot)})
        await db.execute_write("UPDATE notification_bots SET bot_token='' WHERE bot_token!=''")
        temporary_config = config.model_copy(update={"gateway": {
            "root": binding.store_root, "token_file": binding.token_file,
            "project_id": str(binding.project_id),
        }})
        client = await TelegramGatewayClient.from_config(temporary_config)
        await ProjectAccountCatalog(db, client).refresh()
        await db.execute_write("DELETE FROM settings WHERE key IN ('tg_api_id','tg_api_hash') "
                               "OR key LIKE 'auth_pending:%'")
        await asyncio.to_thread(write_private_json, marker, binding.model_dump(mode="json"))
        return binding
    finally:
        if client is not None:
            await client.close()
        if administrator is not None:
            await administrator.close()
        await db.close()


def _migration_copy(snapshot: Path, destination: Path, identity: str):
    if destination.exists():
        with closing(sqlite3.connect(destination.as_uri() + "?mode=ro", uri=True)) as db:
            try:
                matches = db.execute("SELECT id FROM gateway_migration_identity").fetchall() == [(identity,)]
            except sqlite3.DatabaseError:
                matches = False
        if not matches:
            raise ValueError("Migration destination belongs to another file; refusing to overwrite")
        return
    temporary = destination.with_name(destination.name + f".{identity}.tmp")
    temporary.unlink(missing_ok=True)
    destination.parent.mkdir(parents=True, exist_ok=True)
    backup_database(snapshot, temporary)
    with closing(sqlite3.connect(temporary)) as db:
        db.execute("CREATE TABLE gateway_migration_identity(id TEXT PRIMARY KEY)")
        db.execute("INSERT INTO gateway_migration_identity VALUES (?)", (identity,))
        db.commit()
    try:
        os.link(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


async def migrate_project(config_path: str, destination: Path, phones: list[str] | None = None) -> ProjectBinding:
    from tg_agent.config import GLOBAL_DB_NAME, read_global_store_marker

    destination = await asyncio.to_thread(destination.absolute)
    marker = global_marker_path()
    binding = read_binding(marker)
    if binding is not None:
        if binding.database_path != str(destination):
            raise ValueError("Project already migrated to a different database")
        await asyncio.to_thread(write_project_database_config, config_path, str(destination))
        return binding
    root = read_global_store_marker()
    if root is None:
        raise ValueError("No legacy global marker to migrate")
    source = root / GLOBAL_DB_NAME
    config = load_config(config_path)
    pid = read_pid(pid_file_path(config))
    if pid is not None and is_expected_server_process(pid):
        raise ValueError(f"Legacy session owner PID {pid} is active; stop it from its owning project first")
    journal_path = Path.cwd() / "data" / "gateway-migration.json"
    if await asyncio.to_thread(journal_path.exists):
        journal = json.loads(await asyncio.to_thread(journal_path.read_text, encoding="utf-8"))
        if journal["source"] != str(source) or journal["destination"] != str(destination):
            raise ValueError("An unfinished migration already targets a different database")
    else:
        if await asyncio.to_thread(destination.exists):
            raise ValueError("Migration destination already exists; refusing to overwrite")
        identity = str(uuid4())
        journal = {"id": identity, "source": str(source), "destination": str(destination), "phase": "prepared",
                   "backup": str(journal_path.parent / f"gateway-source-{identity}.db")}
        await asyncio.to_thread(write_private_json, journal_path, journal)
    snapshot = Path(journal["backup"])
    await asyncio.to_thread(backup_database, source, snapshot)
    await asyncio.to_thread(_migration_copy, snapshot, destination, journal["id"])
    db = Database(str(destination), session_encryption_secret=config.security.session_encryption_key or None)
    await db.initialize()
    try:
        selected = phones if phones is not None else [row.phone for row in await db.get_account_summaries()]
    finally:
        await db.close()
    if "phones" in journal:
        if phones is not None and list(dict.fromkeys(phones)) != journal["phones"]:
            raise ValueError("Resume this migration with the originally selected accounts")
        selected = journal["phones"]
    else:
        journal["phones"] = list(dict.fromkeys(selected))
        journal["phase"] = "copied"
        await asyncio.to_thread(write_private_json, journal_path, journal)
    await asyncio.to_thread(write_project_database_config, config_path, str(destination))
    binding = await enable_project(config_path, selected, database_path=destination, migration=True, store_root=root)
    journal["phase"] = "committed"
    await asyncio.to_thread(write_private_json, journal_path, journal)
    return binding
