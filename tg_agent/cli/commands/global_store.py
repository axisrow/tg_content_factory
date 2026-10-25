"""`global` CLI group: a shared account store across projects (#1683).

Accounts are bound to the launch directory: the SQLite database
(`database.path`) and the Telegram session cache (`session_cache_dir`) default
to paths under the project's `data/`, so a fresh checkout sees an empty account
list. The `global` commands attach a project to one machine-wide store instead:

* ``global enable`` writes a marker file in the project root; while it exists,
  ``load_config`` resolves the database and session cache into the store
  (default ``~/.tg-agent``, ``$TG_AGENT_GLOBAL_DIR`` overrides). The first
  enable of an empty store seeds it from the project's local accounts —
  session rows are copied as stored ``enc:v2:*`` ciphertext through the normal
  ``add_account`` path, so ``SESSION_ENCRYPTION_KEY`` stays required and no
  secret is ever printed.
* ``global disable`` removes the marker; local files are never deleted.
* ``global status`` reports the effective store without touching any database.

Local paths (logs, images, downloads) are unaffected — only accounts and
sessions travel with the global store.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import typer

from tg_agent.cli import runtime
from tg_agent.cli.commands.common import apply_startup, cli_error, run_async
from tg_agent.config import (
    GLOBAL_DB_NAME,
    GLOBAL_SESSIONS_SUBDIR,
    global_marker_path,
    global_store_root,
    load_config,
    read_global_store_marker,
    resolve_session_encryption_secret,
)
from tg_agent.database import Database


def _print_paths(label: str, root: Path) -> None:
    print(f"{label} database : {root / GLOBAL_DB_NAME}")
    print(f"{label} sessions : {root / GLOBAL_SESSIONS_SUBDIR}")


def _restart_hint() -> None:
    print("Restart any running worker/daemon in this project to apply the change.")


async def _seed_global_store(config, local_db: Database, root: Path) -> int | None:
    """One-time copy of local accounts into an EMPTY global store.

    Returns the number of seeded accounts, 0 when seeding is a no-op, or
    ``None`` when seeding failed (the caller aborts the enable without writing
    the marker). Rows go through the regular ``add_account`` path: an ``enc:v2:*``
    ciphertext is idempotent under ``encrypt()`` and is stored byte-for-byte,
    plaintext rows are upgraded to ``enc:v2`` in the new store, and the
    single-primary invariant holds there from the first insert. Without
    ``SESSION_ENCRYPTION_KEY`` the repository refuses every write — the error
    is reported and the store stays untouched.
    """
    raw_accounts = await local_db.repos.accounts.export_accounts_raw()
    global_db = Database(
        str(root / GLOBAL_DB_NAME),
        session_encryption_secret=resolve_session_encryption_secret(config),
    )
    try:
        await global_db.initialize()
        os.chmod(root / GLOBAL_DB_NAME, 0o600)  # SQLite defaults to 0644; the store holds sessions
        existing = await global_db.get_account_summaries(active_only=False)
        if existing:
            print(
                f"Global store already has {len(existing)} account(s) — "
                f"local accounts left untouched."
            )
            return 0
        if not raw_accounts:
            print("Local database has no accounts; global store starts empty.")
            return 0
        for account in raw_accounts:
            await global_db.repos.accounts.add_account(account)
        print(f"Seeded {len(raw_accounts)} account(s) from the local database into the global store.")
        return len(raw_accounts)
    except RuntimeError as exc:
        # require_session_encryption_key: refuse to half-enable a store whose
        # sessions could not be persisted as safely as the local ones.
        print(f"ERROR: {exc}", file=sys.stderr)
        return None
    finally:
        await global_db.close()


async def enable_impl(config_path: str) -> None:
    """Attach the current project (cwd) to the global account store."""
    root = global_store_root()
    if root == Path.cwd().resolve():
        cli_error("ERROR: the project directory IS the global store root; refusing to enable.")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    (root / GLOBAL_SESSIONS_SUBDIR).mkdir(parents=True, exist_ok=True, mode=0o700)

    config, db = await runtime.init_db(config_path)
    try:
        seeded = await _seed_global_store(config, db, root)
    finally:
        await db.close()
    if seeded is None:
        cli_error("ERROR: seeding failed; project not switched (no marker written).")

    global_marker_path().write_text(f"{root}\n", encoding="utf-8")
    print(f"Global store enabled for this project ({global_marker_path()}).")
    _print_paths("global", root)
    _restart_hint()


def disable_impl(config_path: str) -> None:
    """Detach the current project from the global store (marker removal only)."""
    marker = global_marker_path()
    if not marker.exists():
        print("Global store is not enabled for this project; nothing to do.")
        return
    marker.unlink()
    config = load_config(config_path)
    print("Global store disabled for this project; back to the local data directory.")
    print(f"local database : {config.database.path}")
    print(f"local sessions : {config.telegram_runtime.session_cache_dir}")
    _restart_hint()


def status_impl(config_path: str) -> None:
    """Report which store this project resolves to, without opening any database."""
    root = read_global_store_marker()
    if root is not None:
        print(f"Global store: enabled (marker {global_marker_path()})")
        _print_paths("global", root)
        return
    config = load_config(config_path)
    print(f"Global store: disabled (no {global_marker_path().name} in the project directory)")
    print(f"local database : {config.database.path}")
    print(f"local sessions : {config.telegram_runtime.session_cache_dir}")


# --------------------------------------------------------------------------- #
# global → enable / disable / status  (shared account store, #1683)
# --------------------------------------------------------------------------- #

global_app = typer.Typer(no_args_is_help=True, help="Shared account store across projects")


@global_app.command("enable")
def global_enable(ctx: typer.Context) -> None:
    """Point this project at the global account store (~/.tg-agent)."""
    apply_startup(ctx)
    run_async(enable_impl(ctx.obj.config))


@global_app.command("disable")
def global_disable(ctx: typer.Context) -> None:
    """Return this project to its local data directory."""
    apply_startup(ctx)
    disable_impl(ctx.obj.config)


@global_app.command("status")
def global_status(ctx: typer.Context) -> None:
    """Show whether this project currently uses the global store."""
    apply_startup(ctx)
    status_impl(ctx.obj.config)
