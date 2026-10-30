"""`key` — the machine-level session-encryption key in the OS secret store (#1699).

Explicit management surface for the opt-in secret-store layer: the key is
stored/removed/reported only when the owner runs these commands — nothing is
saved silently. The key itself is never echoed back: stdin/getpass in, store
write, status prints only presence and which source currently wins.
"""

from __future__ import annotations

import getpass
import sys

import typer

from tg_agent.cli.commands.common import apply_startup, cli_error
from tg_agent.config import load_config
from tg_agent.security.secret_store import (
    SecretStoreError,
    backend_name,
    delete_key,
    last_backend_error,
    read_stored_key,
    store_key,
)

key_app = typer.Typer(
    no_args_is_help=True,
    help="Machine-level SESSION_ENCRYPTION_KEY in the OS secret store (#1699)",
)

_BACKEND_LABELS = {
    "keychain": "macOS Keychain",
    "credential-manager": "Windows Credential Manager",
    "secret-tool": "secret-service (secret-tool)",
    "file": "0600 file in the global store root",
    "off": "disabled (TG_AGENT_SECRET_STORE=off)",
}


def _read_key_argument() -> str:
    """Key from stdin when piped (automation), else a double interactive prompt."""
    if not sys.stdin.isatty():
        key = sys.stdin.read().strip()
        if not key:
            cli_error("Empty key on stdin.")
        return key
    key = getpass.getpass("Session encryption key: ")
    if not key.strip():
        cli_error("Empty key.")
    if key != getpass.getpass("Repeat: "):
        cli_error("Keys do not match; nothing stored.")
    return key


@key_app.command("set")
def key_set(ctx: typer.Context) -> None:
    """Store the key in the platform secret store (opt-in, explicit)."""
    apply_startup(ctx)
    key = _read_key_argument()
    try:
        store_key(key)
    except SecretStoreError as exc:
        cli_error(f"ERROR: failed to store the key: {exc}")
    print(f"Key stored in {_BACKEND_LABELS[backend_name()]}.")
    print("Restart any running worker/daemon in this project to pick it up.")


@key_app.command("clear")
def key_clear(ctx: typer.Context) -> None:
    """Remove the key from the platform secret store."""
    apply_startup(ctx)
    try:
        removed = delete_key()
    except SecretStoreError as exc:
        cli_error(f"ERROR: failed to clear the key: {exc}")
    if removed:
        print("Key removed from the secret store.")
        print("Restart any running worker/daemon in this project to pick up the change.")
    else:
        print("No key was stored.")


@key_app.command("status")
def key_status(ctx: typer.Context) -> None:
    """Report the backend, stored state, and which source currently wins."""
    apply_startup(ctx)
    config = load_config(ctx.obj.config)
    backend = backend_name()
    print(f"store backend : {_BACKEND_LABELS[backend]}")
    print(f"stored        : {'yes' if read_stored_key() is not None else 'no'}")
    if error := last_backend_error():
        print(f"backend error : {error}")
    if config.security.session_encryption_key:
        print("env/config    : set (escape hatch — wins over the store)")
        effective = "env/config"
    elif read_stored_key() is not None:
        print("env/config    : unset")
        effective = "secret store"
    else:
        print("env/config    : unset")
        effective = "none (warning + degraded pool, unchanged behavior)"
    print(f"effective     : {effective}")
