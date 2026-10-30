"""Project enrollment, legacy migration, and shared gateway status."""

from __future__ import annotations

from pathlib import Path

import typer

from tg_agent.cli.commands.common import apply_startup, cli_error, run_async
from tg_agent.config import (
    global_marker_path,
    load_config,
    read_global_store_marker,
)


def _restart_hint() -> None:
    print("Restart any running worker/daemon in this project to apply the change.")


def _maybe_suggest_key(config) -> None:
    """Suggest the machine-level key store once; never store anything itself (#1699)."""
    if config.security.session_encryption_key:
        return
    from tg_agent.security.secret_store import backend_name, read_stored_key

    if backend_name() == "off" or read_stored_key() is not None:
        return
    print("Tip: store the session key machine-wide once instead of per-project .env copies: tg-agent key set")


async def enable_impl(config_path: str, phones: list[str] | None = None) -> None:
    """Enroll explicitly selected accounts, keeping content in the project DB."""
    from tg_agent.gateway.enrollment import enable_project

    if not phones:
        cli_error("Select shared accounts explicitly: global enable --account PHONE (repeatable).")
    binding = await enable_project(config_path, phones)
    config = load_config(config_path)
    print(f"Shared gateway enabled: {binding.store_root}")
    print(f"Project database: {config.database.path}")
    _maybe_suggest_key(config)


def disable_impl(config_path: str) -> None:
    """Detach the current project from the global store (marker removal only)."""
    marker = global_marker_path()
    if not marker.exists():
        print("Global store is not enabled for this project; nothing to do.")
        return
    from tg_agent.gateway.project import read_binding

    if read_binding(marker) is not None:
        from tg_agent.gateway.enrollment import disable_project

        run_async(disable_project(config_path))
        print("Shared gateway disabled for this project; project data and shared accounts retained.")
        print(f"Project database: {load_config(config_path).database.path}")
        return
    marker.unlink()
    config = load_config(config_path)
    print("Global store disabled for this project; back to the local data directory.")
    print(f"local database : {config.database.path}")
    print(f"local sessions : {config.telegram_runtime.session_cache_dir}")
    _restart_hint()


def status_impl(config_path: str) -> None:
    """Report which store this project resolves to, without opening any database."""
    from tg_agent.gateway.project import read_binding

    binding = read_binding(global_marker_path())
    if binding is not None:
        from tg_agent.gateway.client import TelegramGatewayClient

        print(f"Shared gateway: {binding.store_root}")
        print(f"Project: {binding.project_id}")
        print(f"Project database: {load_config(config_path).database.path}")
        status = run_async(TelegramGatewayClient.inspect_config(load_config(config_path)))
        print(f"HTTP: {status['http_available']}; dispatcher: {status['dispatcher_running']}")
        for account in status.get("accounts", []):
            print(f"  {account['phone']}: connected={account.get('connected', False)}, "
                  f"primary={bool(account.get('is_primary'))}, active={bool(account['active'])}")
        return
    root = read_global_store_marker()
    if root is not None:
        # Legacy plain-path marker (#1724): inert since the redirect removal —
        # it no longer points this project's database anywhere.
        print(f"Legacy marker detected ({global_marker_path().name}) — it no longer "
              f"redirects the database (owner decision #1724: global auth never "
              f"implies a global database).")
        config = load_config(config_path)
        print(f"project database : {config.database.path}")
        print(f"project sessions : {config.telegram_runtime.session_cache_dir}")
        print("Replace it with a binding to share accounts: global enable --account PHONE "
              f"(store root would be {root}).")
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
def global_enable(
    ctx: typer.Context,
    accounts: list[str] | None = typer.Option(None, "--account", help="Shared account to attach (repeatable)"),
) -> None:
    """Attach selected shared accounts without moving the project database."""
    apply_startup(ctx)
    run_async(enable_impl(ctx.obj.config, phones=accounts))


@global_app.command("disable")
def global_disable(ctx: typer.Context) -> None:
    """Return this project to its local data directory."""
    apply_startup(ctx)
    disable_impl(ctx.obj.config)


@global_app.command("migrate")
def global_migrate(
    ctx: typer.Context,
    project_db: Path = typer.Option(..., "--project-db", help="New independent project database"),
    accounts: list[str] | None = typer.Option(None, "--account", help="Accounts to retain (default: current accounts)"),
) -> None:
    """Migrate legacy shared content into an independent project database."""
    from tg_agent.gateway.enrollment import migrate_project

    apply_startup(ctx)
    run_async(migrate_project(ctx.obj.config, project_db, accounts))
    print(f"Project migrated: {project_db}")


@global_app.command("status")
def global_status(ctx: typer.Context) -> None:
    """Show whether this project currently uses the global store."""
    apply_startup(ctx)
    status_impl(ctx.obj.config)
