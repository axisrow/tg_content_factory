"""Explicit process control for the machine-wide Telegram gateway."""

import json
from pathlib import Path

import typer

from tg_agent.cli.commands.common import apply_startup, cli_error, run_async
from tg_agent.config import global_marker_path, global_store_root

# gateway.catalog/gateway.runtime are imported lazily in their call sites:
# runtime imports the gateway server stack (uvicorn/fastapi/telethon) and must
# not load for every other CLI subcommand's startup.

gateway_app = typer.Typer(no_args_is_help=True, help="Manage the shared Telegram gateway")
accounts_app = typer.Typer(no_args_is_help=True, help="Shared account administration")
gateway_app.add_typer(accounts_app, name="account")


def current_store_root():
    from tg_agent.gateway.project import read_binding

    binding = read_binding(global_marker_path())
    return Path(binding.store_root) if binding else global_store_root()


def require_shared(shared):
    if not shared:
        cli_error("This operation affects every attached project and requires --shared")


async def administer(op, payload=None, **fields):
    """One administrator operation on the shared gateway's relay."""
    from tg_agent.gateway.catalog import GatewayAdministrator

    client = await GatewayAdministrator.connect(current_store_root())
    try:
        return await client.call(op, payload, **fields)
    finally:
        await client.close()


@accounts_app.command("list")
def accounts_list(ctx: typer.Context):
    apply_startup(ctx)
    print(json.dumps({"accounts": run_async(administer("accounts.list"))}))


@accounts_app.command("disable")
def accounts_disable(ctx: typer.Context, phone: str, shared: bool = typer.Option(False, "--shared")):
    require_shared(shared)
    apply_startup(ctx)
    run_async(administer("accounts.update", {"phone": phone, "active": False}))
    print("Shared account disabled")


@accounts_app.command("enable")
def accounts_enable(ctx: typer.Context, phone: str, shared: bool = typer.Option(False, "--shared")):
    require_shared(shared)
    apply_startup(ctx)
    run_async(administer("accounts.update", {"phone": phone, "active": True}))
    print("Shared account enabled")


@accounts_app.command("delete")
def accounts_delete(ctx: typer.Context, phone: str, shared: bool = typer.Option(False, "--shared")):
    require_shared(shared)
    apply_startup(ctx)
    run_async(administer("accounts.update", {"phone": phone, "delete": True}))
    print("Shared account session removed; project archives retained")


@accounts_app.command("import")
def accounts_import(ctx: typer.Context, phone: str, session_file: Path = typer.Option(..., "--session-file"),
                    shared: bool = typer.Option(False, "--shared")):
    require_shared(shared)
    apply_startup(ctx)
    session = session_file.read_text().strip()
    run_async(administer("accounts.import", {"phone": phone, "session": session}))
    print("Shared account imported")


@accounts_app.command("export-session")
def accounts_export(ctx: typer.Context, phone: str, shared: bool = typer.Option(False, "--shared")):
    require_shared(shared)
    apply_startup(ctx)
    print(run_async(administer("accounts.export", phone=phone)))


@accounts_app.command("replace-session")
def accounts_replace(ctx: typer.Context, phone: str, session_file: Path = typer.Option(..., "--session-file"),
                     shared: bool = typer.Option(False, "--shared")):
    require_shared(shared)
    apply_startup(ctx)
    run_async(administer("accounts.import", {"phone": phone, "session": session_file.read_text().strip(),
                                             "replace": True}))
    print("Shared account session replaced")


@gateway_app.command("start")
def gateway_start(ctx: typer.Context):
    apply_startup(ctx)
    from tg_agent.gateway.runtime import ensure_gateway

    record = ensure_gateway(current_store_root())
    print(json.dumps({"state": "up", "port": record["port"], "pid": record["pid"]}))


@gateway_app.command("reply-owner")
def gateway_reply_owner(ctx: typer.Context, phone: str, dialog_id: int,
                        release: bool = typer.Option(False, "--release")):
    """Assign this project as the sole automatic reply/ping owner of a dialog."""
    from tg_agent.cli import runtime
    from tg_agent.gateway.client import TelegramGatewayClient

    apply_startup(ctx)

    async def assign():
        config, db = await runtime.init_db(ctx.obj.config)
        client = None
        try:
            if not config.gateway:
                cli_error("Project is not connected to a shared gateway")
            client = await TelegramGatewayClient.from_config(config)
            account = next((row for row in await client.accounts() if row["phone"] == phone), None)
            if account is None:
                cli_error("Account is not attached to this project")
            await client.reply_owner(account["id"], dialog_id, release=release)
        finally:
            if client is not None:
                await client.close()
            await db.close()
    run_async(assign())
    print("Automatic reply ownership released" if release else "This project owns automatic replies in this dialog")


@gateway_app.command("status")
def gateway_status(ctx: typer.Context):
    apply_startup(ctx)
    from tg_agent.gateway.runtime import endpoint

    status = endpoint(current_store_root())
    print(json.dumps(status or {"state": "down"}))


@gateway_app.command("stop")
def gateway_stop(ctx: typer.Context, shared: bool = typer.Option(False, "--shared")):
    apply_startup(ctx)
    if not shared:
        cli_error("Shared gateway stop requires --shared; `stop` controls only this project's worker")
    from tg_agent.gateway.runtime import stop_gateway

    stop_gateway(current_store_root(), shared=True)
    print("Shared gateway stopped")


@gateway_app.command("restart")
def gateway_restart(ctx: typer.Context, shared: bool = typer.Option(False, "--shared")):
    apply_startup(ctx)
    if not shared:
        cli_error("Shared gateway restart requires --shared; `restart` controls only this project's worker")
    from tg_agent.gateway.runtime import restart_gateway

    record = restart_gateway(current_store_root(), shared=True)
    print(json.dumps({"state": "up", "port": record["port"], "pid": record["pid"]}))
