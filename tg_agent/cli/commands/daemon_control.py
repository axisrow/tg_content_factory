"""Read-only managed-daemon status (#1714): agents check provenance before touching.

``tg-agent daemon status`` reports whether this project's daemon is running,
on which PID file, and who owns it (project root, store root, executable —
recorded and observed). It never signals anything: down is a valid answer,
so the command always exits 0 unless a PID file is unreadably corrupt.
"""

from __future__ import annotations

import typer

from tg_agent.cli.commands.common import apply_startup, cli_error
from tg_agent.cli.process_control import (
    ProcessControlError,
    pid_file_candidates,
    pid_file_path,
)
from tg_agent.config import load_config


def status_impl(config_path: str) -> None:
    # Module-attribute access: test fakes patch process_control members in place.
    from tg_agent.cli import process_control as control

    current = pid_file_path(load_config(config_path))
    live = False
    for candidate in pid_file_candidates(config_path):
        label = "current store" if candidate == current else "previous store"
        pid = control.read_pid(candidate)
        if pid is None:
            print(f"{candidate} ({label}): no PID file")
            continue
        if not control.is_process_alive(pid):
            print(f"{candidate} ({label}): PID {pid} is not running (stale PID file)")
            continue
        if not control.is_expected_server_process(pid):
            print(f"{candidate} ({label}): PID {pid} is not a managed tg-agent daemon")
            continue
        owner = control.read_process_owner(candidate)
        if owner is None:
            print(f"{candidate} ({label}): PID {pid} is a live managed daemon "
                  f"(no provenance record)")
            live = True
            continue
        live = True
        print(f"daemon: up — PID {pid} (PID file: {candidate}, {label})")
        print(f"  project_root: {owner.get('project_root')}")
        print(f"  store_root: {owner.get('store_root')}")
        print(f"  executable (recorded): {owner.get('executable')}")
        print(f"  executable (observed): {control.process_executable(pid)}")
        print(f"  started_at: {owner.get('started_at')}")
    if not live:
        print("daemon: down — no live managed process on any known PID file")


daemon_app = typer.Typer(no_args_is_help=True, help="Managed daemon status (read-only)")


@daemon_app.command("status")
def daemon_status(ctx: typer.Context) -> None:
    """Report whether this project's daemon is running, with provenance (read-only)."""
    apply_startup(ctx)
    try:
        status_impl(ctx.obj.config)
    except ProcessControlError as exc:
        cli_error(str(exc))
