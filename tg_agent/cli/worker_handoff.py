"""Find the project's live daemon: the owner of every account connection.

While the worker daemon runs it holds the Telegram connections. A CLI process
then never opens its own: its Telegram requests run on the daemon's connections
through the daemon's relay (#1868, see ``tg_agent.telegram.relay``), and
queue-shaped work (collection, the hourly ping sweep) is handed to the daemon's
queues. This module answers whether such a daemon exists and where its relay is.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from tg_agent.cli import process_control
from tg_agent.cli.process_control import (
    is_expected_server_process,
    pid_file_path,
    read_pid,
    read_process_owner,
)
from tg_agent.config import AppConfig

# A daemon publishes its relay once its pool is up. One that has been running
# longer than this without a relay is an older version.
RELAY_STARTUP_GRACE_SEC = 180.0
_RELAY_POLL_SEC = 0.25


def running_daemon_pid(config: AppConfig) -> int | None:
    """PID recorded in the daemon PID file, for error messages only.

    Best effort and never raises: liveness is :func:`serve_is_running`'s job.
    """
    try:
        return read_pid(pid_file_path(config))
    except Exception:
        return None


def serve_is_running(config: AppConfig) -> bool:
    """Return True when a managed daemon process owns the sessions AND runs a worker.

    Reads the PID file and verifies the process is alive and really is a
    managed ``tg_agent.main`` daemon (``serve``, ``worker`` or ``restart`` — see
    ``process_control._DAEMON_COMMANDS``), reusing the checks that back
    ``stop``/``restart``. A stale or absent PID file means no daemon, so the
    caller runs directly.

    ``serve --no-worker`` runs no worker and owns no connection, so it counts
    as "no daemon".

    Never raises: a malformed PID file must not break an otherwise valid command,
    and treating it as "no daemon" only falls back to the direct path.
    """
    try:
        pid = read_pid(pid_file_path(config))
        if pid is None or not is_expected_server_process(pid):
            return False
        return "--no-worker" not in process_control._process_command(pid).split()
    except Exception:
        return False


def daemon_is_legacy_panel(config: AppConfig) -> bool:
    """True when the live daemon is the legacy ``serve`` panel.

    Its embedded worker never serves the CLI relay, so waiting for one is
    pointless: the caller fails at once and asks for the worker daemon. Never
    raises.
    """
    try:
        pid = read_pid(pid_file_path(config))
        return pid is not None and "serve" in process_control._process_command(pid).split()
    except Exception:
        return False


def _daemon_age_sec(config: AppConfig) -> float | None:
    try:
        owner = read_process_owner(pid_file_path(config))
        started = datetime.fromisoformat(str(owner["started_at"])) if owner else None
    except Exception:
        return None
    if started is None:
        return None
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - started).total_seconds()


async def wait_for_daemon_relay(config: AppConfig):
    """The live daemon's relay endpoint, waiting while the daemon is still starting.

    Returns ``None`` when the daemon has no relay (an older version) or exits.
    """
    from tg_agent.telegram.relay import read_endpoint, relay_endpoint_path

    path = relay_endpoint_path(config.database.path)
    while True:
        pid = running_daemon_pid(config)
        endpoint = read_endpoint(path)
        if endpoint is not None and endpoint.pid == pid:
            return endpoint
        age = _daemon_age_sec(config)
        if pid is None or age is None or age > RELAY_STARTUP_GRACE_SEC or not serve_is_running(config):
            return None
        await asyncio.sleep(_RELAY_POLL_SEC)
