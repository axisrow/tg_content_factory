"""Detect a running server so CLI commands hand work over instead of duplicating it.

`serve` (web plus the embedded worker) owns every Telegram session: each account's
``data/telegram_sessions/<phone>.session`` file stays open for the process
lifetime. ``SessionMaterializer`` derives that path from the phone number alone,
so a CLI process building its own ``ClientPool`` opens the very same files and
dies with ``sqlite3.OperationalError: database is locked`` inside telethon's
``process_entities``.

The queue that avoids this already exists — ``CollectionService`` writes a PENDING
row when it holds no in-process queue, and the worker's ``_db_pull_loop`` ingests
such rows every few seconds. This module only answers the question that decides
which path a command takes.
"""

from __future__ import annotations

import os

from src.cli import process_control
from src.cli.process_control import is_expected_server_process, pid_file_path, read_pid
from src.config import AppConfig

DIRECT_ENV_VAR = "TGCF_CLI_DIRECT"


def direct_requested() -> bool:
    """True when the operator asked to bypass the hand-off for this run.

    ``dialogs`` spans 27 leaf commands sharing one bridge, so the escape hatch is
    an environment variable rather than 27 duplicated ``--direct`` flags:
    ``TGCF_CLI_DIRECT=1 python -m src.main dialogs send ...``. Commands with a
    signature of their own (``channel collect``) expose a real ``--direct`` flag.
    """
    return os.environ.get(DIRECT_ENV_VAR, "").strip().lower() in {"1", "true", "yes"}


def serve_is_running(config: AppConfig) -> bool:
    """Return True when a managed ``serve`` process owns the sessions AND runs a worker.

    Reads the PID file and verifies the process is alive and really is a
    ``serve`` invocation, reusing the checks that back ``stop``/``restart``. A
    stale or absent PID file means no server, so the caller runs directly.

    ``serve --no-worker`` (the documented split-deployment mode — CLAUDE.md:
    "For split deployments (Docker/k8s) pass --no-worker and run `worker`
    separately") does not embed a worker, so it is not a valid hand-off
    target: queuing there would enqueue commands nothing ever picks up if the
    separate ``worker`` process hasn't been started (or isn't running at all).
    Fail safe and treat that the same as "no server" — the caller falls back
    to the direct path instead of a silent black hole. This module does not
    yet detect a standalone ``worker`` process as a valid hand-off target
    either (it doesn't register a PID file); that split-deployment case is
    tracked separately.

    Never raises: a malformed PID file must not break an otherwise valid command,
    and treating it as "no server" only falls back to today's behaviour.
    """
    try:
        pid = read_pid(pid_file_path(config))
        if pid is None or not is_expected_server_process(pid):
            return False
        return "--no-worker" not in process_control._process_command(pid).split()
    except Exception:
        return False
