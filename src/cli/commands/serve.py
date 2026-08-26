from __future__ import annotations

import argparse
import logging
import sys

import uvicorn

from src.cli.process_control import (
    pid_file_path,
    register_current_process,
    unregister_current_process,
)
from src.config import load_config
from src.web.app import create_app

# Loopback addresses that keep the web panel unreachable from the local
# network by default. Anything else (0.0.0.0, a LAN IP, ...) is an
# intentional "expose to the network" choice and demands a real password.
_LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}
_WEAK_PASSWORDS = {"changeme", "admin", "password"}
# Denylist alone only caught the 3 literal defaults above — a review (#1305
# round 2) found that once the panel is actually network-reachable (e.g. the
# Docker deployment), anything else ("123456", "x", ...) sailed through as
# "strong". A minimum length keeps the fail-fast guarantee honest without
# turning this into a full entropy/strength policy, which stays out of scope.
_MIN_PASSWORD_LENGTH = 8


def serve_web(config_path: str, *, web_pass: str | None = None, no_worker: bool = False) -> None:
    """Start the web server (and, by default, the embedded Telegram worker).

    Shared body for both CLI entry points — the argparse ``run`` wrapper below
    and the Typer ``serve`` command (``src/cli/typer_commands.py``). ``uvicorn``
    owns the event loop, so this stays a plain ``def`` (no async-bridge). Exits
    via ``sys.exit(1)`` when no web password is configured, the password is a
    known-weak default while bound to a non-loopback host (#1303), or another
    managed server is already running, matching the pre-migration behaviour.
    """
    config = load_config(config_path)
    if web_pass:
        config.web.password = web_pass
    if not config.web.password:
        logging.error("WEB_PASS must be set for web panel authentication")
        sys.exit(1)

    is_loopback = config.web.host in _LOOPBACK_HOSTS
    is_weak_password = (
        config.web.password.lower() in _WEAK_PASSWORDS or len(config.web.password) < _MIN_PASSWORD_LENGTH
    )
    if not is_loopback and is_weak_password:
        logging.error(
            "Refusing to bind web panel to %s with a weak WEB_PASS (a known-weak "
            "default, or shorter than %d characters). Set a strong WEB_PASS, or "
            "use --web-pass, before exposing the panel beyond localhost.",
            config.web.host,
            _MIN_PASSWORD_LENGTH,
        )
        sys.exit(1)
    if not is_loopback:
        logging.warning(
            "Web panel is binding to %s, not localhost — it will be reachable "
            "from the local network. Make sure WEB_PASS is strong.",
            config.web.host,
        )

    app = create_app(config)
    # The lifespan hook in src/web/app.py spawns an embedded Telegram worker
    # unless this flag is set. By default `serve` owns the worker too;
    # `--no-worker` is for split deployments where `python -m src.main worker`
    # runs in its own process / container.
    app.state.embed_worker = not no_worker
    pid_path = pid_file_path(config)
    try:
        register_current_process(pid_path)
    except RuntimeError as exc:
        logging.error(str(exc))
        sys.exit(1)

    try:
        uvicorn.run(app, host=config.web.host, port=config.web.port, timeout_graceful_shutdown=150)
    except KeyboardInterrupt:
        pass
    finally:
        unregister_current_process(pid_path)


def run(args: argparse.Namespace) -> None:
    serve_web(
        args.config,
        web_pass=getattr(args, "web_pass", None),
        no_worker=getattr(args, "no_worker", False),
    )
