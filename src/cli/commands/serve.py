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


def serve_web(config_path: str, *, web_pass: str | None = None, no_worker: bool = False) -> None:
    """Start the web server (and, by default, the embedded Telegram worker).

    Shared body for both CLI entry points — the argparse ``run`` wrapper below
    and the Typer ``serve`` command (``src/cli/typer_commands.py``). ``uvicorn``
    owns the event loop, so this stays a plain ``def`` (no async-bridge). Exits
    via ``sys.exit(1)`` when no web password is configured or another managed
    server is already running, matching the pre-migration behaviour.
    """
    config = load_config(config_path)
    if web_pass:
        config.web.password = web_pass
    if not config.web.password:
        logging.error("WEB_PASS must be set for web panel authentication")
        sys.exit(1)

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
