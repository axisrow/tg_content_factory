from __future__ import annotations

import argparse
import logging
import sys

import uvicorn

from tg_agent.cli.process_control import (
    ensure_no_sibling_daemon,
    pid_file_path,
    register_current_process,
    unregister_current_process,
)
from tg_agent.config import load_config
from tg_agent.security.password_policy import is_strong_password

# Loopback addresses that keep the web panel unreachable from the local
# network by default. Anything else (0.0.0.0, a LAN IP, ...) is an
# intentional "expose to the network" choice and demands a real password.
_LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}


def _load_web_surface():
    """Import the optional web panel package (#1720 wave 3) or exit with an install hint.

    Lazy so the rest of the CLI never imports FastAPI/Jinja2 app code; the
    core must not import ``tg_webui`` at module level (import-linter
    ``runtime_no_web_imports``). Only a missing ``tg_webui`` itself gets the
    hint — a broken import inside an installed package (say, one of its own
    dependencies missing) propagates with its real traceback.
    """
    try:
        from tg_webui.app import create_app
    except ModuleNotFoundError as exc:
        if exc.name != "tg_webui":
            raise
        logging.error(
            "The legacy web panel is the optional tg-webui package, which is not installed "
            "(from a repository checkout: pip install -e packages/tg_webui). "
            "The worker daemon (`restart`/`worker`) needs no web panel."
        )
        sys.exit(1)
    return create_app


def serve_web(config_path: str, *, web_pass: str | None = None, no_worker: bool = False) -> None:
    """Start the web server (and, by default, the embedded Telegram worker).

    Shared body for both CLI entry points — the argparse ``run`` wrapper below
    and the Typer ``serve`` command (``tg_agent/cli/typer_commands.py``). ``uvicorn``
    owns the event loop, so this stays a plain ``def`` (no async-bridge). Exits
    via ``sys.exit(1)`` when no web password is configured, the password has
    an obviously-weak password while bound to a non-loopback host (#1303), or
    another managed server is already running,
    matching the pre-migration behaviour.
    """
    create_app = _load_web_surface()
    config = load_config(config_path)
    if web_pass:
        config.web.password = web_pass
    if not config.web.password:
        logging.error("WEB_PASS must be set for web panel authentication")
        sys.exit(1)

    is_loopback = config.web.host in _LOOPBACK_HOSTS
    is_weak_password = not is_strong_password(config.web.password)
    if not is_loopback and is_weak_password:
        logging.error(
            "Refusing to bind web panel to %s with an obviously-weak WEB_PASS "
            "(a common password, predictable pattern, or low-entropy "
            "shape like a repeated character or sequential run). Set a real, "
            "unique WEB_PASS, or use --web-pass, before exposing the panel "
            "beyond localhost.",
            config.web.host,
        )
        sys.exit(1)
    if not is_loopback:
        logging.warning(
            "Web panel is binding to %s, not localhost — it will be reachable "
            "from the local network. Make sure WEB_PASS is strong.",
            config.web.host,
        )

    app = create_app(config)
    # The lifespan hook in tg_webui/app.py spawns an embedded Telegram worker
    # unless this flag is set. By default `serve` owns the worker too;
    # `--no-worker` is for split deployments where `python -m tg_agent.main worker`
    # runs in its own process / container.
    app.state.embed_worker = not no_worker
    pid_path = pid_file_path(config)
    try:
        # Refuse a double-spawn next to a daemon still registered on a
        # previous store path — it would end degraded (#1698).
        ensure_no_sibling_daemon(pid_path, config_path)
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
