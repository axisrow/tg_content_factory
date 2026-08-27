"""Native Google ADK entrypoint for the development UI and eval runner.

Google ADK's CLI accepts a *parent* directory and discovers agent packages
under it. This package therefore lives below ``adk/`` rather than directly in
the directory passed to ``adk web``.

Usage from the repository root::

    pip install -e ".[adk]"
    TG_CONFIG_PATH=/path/to/config.yaml adk web adk

The native UI runs without an in-process Telegram pool. Pool-dependent tools
return their normal unavailable response, while database-backed tools remain
available through the project's MCP server.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from src.agent.adk_backend import build_adk_agent
from src.agent.prompt_template import (
    AGENT_PROMPT_TEMPLATE_SETTING,
    DEFAULT_AGENT_PROMPT_TEMPLATE,
    build_prompt_template_context,
    render_prompt_template,
    validate_prompt_template,
)
from src.cli.dotenv import load_cli_dotenv
from src.config import AppConfig, load_config


def _configured_prompt(config: AppConfig) -> str:
    """Read and render the saved agent prompt, falling back safely if unavailable.

    The native ADK entrypoint is imported synchronously by ``adk web`` and
    ``adk eval``. A read-only SQLite connection keeps this adapter independent
    of the application's async database lifecycle and avoids creating a new DB
    when the configured path does not exist. Session-specific context variables
    are empty here; normal production requests render them per thread.
    """
    database_path = config.database.path
    if database_path == ":memory:":
        return DEFAULT_AGENT_PROMPT_TEMPLATE
    path = Path(database_path)
    if not path.exists():
        return DEFAULT_AGENT_PROMPT_TEMPLATE

    try:
        uri = f"file:{path.resolve()}?mode=ro"
        with sqlite3.connect(uri, uri=True) as connection:
            row = connection.execute(
                "SELECT value FROM settings WHERE key = ?",
                (AGENT_PROMPT_TEMPLATE_SETTING,),
            ).fetchone()
    except sqlite3.Error:
        return DEFAULT_AGENT_PROMPT_TEMPLATE

    template = str(row[0]).strip() if row and row[0] else ""
    if not template:
        return DEFAULT_AGENT_PROMPT_TEMPLATE
    try:
        validate_prompt_template(template)
        return render_prompt_template(template, build_prompt_template_context([]))
    except (ValueError, KeyError):
        return DEFAULT_AGENT_PROMPT_TEMPLATE


_CONFIG_PATH = os.path.abspath(os.environ.get("TG_CONFIG_PATH", "").strip() or "config.yaml")
# ``load_config`` expands ${ENV_VAR} values while reading YAML. Match the
# regular CLI startup order so an .env beside TG_CONFIG_PATH is available for
# those substitutions before the config is parsed.
load_cli_dotenv(_CONFIG_PATH)
_CONFIG = load_config(_CONFIG_PATH)

# ADK's CLI requires a module-level ``root_agent``. Imports in
# ``build_adk_agent`` stay lazy so the rest of the application does not gain a
# hard dependency on the optional [adk] extra.
root_agent = build_adk_agent(
    _CONFIG,
    config_path=_CONFIG_PATH,
    system_prompt=_configured_prompt(_CONFIG),
)

__all__ = ["root_agent"]
