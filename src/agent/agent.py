"""Native Google ADK entrypoint for the development UI and eval runner.

Google ADK discovers an agent directory by importing its ``agent`` module and
reading ``root_agent``.  The regular TG Agent runtime remains the owner of
production sessions; this module only adapts the same MCP-backed agent to
ADK's ``adk web`` and ``adk eval`` workflows.

Usage from the repository root::

    pip install -e ".[adk]"
    TG_CONFIG_PATH=/path/to/config.yaml adk web src/agent

The native UI runs without an in-process Telegram pool. Pool-dependent tools
return their normal unavailable response, while database-backed tools remain
available through the project's MCP server.
"""

from __future__ import annotations

import os

from src.agent.adk_backend import build_adk_agent
from src.agent.prompt_template import DEFAULT_AGENT_PROMPT_TEMPLATE
from src.config import load_config

_CONFIG_PATH = os.path.abspath(os.environ.get("TG_CONFIG_PATH", "").strip() or "config.yaml")

# ADK's CLI requires a module-level ``root_agent``. Imports in
# ``build_adk_agent`` stay lazy so the rest of the application does not gain a
# hard dependency on the optional [adk] extra.
root_agent = build_adk_agent(
    load_config(_CONFIG_PATH),
    config_path=_CONFIG_PATH,
    system_prompt=DEFAULT_AGENT_PROMPT_TEMPLATE,
)

__all__ = ["root_agent"]
