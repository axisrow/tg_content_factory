"""CLI startup must stay light: heavy runtime deps are lazy imports.

The Typer command hub used to import the entire agent-tool registry
(claude_agent_sdk + mcp via ``commands/account.py``), the worker runtime
bootstrap (``commands/worker.py``), uvicorn (``commands/serve.py``) and
jsonschema (``commands/plugin.py``) on EVERY subcommand — ~15s of import
time per CLI invocation and per subprocess test that spawns one. All four
are lazy now; this guard keeps them that way.

Pure startup check: one subprocess, no DB, no network — ``integration`` via
the real-subprocess heuristic.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

# Top-level packages that must NOT load during CLI startup: the seconds-scale
# stacks (claude_agent_sdk+mcp+jsonschema were ~11s of the former ~15s startup).
# Known acceptable remainders: uvicorn/fastapi/telethon corners (~0.3s via the
# gateway protocol models) and aiohttp — milliseconds-scale, not guarded.
FORBIDDEN = ("claude_agent_sdk", "mcp", "mcp_types", "jsonschema")


def test_cli_startup_does_not_import_heavy_runtime() -> None:
    repo = Path(__file__).resolve().parents[1]
    code = (
        "import sys; import tg_agent.cli.typer_commands; bad = "
        + repr(FORBIDDEN)
        + "; print('\\n'.join(m for m in sys.modules if m.split('.')[0] in bad))"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
        # cwd pins the import to THIS checkout: a stale editable install of
        # tg-agent elsewhere must not answer for it.
        cwd=repo,
    )
    loaded = proc.stdout.split()
    assert loaded == [], f"CLI startup eagerly imported heavy modules: {loaded}"
