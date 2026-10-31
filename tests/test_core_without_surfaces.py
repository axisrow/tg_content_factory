"""The core runs without the legacy UI surfaces (#1720).

The web panel (``tg_webui``) and the agent-chat TUI (``tg_tui``) are optional
monorepo packages, and their third-party dependencies (jinja2, python-multipart,
itsdangerous, qrcode, textual) are no longer core requirements. These tests run
in a fresh interpreter where all of them are hidden by a meta-path finder that
raises ``ModuleNotFoundError`` exactly as for an absent distribution, and pin:

* every ``tg_agent`` module imports — no core module reaches for a UI package;
* the CLI starts (``--help``);
* ``serve`` and the interactive ``agent chat`` exit 1 with an install hint
  instead of a traceback.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

HIDDEN = (
    "tg_webui",
    "tg_tui",
    "jinja2",
    "textual",
    "qrcode",
    "itsdangerous",
    "multipart",
    "python_multipart",
)

_HIDER = textwrap.dedent(
    f"""
    import importlib.abc
    import sys

    HIDDEN = {set(HIDDEN)!r}

    class _Hider(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            top = fullname.partition(".")[0]
            if top in HIDDEN:
                raise ModuleNotFoundError(f"No module named {{top!r}}", name=top)
            return None

    sys.meta_path.insert(0, _Hider())
    """
)

_WALK = textwrap.dedent(
    """
    import importlib
    import pkgutil

    import tg_agent

    failures = []
    for info in pkgutil.walk_packages(tg_agent.__path__, "tg_agent."):
        try:
            importlib.import_module(info.name)
        except Exception as exc:  # noqa: BLE001 - report every broken module
            failures.append(f"{info.name}: {exc!r}")
    print("\\n".join(failures))
    sys.exit(1 if failures else 0)
    """
)

_CLI = textwrap.dedent(
    """
    import runpy

    sys.argv = ["tg-agent", *sys.argv[1:]]
    runpy.run_module("tg_agent.main", run_name="__main__")
    """
)

# Cold interpreters importing the whole core (agent SDKs, langchain, sklearn)
# can exceed the suite-wide 120s deadlock guard on a busy xdist runner.
pytestmark = pytest.mark.timeout(300)


def _run(tmp_path: Path, body: str, *args: str) -> subprocess.CompletedProcess[str]:
    script = tmp_path / "probe.py"
    script.write_text(_HIDER + body, encoding="utf-8")
    env = {
        **os.environ,
        "TG_AGENT_SECRET_STORE": "off",
        "PYTHONPATH": os.pathsep.join(filter(None, [str(REPO_ROOT), os.environ.get("PYTHONPATH")])),
    }
    return subprocess.run(  # noqa: S603 - fixed interpreter, test-owned script
        [sys.executable, str(script), *args],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=240,
        check=False,
    )


def test_every_core_module_imports_without_ui_packages(tmp_path):
    result = _run(tmp_path, _WALK)
    assert result.returncode == 0, f"core modules failed to import:\n{result.stdout}\n{result.stderr}"


def test_cli_help_without_ui_packages(tmp_path):
    result = _run(tmp_path, _CLI, "--help")
    assert result.returncode == 0, result.stderr
    assert "serve" in result.stdout


def test_serve_without_tg_webui_exits_with_install_hint(tmp_path):
    result = _run(tmp_path, _CLI, "serve", "--web-pass", "unused")
    assert result.returncode == 1
    output = result.stdout + result.stderr
    assert "pip install -e packages/tg_webui" in output
    assert "Traceback" not in output


def test_interactive_agent_chat_without_tg_tui_exits_with_install_hint(tmp_path):
    result = _run(tmp_path, _CLI, "agent", "chat")
    assert result.returncode == 1
    output = result.stdout + result.stderr
    assert "pip install -e packages/tg_tui" in output
    assert "Traceback" not in output
