"""Gate + CLI runner for the live Codex CLI smoke suite.

Opt-in only: these tests drive the real Codex SDK (image generation and the
``CodexSdkBackend`` agent path, which spawns a real ``mcp-server`` subprocess),
so they take tens of seconds each and are skipped unless ``RUN_CODEX_CLI_LIVE``
is explicitly truthy. Mirrors the gate style of ``codex_image_live`` — never
auto-enabled, so CI stays green and silent.

Unlike unit tests, these run against the project's real ``config.yaml`` and DB
(no tmp-DB / fakes) so the live path is exercised end to end.
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

from tests.cli_real_tg_integration._live_readiness import _gate_enabled

GATE_ENV = "RUN_CODEX_CLI_LIVE"
CONFIG_ENV = "CODEX_CLI_LIVE_CONFIG"

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if _gate_enabled(GATE_ENV):
        return
    skip_marker = pytest.mark.skip(
        reason=f"live Codex CLI suite disabled; set {GATE_ENV}=1 to run — opt-in only, never auto-enabled"
    )
    here = os.path.dirname(os.path.abspath(__file__))
    for item in items:
        if os.path.abspath(str(item.fspath)).startswith(here):
            item.add_marker(skip_marker)


@dataclass
class CodexCliEnv:
    """Resolved live environment: the real config path the CLI runs against."""

    config_path: str

    def run(self, *args: str, timeout: float = 180.0) -> subprocess.CompletedProcess:
        """Run ``python -m src.main --config <cfg> <args>`` as a real subprocess.

        Returns the completed process (stdout/stderr captured as text). Fails the
        test on timeout rather than hanging.
        """
        cmd = [sys.executable, "-m", "src.main", "--config", self.config_path, *args]
        env = dict(os.environ)
        env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(_REPO_ROOT), env.get("PYTHONPATH", "")]))
        try:
            return subprocess.run(
                cmd,
                cwd=str(_REPO_ROOT),
                env=env,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as exc:
            pytest.fail(f"CLI timed out after {timeout}s: {' '.join(args)}\nstdout:\n{exc.stdout}")


@pytest.fixture(scope="session")
def codex_cli_env() -> CodexCliEnv:
    """Real config the live CLI runs against (override via CODEX_CLI_LIVE_CONFIG)."""
    cfg = os.environ.get(CONFIG_ENV, "").strip() or str(_REPO_ROOT / "config.yaml")
    return CodexCliEnv(config_path=cfg)
