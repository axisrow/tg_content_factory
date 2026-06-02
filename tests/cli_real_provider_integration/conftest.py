"""conftest for cli_real_provider_integration tests.

Provides a lightweight subprocess runner and a temporary project config/DB
fixture for real-provider CLI smoke tests.  No Telegram credentials needed.
"""
from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SOURCE_ROOT = Path(__file__).resolve().parents[2]
_REPO_ROOT = _SOURCE_ROOT

#: Default subprocess timeout for non-network CLI commands (seconds).
PROVIDER_CLI_DEFAULT_TIMEOUT = 60.0

#: Default subprocess timeout for network-bound commands (seconds).
PROVIDER_CLI_NETWORK_TIMEOUT = 90.0


# ---------------------------------------------------------------------------
# Subprocess helpers
# ---------------------------------------------------------------------------


def _build_cli_env(extra_env: dict[str, str] | None = None) -> dict[str, str]:
    """Return an os.environ copy with PYTHONPATH and PYTHONSAFEPATH set."""
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        f"{_SOURCE_ROOT}{os.pathsep}{existing}" if existing else str(_SOURCE_ROOT)
    )
    env["PYTHONSAFEPATH"] = "1"
    if extra_env:
        env.update(extra_env)
    return env


def cli_run(
    config_path: Path,
    *args: str,
    timeout: float = PROVIDER_CLI_DEFAULT_TIMEOUT,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess:
    """Run ``python -m src.main --config <config_path> *args`` as a subprocess."""
    cmd = [
        sys.executable,
        "-m",
        "src.main",
        "--config",
        str(config_path),
        *args,
    ]
    return subprocess.run(  # noqa: S603 – controlled CLI module invocation
        cmd,
        cwd=str(_REPO_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        env=_build_cli_env(extra_env),
        check=False,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def provider_tmp_env(tmp_path: Path) -> tuple[Path, Path]:
    """Return ``(config_path, db_path)`` for an isolated throw-away project.

    The config points ``database.path`` to a fresh SQLite file inside
    ``tmp_path``.  Telegram credentials are intentionally absent so the CLI
    init does not block on connection.
    """
    db_path = tmp_path / "test.db"
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        textwrap.dedent(f"""\
            telegram:
              api_id: 1
              api_hash: "testhash"

            web:
              host: "127.0.0.1"
              port: 8099
              password: ""

            scheduler:
              collect_interval_minutes: 30
              delay_between_channels_sec: 2
              delay_between_requests_sec: 1
              max_flood_wait_sec: 300
              stats_all_max_channels_per_run: 10
              stats_all_cooldown_sec: 600
              stats_all_worker_count: 1

            database:
              path: "{db_path}"

            llm:
              enabled: true
              provider: "openai"
              model: "gpt-4o-mini"
              api_key: ""

            security:
              session_encryption_key: ""
        """),
        encoding="utf-8",
    )
    return config_path, db_path
