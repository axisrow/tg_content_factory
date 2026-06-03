"""Live CLI smoke for the Codex SDK feature — real Codex, real MCP, real DB.

Opt-in via ``RUN_CODEX_CLI_LIVE=1`` (see conftest). Exercises the actual user
paths end to end:

- ``image providers`` / ``image models --provider codex`` — codex self-registers.
- ``image generate --model codex:gpt-5.4`` — real Codex writes a PNG.
- ``agent test-tools`` routed through the Codex backend — proves Codex starts,
  spawns our ``mcp-server`` subprocess, connects over MCP, and actually CALLS a
  project tool (the central previously-unverified risk).

Each test skips gracefully when Codex is unavailable (SDK missing or the Codex
CLI is not authenticated).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.codex_cli_live

_PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


def _require_codex() -> None:
    from src.services.provider_adapters import codex_available

    codex_available.cache_clear()
    if not codex_available():
        pytest.skip("Codex SDK not installed or Codex CLI not authenticated")


def _assert_cli_ok(result, *, where: str) -> None:
    assert result.returncode == 0, (
        f"{where} failed (rc={result.returncode}):\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )


# ── quick smoke (no real generation) ──


def test_image_providers_lists_codex(codex_cli_env):
    _require_codex()
    result = codex_cli_env.run("image", "providers", timeout=60)
    _assert_cli_ok(result, where="image providers")
    assert "codex" in result.stdout, result.stdout


def test_image_models_codex_catalog(codex_cli_env):
    _require_codex()
    result = codex_cli_env.run("image", "models", "--provider", "codex", timeout=60)
    _assert_cli_ok(result, where="image models --provider codex")
    assert "codex:gpt-5.4" in result.stdout, result.stdout


# ── real generation (slow) ──


@pytest.mark.timeout(300)
def test_image_generate_codex_writes_png(codex_cli_env):
    _require_codex()
    result = codex_cli_env.run(
        "image", "generate", "a small friendly robot, square", "--model", "codex:gpt-5.4", timeout=300
    )
    _assert_cli_ok(result, where="image generate codex:gpt-5.4")

    match = re.search(r"Result:\s*(\S+\.png)", result.stdout)
    assert match, f"no 'Result: <path>.png' in output:\n{result.stdout}"
    path = Path(match.group(1))
    try:
        assert path.exists(), f"image not created: {path}"
        data = path.read_bytes()
        assert data.startswith(_PNG_MAGIC), "file is not a valid PNG"
    finally:
        if path.exists():
            path.unlink()


# Two full Codex turns (each spawns an mcp-server subprocess and makes a real
# tool call) take a few minutes; give the whole test generous headroom.
@pytest.mark.timeout(900)
def test_agent_test_tools_via_codex_backend(codex_cli_env):
    """Codex backend → mcp-server subprocess → real project tool call.

    ``agent test-tools`` sends a prompt that must trigger a tool, then asserts
    tool_start/tool_end events arrived (exit 0). Routing it through the codex
    backend proves the full live MCP wiring, not the fake-SDK unit path.
    """
    _require_codex()
    env = codex_cli_env
    _assert_cli_ok(env.run("settings", "set", "agent_dev_mode_enabled", "1"), where="enable dev mode")
    try:
        _assert_cli_ok(
            env.run("settings", "set", "agent_backend_override", "codex"),
            where="select codex backend",
        )
        result = env.run("agent", "test-tools", timeout=720)
        # _test_tools exits non-zero if any case failed; 0 means a tool was called.
        assert result.returncode == 0, (
            f"agent test-tools via codex failed (rc={result.returncode}):\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        assert "OK — инструменты вызваны" in result.stdout, result.stdout
    finally:
        # Restore production settings — never leave the agent pinned to codex.
        env.run("settings", "set", "agent_backend_override", "auto")
        env.run("settings", "set", "agent_dev_mode_enabled", "0")
