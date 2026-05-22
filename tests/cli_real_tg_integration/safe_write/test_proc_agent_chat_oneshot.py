"""`agent chat -p "<prompt>"` — non-interactive one-shot chat.

Uses whatever agent backend the project is configured with (claude-agent-sdk
when ANTHROPIC_API_KEY/CLAUDE_CODE_OAUTH_TOKEN is set, otherwise the
deepagents fallback with the configured LLM provider). The command creates
an agent thread + messages, so the test cleans up by deleting the freshly
created thread on the way out — keeping the side-effect reversible.
"""
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.real_tg_safe

_THREAD_ROW_RE = re.compile(r"^\[(\d+)\]", re.MULTILINE)
_WORKTREE_ROOT = Path(__file__).resolve().parents[3]


def _direct_cli(cli_env, *args: str, timeout: float = 20.0) -> subprocess.CompletedProcess:
    """Bypass run_cli's pytest.skip-on-timeout for cleanup code.

    The shared run_cli fixture calls pytest.skip() on TimeoutExpired, which
    raises Skipped. If that happens inside a `finally` block it replaces any
    in-flight AssertionError from the test body — masking real failures AND
    leaking the resource we were trying to clean up. Cleanup must therefore
    run its CLI invocations through plain subprocess.run, never raising.
    """
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        f"{_WORKTREE_ROOT}{os.pathsep}{existing}" if existing else str(_WORKTREE_ROOT)
    )
    env["PYTHONSAFEPATH"] = "1"
    return subprocess.run(
        [sys.executable, "-m", "src.main", *args],
        cwd=str(cli_env.repo_root),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        env=env,
        check=False,
    )


def test_proc_agent_chat_oneshot(run_cli, assert_cli_ok, cli_env):
    # Record threads that existed before so we can compute the new one. This
    # must run BEFORE the try/finally so a failure here doesn't leak a thread
    # that was never created in the first place.
    before = run_cli("agent", "threads")
    assert_cli_ok(before)
    pre_ids = set(_THREAD_ROW_RE.findall(before.stdout))

    try:
        result = run_cli(
            "agent", "chat", "-p", "Say literally just ok and stop.", timeout=180
        )
        assert_cli_ok(result)
        assert result.stdout.strip(), (
            f"`agent chat -p` produced empty stdout: {result.stderr!r}"
        )
    finally:
        # Cleanup uses _direct_cli (not run_cli) so that a CLI timeout here
        # raises TimeoutExpired (which we catch) instead of pytest.skip()
        # (which would replace any AssertionError from the try block above).
        try:
            after = _direct_cli(cli_env, "agent", "threads")
        except subprocess.TimeoutExpired:
            after = None

        leak_msg = None
        if after is None:
            leak_msg = (
                "cleanup `agent threads` timed out; any thread created by "
                "the chat above may be leaked — inspect the DB manually."
            )
        elif after.returncode != 0:
            leak_msg = (
                f"cleanup `agent threads` failed (returncode={after.returncode}); "
                f"any thread created above may be leaked: stderr={after.stderr!r}"
            )
        else:
            post_ids = set(_THREAD_ROW_RE.findall(after.stdout))
            new_ids = post_ids - pre_ids
            for tid in new_ids:
                try:
                    cleanup = _direct_cli(
                        cli_env, "agent", "thread-delete", "--thread-id", tid
                    )
                except subprocess.TimeoutExpired:
                    leak_msg = f"agent thread {tid} leaked: thread-delete timed out"
                    break
                if cleanup.returncode != 0:
                    leak_msg = (
                        f"agent thread {tid} leaked: thread-delete failed "
                        f"with stderr={cleanup.stderr!r}"
                    )
                    break

        # Only fail on cleanup if the try block didn't already raise. Python
        # propagates the original exception when finally completes without
        # raising; if we raise here we mask the real test failure.
        if leak_msg and sys.exc_info()[0] is None:
            pytest.fail(leak_msg)
