"""`agent chat -p "<prompt>"` — non-interactive one-shot chat.

Uses whatever agent backend the project is configured with (claude-agent-sdk
when ANTHROPIC_API_KEY/CLAUDE_CODE_OAUTH_TOKEN is set, otherwise the
deepagents fallback with the configured LLM provider). The command creates
an agent thread + messages, so the test cleans up by deleting the freshly
created thread on the way out — keeping the side-effect reversible.
"""
import re
import subprocess
import sys

import pytest

from tests.cli_real_tg_integration.conftest import cli_run_direct

pytestmark = pytest.mark.real_tg_safe

_THREAD_ROW_RE = re.compile(r"^\[(\d+)\]", re.MULTILINE)


@pytest.mark.timeout(240)
def test_proc_agent_chat_oneshot(run_cli, assert_cli_ok, cli_real_cli_env):
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
        # Cleanup uses cli_run_direct (not run_cli) so that a CLI timeout here
        # raises TimeoutExpired (which we catch) instead of pytest.skip()
        # (which would replace any AssertionError from the try block above).
        try:
            after = cli_run_direct(cli_real_cli_env, "agent", "threads")
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
                    cleanup = cli_run_direct(
                        cli_real_cli_env, "agent", "thread-delete", tid
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
