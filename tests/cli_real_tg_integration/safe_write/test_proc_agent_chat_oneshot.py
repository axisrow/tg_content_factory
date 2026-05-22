"""`agent chat -p "<prompt>"` — non-interactive one-shot chat.

Uses whatever agent backend the project is configured with (claude-agent-sdk
when ANTHROPIC_API_KEY/CLAUDE_CODE_OAUTH_TOKEN is set, otherwise the
deepagents fallback with the configured LLM provider). The command creates
an agent thread + messages, so the test cleans up by deleting the freshly
created thread on the way out — keeping the side-effect reversible.
"""
import re

import pytest

pytestmark = pytest.mark.real_tg_safe


_THREAD_ROW_RE = re.compile(r"^\[(\d+)\]", re.MULTILINE)


def test_proc_agent_chat_oneshot(run_cli, assert_cli_ok):
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
        # Cleanup runs unconditionally: any assertion failure, run_cli
        # timeout-skip, or unexpected exception above must NOT leave a
        # half-created thread persisting in the user's real DB.
        after = run_cli("agent", "threads")
        if after.returncode == 0:
            post_ids = set(_THREAD_ROW_RE.findall(after.stdout))
            new_ids = post_ids - pre_ids
            for tid in new_ids:
                cleanup = run_cli("agent", "thread-delete", "--thread-id", tid)
                if cleanup.returncode != 0:
                    pytest.fail(
                        f"agent thread {tid} leaked: thread-delete failed "
                        f"with stderr={cleanup.stderr!r}"
                    )
        else:
            # `agent threads` itself failed — can't reliably diff. Surface
            # so the operator knows to inspect the DB manually.
            pytest.fail(
                f"cleanup `agent threads` failed (returncode={after.returncode}); "
                f"any thread created by the chat above may be leaked: "
                f"stderr={after.stderr!r}"
            )
