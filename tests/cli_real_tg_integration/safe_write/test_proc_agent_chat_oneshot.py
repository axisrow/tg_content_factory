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


_THREAD_ID_RE = re.compile(r"thread\s*[:=]?\s*\[?(\d+)\]?", re.IGNORECASE)


def test_proc_agent_chat_oneshot(run_cli, assert_cli_ok):
    # Record threads that existed before so we can compute the new one.
    before = run_cli("agent", "threads")
    assert_cli_ok(before)
    pre_ids = set(re.findall(r"^\[(\d+)\]", before.stdout, re.MULTILINE))

    result = run_cli(
        "agent", "chat", "-p", "Say literally just ok and stop.", timeout=180
    )
    assert_cli_ok(result)
    assert result.stdout.strip(), (
        f"`agent chat -p` produced empty stdout: {result.stderr!r}"
    )

    # Cleanup: delete the thread the command just created so the user's
    # `agent threads` list returns to its prior state.
    after = run_cli("agent", "threads")
    assert_cli_ok(after)
    post_ids = set(re.findall(r"^\[(\d+)\]", after.stdout, re.MULTILINE))
    new_ids = post_ids - pre_ids
    for tid in new_ids:
        cleanup = run_cli("agent", "thread-delete", "--thread-id", tid)
        # Cleanup failures shouldn't mask the chat assertion above; just log.
        if cleanup.returncode != 0:
            print(f"cleanup: failed to delete thread {tid}: {cleanup.stderr}")
