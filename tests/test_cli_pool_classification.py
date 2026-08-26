"""Every CLI site that opens a Telegram pool must declare how it behaves vs `serve`.

`serve` owns the Telegram session files (``data/telegram_sessions/<phone>.session``,
keyed by phone alone), so a CLI process building its own ``ClientPool`` opens the
same files and risks ``database is locked``. Two commands already learned that the
hard way.

This is the guard that stops the class from coming back: a new ``init_pool`` call
site is a deliberate decision about concurrency, and leaving it unclassified fails
here rather than silently shipping another competing connection.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

COMMANDS_DIR = Path(__file__).resolve().parent.parent / "src" / "cli" / "commands"

# Hands work to the running worker through a queue instead of connecting itself.
HANDS_OFF = {
    ("channel.py", "collect_impl"),
    ("dialogs.py", "_dispatch"),
}

# Runs anyway: read-only diagnostics that survive the contention. The native
# backend falls back to a fileless StringSession (src/telegram/backends.py), which
# is why `account info` completed in production even while three of four sessions
# hit the lock. Blocking these would remove working functionality for no gain.
RUNS_ANYWAY = {
    ("account.py", "info_impl"),
    ("search.py", "search_impl"),
    ("messages.py", "messages_read_impl"),
    ("photo_loader.py", "_run_with_services"),
    ("test.py", "_run_tg_pool_init_step"),
    ("channel.py", "stats_impl"),
    ("channel.py", "refresh_types_impl"),
    ("channel.py", "refresh_meta_impl"),
    ("channel.py", "add_impl"),
    ("channel.py", "import_impl"),
    ("channel.py", "add_bulk_impl"),
    ("channel.py", "list_for_import_impl"),
    ("collect.py", "collect_impl"),
    ("collect.py", "collect_sample_impl"),
    ("scheduler.py", "start_impl"),
    ("scheduler.py", "trigger_impl"),
    ("notification.py", "_build"),
    ("pipeline.py", "_pipeline_run"),
    ("pipeline.py", "_pipeline_publish"),
    ("pipeline.py", "_pipeline_generate"),
}

# Intentionally untouched: interactive auth (a live phone_code_hash bound to the
# session cannot round-trip through a queue) and long-lived processes with their
# own lifecycle.
EXEMPT = {
    ("account.py", "verify_code_impl"),
    ("agent.py", "chat_impl"),
}

CLASSIFIED = HANDS_OFF | RUNS_ANYWAY | EXEMPT


def _pool_call_sites() -> set[tuple[str, str]]:
    """Find every ``init_pool`` call, attributed to its enclosing function."""
    found: set[tuple[str, str]] = set()

    for path in sorted(COMMANDS_DIR.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))

        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for inner in ast.walk(node):
                if not isinstance(inner, ast.Call):
                    continue
                func = inner.func
                if isinstance(func, ast.Attribute) and func.attr == "init_pool":
                    found.add((path.name, node.name))

    return found


def test_every_pool_call_site_is_classified():
    """A new ``init_pool`` call must be classified before it can ship.

    Add the (file, function) pair to HANDS_OFF, RUNS_ANYWAY or EXEMPT above —
    and prefer HANDS_OFF whenever the operation has a queue equivalent.
    """
    unclassified = _pool_call_sites() - CLASSIFIED

    assert not unclassified, (
        "These CLI functions open a Telegram pool without declaring how they "
        f"behave while `serve` is running: {sorted(unclassified)}. "
        "Classify each in tests/test_cli_pool_classification.py."
    )


def test_classification_has_no_stale_entries():
    """Keep the lists honest: a removed call site must not linger as a claim."""
    stale = CLASSIFIED - _pool_call_sites()

    assert not stale, (
        f"These entries no longer open a pool and should be dropped: {sorted(stale)}"
    )


def test_the_three_classes_are_disjoint():
    """A call site has exactly one behaviour; overlap would make the guard lie."""
    assert not (HANDS_OFF & RUNS_ANYWAY)
    assert not (HANDS_OFF & EXEMPT)
    assert not (RUNS_ANYWAY & EXEMPT)


@pytest.mark.parametrize("file_name, func_name", sorted(HANDS_OFF))
def test_hands_off_sites_consult_the_detector(file_name: str, func_name: str):
    """A HANDS_OFF claim must be backed by a real ``serve_is_running`` check.

    Without this the entry is just a comment: the function could keep connecting
    unconditionally while the list claims otherwise.
    """
    source = (COMMANDS_DIR / file_name).read_text(encoding="utf-8")
    tree = ast.parse(source)

    target = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == func_name
    )
    calls = {
        inner.func.attr
        for inner in ast.walk(target)
        if isinstance(inner, ast.Call) and isinstance(inner.func, ast.Attribute)
    }

    assert "serve_is_running" in calls, (
        f"{file_name}:{func_name} is listed as handing off to the worker but never "
        "calls worker_handoff.serve_is_running()"
    )
